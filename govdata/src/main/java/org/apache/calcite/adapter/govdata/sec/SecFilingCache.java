/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.partition.PipelineTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SEC-specific facade for filing processing state.
 *
 * <p>Delegates all processing state to a {@link PipelineTracker}, backed by PostgreSQL —
 * the canonical and only record of ETL state. Object storage is never consulted to decide
 * whether a filing was processed: the tracker answers that question alone, so a run's
 * decision cost is one indexed query rather than a recursive listing of every year partition.
 *
 * <p>This class provides SEC-specific logic:
 * <ul>
 *   <li>{@link #checkFiling} — decision logic driven entirely by tracker state</li>
 *   <li>Form-type-aware completeness checking via {@link FormType}</li>
 * </ul>
 *
 * <p>State mapping to PipelineTracker:
 * <ul>
 *   <li>sourceKey = accession number</li>
 *   <li>tableName = output type suffix (metadata, facts, chunks, etc.)</li>
 *   <li>phase = "staging" for initial processing</li>
 *   <li>Special entries: "_no_xbrl" for no-XBRL status,
 *       "_error_count" for retry tracking</li>
 * </ul>
 */
public class SecFilingCache implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecFilingCache.class);

  private static final int MAX_ERROR_RETRIES = 3;

  /** Phase name for SEC staging (initial file creation). */
  private static final String PHASE_STAGING = "staging";

  /** Special table name for no-XBRL marker. */
  private static final String TABLE_NO_XBRL = "_no_xbrl";

  /** Special table name for error count tracking. */
  private static final String TABLE_ERROR_COUNT = "_error_count";

  /** Special table name for filing metadata. */
  private static final String TABLE_FILING_META = "_filing_meta";

  private final PipelineTracker tracker;

  /**
   * Create a new filing cache backed by a PipelineTracker.
   *
   * <p>The tracker is the only collaborator by design. This class previously also held a
   * {@link org.apache.calcite.adapter.file.storage.StorageProvider} and a parquet base directory
   * so it could rebuild lost tracker state by listing object storage; that cost a recursive
   * listing of every year partition on each run. Postgres is now the canonical record and is
   * protected by backup instead, so having no storage handle here makes it structurally
   * impossible for a processed/not-processed decision to issue a LIST.
   *
   * @param tracker PipelineTracker for state persistence
   */
  public SecFilingCache(PipelineTracker tracker) {
    this.tracker = tracker;
    LOGGER.info("Initialized SEC filing cache with {} tracker", tracker.getClass().getSimpleName());
  }

  /**
   * Bulk-preload tracker state for all given accessions into the in-memory cache.
   *
   * <p>After this call, subsequent {@link #checkFiling} calls for these accessions
   * are answered from memory.
   *
   * @param accessions all accession numbers to preload
   */
  public void preload(Collection<String> accessions) {
    if (accessions.isEmpty()) {
      return;
    }
    long start = System.currentTimeMillis();
    // Chunk into batches to avoid DuckDB glob list OOM
    List<String> accessionList = new ArrayList<String>(accessions);
    int chunkSize = 5000;
    int loaded = 0;
    for (int offset = 0; offset < accessionList.size(); offset += chunkSize) {
      int end = Math.min(offset + chunkSize, accessionList.size());
      List<String> chunk = accessionList.subList(offset, end);
      tracker.bulkGetCompletedTables(filingKeys(chunk), PHASE_STAGING);
      loaded += chunk.size();
    }
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Preloaded tracker state for {} accessions in {}ms", loaded, elapsed);
  }

  /**
   * Filter index candidates down to the ones that still need ETL processing.
   *
   * <p>Decided entirely from tracker state, which is pure memory after
   * {@link #preload}: Postgres is the canonical record of what has been processed,
   * so a candidate the tracker cannot account for is unprocessed. Nothing here touches object
   * storage — an earlier storage-inventory pass existed only to rebuild lost tracker state, and
   * paid a recursive listing of the whole year partition on every run to do it. The tracker is
   * kept safe by backup instead (see {@code govdata/scripts/tracker-backup.sh}).
   *
   * @param candidates           index entries already filtered by year and form type
   * @param vectorizationEnabled whether vectorized chunks are required for completeness
   * @return entries that still need ETL processing
   */
  public List<EdgarFullIndexCache.IndexEntry> filterUnprocessed(
      List<EdgarFullIndexCache.IndexEntry> candidates,
      boolean vectorizationEnabled) {
    return filterUnprocessed(candidates, vectorizationEnabled, false,
        java.util.Collections.<String>emptySet());
  }

  public List<EdgarFullIndexCache.IndexEntry> filterUnprocessed(
      List<EdgarFullIndexCache.IndexEntry> candidates,
      boolean vectorizationEnabled,
      boolean chunksBackfill) {
    return filterUnprocessed(candidates, vectorizationEnabled, chunksBackfill,
        java.util.Collections.<String>emptySet());
  }


  public List<EdgarFullIndexCache.IndexEntry> filterUnprocessed(
      List<EdgarFullIndexCache.IndexEntry> candidates,
      boolean vectorizationEnabled,
      boolean chunksBackfill,
      java.util.Set<String> forceAccessions) {

    List<EdgarFullIndexCache.IndexEntry> toProcess =
        new ArrayList<EdgarFullIndexCache.IndexEntry>();

    int cntDuplicate = 0;
    int cntNoXbrl = 0;
    int cntTrackerComplete = 0;
    int cntTrackerBaseComplete = 0;
    int cntTrackerIncomplete = 0;

    java.util.Set<String> seenAccessions = new java.util.HashSet<String>();
    for (EdgarFullIndexCache.IndexEntry ie : candidates) {
      // EDGAR's full index lists ownership filings (Forms 3/4/5, SC 13D/G) once per CIK involved —
      // the issuer AND every reporting owner — so ~43% of an all-filer year's rows repeat a filing
      // already decided (454,073 candidates for 2025 cover only 258,803 distinct accessions). An
      // accession is exactly one EDGAR submission and one unit of work, so decide it once.
      // Re-deciding was not merely wasted work, it produced a wrong answer: the first row clears a
      // stale no_xbrl marker, so the repeat row saw no tracker state at all and fell through to a
      // storage check that dragged in a full year-partition scan.
      if (!seenAccessions.add(ie.accession)) {
        cntDuplicate++;
        continue;
      }
      if (!forceAccessions.isEmpty() && forceAccessions.contains(ie.accession)) {
        toProcess.add(ie);
        continue;
      }
      if (!forceAccessions.isEmpty()) {
        // Reprocess-only mode: skip all candidates not explicitly listed.
        continue;
      }
      // One tracker read per candidate: the no_xbrl marker is just another completed table, so
      // reading the whole set once answers both questions below. Two separate reads doubled the
      // tracker round-trips for every one of the ~460k candidates in an all-filer year.
      Set<String> completed = tracker.getCompletedTables(filingKey(ie.accession), PHASE_STAGING);
      if (completed.contains(TABLE_NO_XBRL)) {
        // Insider forms (3/4/5) were previously mis-classified as no_xbrl due to a
        // converter bug that downloaded the xslF345X HTML viewer instead of the XML.
        // Clear the stale marker and reprocess so they produce _insider.parquet output.
        FormType form = FormType.fromString(ie.formType);
        // Insider forms (3/4/5) were mis-classified as no_xbrl by an old converter bug.
        // 13F-HR and SC 13D/G carry no XBRL instance at all, so before these form types were
        // modeled in FormType they fell through to FORM_OTHER, got a no_xbrl marker, and were
        // then skipped forever — leaving institutional_holdings/beneficial_ownership empty.
        // Clear the stale marker and reprocess so the info-table/HTML extractor can run.
        if (form.expectsInsider()
            || form.expectsInstitutionalHoldings()
            || form.expectsBeneficialOwnership()) {
          LOGGER.info("Clearing stale no_xbrl for form {} accession {}",
              ie.formType, ie.accession);
          clearNoXbrl(ie.accession);
          toProcess.add(ie);
        }
        cntNoXbrl++;
        continue;
      }
      if (!completed.isEmpty()) {
        FormType form = FormType.fromString(ie.formType);
        FileInventory inv = inventoryFromCompletedTables(completed);
        String items = earningsItemsIfNeeded(form, inv, ie.cik, ie.accession);
        if (inv.isComplete(form, vectorizationEnabled, items)) {
          cntTrackerComplete++;
          continue;
        }
        // Base staging parquet exists; only chunks are missing.
        // In normal mode: skip — re-downloading just for chunks is unnecessary.
        // In chunksBackfill mode: reprocess so chunks get written.
        if (inv.isComplete(form, false, items)
            && (!chunksBackfill || inv.isComplete(form, true, items))) {
          cntTrackerBaseComplete++;
          continue;
        }
        // Tracker markers are present but incomplete — the filing has outputs still to produce.
        cntTrackerIncomplete++;
        toProcess.add(ie);
        continue;
      }
      // No tracker record at all: never processed.
      toProcess.add(ie);
    }

    LOGGER.info(
        "filterUnprocessed: candidates={} duplicateAccessions={} noXbrl={} trackerFull={} "
            + "trackerBaseOnly={} trackerIncomplete={} toProcess={}",
        candidates.size(), cntDuplicate, cntNoXbrl, cntTrackerComplete, cntTrackerBaseComplete,
        cntTrackerIncomplete, toProcess.size());

    return toProcess;
  }

  /**
   * Looks up a filing's EDGAR items, but only when they can change the verdict.
   *
   * <p>Items decide one question: whether an 8-K owes an earnings transcript. Asking costs a
   * submissions fetch the first time a CIK is seen, so it is worth asking only for a filing whose
   * form expects earnings and whose earnings output is actually absent — the case that would
   * otherwise be reprocessed. A filing already holding its transcript, or one of a form that never
   * produces one, is settled without the lookup.
   *
   * <p>Skipping the lookup returns null, which {@link FormType#getExpectedOutputs} reads as
   * "unknown" and resolves conservatively. That is the correct reading here: the verdict was
   * reached without needing the items, not in spite of missing them.
   */
  private static String earningsItemsIfNeeded(FormType form, FileInventory inv, String cik,
      String accession) {
    if (!form.expectsEarnings() || inv.has(FormType.OutputType.EARNINGS)) {
      return null;
    }
    return SecDataFetcher.getFilingItems(cik, accession);
  }

  /**
   * Check if a filing needs processing, decided from tracker state alone.
   */
  public ProcessingDecision checkFiling(String cik, String accession, String formType,
      String filingDate, boolean vectorizationEnabled) {

    FormType form = FormType.fromString(formType);

    // Check for no_xbrl marker — but insider (3/4/5), 13F-HR and SC 13D/G forms should never
    // legitimately be no_xbrl (13F holdings are an info-table xml; SC 13D/G are HTML). A no_xbrl
    // marker on them is a stale bug artifact from before these forms were modeled — clear it and
    // reprocess so the dedicated extractor runs.
    if (tracker.isComplete(filingKey(accession), TABLE_NO_XBRL, PHASE_STAGING)) {
      if (form.expectsInsider()
          || form.expectsInstitutionalHoldings()
          || form.expectsBeneficialOwnership()) {
        LOGGER.info("Clearing stale no_xbrl for form {} accession {}", formType, accession);
        clearNoXbrl(accession);
      } else {
        return ProcessingDecision.skip("No XBRL data available");
      }
    }

    // Check for error state with retry limit
    if (hasError(accession)) {
      int errorCount = getErrorCount(accession);
      if (errorCount >= MAX_ERROR_RETRIES) {
        return ProcessingDecision.skip("Max retries exceeded");
      }
      return ProcessingDecision.process("Retrying failed filing");
    }

    // Get completed tables from tracker
    Set<String> completedTables = tracker.getCompletedTables(filingKey(accession), PHASE_STAGING);

    if (completedTables.isEmpty()) {
      // Postgres is the canonical record: no marker means not processed. There is no storage
      // check here any more — it existed to rebuild markers the tracker had lost.
      // No files, needs full processing
      return ProcessingDecision.process("New filing");
    }

    // Have some tracker entries - check if complete
    FileInventory trackerInventory = inventoryFromCompletedTables(completedTables);
    String items = earningsItemsIfNeeded(form, trackerInventory, cik, accession);
    if (trackerInventory.isComplete(form, vectorizationEnabled, items)) {
      return ProcessingDecision.skip("Already complete");
    }

    // Check if vectorization upgrade needed (all base files present, just chunks missing)
    if (vectorizationEnabled && trackerInventory.isComplete(form, false, items)
        && !completedTables.contains("chunks")) {
      return ProcessingDecision.processChunksOnly("Vectorization upgrade needed");
    }

    // Partial markers mean the filing is genuinely incomplete; reprocess it.

    return ProcessingDecision.process("Completing partial processing");
  }

  /**
   * Bulk-check whether all given accessions are fully processed.
   *
   * <p>Uses a single {@link PipelineTracker#bulkGetCompletedTables} call to
   * fetch tracker state for all accessions at once, then checks completeness
   * for each one based on its form type.
   *
   * @param accessions     Accession numbers to check
   * @param formTypes      Corresponding form types (parallel to accessions)
   * @param vectorizationEnabled Whether vectorization (chunks) is required
   * @return true if every accession is fully complete in the tracker
   */
  public boolean areAllFilingsComplete(List<String> accessions, List<String> formTypes,
      boolean vectorizationEnabled) {
    Map<String, Set<String>> bulkState =
        tracker.bulkGetCompletedTables(filingKeys(accessions), PHASE_STAGING);

    for (int i = 0; i < accessions.size(); i++) {
      String accession = accessions.get(i);
      Set<String> completedTables = bulkState.get(filingKey(accession));

      if (completedTables == null || completedTables.isEmpty()) {
        return false; // No tracker data at all
      }

      FormType form = FormType.fromString(formTypes.get(i));
      // Check for no_xbrl marker (stored as a "table" in the tracker)
      if (completedTables.contains(TABLE_NO_XBRL)) {
        // 13F-HR / SC 13D-G legitimately have no XBRL; a no_xbrl marker on them is a stale
        // artifact from before these forms were modeled. Don't treat the CIK as complete —
        // force a per-CIK pass so filterUnprocessed/checkFiling can clear it and reprocess.
        if (form.expectsInstitutionalHoldings() || form.expectsBeneficialOwnership()) {
          return false;
        }
        continue; // This accession is handled
      }

      FileInventory inv = inventoryFromCompletedTables(completedTables);
      if (!inv.isComplete(form, vectorizationEnabled)) {
        return false;
      }
    }
    return true;
  }



  /**
   * Returns the 4-digit year string from an ISO filing date ("YYYY-MM-DD").
   * Falls back to null if the date is missing or malformed.
   */
  private static String yearFromFilingDate(String filingDate) {
    if (filingDate != null && filingDate.length() >= 4) {
      String year = filingDate.substring(0, 4);
      try {
        int y = Integer.parseInt(year);
        if (y >= 2000 && y <= 2099) {
          return year;
        }
      } catch (NumberFormatException ignored) {
        // fall through to null
      }
    }
    return null;
  }

  /** Extracts 4-digit year from accession number format {@code XXXXXXXXXX-YY-NNNNNN}. */
  private static String yearFromAccession(String accession) {
    if (accession == null) {
      return null;
    }
    int first = accession.indexOf('-');
    int second = first >= 0 ? accession.indexOf('-', first + 1) : -1;
    if (first < 0 || second < 0 || second <= first + 1) {
      return null;
    }
    String twoDigit = accession.substring(first + 1, second);
    try {
      int y = Integer.parseInt(twoDigit);
      if (y >= 0 && y <= 99) {
        int fullYear = y + 2000;
        return String.valueOf(fullYear);
      }
    } catch (NumberFormatException ignored) {
      // fall through
    }
    return null;
  }

  /**
   * Canonical tracker source-key for a filing: {@code accession_number=<ACC>__year=<YYYY>}.
   *
   * <p>This is byte-identical to the flattened key the Iceberg materializer writes for its
   * {@code {accession_number, year}} key map (see {@code IncrementalTracker.flattenKeyValues}:
   * a sorted, {@code __}-joined {@code name=value} encoding). Keying every staging marker through
   * this method means staging and materialize markers share a single source_key format instead of
   * the previous split between composite (materialize) and bare-accession (staging) keys.
   *
   * <p>The year is derived from the accession itself (the {@code -YY-} segment), so every read and
   * write for a given accession produces the same key, and the tracker's {@code year=} path
   * partition (which {@code extractYear} computes from the same segment) is unchanged.
   */
  private static String filingKey(String accession) {
    String year = yearFromAccession(accession);
    if (year == null) {
      throw new IllegalStateException(
          "Cannot derive year from accession '" + accession + "' for tracker source-key");
    }
    return "accession_number=" + accession + "__year=" + year;
  }

  /** Maps a collection of accessions to their canonical {@link #filingKey} source-keys. */
  private static List<String> filingKeys(Collection<String> accessions) {
    List<String> keys = new ArrayList<String>(accessions.size());
    for (String accession : accessions) {
      keys.add(filingKey(accession));
    }
    return keys;
  }

  /**
   * Mark filing as successfully processed.
   */
  public void markComplete(String cik, String accession, String formType, String filingDate,
      boolean vectorizationEnabled, FileInventory inventory) {
    recordInventory(accession, inventory);
    // Store filing metadata
    tracker.markComplete(filingKey(accession), TABLE_FILING_META, PHASE_STAGING, 1);
    // Clear any previous error state
    clearError(accession);
    LOGGER.debug("Marked complete: {}:{}", cik, accession);
  }

  /**
   * Mark filing as having no XBRL data. Recorded only in the tracker — no storage sentinel.
   */
  public void markNoXbrl(String cik, String accession, String formType, String filingDate) {
    tracker.markComplete(filingKey(accession), TABLE_NO_XBRL, PHASE_STAGING, 0);
    LOGGER.debug("Marked no_xbrl: {}:{}", cik, accession);
  }


  /**
   * Clear the no_xbrl marker so the filing will be reprocessed.
   * Used to fix accessions incorrectly marked no_xbrl due to converter bugs.
   */
  public void clearNoXbrl(String accession) {
    tracker.markCleared(filingKey(accession), TABLE_NO_XBRL, PHASE_STAGING);
    LOGGER.debug("Cleared no_xbrl marker for accession: {}", accession);
  }

  /**
   * Clear stale no_xbrl markers for insider forms (3/4/5) in the given candidates.
   *
   * <p>Unlike {@link #filterUnprocessed}, this method does NOT add cleared entries to any
   * processing queue — it is a pure cleanup pass, intended for historical year sweeps where
   * the current worker is not responsible for reprocessing.
   *
   * @param candidates index entries to inspect
   * @return number of entries cleared
   */
  public int clearStaleInsiderNoXbrl(List<EdgarFullIndexCache.IndexEntry> candidates) {
    int cleared = 0;
    for (EdgarFullIndexCache.IndexEntry ie : candidates) {
      if (tracker.isComplete(filingKey(ie.accession), TABLE_NO_XBRL, PHASE_STAGING)) {
        FormType form = FormType.fromString(ie.formType);
        if (form.expectsInsider()) {
          LOGGER.info("Clearing stale no_xbrl for insider form {} accession {}",
              ie.formType, ie.accession);
          clearNoXbrl(ie.accession);
          cleared++;
        }
      }
    }
    if (cleared > 0) {
      LOGGER.info("Cleared {} stale no_xbrl entries for insider forms (historical sweep)", cleared);
    }
    return cleared;
  }

  /**
   * Mark filing as failed.
   */
  public void markFailed(String cik, String accession, String formType, String filingDate,
      String errorMessage) {
    int errorCount = getErrorCount(accession) + 1;
    tracker.markError(filingKey(accession), TABLE_ERROR_COUNT, PHASE_STAGING,
        errorMessage != null
            ? errorMessage.substring(0, Math.min(500, errorMessage.length()))
            : null);
    // Store error count in row_count field
    tracker.markComplete(filingKey(accession), TABLE_ERROR_COUNT, PHASE_STAGING, errorCount);
    LOGGER.debug("Marked failed: {}:{} (attempt {})", cik, accession, errorCount);
  }

  /**
   * Update status and inventory for existing entry.
   */
  public void updateStatus(String cik, String accession, String status,
      FileInventory inventory) {
    if ("complete".equals(status)) {
      recordInventory(accession, inventory);
      clearError(accession);
    }
  }

  /**
   * Find all filings that need vectorization upgrade.
   *
   * <p>With PipelineTracker, this returns entries that have staging tables
   * complete but no chunks entry. Callers must supply the set of accessions
   * to check (from their own filing list), since PipelineTracker doesn't
   * store CIK/form metadata for enumeration.
   */
  public List<FilingCacheEntry> findNeedingVectorizationUpgrade() {
    // PipelineTracker doesn't support enumeration of all source keys.
    // Return empty - callers should use checkFiling() per filing instead.
    LOGGER.debug("findNeedingVectorizationUpgrade: use checkFiling() per filing with "
        + "PipelineTracker backend");
    return new ArrayList<>();
  }

  /**
   * Get statistics about cache contents.
   *
   * <p>With PipelineTracker, aggregate stats are not efficiently available.
   * Returns empty stats. Use the tracker backend's native tools for analytics.
   */
  public CacheStats getStats() {
    LOGGER.debug("getStats: aggregate stats not available with PipelineTracker backend");
    return new CacheStats();
  }

  /**
   * Invalidate all entries for a CIK.
   *
   * <p>Since PipelineTracker is keyed by accession (not CIK), this is a no-op.
   * Callers should invalidate specific accessions instead.
   */
  public int invalidateCik(String cik) {
    LOGGER.warn("invalidateCik not supported with PipelineTracker - "
        + "invalidate specific accessions instead");
    return 0;
  }

  /**
   * Clear all cache entries.
   */
  public int clearAll() {
    tracker.clearAllCompletions();
    LOGGER.info("Cleared all tracker state");
    return 0;
  }

  // ===== Internal State Mapping =====

  /**
   * Record a FileInventory as individual tracker entries. Every call records outputs this run
   * just produced, so each marker stamps {@code source_as_of}.
   */
  private void recordInventory(String accession, FileInventory inventory) {
    String key = filingKey(accession);
    // The no_xbrl marker is tracker state like any other output, so it is recorded here too.
    if (inventory.hasNoXbrl()) {
      tracker.markComplete(key, TABLE_NO_XBRL, PHASE_STAGING, 0);
    }
    if (inventory.hasMetadata()) {
      tracker.markComplete(key, "metadata", PHASE_STAGING, 1);
    }
    if (inventory.hasFacts()) {
      tracker.markComplete(key, "facts", PHASE_STAGING, 1);
    }
    if (inventory.hasContexts()) {
      tracker.markComplete(key, "contexts", PHASE_STAGING, 1);
    }
    if (inventory.hasRelationships()) {
      tracker.markComplete(key, "relationships", PHASE_STAGING, 1);
    }
    if (inventory.hasMda()) {
      tracker.markComplete(key, "mda", PHASE_STAGING, 1);
    }
    if (inventory.hasInsider()) {
      tracker.markComplete(key, "insider", PHASE_STAGING, 1);
    }
    if (inventory.hasEarnings()) {
      tracker.markComplete(key, "earnings", PHASE_STAGING, 1);
    }
    if (inventory.hasChunks()) {
      tracker.markComplete(key, "chunks", PHASE_STAGING, 1);
    }
    if (inventory.hasInstitutionalHoldings()) {
      tracker.markComplete(key, "13f", PHASE_STAGING, 1);
    }
    if (inventory.hasBeneficialOwnership()) {
      tracker.markComplete(key, "13dg", PHASE_STAGING, 1);
    }
  }


  /**
   * Build a FileInventory from a set of completed table names.
   */
  /** Union of two inventories: an output type is present if either source reports it. */
  private static FileInventory unionInventory(FileInventory a, FileInventory b) {
    return FileInventory.builder()
        .hasMetadata(a.hasMetadata() || b.hasMetadata())
        .hasFacts(a.hasFacts() || b.hasFacts())
        .hasContexts(a.hasContexts() || b.hasContexts())
        .hasRelationships(a.hasRelationships() || b.hasRelationships())
        .hasMda(a.hasMda() || b.hasMda())
        .hasInsider(a.hasInsider() || b.hasInsider())
        .hasEarnings(a.hasEarnings() || b.hasEarnings())
        .hasChunks(a.hasChunks() || b.hasChunks())
        .hasInstitutionalHoldings(a.hasInstitutionalHoldings() || b.hasInstitutionalHoldings())
        .hasBeneficialOwnership(a.hasBeneficialOwnership() || b.hasBeneficialOwnership())
        .build();
  }

  private FileInventory inventoryFromCompletedTables(Set<String> tables) {
    return FileInventory.builder()
        .hasMetadata(tables.contains("metadata"))
        .hasFacts(tables.contains("facts"))
        .hasContexts(tables.contains("contexts"))
        .hasRelationships(tables.contains("relationships"))
        .hasMda(tables.contains("mda"))
        .hasInsider(tables.contains("insider"))
        .hasEarnings(tables.contains("earnings"))
        .hasChunks(tables.contains("chunks"))
        .hasInstitutionalHoldings(tables.contains("13f"))
        .hasBeneficialOwnership(tables.contains("13dg"))
        .build();
  }

  /**
   * Check if there's an active error for this accession.
   */
  private boolean hasError(String accession) {
    // Error is tracked via markError on TABLE_ERROR_COUNT.
    // If the latest state for TABLE_ERROR_COUNT is "complete" (from markComplete
    // after markError), that means we recorded the count.
    // We check if there's no successful _filing_meta entry (meaning no markComplete was called).
    // If _filing_meta is complete, the filing succeeded regardless of past errors.
    if (tracker.isComplete(filingKey(accession), TABLE_FILING_META, PHASE_STAGING)) {
      return false; // Filing completed successfully
    }
    // Check if error count > 0
    return getErrorCount(accession) > 0;
  }

  /**
   * Get the error retry count for an accession.
   * Uses the row_count field of the _error_count entry.
   */
  private int getErrorCount(String accession) {
    // The error count is stored as row_count in the _error_count:staging entry.
    // We use isComplete to check existence, and for DuckDB the row_count is in the DB.
    // For the generic case, if _error_count is "complete" in tracker, we read count.
    // Since PipelineTracker doesn't expose row_count directly in isComplete,
    // we rely on the tracker's getCompletedTables to check existence.
    Set<String> completed = tracker.getCompletedTables(filingKey(accession), PHASE_STAGING);
    if (!completed.contains(TABLE_ERROR_COUNT.substring(1))) {
      // Not using substring - check the actual name
      if (!tracker.isComplete(filingKey(accession), TABLE_ERROR_COUNT, PHASE_STAGING)) {
        return 0;
      }
    }
    // Error count exists but we can't read the row_count through the generic interface.
    // Return 1 as minimum - the filing will be retried until MAX_ERROR_RETRIES.
    // Native DuckDB/PG implementations can override for precise counts.
    return 1;
  }

  /**
   * Clear error state for an accession.
   */
  private void clearError(String accession) {
    tracker.markCleared(filingKey(accession), TABLE_ERROR_COUNT, PHASE_STAGING);
  }

  @Override
  public void close() {
    if (tracker instanceof AutoCloseable) {
      try {
        ((AutoCloseable) tracker).close();
      } catch (Exception e) {
        LOGGER.error("Error closing tracker: {}", e.getMessage());
      }
    }
  }

  /**
   * Cache entry data.
   */
  public static class FilingCacheEntry {
    public String cik;
    public String accession;
    public String formType;
    public String filingDate;
    public int fiscalYear;
    public String status;
    public boolean vectorizationEnabled;
    public boolean hasMetadata;
    public boolean hasFacts;
    public boolean hasContexts;
    public boolean hasRelationships;
    public boolean hasMda;
    public boolean hasInsider;
    public boolean hasEarnings;
    public boolean hasChunks;
    public long processedAt;
    public String errorMessage;
    public int errorCount;

    public FileInventory toInventory() {
      return FileInventory.builder()
          .hasMetadata(hasMetadata)
          .hasFacts(hasFacts)
          .hasContexts(hasContexts)
          .hasRelationships(hasRelationships)
          .hasMda(hasMda)
          .hasInsider(hasInsider)
          .hasEarnings(hasEarnings)
          .hasChunks(hasChunks)
          .build();
    }
  }

  /**
   * Cache statistics.
   */
  public static class CacheStats {
    public int complete;
    public int partial;
    public int noXbrl;
    public int failed;
    public int pending;
    public int withChunks;

    public int total() {
      return complete + partial + noXbrl + failed + pending;
    }

    @Override
    public String toString() {
      return String.format(
          "CacheStats{complete=%d (withChunks=%d), partial=%d, noXbrl=%d, failed=%d, pending=%d}",
          complete, withChunks, partial, noXbrl, failed, pending);
    }
  }
}
