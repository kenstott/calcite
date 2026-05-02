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
import org.apache.calcite.adapter.file.partition.S3HivePipelineTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SEC-specific facade for filing processing state.
 *
 * <p>Delegates all processing state to a {@link PipelineTracker}, which can be
 * backed by DuckDB (local), S3 (distributed), or PostgreSQL (shared).
 *
 * <p>This class provides SEC-specific logic:
 * <ul>
 *   <li>{@link #checkFiling} — decision logic with self-healing from S3</li>
 *   <li>{@link #checkS3Files} — S3 file inventory checks</li>
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
  private final StorageProvider storageProvider;
  private final String parquetBaseDir;

  /**
   * In-memory cache of known parquet file paths in S3.
   * Populated by {@link #preloadFileInventory(int, int)} to eliminate per-file LIST ops during
   * self-healing checks.  When null, fileExists() falls back to live S3 queries.
   */
  private volatile java.util.Set<String> s3FileCache = null;

  /**
   * Secondary index: parquet filename (without directory) → full S3 path.
   * Populated alongside {@link #s3FileCache} to enable O(1) lookups when the
   * filing date (and therefore year partition) is unknown.
   */
  private volatile java.util.Map<String, String> s3FileCacheByName = null;

  /**
   * Create a new filing cache backed by a PipelineTracker.
   *
   * @param tracker PipelineTracker for state persistence
   * @param storageProvider Storage provider for S3 file checks
   * @param parquetBaseDir Base directory for parquet files
   */
  public SecFilingCache(PipelineTracker tracker, StorageProvider storageProvider,
      String parquetBaseDir) {
    this.tracker = tracker;
    this.storageProvider = storageProvider;
    this.parquetBaseDir = parquetBaseDir;
    LOGGER.info("Initialized SEC filing cache with {} tracker", tracker.getClass().getSimpleName());
  }

  /**
   * Bulk-preload tracker state for all given accessions into the in-memory cache.
   *
   * <p>After this call, subsequent {@link #checkFiling} calls for these accessions
   * will use cached data instead of individual S3 queries.
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
      tracker.bulkGetCompletedTables(chunk, PHASE_STAGING);
      loaded += chunk.size();
    }
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Preloaded tracker state for {} accessions in {}ms", loaded, elapsed);
  }

  /**
   * Scans only the {@code source=sec/year=YYYY/} partitions for {@code startYear..endYear}
   * and caches all known file paths in memory.
   *
   * <p>After this call, {@link #checkS3Files} answers existence queries from the
   * in-memory set — zero additional Class A LIST ops per filing check.
   *
   * <p>Cost: one paginated {@code ListObjectsV2} scan per year partition
   * (roughly 1 Class A op per 1 000 files in each year).  Scoping to the worker's
   * actual year range avoids listing the full multi-million-file {@code source=sec/}
   * prefix that would otherwise cost 1 000+ Class A ops and take many minutes.
   *
   * <p>Thread-safety note: both caches are replaced atomically.  Workers operating on
   * disjoint filing ranges see the same consistent snapshot.  Files written by other
   * workers AFTER this scan are not in the cache; they will not be visible via the
   * cache but the tracker will have marked them complete before self-healing would
   * ever run for those filings.
   *
   * @param startYear first year partition to include (e.g. 2026)
   * @param endYear   last year partition to include (inclusive)
   */
  public void preloadFileInventory(int startYear, int endYear) {
    long start = System.currentTimeMillis();
    String secDir = storageProvider.resolvePath(parquetBaseDir, "source=sec");
    java.util.Set<String> cache = new java.util.HashSet<String>();
    java.util.Map<String, String> byName = new java.util.HashMap<String, String>();
    int totalCount = 0;
    for (int year = startYear; year <= endYear; year++) {
      String yearDir = storageProvider.resolvePath(secDir, "year=" + year);
      try {
        List<StorageProvider.FileEntry> entries = storageProvider.listFiles(yearDir, true);
        int yearCount = 0;
        for (StorageProvider.FileEntry entry : entries) {
          if (!entry.isDirectory()) {
            String path = entry.getPath();
            cache.add(path);
            int slash = path.lastIndexOf('/');
            String name = (slash >= 0) ? path.substring(slash + 1) : path;
            byName.put(name, path);
            yearCount++;
          }
        }
        LOGGER.debug("preloadFileInventory: year={} loaded {} files", year, yearCount);
        totalCount += yearCount;
      } catch (IOException e) {
        LOGGER.warn("preloadFileInventory: year={} scan failed — {}", year, e.getMessage());
      }
    }
    this.s3FileCacheByName = java.util.Collections.unmodifiableMap(byName);
    this.s3FileCache = java.util.Collections.unmodifiableSet(cache);
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("preloadFileInventory: cached {} sec parquet paths (years {}-{}) in {}ms",
        totalCount, startYear, endYear, elapsed);
  }

  /**
   * Filter index candidates to the unprocessed work queue, self-healing tracker state in
   * parallel for accessions whose S3 files exist but have no tracker entry.
   *
   * <p>Pass 1 is pure in-memory (uses preloaded tracker + file inventory — no R2 ops).
   * Pass 2 writes self-heal tracker entries concurrently, reducing wall-clock time from
   * O(n &times; R2_latency) to O(n / threads &times; R2_latency).
   *
   * @param candidates           index entries already filtered by year and form type
   * @param vectorizationEnabled whether vectorized chunks are required for completeness
   * @param selfHealThreads      thread-pool size for parallel self-heal writes (capped at 50)
   * @return entries that still need ETL processing
   */
  public List<EdgarFullIndexCache.IndexEntry> filterAndSelfHeal(
      List<EdgarFullIndexCache.IndexEntry> candidates,
      boolean vectorizationEnabled,
      int selfHealThreads) {

    List<EdgarFullIndexCache.IndexEntry> toProcess =
        new ArrayList<EdgarFullIndexCache.IndexEntry>();
    final List<EdgarFullIndexCache.IndexEntry> toSelfHeal =
        new ArrayList<EdgarFullIndexCache.IndexEntry>();

    int cntNoXbrl = 0;
    int cntTrackerComplete = 0;
    int cntTrackerBaseComplete = 0;
    int cntTrackerIncomplete = 0;
    int cntS3Complete = 0;
    int cntS3Partial = 0;
    int cntNoFiles = 0;

    for (EdgarFullIndexCache.IndexEntry ie : candidates) {
      if (tracker.isComplete(ie.accession, TABLE_NO_XBRL, PHASE_STAGING)) {
        // Insider forms (3/4/5) were previously mis-classified as no_xbrl due to a
        // converter bug that downloaded the xslF345X HTML viewer instead of the XML.
        // Clear the stale marker and reprocess so they produce _insider.parquet output.
        FormType form = FormType.fromString(ie.formType);
        if (form.expectsInsider()) {
          LOGGER.info("Clearing stale no_xbrl for insider form {} accession {}",
              ie.formType, ie.accession);
          clearNoXbrl(ie.accession);
          toProcess.add(ie);
        }
        cntNoXbrl++;
        continue;
      }
      Set<String> completed = tracker.getCompletedTables(ie.accession, PHASE_STAGING);
      if (!completed.isEmpty()) {
        FormType form = FormType.fromString(ie.formType);
        FileInventory inv = inventoryFromCompletedTables(completed);
        if (inv.isComplete(form, vectorizationEnabled)) {
          cntTrackerComplete++;
          continue;
        }
        // Base staging parquet exists; only chunks are missing. Skip — re-downloading from SEC
        // just to add chunks would regenerate all staging parquet unnecessarily.
        if (inv.isComplete(form, false)) {
          cntTrackerBaseComplete++;
          continue;
        }
        cntTrackerIncomplete++;
        toProcess.add(ie);
        continue;
      }
      FileInventory s3Inv = checkS3Files(ie.cik, ie.accession, ie.filingDate);
      if (s3Inv.hasAnyFiles()) {
        toSelfHeal.add(ie);
        FormType form = FormType.fromString(ie.formType);
        // Only process if base staging files are also incomplete, not just chunks.
        if (!s3Inv.isComplete(form, false)) {
          cntS3Partial++;
          toProcess.add(ie);
        } else {
          cntS3Complete++;
        }
      } else {
        cntNoFiles++;
        toProcess.add(ie);
      }
    }

    LOGGER.info(
        "filterAndSelfHeal: candidates={} noXbrl={} trackerFull={} trackerBaseOnly={} "
            + "trackerIncomplete={} s3Complete={} s3Partial={} noFiles={} toProcess={}",
        candidates.size(), cntNoXbrl, cntTrackerComplete, cntTrackerBaseComplete,
        cntTrackerIncomplete, cntS3Complete, cntS3Partial, cntNoFiles, toProcess.size());

    if (!toSelfHeal.isEmpty()) {
      int poolSize = Math.min(selfHealThreads > 0 ? selfHealThreads : 1, 50);
      LOGGER.info("Self-healing {} accessions with {} threads", toSelfHeal.size(), poolSize);
      long healStart = System.currentTimeMillis();
      java.util.concurrent.ExecutorService pool =
          java.util.concurrent.Executors.newFixedThreadPool(poolSize);
      for (final EdgarFullIndexCache.IndexEntry ie : toSelfHeal) {
        pool.submit(new Runnable() {
          @Override public void run() {
            FileInventory inv = checkS3Files(ie.cik, ie.accession, ie.filingDate);
            recordInventory(ie.accession, inv);
          }
        });
      }
      pool.shutdown();
      try {
        pool.awaitTermination(30, java.util.concurrent.TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Self-heal batch interrupted");
      }
      LOGGER.info("Self-heal complete: {} accessions in {}ms",
          toSelfHeal.size(), System.currentTimeMillis() - healStart);
    }

    return toProcess;
  }

  /**
   * Check if a filing needs processing.
   *
   * <p>Implements self-healing: if files exist in S3 but tracker has no state,
   * records the state and returns SKIP.
   */
  public ProcessingDecision checkFiling(String cik, String accession, String formType,
      String filingDate, boolean vectorizationEnabled) {

    FormType form = FormType.fromString(formType);

    // Check for no_xbrl marker — but insider forms (3/4/5) should never be no_xbrl;
    // if they have this marker it's a stale bug artifact, clear it and reprocess.
    if (tracker.isComplete(accession, TABLE_NO_XBRL, PHASE_STAGING)) {
      if (form.expectsInsider()) {
        LOGGER.info("Clearing stale no_xbrl for insider form {} accession {}", formType, accession);
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
    Set<String> completedTables = tracker.getCompletedTables(accession, PHASE_STAGING);

    if (completedTables.isEmpty()) {
      // Not in tracker - check if files exist (self-healing)
      FileInventory inventory = checkS3Files(cik, accession, filingDate);
      if (inventory.isComplete(form, vectorizationEnabled)) {
        // Files exist, heal tracker
        recordInventory(accession, inventory);
        LOGGER.info("Self-healed tracker for {}:{} - files exist in S3", cik, accession);
        return ProcessingDecision.skip("Self-healed: files exist");
      }
      if (inventory.hasAnyFiles()) {
        // Check if only chunks are missing (vectorization upgrade)
        if (vectorizationEnabled && inventory.isComplete(form, false) && !inventory.hasChunks()) {
          recordInventory(accession, inventory);
          LOGGER.info("Self-healed tracker for {}:{} - needs vectorization upgrade",
              cik, accession);
          return ProcessingDecision.processChunksOnly(
              "Self-healed: vectorization upgrade needed");
        }
        // Partial files exist - record what we found and request completion
        recordInventory(accession, inventory);
        LOGGER.debug("Partial files for {}:{} - needs completion", cik, accession);
        return ProcessingDecision.process("Partial files, need completion");
      }
      // No files, needs full processing
      return ProcessingDecision.process("New filing");
    }

    // Have some tracker entries - check if complete
    FileInventory trackerInventory = inventoryFromCompletedTables(completedTables);
    if (trackerInventory.isComplete(form, vectorizationEnabled)) {
      return ProcessingDecision.skip("Already complete");
    }

    // Check if vectorization upgrade needed (all base files present, just chunks missing)
    if (vectorizationEnabled && trackerInventory.isComplete(form, false)
        && !completedTables.contains("chunks")) {
      return ProcessingDecision.processChunksOnly("Vectorization upgrade needed");
    }

    // Partial state in tracker - verify against S3 (self-healing for partial)
    FileInventory s3Inventory = checkS3Files(cik, accession, filingDate);
    if (s3Inventory.isComplete(form, vectorizationEnabled)) {
      recordInventory(accession, s3Inventory);
      return ProcessingDecision.skip("Self-healed: now complete");
    }

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
        tracker.bulkGetCompletedTables(accessions, PHASE_STAGING);

    for (int i = 0; i < accessions.size(); i++) {
      String accession = accessions.get(i);
      Set<String> completedTables = bulkState.get(accession);

      if (completedTables == null || completedTables.isEmpty()) {
        return false; // No tracker data at all
      }

      // Check for no_xbrl marker (stored as a "table" in the tracker)
      if (completedTables.contains(TABLE_NO_XBRL)) {
        continue; // This accession is handled
      }

      FormType form = FormType.fromString(formTypes.get(i));
      FileInventory inv = inventoryFromCompletedTables(completedTables);
      if (!inv.isComplete(form, vectorizationEnabled)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check which output files exist in S3.
   */
  public FileInventory checkS3Files(String cik, String accession) {
    return checkS3Files(cik, accession, null);
  }

  /**
   * Check which output files exist in S3.
   *
   * @param filingDate ISO date string (e.g. "2024-03-15") used to build an exact year= partition
   *                   path. When non-null this avoids a glob scan across all years (saves
   *                   10 Class A LIST ops per call). Pass null to fall back to the year=* glob.
   */
  public FileInventory checkS3Files(String cik, String accession, String filingDate) {
    String secDir = storageProvider.resolvePath(parquetBaseDir, "source=sec");

    FileInventory.Builder builder = FileInventory.builder();

    builder.hasMetadata(fileExists(secDir, cik, accession, "metadata", filingDate));
    builder.hasFacts(fileExists(secDir, cik, accession, "facts", filingDate));
    builder.hasContexts(fileExists(secDir, cik, accession, "contexts", filingDate));
    builder.hasRelationships(fileExists(secDir, cik, accession, "relationships", filingDate));
    builder.hasMda(fileExists(secDir, cik, accession, "mda", filingDate));
    builder.hasInsider(fileExists(secDir, cik, accession, "insider", filingDate));
    builder.hasEarnings(fileExists(secDir, cik, accession, "earnings", filingDate));
    builder.hasChunks(fileExists(secDir, cik, accession, "chunks", filingDate));

    return builder.build();
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

  private boolean fileExists(String secDir, String cik, String accession, String suffix,
      String filingDate) {
    String fileName = cik + "_" + accession + "_" + suffix + ".parquet";

    // Fast path: check in-memory cache populated by preloadFileInventory() — zero Class A ops.
    java.util.Set<String> cache = this.s3FileCache;
    if (cache != null) {
      String year = yearFromFilingDate(filingDate);
      if (year != null) {
        String exactPath = storageProvider.resolvePath(secDir, "year=" + year + "/" + fileName);
        return cache.contains(exactPath);
      }
      // No year available; use filename-keyed map for O(1) lookup instead of O(N) scan.
      java.util.Map<String, String> byName = this.s3FileCacheByName;
      if (byName != null) {
        return byName.containsKey(fileName);
      }
      // byName not populated (e.g. populated by old callers); fall back to linear scan.
      for (String path : cache) {
        if (path.endsWith("/" + fileName)) {
          return true;
        }
      }
      return false;
    }

    // Slow path: live S3 query.  Use exact year partition when available to avoid the
    // 11-iteration year=* glob that generates 11 Class A LIST ops per call.
    String year = yearFromFilingDate(filingDate);
    String yearSegment = (year != null) ? "year=" + year : "year=*";
    String pattern = storageProvider.resolvePath(secDir, yearSegment + "/" + fileName);
    try {
      return storageProvider.exists(pattern);
    } catch (IOException e) {
      LOGGER.debug("Error checking file existence: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Mark filing as successfully processed.
   */
  public void markComplete(String cik, String accession, String formType, String filingDate,
      boolean vectorizationEnabled, FileInventory inventory) {
    recordInventory(accession, inventory);
    // Store filing metadata
    tracker.markComplete(accession, TABLE_FILING_META, PHASE_STAGING, 1);
    // Clear any previous error state
    clearError(accession);
    LOGGER.debug("Marked complete: {}:{}", cik, accession);
  }

  /**
   * Mark filing as having no XBRL data.
   */
  public void markNoXbrl(String cik, String accession, String formType, String filingDate) {
    tracker.markComplete(accession, TABLE_NO_XBRL, PHASE_STAGING, 0);
    LOGGER.debug("Marked no_xbrl: {}:{}", cik, accession);
  }

  /**
   * Clear the no_xbrl marker so the filing will be reprocessed.
   * Used to fix accessions incorrectly marked no_xbrl due to converter bugs.
   */
  public void clearNoXbrl(String accession) {
    tracker.markCleared(accession, TABLE_NO_XBRL, PHASE_STAGING);
    LOGGER.debug("Cleared no_xbrl marker for accession: {}", accession);
  }

  /**
   * Clear stale no_xbrl markers for insider forms (3/4/5) in the given candidates.
   *
   * <p>Unlike {@link #filterAndSelfHeal}, this method does NOT add cleared entries to any
   * processing queue — it is a pure cleanup pass, intended for historical year sweeps where
   * the current worker is not responsible for reprocessing.
   *
   * @param candidates index entries to inspect
   * @return number of entries cleared
   */
  public int clearStaleInsiderNoXbrl(List<EdgarFullIndexCache.IndexEntry> candidates) {
    int cleared = 0;
    for (EdgarFullIndexCache.IndexEntry ie : candidates) {
      if (tracker.isComplete(ie.accession, TABLE_NO_XBRL, PHASE_STAGING)) {
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
    tracker.markError(accession, TABLE_ERROR_COUNT, PHASE_STAGING,
        errorMessage != null
            ? errorMessage.substring(0, Math.min(500, errorMessage.length()))
            : null);
    // Store error count in row_count field
    tracker.markComplete(accession, TABLE_ERROR_COUNT, PHASE_STAGING, errorCount);
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
   * Record a FileInventory as individual tracker entries.
   */
  private void recordInventory(String accession, FileInventory inventory) {
    if (inventory.hasMetadata()) {
      tracker.markComplete(accession, "metadata", PHASE_STAGING, 1);
    }
    if (inventory.hasFacts()) {
      tracker.markComplete(accession, "facts", PHASE_STAGING, 1);
    }
    if (inventory.hasContexts()) {
      tracker.markComplete(accession, "contexts", PHASE_STAGING, 1);
    }
    if (inventory.hasRelationships()) {
      tracker.markComplete(accession, "relationships", PHASE_STAGING, 1);
    }
    if (inventory.hasMda()) {
      tracker.markComplete(accession, "mda", PHASE_STAGING, 1);
    }
    if (inventory.hasInsider()) {
      tracker.markComplete(accession, "insider", PHASE_STAGING, 1);
    }
    if (inventory.hasEarnings()) {
      tracker.markComplete(accession, "earnings", PHASE_STAGING, 1);
    }
    if (inventory.hasChunks()) {
      tracker.markComplete(accession, "chunks", PHASE_STAGING, 1);
    }
  }

  /**
   * Build a FileInventory from a set of completed table names.
   */
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
    if (tracker.isComplete(accession, TABLE_FILING_META, PHASE_STAGING)) {
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
    Set<String> completed = tracker.getCompletedTables(accession, PHASE_STAGING);
    if (!completed.contains(TABLE_ERROR_COUNT.substring(1))) {
      // Not using substring - check the actual name
      if (!tracker.isComplete(accession, TABLE_ERROR_COUNT, PHASE_STAGING)) {
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
    tracker.markCleared(accession, TABLE_ERROR_COUNT, PHASE_STAGING);
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
   * Returns the ETL high-water mark date for the given run key, or null if not set.
   *
   * @param runKey identifies the specific ETL job (e.g. "2026_2026_8-K,4,13F-HR")
   */
  public LocalDate readEtlHighWaterMark(String runKey) {
    if (tracker instanceof S3HivePipelineTracker) {
      return ((S3HivePipelineTracker) tracker).readEtlHighWaterMark(runKey);
    }
    return null;
  }

  /**
   * Persists the ETL high-water mark date for the given run key. No-op for non-S3 trackers.
   *
   * @param runKey identifies the specific ETL job (e.g. "2026_2026_8-K,4,13F-HR")
   * @param date   the high-water mark to store
   */
  public void writeEtlHighWaterMark(String runKey, LocalDate date) {
    if (tracker instanceof S3HivePipelineTracker) {
      ((S3HivePipelineTracker) tracker).writeEtlHighWaterMark(runKey, date);
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
