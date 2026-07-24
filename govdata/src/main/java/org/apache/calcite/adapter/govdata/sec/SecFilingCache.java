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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.partition.PipelineTracker;
import org.apache.calcite.adapter.file.partition.S3HivePipelineTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  /** Matches batch-merged parquet files written by LocalStagingStorageProvider. */
  private static final Pattern BATCH_FILE_PATTERN =
      Pattern.compile("^([a-z_]+)_batch_(\\d+)\\.parquet$", Pattern.CASE_INSENSITIVE);

  /** Base table types always expected for a complete 10-K/10-Q filing. */
  private static final String[] BASE_FILE_TYPES =
      {"metadata", "facts", "contexts", "relationships", "mda"};

  /**
   * Output-type suffix -&gt; materialized Iceberg table name. The Iceberg tables are the
   * authoritative record of what was processed: a filing present in a table was processed
   * for that output type, regardless of whether its staging {@code *_batch_*.parquet} still
   * exists (staging is removed after materialization and its chunked reads are unreliable).
   * {@link #populateAccessionIndexFromIceberg} reads these to build the presence index.
   */
  private static final java.util.Map<String, String> ICEBERG_TABLE_BY_TYPE;

  static {
    java.util.Map<String, String> m = new java.util.LinkedHashMap<String, String>();
    m.put("metadata", "filing_metadata");
    m.put("facts", "financial_line_items");
    m.put("contexts", "filing_contexts");
    m.put("relationships", "xbrl_relationships");
    m.put("mda", "mda_sections");
    m.put("insider", "insider_transactions");
    m.put("earnings", "earnings_transcripts");
    m.put("chunks", "vectorized_chunks");
    m.put("13f", "institutional_holdings");
    m.put("13dg", "beneficial_ownership");
    ICEBERG_TABLE_BY_TYPE = java.util.Collections.unmodifiableMap(m);
  }

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
   * Tertiary index: accession number → set of output types present in storage, keyed by
   * accession ALONE (any CIK). Populated alongside {@link #s3FileCacheByName}.
   *
   * <p>Rationale: an accession number identifies exactly one EDGAR submission, but EDGAR's
   * full index lists ownership filings (Forms 3/4/5, SC 13D/G) once per CIK involved — the
   * issuer AND every reporting owner. The ETL stores each filing only under the ISSUER CIK,
   * so a reporting-owner candidate's {@code cik_accession_type.parquet} never matches the
   * CIK-keyed {@link #s3FileCacheByName}, and the filing is falsely seen as missing and
   * reprocessed on every run. Because the accession is globally unique to one filing, matching
   * existence on (accession, type) regardless of CIK is correct and heals that miss.
   */
  private volatile java.util.Map<String, java.util.Set<String>> s3AccessionTypes = null;

  /** Parses a virtual/individual parquet filename into (accession, outputType). */
  private static final Pattern FILE_NAME_PATTERN =
      Pattern.compile("^\\d+_(\\d{10}-\\d{2}-\\d{6})_(.+)\\.parquet$");

  /** Lazily-initialized DuckDB connection used to read batch parquet files during preload. */
  private Connection duckdbConn = null;

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
      tracker.bulkGetCompletedTables(filingKeys(chunk), PHASE_STAGING);
      loaded += chunk.size();
    }
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Preloaded tracker state for {} accessions in {}ms", loaded, elapsed);
  }

  /**
   * Backfills no_xbrl sentinel parquet files to S3 for filings already marked
   * {@code _no_xbrl} in the tracker but lacking a sentinel file in S3.
   *
   * <p>The original {@link #writeNoXbrlSentinel} had a bug where it returned early when
   * {@code filingDate} was empty, so historical no_xbrl filings were never written to S3.
   * This one-time backfill corrects that gap so the self-heal path can skip no_xbrl
   * filings without needing a tracker.
   *
   * <p>Requires that {@link #preloadFileInventory} has already been called so the
   * in-memory byName cache can be used to skip sentinels that already exist.
   *
   * @param candidates index entries to inspect (typically all EDGAR year candidates)
   * @return number of sentinels newly written to S3
   */
  public int backfillNoXbrlSentinels(List<EdgarFullIndexCache.IndexEntry> candidates) {
    if (candidates.isEmpty()) {
      return 0;
    }
    List<String> accessions = new ArrayList<String>(candidates.size());
    for (EdgarFullIndexCache.IndexEntry ie : candidates) {
      accessions.add(ie.accession);
    }
    Map<String, Set<String>> bulk = tracker.bulkGetCompletedTables(filingKeys(accessions), PHASE_STAGING);
    int written = 0;
    int alreadyExists = 0;
    int notNoXbrl = 0;
    for (EdgarFullIndexCache.IndexEntry ie : candidates) {
      Set<String> tables = bulk.get(filingKey(ie.accession));
      if (tables == null || !tables.contains(TABLE_NO_XBRL)) {
        notNoXbrl++;
        continue;
      }
      String fileName = ie.cik + "_" + ie.accession + "_no_xbrl.parquet";
      java.util.Map<String, String> byName = this.s3FileCacheByName;
      if (byName != null && byName.containsKey(fileName)) {
        alreadyExists++;
        continue;
      }
      writeNoXbrlSentinel(ie.cik, ie.accession, ie.filingDate);
      written++;
    }
    LOGGER.info(
        "backfillNoXbrlSentinels: {} sentinels written, {} already existed, {} not no_xbrl",
        written, alreadyExists, notNoXbrl);
    return written;
  }

  /**
   * Scans only the {@code sec/year=YYYY/} partitions for {@code startYear..endYear}
   * and caches all known file paths in memory.
   *
   * <p>After this call, {@link #checkS3Files} answers existence queries from the
   * in-memory set — zero additional Class A LIST ops per filing check.
   *
   * <p>Cost: one paginated {@code ListObjectsV2} scan per year partition
   * (roughly 1 Class A op per 1 000 files in each year).  Scoping to the worker's
   * actual year range avoids listing the full sec/}
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
    String secDir = parquetBaseDir;
    // Scan one year before startYear to catch Jan–Apr 10-K/Q filings whose output was written
    // to year=(startYear-1) by getPartitionYear's fiscal-year heuristic.
    int scanStart = startYear - 1;
    LOGGER.info("preloadFileInventory: scanning secDir={} years {}-{} (fiscal buffer from {})",
        secDir, startYear, endYear, scanStart);
    java.util.Set<String> cache = new java.util.HashSet<String>();
    java.util.Map<String, String> byName = new java.util.HashMap<String, String>();
    int totalVirtual = 0;
    int totalBatchFiles = 0;
    for (int year = scanStart; year <= endYear; year++) {
      String yearDir = storageProvider.resolvePath(secDir, "year=" + year);
      int[] result = scanYearIntoCache(yearDir, year, cache, byName);
      totalVirtual += result[0];
      totalBatchFiles += result[1];
    }

    // Legacy fallback: previous builds wrote staging to sec/sec/ due to a
    // double-append bug. Scan that subtree too so self-healing can find those files.
    String legacySecDir = storageProvider.resolvePath(parquetBaseDir, "sec");
    if (!legacySecDir.equals(secDir)) {
      int legacyVirtual = 0;
      int legacyBatchFiles = 0;
      for (int year = scanStart; year <= endYear; year++) {
        String legacyYearDir = storageProvider.resolvePath(legacySecDir, "year=" + year);
        int[] result = scanYearIntoCache(legacyYearDir, year, cache, byName);
        legacyVirtual += result[0];
        legacyBatchFiles += result[1];
      }
      if (legacyVirtual > 0) {
        LOGGER.info("preloadFileInventory: found {} virtual entries at legacy double-path {}",
            legacyVirtual, legacySecDir);
      }
      totalVirtual += legacyVirtual;
      totalBatchFiles += legacyBatchFiles;
    }

    // Build the accession-keyed presence index. Authoritative source: the materialized
    // Iceberg tables (see ICEBERG_TABLE_BY_TYPE) — a filing present in a table was processed
    // for that output type regardless of whether its staging parquet still exists. Supplement
    // with any staging filenames still present. Keyed by accession alone (globally unique to
    // one filing) so ownership filings stored under the issuer CIK are found regardless of
    // which CIK a candidate carries.
    java.util.Map<String, java.util.Set<String>> byAccession =
        new java.util.HashMap<String, java.util.Set<String>>();
    for (String name : byName.keySet()) {
      Matcher am = FILE_NAME_PATTERN.matcher(name);
      if (am.matches()) {
        addAccessionType(byAccession, am.group(1), am.group(2));
      }
    }
    int stagingAccessions = byAccession.size();
    populateAccessionIndexFromIceberg(scanStart, endYear, byAccession);

    this.s3FileCacheByName = java.util.Collections.unmodifiableMap(byName);
    this.s3AccessionTypes = java.util.Collections.unmodifiableMap(byAccession);
    this.s3FileCache = java.util.Collections.unmodifiableSet(cache);
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("preloadFileInventory: {} distinct accessions in presence index "
        + "({} from staging, {} added from Iceberg), {} staging virtual entries, "
        + "{} batch files (years {}-{}) in {}ms",
        byAccession.size(), stagingAccessions, byAccession.size() - stagingAccessions,
        totalVirtual, totalBatchFiles, startYear, endYear, elapsed);
  }

  /**
   * Scans one year partition into the in-memory cache.
   *
   * @return int[]{virtualEntriesAdded, batchFilesFound}
   */
  private int[] scanYearIntoCache(String yearDir, int year,
      java.util.Set<String> cache, java.util.Map<String, String> byName) {
    try {
      List<StorageProvider.FileEntry> entries = storageProvider.listFiles(yearDir, true);
      java.util.Map<String, List<String>> batchByType =
          new java.util.LinkedHashMap<String, List<String>>();
      int yearCount = 0;
      int batchCount = 0;
      for (StorageProvider.FileEntry entry : entries) {
        if (!entry.isDirectory()) {
          String path = entry.getPath();
          int slash = path.lastIndexOf('/');
          String name = (slash >= 0) ? path.substring(slash + 1) : path;
          Matcher m = BATCH_FILE_PATTERN.matcher(name);
          if (m.matches()) {
            String tableType = m.group(1).toLowerCase();
            List<String> paths = batchByType.get(tableType);
            if (paths == null) {
              paths = new ArrayList<String>();
              batchByType.put(tableType, paths);
            }
            paths.add(path);
            batchCount++;
          } else {
            cache.add(path);
            byName.put(name, path);
            yearCount++;
          }
        }
      }
      for (Map.Entry<String, List<String>> batchEntry : batchByType.entrySet()) {
        yearCount += populateCacheFromBatchFiles(
            batchEntry.getKey(), batchEntry.getValue(), yearDir, year, cache, byName);
      }
      LOGGER.debug("preloadFileInventory: yearDir={} loaded {} virtual entries from {} batch files",
          yearDir, yearCount, batchCount);
      return new int[]{yearCount, batchCount};
    } catch (IOException e) {
      LOGGER.warn("preloadFileInventory: year={} scan failed — {}", year, e.getMessage());
      return new int[]{0, 0};
    }
  }

  /** Chunk size for DuckDB batch-parquet reads. Small to avoid R2/SSL connection drops. */
  private static final int DUCKDB_BATCH_CHUNK_SIZE = 5;

  private int populateCacheFromBatchFiles(String tableType, List<String> batchPaths,
      String yearDir, int year, java.util.Set<String> cache,
      java.util.Map<String, String> byName) {
    int total = 0;
    int chunkStart = 0;
    while (chunkStart < batchPaths.size()) {
      int chunkEnd = Math.min(chunkStart + DUCKDB_BATCH_CHUNK_SIZE, batchPaths.size());
      List<String> chunk = batchPaths.subList(chunkStart, chunkEnd);
      total += populateCacheFromChunk(tableType, chunk, yearDir, year, cache, byName);
      chunkStart = chunkEnd;
    }
    LOGGER.info("populateCacheFromBatchFiles: type={} batchFiles={} accessions={}",
        tableType, batchPaths.size(), total);
    return total;
  }

  private int populateCacheFromChunk(String tableType, List<String> chunk,
      String yearDir, int year, java.util.Set<String> cache,
      java.util.Map<String, String> byName) {
    int result = tryPopulateChunk(tableType, chunk, yearDir, year, cache, byName);
    if (result == 0 && !chunk.isEmpty()) {
      // Retry once with a fresh DuckDB connection in case the old one was broken by an SSL drop.
      resetDuckdbConn();
      result = tryPopulateChunk(tableType, chunk, yearDir, year, cache, byName);
      if (result > 0) {
        LOGGER.info("populateCacheFromChunk: retry succeeded for type={} year={} chunk-size={}",
            tableType, year, chunk.size());
      }
    }
    return result;
  }

  private int tryPopulateChunk(String tableType, List<String> chunk,
      String yearDir, int year, java.util.Set<String> cache,
      java.util.Map<String, String> byName) {
    Connection conn = getOrCreateDuckdbConn();
    if (conn == null) {
      LOGGER.warn("tryPopulateChunk: no DuckDB connection, skipping type={}", tableType);
      return 0;
    }
    try {
      StringBuilder pathList = new StringBuilder();
      for (int i = 0; i < chunk.size(); i++) {
        if (i > 0) {
          pathList.append(", ");
        }
        pathList.append("'").append(chunk.get(i)).append("'");
      }
      String sql = "SELECT DISTINCT cik, accession_number FROM read_parquet(["
          + pathList + "], union_by_name=true)";
      int count = 0;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String cik = rs.getString("cik");
          String accession = rs.getString("accession_number");
          if (cik == null || accession == null) {
            continue;
          }
          addVirtualEntry(yearDir, cik, accession, tableType, cache, byName);
          count++;
          if ("chunks".equalsIgnoreCase(tableType)) {
            for (String baseType : BASE_FILE_TYPES) {
              addVirtualEntry(yearDir, cik, accession, baseType, cache, byName);
            }
          }
        }
      }
      return count;
    } catch (Exception e) {
      LOGGER.warn("tryPopulateChunk: type={} year={} chunk-size={} failed — {}",
          tableType, year, chunk.size(), e.getMessage());
      resetDuckdbConn();
      return 0;
    }
  }

  private void addVirtualEntry(String yearDir, String cik, String accession,
      String tableType, java.util.Set<String> cache, java.util.Map<String, String> byName) {
    String name = cik + "_" + accession + "_" + tableType + ".parquet";
    String path = storageProvider.resolvePath(yearDir, name);
    cache.add(path);
    byName.put(name, path);
  }

  /** Records that {@code accession} has output {@code type} present, creating the set if needed. */
  private static void addAccessionType(java.util.Map<String, java.util.Set<String>> byAccession,
      String accession, String type) {
    java.util.Set<String> types = byAccession.get(accession);
    if (types == null) {
      types = new java.util.HashSet<String>();
      byAccession.put(accession, types);
    }
    types.add(type);
  }

  /**
   * Merges the authoritative processed-filing record from the materialized Iceberg tables into
   * the accession-keyed presence index. For each output type in {@link #ICEBERG_TABLE_BY_TYPE},
   * reads {@code DISTINCT accession_number} from the table (scoped to the scanned year range via
   * the {@code year} partition column) and records that each accession has that output type.
   *
   * <p>This is the reliable "was it processed?" signal: staging {@code *_batch_*.parquet} files
   * are removed after materialization and their chunked reads drop accessions on transient
   * errors, so the file-existence check alone perpetually re-queues already-materialized filings.
   * The Iceberg table is the source of truth. A per-table failure is logged and skipped so the
   * remaining types still populate.
   *
   * @param startYear   first year partition to include (the fiscal-buffer-adjusted scan start)
   * @param endYear     last year partition to include (inclusive)
   * @param byAccession presence index to merge into (accession -&gt; set of output-type suffixes)
   */
  private void populateAccessionIndexFromIceberg(int startYear, int endYear,
      java.util.Map<String, java.util.Set<String>> byAccession) {
    Connection conn = getOrCreateDuckdbConn();
    if (conn == null) {
      LOGGER.warn("populateAccessionIndexFromIceberg: no DuckDB connection — skipping Iceberg "
          + "presence index; self-heal falls back to staging + tracker only");
      return;
    }
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL iceberg");
      stmt.execute("LOAD iceberg");
    } catch (Exception e) {
      LOGGER.debug("populateAccessionIndexFromIceberg: iceberg extension load note — {}",
          e.getMessage());
    }
    for (Map.Entry<String, String> entry : ICEBERG_TABLE_BY_TYPE.entrySet()) {
      String type = entry.getKey();
      String tablePath = storageProvider.resolvePath(parquetBaseDir, entry.getValue());
      String sql = "SELECT DISTINCT accession_number FROM iceberg_scan('" + tablePath
          + "', allow_moved_paths=true) WHERE accession_number IS NOT NULL "
          + "AND year BETWEEN " + startYear + " AND " + endYear;
      long queryStart = System.currentTimeMillis();
      int typeCount = 0;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String accession = rs.getString(1);
          if (accession != null) {
            addAccessionType(byAccession, accession, type);
            typeCount++;
          }
        }
      } catch (Exception e) {
        LOGGER.warn("populateAccessionIndexFromIceberg: type={} table={} failed — {}",
            type, entry.getValue(), e.getMessage());
        continue;
      }
      LOGGER.info("populateAccessionIndexFromIceberg: type={} table={} accessions={} in {}ms",
          type, entry.getValue(), typeCount, System.currentTimeMillis() - queryStart);
    }
  }

  private Connection getOrCreateDuckdbConn() {
    if (duckdbConn == null) {
      try {
        duckdbConn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);
      } catch (Exception e) {
        LOGGER.warn("getOrCreateDuckdbConn: failed to initialize DuckDB — {}", e.getMessage());
      }
    }
    return duckdbConn;
  }

  private void resetDuckdbConn() {
    if (duckdbConn != null) {
      try {
        duckdbConn.close();
      } catch (Exception ignored) {
        // best-effort close
      }
      duckdbConn = null;
    }
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
    return filterAndSelfHeal(candidates, vectorizationEnabled, selfHealThreads, false,
        java.util.Collections.<String>emptySet());
  }

  public List<EdgarFullIndexCache.IndexEntry> filterAndSelfHeal(
      List<EdgarFullIndexCache.IndexEntry> candidates,
      boolean vectorizationEnabled,
      int selfHealThreads,
      boolean chunksBackfill) {
    return filterAndSelfHeal(candidates, vectorizationEnabled, selfHealThreads, chunksBackfill,
        java.util.Collections.<String>emptySet());
  }

  public List<EdgarFullIndexCache.IndexEntry> filterAndSelfHeal(
      List<EdgarFullIndexCache.IndexEntry> candidates,
      boolean vectorizationEnabled,
      int selfHealThreads,
      boolean chunksBackfill,
      java.util.Set<String> forceAccessions) {

    List<EdgarFullIndexCache.IndexEntry> toProcess =
        new ArrayList<EdgarFullIndexCache.IndexEntry>();
    final List<EdgarFullIndexCache.IndexEntry> toSelfHeal =
        new ArrayList<EdgarFullIndexCache.IndexEntry>();

    int cntNoXbrl = 0;
    int cntTrackerComplete = 0;
    int cntTrackerBaseComplete = 0;
    int cntTrackerIncomplete = 0;
    int cntTrackerHealed = 0;
    int cntS3Complete = 0;
    int cntS3Partial = 0;
    int cntNoFiles = 0;

    for (EdgarFullIndexCache.IndexEntry ie : candidates) {
      if (!forceAccessions.isEmpty() && forceAccessions.contains(ie.accession)) {
        toProcess.add(ie);
        continue;
      }
      if (!forceAccessions.isEmpty()) {
        // Reprocess-only mode: skip all candidates not explicitly listed.
        continue;
      }
      if (tracker.isComplete(filingKey(ie.accession), TABLE_NO_XBRL, PHASE_STAGING)) {
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
      Set<String> completed = tracker.getCompletedTables(filingKey(ie.accession), PHASE_STAGING);
      if (!completed.isEmpty()) {
        FormType form = FormType.fromString(ie.formType);
        FileInventory inv = inventoryFromCompletedTables(completed);
        if (inv.isComplete(form, vectorizationEnabled)) {
          cntTrackerComplete++;
          continue;
        }
        // Base staging parquet exists; only chunks are missing.
        // In normal mode: skip — re-downloading just for chunks is unnecessary.
        // In chunksBackfill mode: reprocess so chunks get written.
        if (inv.isComplete(form, false) && (!chunksBackfill || inv.isComplete(form, true))) {
          cntTrackerBaseComplete++;
          continue;
        }
        // Tracker markers are incomplete, but the missing outputs may already exist in
        // storage: a completion marker can be lost (tracker/storage divergence) while the
        // parquet survives, so reprocessing on tracker state alone reconverts a filing whose
        // output is already in S3/R2 on EVERY run (e.g. insider forms marked 'insider' but
        // missing 'metadata'). Consult the preloaded storage inventory (in-memory, zero LIST
        // ops) before giving up: if the union of tracker + storage satisfies the form's
        // expected outputs, self-heal the absent markers from storage and skip. This extends
        // the existence-heal below (which only fires for accessions with NO tracker record) to
        // the partial-tracker case.
        FileInventory storageInv = checkS3Files(ie.cik, ie.accession, ie.filingDate);
        FileInventory healed = unionInventory(inv, storageInv);
        if (healed.isComplete(form, vectorizationEnabled)
            || (healed.isComplete(form, false)
                && (!chunksBackfill || healed.isComplete(form, true)))) {
          toSelfHeal.add(ie);
          cntTrackerHealed++;
          continue;
        }
        cntTrackerIncomplete++;
        toProcess.add(ie);
        continue;
      }
      FileInventory s3Inv = checkS3Files(ie.cik, ie.accession, ie.filingDate);
      if (s3Inv.hasNoXbrl()) {
        // no_xbrl sentinel file found in S3 — filing was processed, has no XBRL data.
        // Skip without adding to self-heal queue (no parquet to record).
        cntNoXbrl++;
      } else if (s3Inv.hasAnyFiles()) {
        toSelfHeal.add(ie);
        FormType form = FormType.fromString(ie.formType);
        // Only process if base staging files are also incomplete, not just chunks.
        if (!s3Inv.isComplete(form, false)) {
          cntS3Partial++;
          if (cntS3Partial <= 5) {
            LOGGER.info("s3Partial sample: accession={} formType={} inventory={}",
                ie.accession, ie.formType, s3Inv);
          }
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
            + "trackerIncomplete={} trackerHealed={} s3Complete={} s3Partial={} noFiles={} "
            + "toProcess={}",
        candidates.size(), cntNoXbrl, cntTrackerComplete, cntTrackerBaseComplete,
        cntTrackerIncomplete, cntTrackerHealed, cntS3Complete, cntS3Partial, cntNoFiles,
        toProcess.size());

    if (!toSelfHeal.isEmpty()) {
      int poolSize = Math.min(selfHealThreads > 0 ? selfHealThreads : 1, 50);
      LOGGER.info("Self-healing {} accessions with {} threads", toSelfHeal.size(), poolSize);
      long healStart = System.currentTimeMillis();
      java.util.concurrent.ExecutorService pool =
          java.util.concurrent.Executors.newFixedThreadPool(poolSize);
      for (final EdgarFullIndexCache.IndexEntry ie : toSelfHeal) {
        @SuppressWarnings("FutureReturnValueIgnored")
        java.util.concurrent.Future<?> ignored = pool.submit(new Runnable() {
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
        // force a per-CIK pass so filterAndSelfHeal/checkFiling can clear it and reprocess.
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
    String secDir = parquetBaseDir;

    FileInventory.Builder builder = FileInventory.builder();

    builder.hasNoXbrl(fileExists(secDir, cik, accession, "no_xbrl", filingDate));
    builder.hasMetadata(fileExists(secDir, cik, accession, "metadata", filingDate));
    builder.hasFacts(fileExists(secDir, cik, accession, "facts", filingDate));
    builder.hasContexts(fileExists(secDir, cik, accession, "contexts", filingDate));
    builder.hasRelationships(fileExists(secDir, cik, accession, "relationships", filingDate));
    builder.hasMda(fileExists(secDir, cik, accession, "mda", filingDate));
    builder.hasInsider(fileExists(secDir, cik, accession, "insider", filingDate));
    builder.hasEarnings(fileExists(secDir, cik, accession, "earnings", filingDate));
    builder.hasChunks(fileExists(secDir, cik, accession, "chunks", filingDate));
    builder.hasInstitutionalHoldings(fileExists(secDir, cik, accession, "13f", filingDate));
    builder.hasBeneficialOwnership(fileExists(secDir, cik, accession, "13dg", filingDate));

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
   * {@code {accession_number, year}} key map (see {@code S3HivePipelineTracker.flattenKeyValues}:
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

  private boolean fileExists(String secDir, String cik, String accession, String suffix,
      String filingDate) {
    String fileName = cik + "_" + accession + "_" + suffix + ".parquet";

    // Fast path: check in-memory cache populated by preloadFileInventory() — zero Class A ops.
    java.util.Set<String> cache = this.s3FileCache;
    if (cache != null) {
      // Try the canonical exact path first (O(1)).
      String year = yearFromFilingDate(filingDate);
      if (year != null) {
        String exactPath = storageProvider.resolvePath(secDir, "year=" + year + "/" + fileName);
        if (cache.contains(exactPath)) {
          return true;
        }
      }
      // Fall through to byName — handles files at legacy paths (e.g. after a path-bug fix
      // where existing files live at a different directory than the current secDir).
      java.util.Map<String, String> byName = this.s3FileCacheByName;
      if (byName != null) {
        if (byName.containsKey(fileName)) {
          return true;
        }
        // CIK-keyed miss: fall back to the accession-keyed index. An accession identifies one
        // filing, so if this output type exists under ANY CIK the filing is present. This heals
        // ownership-form candidates (Forms 3/4/5, SC 13D/G) that EDGAR indexes under a reporting
        // owner's CIK while the ETL stored the output under the issuer's CIK — without which those
        // candidates are perpetually seen as missing and reprocessed every run.
        java.util.Map<String, java.util.Set<String>> byAccession = this.s3AccessionTypes;
        if (byAccession != null) {
          java.util.Set<String> types = byAccession.get(accession);
          return types != null && types.contains(suffix);
        }
        return false;
      }
      // byName not populated; fall back to linear scan.
      for (String path : cache) {
        if (path.endsWith("/" + fileName)) {
          return true;
        }
      }
      return false;
    }

    // Slow path: live S3 query.
    // First try the DuckDB batch-file lookup since production uses batch-only organization
    // (per-accession files never exist; only {type}_batch_NNNN.parquet files are in S3).
    String year = yearFromFilingDate(filingDate);
    if (year != null) {
      Boolean batchResult = existsInBatchFiles(secDir, cik, accession, suffix, year);
      if (batchResult != null) {
        return batchResult;
      }
    }

    // Final fallback: check for a per-accession file (legacy / non-batch S3 layout).
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
   * Checks whether {@code accession} appears in any {@code {suffix}_batch_*.parquet} file
   * in {@code secDir/year={year}/}.
   *
   * <p>Used as the primary slow-path for production S3 which stores batch-merged parquet
   * (not individual per-accession files). Only the first few batch files are queried to
   * keep latency reasonable; if the accession isn't found, returns {@code false}.
   *
   * @return {@code true} / {@code false} if batch files exist and DuckDB answered,
   *         {@code null} if no batch files found (caller should try legacy path)
   */
  private Boolean existsInBatchFiles(String secDir, String cik, String accession,
      String suffix, String year) {
    String yearDir = storageProvider.resolvePath(secDir, "year=" + year);
    List<String> batchPaths;
    try {
      List<StorageProvider.FileEntry> entries = storageProvider.listFiles(yearDir, false);
      batchPaths = new ArrayList<String>();
      String prefix = suffix.toLowerCase() + "_batch_";
      for (StorageProvider.FileEntry e : entries) {
        if (!e.isDirectory()) {
          String path = e.getPath();
          int slash = path.lastIndexOf('/');
          String name = (slash >= 0) ? path.substring(slash + 1) : path;
          if (name.startsWith(prefix) && name.endsWith(".parquet")) {
            batchPaths.add(path);
            if (batchPaths.size() >= DUCKDB_BATCH_CHUNK_SIZE) {
              break;
            }
          }
        }
      }
    } catch (IOException e) {
      LOGGER.debug("existsInBatchFiles: list failed for {} — {}", yearDir, e.getMessage());
      return null;
    }
    if (batchPaths.isEmpty()) {
      return null;
    }
    Connection conn = getOrCreateDuckdbConn();
    if (conn == null) {
      return null;
    }
    try {
      StringBuilder pathList = new StringBuilder();
      for (int i = 0; i < batchPaths.size(); i++) {
        if (i > 0) {
          pathList.append(", ");
        }
        pathList.append("'").append(batchPaths.get(i)).append("'");
      }
      String safe = accession.replace("'", "''");
      String sql = "SELECT 1 FROM read_parquet([" + pathList
          + "]) WHERE accession_number = '" + safe + "' LIMIT 1";
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        return rs.next();
      }
    } catch (Exception e) {
      LOGGER.debug("existsInBatchFiles: DuckDB query failed for {}/{} — {}", cik, accession,
          e.getMessage());
      resetDuckdbConn();
      return null;
    }
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
   * Mark filing as having no XBRL data.
   *
   * <p>Also writes a minimal sentinel parquet file to S3 ({@code {cik}_{accession}_no_xbrl.parquet})
   * so that the self-heal path can detect and skip this filing even without a tracker.
   */
  public void markNoXbrl(String cik, String accession, String formType, String filingDate) {
    tracker.markComplete(filingKey(accession), TABLE_NO_XBRL, PHASE_STAGING, 0);
    writeNoXbrlSentinel(cik, accession, filingDate);
    LOGGER.debug("Marked no_xbrl: {}:{}", cik, accession);
  }

  private void writeNoXbrlSentinel(String cik, String accession, String filingDate) {
    String year = yearFromFilingDate(filingDate);
    if (year == null) {
      year = yearFromAccession(accession);
    }
    if (year == null) {
      LOGGER.warn("writeNoXbrlSentinel: cannot determine year for accession={}, skipping", accession);
      return;
    }
    String yearDir = storageProvider.resolvePath(parquetBaseDir, "year=" + year);
    String fileName = cik + "_" + accession + "_no_xbrl.parquet";
    String destPath = storageProvider.resolvePath(yearDir, fileName);
    File tmp = null;
    try {
      tmp = File.createTempFile("no_xbrl_sentinel_", ".parquet");
      Connection conn = getOrCreateDuckdbConn();
      if (conn == null) {
        LOGGER.warn("writeNoXbrlSentinel: no DuckDB connection, skipping sentinel for {}", accession);
        return;
      }
      String safeCik = cik.replace("'", "''");
      String safeAccession = accession.replace("'", "''");
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("COPY (SELECT '" + safeCik + "' AS cik, '"
            + safeAccession + "' AS accession_number) TO '"
            + tmp.getAbsolutePath().replace("\\", "/") + "' (FORMAT PARQUET)");
      }
      try (InputStream in = new FileInputStream(tmp)) {
        storageProvider.writeFile(destPath, in);
      }
      LOGGER.debug("Wrote no_xbrl sentinel: {}", destPath);
    } catch (Exception e) {
      LOGGER.warn("writeNoXbrlSentinel: failed for accession={} — {}", accession, e.getMessage());
    } finally {
      if (tmp != null) {
        tmp.delete();
      }
    }
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
   * Record a FileInventory as individual tracker entries.
   */
  private void recordInventory(String accession, FileInventory inventory) {
    String key = filingKey(accession);
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
    if (duckdbConn != null) {
      try {
        duckdbConn.close();
      } catch (SQLException e) {
        LOGGER.error("Error closing DuckDB connection: {}", e.getMessage());
      }
      duckdbConn = null;
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
