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
import org.apache.calcite.adapter.file.storage.S3StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Diagnostic integration test mirroring worker-02 (10-K/10-Q, year 2025) behavior.
 *
 * <p>Connects to the production S3/R2 bucket and verifies that:
 * <ol>
 *   <li>Staging parquet files exist for year-2025 10-K/10-Q filings at the expected path.</li>
 *   <li>{@link SecFilingCache#preloadFileInventory(int, int)} populates an in-memory
 *       inventory from those real files.</li>
 *   <li>{@link SecFilingCache#filterAndSelfHeal} returns a significantly smaller
 *       "to-process" set than the candidate input — i.e. self-healing finds the files
 *       and skips already-done work rather than re-queueing everything.</li>
 * </ol>
 *
 * <p>Required environment variables (same as worker-02 .env.prod):
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}</li>
 *   <li>{@code AWS_SECRET_ACCESS_KEY}</li>
 *   <li>{@code AWS_ENDPOINT_OVERRIDE} — e.g. R2 endpoint URL</li>
 *   <li>{@code GOVDATA_PARQUET_DIR} — defaults to {@code s3://govdata-parquet-v1/source=sec}</li>
 *   <li>{@code CALCITE_TRACKER_S3_BUCKET} — defaults to {@code s3://govdata-tracker-v1}</li>
 * </ul>
 */
@Tag("integration")
public class Worker02SelfHealIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(Worker02SelfHealIntegrationTest.class);

  // Worker-02 processes 10-K and 10-Q filings for year 2025
  private static final int YEAR = 2025;
  private static final String[] FORM_TYPES = {"10-K", "10-Q", "10-K/A", "10-Q/A"};

  private static final Pattern BATCH_FILE_PATTERN =
      Pattern.compile("^([a-z_]+)_batch_(\\d+)\\.parquet$", Pattern.CASE_INSENSITIVE);

  // Minimum files that must exist at the primary path to consider the test meaningful
  private static final int MIN_FILES_FOR_TEST = 10;

  @BeforeAll
  static void checkCredentials() {
    assumeTrue(hasCredentials(),
        "Skipping: production S3 credentials not set "
            + "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_OVERRIDE)");
  }

  /**
   * Step 1 (diagnostic): Reports how many parquet files exist at the expected primary path
   * and at the legacy double-path. Fails if NEITHER path has any files.
   */
  @Test
  void stagingParquetExistsAtExpectedPath() throws IOException {
    String govdataParquetDir = getGovdataParquetDir();
    StorageProvider storage = buildStorageProvider();

    // govdataParquetDir already contains /source=sec (mirrors production SecFilingCache.parquetBaseDir)
    String primaryYearDir =
        storage.resolvePath(govdataParquetDir, "year=" + YEAR);
    // Legacy path: files written before Fix 1 lived at source=sec/source=sec/year=YYYY
    String legacySecDir =
        storage.resolvePath(govdataParquetDir, "source=sec");
    String legacyYearDir =
        storage.resolvePath(legacySecDir, "year=" + YEAR);

    LOGGER.info("=== Probing staging parquet paths for year={} ===", YEAR);
    LOGGER.info("govdataParquetDir: {}", govdataParquetDir);
    LOGGER.info("Primary year dir:  {}", primaryYearDir);
    LOGGER.info("Legacy year dir:   {}", legacyYearDir);

    int primaryCount = countFiles(storage, primaryYearDir);
    int legacyCount = countFiles(storage, legacyYearDir);

    LOGGER.info("Primary path files: {}", primaryCount);
    LOGGER.info("Legacy path files:  {}", legacyCount);
    LOGGER.info("Total accessible:   {}", primaryCount + legacyCount);

    assertTrue(primaryCount + legacyCount > 0,
        "No staging parquet files found at either primary (" + primaryYearDir
            + ") or legacy (" + legacyYearDir + ") for year=" + YEAR
            + ". Has ETL run for 2025 10-K/10-Q yet?");
  }

  /**
   * Step 2: {@link SecFilingCache#preloadFileInventory} must populate a non-empty cache
   * from live S3 data. This verifies the scan path is correct end-to-end.
   */
  @Test
  void preloadFileInventoryFindsFilesInLiveS3() throws Exception {
    String govdataParquetDir = getGovdataParquetDir();
    StorageProvider storage = buildStorageProvider();

    int primaryCount = countFiles(storage,
        storage.resolvePath(govdataParquetDir, "year=" + YEAR));
    int legacyCount = countFiles(storage,
        storage.resolvePath(
            storage.resolvePath(govdataParquetDir, "source=sec"), "year=" + YEAR));
    assumeTrue(primaryCount + legacyCount >= MIN_FILES_FOR_TEST,
        "Fewer than " + MIN_FILES_FOR_TEST + " staging files found — skipping preload test");

    SecFilingCache cache = new SecFilingCache(
        PipelineTracker.NOOP_PIPELINE, storage, govdataParquetDir);

    cache.preloadFileInventory(YEAR, YEAR);

    // Extract real (cik, accession) pairs from batch parquet files via DuckDB
    List<String[]> samplePairs = readCikAccessionPairs(storage, govdataParquetDir, 5);
    LOGGER.info("Sample (cik, accession) pairs from S3 batch files: {}", samplePairs.size());
    assertFalse(samplePairs.isEmpty(),
        "Could not extract any (cik, accession) pairs from S3 batch files");

    for (String[] pair : samplePairs) {
      String cik = pair[0];
      String accession = pair[1];
      LOGGER.info("Probing sample: cik={} accession={}", cik, accession);

      FileInventory inv = cache.checkS3Files(cik, accession, null);
      assertTrue(inv.hasAnyFiles(),
          "preloadFileInventory cached files but checkS3Files returned empty for "
              + cik + "/" + accession
              + " — possible batch-file parsing or virtual-entry mismatch");
    }

    LOGGER.info("preloadFileInventory test passed: sampled {} accessions, all found in cache",
        samplePairs.size());
  }

  /**
   * Step 3 (the key assertion): {@link SecFilingCache#filterAndSelfHeal} must skip the vast
   * majority of 2025 10-K/10-Q candidates when staging parquet already exists in S3.
   *
   * <p>On a fresh tracker (NOOP), self-healing drives skip decisions purely from S3 file
   * existence. The test passes if fewer than 50% of candidates need processing — confirming
   * the self-heal path finds files and does not blindly re-queue everything.
   */
  @Test
  void filterAndSelfHealSkipsMostCandidatesWhenFilesExist() throws Exception {
    String govdataParquetDir = getGovdataParquetDir();
    StorageProvider storage = buildStorageProvider();

    int primaryCount = countFiles(storage,
        storage.resolvePath(govdataParquetDir, "year=" + YEAR));
    int legacyCount = countFiles(storage,
        storage.resolvePath(
            storage.resolvePath(govdataParquetDir, "source=sec"), "year=" + YEAR));
    assumeTrue(primaryCount + legacyCount >= MIN_FILES_FOR_TEST,
        "Fewer than " + MIN_FILES_FOR_TEST + " staging files found — skipping self-heal test");

    SecFilingCache cache = new SecFilingCache(
        PipelineTracker.NOOP_PIPELINE, storage, govdataParquetDir);
    cache.preloadFileInventory(YEAR, YEAR);

    // Build candidates from the actual files present in S3
    List<EdgarFullIndexCache.IndexEntry> candidates = buildCandidatesFromS3(storage,
        govdataParquetDir);
    assumeTrue(!candidates.isEmpty(),
        "Could not build any candidates from S3 listing — check filename format");

    LOGGER.info("Running filterAndSelfHeal on {} candidates (year={}, form types: 10-K/10-Q)",
        candidates.size(), YEAR);

    List<EdgarFullIndexCache.IndexEntry> toProcess =
        cache.filterAndSelfHeal(candidates, false, 1);

    int skipCount = candidates.size() - toProcess.size();
    double skipRatio = candidates.size() > 0
        ? (double) skipCount / candidates.size() : 0.0;

    LOGGER.info("filterAndSelfHeal: candidates={} skipped={} toProcess={} skipRatio={}%",
        candidates.size(), skipCount, toProcess.size(),
        String.format("%.1f", skipRatio * 100));

    assertTrue(skipRatio >= 0.5,
        "Expected >=50% of candidates to be skipped by self-healing, but only "
            + String.format("%.1f", skipRatio * 100) + "% were skipped ("
            + skipCount + "/" + candidates.size() + "). "
            + "This means filterAndSelfHeal is not finding existing staging files. "
            + "Check that preloadFileInventory is scanning the correct path. "
            + "govdataParquetDir=" + govdataParquetDir);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static String getGovdataParquetDir() {
    // Production: sec-schema.yaml sets materializeDirectory="${GOVDATA_PARQUET_DIR}/source=sec".
    // FileSchemaBuilder resolves that value and passes it as govdataParquetDir to SecFilingCache.
    // So the value already contains /source=sec — we must do the same append here.
    String raw = System.getenv("GOVDATA_PARQUET_DIR");
    if (raw != null && !raw.isEmpty()) {
      // Strip any trailing slash before appending
      String base = raw.endsWith("/") ? raw.substring(0, raw.length() - 1) : raw;
      return base + "/source=sec";
    }
    return "s3://govdata-parquet-v1/source=sec";
  }

  private static StorageProvider buildStorageProvider() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    config.put("endpoint", System.getenv("AWS_ENDPOINT_OVERRIDE"));
    return new S3StorageProvider(config);
  }

  private static int countFiles(StorageProvider storage, String path) {
    try {
      List<StorageProvider.FileEntry> entries = storage.listFiles(path, true);
      int count = 0;
      for (StorageProvider.FileEntry e : entries) {
        if (!e.isDirectory()) {
          count++;
        }
      }
      LOGGER.debug("countFiles({}): {} files", path, count);
      return count;
    } catch (IOException e) {
      LOGGER.warn("countFiles({}): list failed — {}", path, e.getMessage());
      return 0;
    }
  }

  /**
   * Reads up to {@code limit} distinct (cik, accession_number) pairs from batch parquet files
   * in the year directory using DuckDB. Falls back to the legacy double-path if the primary
   * year directory has no batch files.
   */
  private static List<String[]> readCikAccessionPairs(StorageProvider storage,
      String govdataParquetDir, int limit) throws Exception {
    String yearDir = storage.resolvePath(govdataParquetDir, "year=" + YEAR);
    List<String[]> pairs = readCikAccessionPairsFromDir(storage, yearDir, limit);
    if (pairs.isEmpty()) {
      String legacyYearDir = storage.resolvePath(
          storage.resolvePath(govdataParquetDir, "source=sec"), "year=" + YEAR);
      pairs = readCikAccessionPairsFromDir(storage, legacyYearDir, limit);
    }
    return pairs;
  }

  // Query at most this many batch files per DuckDB call to avoid R2 SSL connection drops.
  private static final int MAX_BATCH_FILES_PER_QUERY = 5;

  private static List<String[]> readCikAccessionPairsFromDir(StorageProvider storage,
      String yearDir, int limit) throws Exception {
    List<StorageProvider.FileEntry> entries;
    try {
      entries = storage.listFiles(yearDir, true);
    } catch (IOException e) {
      LOGGER.warn("readCikAccessionPairsFromDir: list failed for {} — {}", yearDir, e.getMessage());
      return new ArrayList<String[]>();
    }
    List<String> batchPaths = new ArrayList<String>();
    for (StorageProvider.FileEntry e : entries) {
      if (!e.isDirectory()) {
        String path = e.getPath();
        int slash = path.lastIndexOf('/');
        String name = slash >= 0 ? path.substring(slash + 1) : path;
        if (BATCH_FILE_PATTERN.matcher(name).matches()) {
          batchPaths.add(path);
          if (batchPaths.size() >= MAX_BATCH_FILES_PER_QUERY) {
            break;
          }
        }
      }
    }
    if (batchPaths.isEmpty()) {
      return new ArrayList<String[]>();
    }
    StringBuilder pathList = new StringBuilder();
    for (int i = 0; i < batchPaths.size(); i++) {
      if (i > 0) {
        pathList.append(", ");
      }
      pathList.append("'").append(batchPaths.get(i)).append("'");
    }
    String sql = "SELECT DISTINCT cik, accession_number FROM read_parquet(["
        + pathList + "]) LIMIT " + limit;
    List<String[]> pairs = new ArrayList<String[]>();
    try (Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storage);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        pairs.add(new String[]{rs.getString("cik"), rs.getString("accession_number")});
      }
    }
    return pairs;
  }

  /**
   * Builds {@link EdgarFullIndexCache.IndexEntry} candidates by reading real (cik, accession)
   * pairs from a small sample of batch parquet files in S3 via DuckDB.
   * Limits queries to {@link #MAX_BATCH_FILES_PER_QUERY} files to avoid R2 SSL drops.
   */
  private static List<EdgarFullIndexCache.IndexEntry> buildCandidatesFromS3(
      StorageProvider storage, String govdataParquetDir) throws Exception {

    String yearDir = storage.resolvePath(govdataParquetDir, "year=" + YEAR);
    List<String[]> pairs;
    try {
      pairs = readCikAccessionPairsFromDir(storage, yearDir, 200);
    } catch (Exception e) {
      LOGGER.warn("buildCandidatesFromS3: primary path failed, trying legacy — {}", e.getMessage());
      String legacyYearDir = storage.resolvePath(
          storage.resolvePath(govdataParquetDir, "source=sec"), "year=" + YEAR);
      pairs = readCikAccessionPairsFromDir(storage, legacyYearDir, 200);
    }

    List<EdgarFullIndexCache.IndexEntry> candidates =
        new ArrayList<EdgarFullIndexCache.IndexEntry>();
    for (String[] pair : pairs) {
      candidates.add(new EdgarFullIndexCache.IndexEntry(
          "", "10-K", pair[0], YEAR + "-01-01", pair[1], YEAR, 1));
    }
    LOGGER.info("Built {} candidates from S3 batch files", candidates.size());
    return candidates;
  }

  private static boolean hasCredentials() {
    return isSet("AWS_ACCESS_KEY_ID")
        && isSet("AWS_SECRET_ACCESS_KEY")
        && isSet("AWS_ENDPOINT_OVERRIDE");
  }

  private static boolean isSet(String envVar) {
    String val = System.getenv(envVar);
    return val != null && !val.isEmpty();
  }
}
