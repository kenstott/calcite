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
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Non-circular integration test for both {@code filterAndSelfHeal} code paths using
 * real EDGAR index candidates (not derived from the same batch files being tested).
 *
 * <p>Path 1 (self-heal): Uses {@code PipelineTracker.NOOP_PIPELINE} so no tracker state
 * exists. {@link SecFilingCache#preloadFileInventory} must populate the cache from live S3
 * batch files. Once worker-02 has fully processed all 2025 10-K/10-Q/10-K/A/10-Q/A candidates
 * (including FY2024 filings submitted in early 2025), all must be skipped (100%).
 *
 * <p>Path 2 (tracker-based): Uses a real S3-backed tracker. After full processing, tracker
 * state must cause all candidates to be skipped (100%).
 *
 * <p>Candidates are sourced from the locally cached EDGAR full-index files at
 * {@code govdata/test-run/cache/sec/full-index/}, NOT from the same batch parquet files
 * being tested — making this test non-circular.
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}</li>
 *   <li>{@code AWS_SECRET_ACCESS_KEY}</li>
 *   <li>{@code AWS_ENDPOINT_OVERRIDE}</li>
 *   <li>{@code GOVDATA_PARQUET_DIR} — defaults to {@code s3://govdata-parquet-v1}</li>
 *   <li>{@code CALCITE_TRACKER_S3_BUCKET} — required for Path 2 tracker test</li>
 * </ul>
 */
@Tag("integration")
public class Worker02BothPathsIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(Worker02BothPathsIntegrationTest.class);

  private static final int YEAR = 2025;
  private static final List<String> FORM_TYPES = Arrays.asList("10-K", "10-Q", "10-K/A", "10-Q/A");

  // Minimum EDGAR index candidates to make the test meaningful
  private static final int MIN_CANDIDATES = 100;

  // All candidates must be skipped after worker-02 has fully processed all 2025 filings
  private static final double MIN_SKIP_RATIO = 1.0;

  // Limit tracker path test to a sample to avoid per-accession S3 calls for all 7000+ entries
  private static final int TRACKER_TEST_SAMPLE = 200;

  private static final String EDGAR_CACHE_PATH = resolveEdgarCachePath();

  private static String resolveEdgarCachePath() {
    // Gradle sets test working dir to {module}/build. Walk up until we find test-run/cache/sec.
    File dir = new File("").getAbsoluteFile();
    for (int i = 0; i < 4; i++) {
      File candidate = new File(dir, "test-run/cache/sec");
      if (candidate.exists()) {
        return candidate.getPath();
      }
      if (dir.getParentFile() != null) {
        dir = dir.getParentFile();
      }
    }
    // Last resort: return the sibling path from cwd
    return new File("").getAbsoluteFile().getParent() + "/test-run/cache/sec";
  }

  @BeforeAll
  static void checkCredentials() {
    assumeTrue(hasCredentials(),
        "Skipping: production S3 credentials not set "
            + "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_OVERRIDE)");
  }

  /**
   * Path 1: Self-heal code path.
   *
   * <p>Uses NOOP tracker so no tracker state exists. {@link SecFilingCache#preloadFileInventory}
   * loads batch parquet files from live S3. {@link SecFilingCache#filterAndSelfHeal} must
   * skip >=50% of real EDGAR index candidates because the batch files already exist.
   */
  @Test
  void selfHealPathSkipsMostCandidatesFromEdgarIndex() throws Exception {
    List<EdgarFullIndexCache.IndexEntry> candidates = loadEdgarIndexCandidates();
    assumeTrue(candidates.size() >= MIN_CANDIDATES,
        "Fewer than " + MIN_CANDIDATES + " EDGAR index candidates for year=" + YEAR
            + " — EDGAR index may not be cached at " + EDGAR_CACHE_PATH);

    String govdataParquetDir = getParquetBaseDir();
    StorageProvider storage = buildStorageProvider();

    SecFilingCache cache = new SecFilingCache(
        PipelineTracker.NOOP_PIPELINE, storage, govdataParquetDir);
    cache.preloadFileInventory(YEAR, YEAR);

    LOGGER.info("[Path1/SelfHeal] Calling filterAndSelfHeal on {} EDGAR candidates (NOOP tracker)",
        candidates.size());

    List<EdgarFullIndexCache.IndexEntry> toProcess =
        cache.filterAndSelfHeal(candidates, false, 1);

    int skipCount = candidates.size() - toProcess.size();
    double skipRatio = (double) skipCount / candidates.size();

    LOGGER.info("[Path1/SelfHeal] candidates={} skipped={} toProcess={} skipRatio={}%",
        candidates.size(), skipCount, toProcess.size(),
        String.format("%.1f", skipRatio * 100));

    assertTrue(skipRatio >= MIN_SKIP_RATIO,
        String.format(
            "[Path1/SelfHeal] Expected >=%.0f%% skip ratio but got %.1f%% (%d/%d skipped). "
                + "preloadFileInventory may not be finding batch parquet files in S3. "
                + "govdataParquetDir=%s",
            MIN_SKIP_RATIO * 100, skipRatio * 100, skipCount, candidates.size(),
            govdataParquetDir));
  }

  /**
   * Path 2: Tracker-based skip path.
   *
   * <p>Uses a real S3-backed tracker (state from actual worker runs). The tracker should
   * have processed state for the majority of 2025 10-K/10-Q filings. Verifies that the
   * tracker-based skip path is working alongside self-heal.
   *
   * <p>Skipped if {@code CALCITE_TRACKER_S3_BUCKET} is not set.
   */
  @Test
  void trackerPathSkipsMostCandidatesFromEdgarIndex() throws Exception {
    String trackerBucket = System.getenv("CALCITE_TRACKER_S3_BUCKET");
    assumeTrue(trackerBucket != null && !trackerBucket.isEmpty(),
        "Skipping Path 2: CALCITE_TRACKER_S3_BUCKET not set");

    List<EdgarFullIndexCache.IndexEntry> allCandidates = loadEdgarIndexCandidates();
    assumeTrue(allCandidates.size() >= MIN_CANDIDATES,
        "Fewer than " + MIN_CANDIDATES + " EDGAR index candidates — EDGAR index not cached");

    // Use a limited sample to avoid per-accession S3 tracker calls for all 7000+ entries
    List<EdgarFullIndexCache.IndexEntry> candidates =
        allCandidates.subList(0, Math.min(TRACKER_TEST_SAMPLE, allCandidates.size()));

    String govdataParquetDir = getParquetBaseDir();
    StorageProvider storage = buildStorageProvider();

    // Build a real tracker from S3
    PipelineTracker tracker = buildS3Tracker(trackerBucket);

    SecFilingCache cache = new SecFilingCache(tracker, storage, govdataParquetDir);
    cache.preloadFileInventory(YEAR, YEAR);

    // Bulk-preload tracker state for the sample accessions (mirrors SecSchemaFactory behavior)
    List<String> accessionNumbers = new java.util.ArrayList<String>();
    for (EdgarFullIndexCache.IndexEntry e : candidates) {
      accessionNumbers.add(e.accession);
    }
    cache.preload(accessionNumbers);

    LOGGER.info("[Path2/Tracker] Calling filterAndSelfHeal on {} EDGAR candidates (real tracker)",
        candidates.size());

    List<EdgarFullIndexCache.IndexEntry> toProcess =
        cache.filterAndSelfHeal(candidates, false, 1);

    int skipCount = candidates.size() - toProcess.size();
    double skipRatio = (double) skipCount / candidates.size();

    LOGGER.info("[Path2/Tracker] candidates={} skipped={} toProcess={} skipRatio={}%",
        candidates.size(), skipCount, toProcess.size(),
        String.format("%.1f", skipRatio * 100));

    assertTrue(skipRatio >= MIN_SKIP_RATIO,
        String.format(
            "[Path2/Tracker] Expected >=%.0f%% skip ratio but got %.1f%% (%d/%d skipped). "
                + "Tracker at %s may not have state for year=%d 10-K/10-Q filings.",
            MIN_SKIP_RATIO * 100, skipRatio * 100, skipCount, candidates.size(),
            trackerBucket, YEAR));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Loads real 2025 10-K/10-Q candidates from the locally cached EDGAR full-index.
   * Uses {@link LocalFileStorageProvider} to read from {@code govdata/test-run/cache/sec/}.
   * The quarterly index files (company.idx) are read directly — no network calls needed.
   */
  private static List<EdgarFullIndexCache.IndexEntry> loadEdgarIndexCandidates()
      throws Exception {
    File cacheDir = new File(EDGAR_CACHE_PATH);
    if (!cacheDir.exists()) {
      LOGGER.warn("EDGAR cache dir not found: {}", EDGAR_CACHE_PATH);
      return java.util.Collections.emptyList();
    }

    // LocalFileStorageProvider reads from the filesystem; cacheBasePath points
    // one level up from full-index/ so EdgarFullIndexCache can find
    // {cacheBasePath}/full-index/{year}/QTR{N}/company.idx
    StorageProvider localStorage = new LocalFileStorageProvider();
    // EdgarFullIndexCache expects cacheBasePath as the root containing "full-index/"
    // Our files are at govdata/test-run/cache/sec/full-index/2025/QTR1/company.idx
    // so cacheBasePath = EDGAR_CACHE_PATH (which is govdata/test-run/cache/sec)
    EdgarFullIndexCache indexCache = new EdgarFullIndexCache(
        localStorage, EDGAR_CACHE_PATH, YEAR, YEAR);

    List<EdgarFullIndexCache.IndexEntry> entries =
        indexCache.getActiveAccessions(YEAR, FORM_TYPES, null);

    LOGGER.info("Loaded {} candidates from local EDGAR index (year={}, formTypes={})",
        entries.size(), YEAR, FORM_TYPES);
    return entries;
  }

  private static String getParquetBaseDir() {
    String raw = System.getenv("GOVDATA_PARQUET_DIR");
    if (raw != null && !raw.isEmpty()) {
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

  /**
   * Builds a real S3-backed PipelineTracker from the tracker bucket env var.
   * Mirrors the construction in PipelineTrackerFactory.createS3Tracker().
   */
  private static PipelineTracker buildS3Tracker(String trackerBucket) {
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    Map<String, String> trackerConfig = new HashMap<String, String>();
    trackerConfig.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    trackerConfig.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    trackerConfig.put("endpoint", endpoint);
    return new S3HivePipelineTracker(trackerBucket, endpoint, trackerConfig);
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
