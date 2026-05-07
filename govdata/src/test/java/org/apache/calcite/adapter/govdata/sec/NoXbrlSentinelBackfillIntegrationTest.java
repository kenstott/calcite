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
 * One-time backfill integration test: writes no_xbrl sentinel parquet files to S3 for
 * all 2025 10-K/Q candidates that the tracker marks {@code _no_xbrl} but whose sentinel
 * file was never written due to the old {@code writeNoXbrlSentinel} bug.
 *
 * <p>Background: the original bug returned early when {@code filingDate} was empty,
 * so historical no_xbrl filings have no sentinel in S3. Without a sentinel the
 * self-heal path (NOOP tracker) cannot detect them and re-queues them unnecessarily.
 *
 * <p>After running this test once, {@link Worker02BothPathsIntegrationTest}
 * (Path 1/self-heal) should show a materially higher skip ratio for the no_xbrl
 * bucket.
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}</li>
 *   <li>{@code AWS_SECRET_ACCESS_KEY}</li>
 *   <li>{@code AWS_ENDPOINT_OVERRIDE}</li>
 *   <li>{@code GOVDATA_PARQUET_DIR} — defaults to {@code s3://govdata-parquet-v1}</li>
 *   <li>{@code CALCITE_TRACKER_S3_BUCKET} — required (tracker holds _no_xbrl state)</li>
 * </ul>
 */
@Tag("integration")
public class NoXbrlSentinelBackfillIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NoXbrlSentinelBackfillIntegrationTest.class);

  private static final int YEAR = 2025;
  private static final List<String> FORM_TYPES = Arrays.asList("10-K", "10-Q", "10-K/A", "10-Q/A");
  private static final int MIN_CANDIDATES = 100;

  private static final String EDGAR_CACHE_PATH = resolveEdgarCachePath();

  private static String resolveEdgarCachePath() {
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
    return new File("").getAbsoluteFile().getParent() + "/test-run/cache/sec";
  }

  @BeforeAll
  static void checkCredentials() {
    assumeTrue(hasCredentials(),
        "Skipping: production S3 credentials not set "
            + "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_OVERRIDE)");
  }

  /**
   * Backfills no_xbrl sentinels for all 2025 EDGAR candidates that are marked {@code _no_xbrl}
   * in the real tracker but have no sentinel parquet in S3.
   *
   * <p>This is a write operation against production S3 — it creates sentinel files.
   * It is idempotent: sentinels already in the S3 cache are skipped.
   */
  @Test
  void backfillNoXbrlSentinelsForYear2025() throws Exception {
    String trackerBucket = System.getenv("CALCITE_TRACKER_S3_BUCKET");
    assumeTrue(trackerBucket != null && !trackerBucket.isEmpty(),
        "Skipping: CALCITE_TRACKER_S3_BUCKET not set — cannot query tracker for _no_xbrl entries");

    List<EdgarFullIndexCache.IndexEntry> candidates = loadEdgarIndexCandidates();
    assumeTrue(candidates.size() >= MIN_CANDIDATES,
        "Fewer than " + MIN_CANDIDATES + " EDGAR index candidates for year=" + YEAR
            + " — EDGAR index may not be cached at " + EDGAR_CACHE_PATH);

    LOGGER.info("Loaded {} EDGAR 2025 candidates for backfill", candidates.size());

    String govdataParquetDir = getParquetBaseDir();
    StorageProvider storage = buildStorageProvider();
    S3HivePipelineTracker tracker = buildS3Tracker(trackerBucket);

    SecFilingCache cache = new SecFilingCache(tracker, storage, govdataParquetDir);

    // Preload S3 file inventory so backfill can skip sentinels already in S3
    cache.preloadFileInventory(YEAR, YEAR);

    LOGGER.info("Starting no_xbrl sentinel backfill for {} candidates ...", candidates.size());
    long start = System.currentTimeMillis();
    int written = cache.backfillNoXbrlSentinels(candidates);
    long elapsed = System.currentTimeMillis() - start;

    LOGGER.info("Backfill complete: {} sentinels written in {}ms", written, elapsed);

    // The test passes as long as backfill ran without errors.
    // If written > 0, the historical gap has been (partially) repaired.
    // If written == 0, either all sentinels already existed or there are no _no_xbrl filings.
    assertTrue(written >= 0,
        "backfillNoXbrlSentinels returned negative count — unexpected error");

    LOGGER.info("After backfill: re-run Worker02BothPathsIntegrationTest (Path 1/self-heal) "
        + "to verify the noFiles count has decreased by ~{}", written);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static List<EdgarFullIndexCache.IndexEntry> loadEdgarIndexCandidates()
      throws Exception {
    File cacheDir = new File(EDGAR_CACHE_PATH);
    if (!cacheDir.exists()) {
      LOGGER.warn("EDGAR cache dir not found: {}", EDGAR_CACHE_PATH);
      return java.util.Collections.emptyList();
    }
    StorageProvider localStorage = new LocalFileStorageProvider();
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
      return base + "/sec";
    }
    return "s3://govdata-parquet-v1/sec";
  }

  private static StorageProvider buildStorageProvider() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    config.put("endpoint", System.getenv("AWS_ENDPOINT_OVERRIDE"));
    return new S3StorageProvider(config);
  }

  private static S3HivePipelineTracker buildS3Tracker(String trackerBucket) {
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    Map<String, String> config = new HashMap<String, String>();
    config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    config.put("endpoint", endpoint);
    return new S3HivePipelineTracker(trackerBucket, endpoint, config);
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
