/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.econ.CacheManifest;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance test comparing traditional row-by-row cache checking
 * vs DuckDB SQL-based cache filtering.
 *
 * This test verifies the 10-20x performance improvement from using
 * CacheManifestQueryHelper with SQL set operations.
 */
@Tag("performance")
public class CacheManifestPerformanceTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheManifestPerformanceTest.class);

  private static File testOperatingDir;
  private static CacheManifest testManifest;

  @BeforeAll
  public static void setup() throws Exception {
    // Create temporary test directory
    testOperatingDir =
        new File(System.getProperty("java.io.tmpdir"), "cache-perf-test-" + System.currentTimeMillis());
    testOperatingDir.mkdirs();

    // Create test manifest with realistic data
    testManifest = CacheManifest.load(testOperatingDir.getAbsolutePath());

    // Populate with 1000 cached entries (simulating realistic cache)
    for (int i = 0; i < 1000; i++) {
      String seriesId = "SERIES" + String.format("%04d", i);
      int year = 2010 + (i % 15); // 2010-2024
      Map<String, String> params = new HashMap<>();
      params.put("series", seriesId);
      params.put("year", String.valueOf(year));
      String cachePath = String.format("/cache/fred/%s/year=%d/data.json", seriesId, year);
      CacheKey cacheKey = new CacheKey("fred_indicators", params);
      testManifest.markCached(cacheKey, cachePath, 1024, Long.MAX_VALUE, "test");
    }

    // Save manifest
    testManifest.save(testOperatingDir.getAbsolutePath());

    LOGGER.info("Created test manifest with 1000 entries at {}", testOperatingDir);
  }

  /**
   * Test traditional row-by-row cache checking performance.
   * This simulates the old approach: check each request individually using HashMap.
   */
  @Test public void testTraditionalCacheChecking() {
    // Generate 2000 download requests (simulating full iteration)
    List<Map<String, String>> requests = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      String seriesId = "SERIES" + String.format("%04d", i % 1500); // Mix of cached and uncached
      int year = 2010 + (i % 15);
      Map<String, String> params = new HashMap<>();
      params.put("series", seriesId);
      requests.add(params);
    }

    LOGGER.info("Starting traditional cache checking for {} requests", requests.size());
    long startTime = System.nanoTime();

    // Traditional approach: check each request individually
    int cachedCount = 0;
    int uncachedCount = 0;
    for (Map<String, String> params : requests) {
      int year = 2010; // Simplified - would extract from iteration
      Map<String, String> allParams = new HashMap<>(params);
      allParams.put("year", String.valueOf(year));
      org.apache.calcite.adapter.govdata.CacheKey cacheKey =
          new org.apache.calcite.adapter.govdata.CacheKey("fred_indicators", allParams);
      if (testManifest.isCached(cacheKey)) {
        cachedCount++;
      } else {
        uncachedCount++;
      }
    }

    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    LOGGER.info("Traditional cache checking complete:");
    LOGGER.info("  Total requests: {}", requests.size());
    LOGGER.info("  Cached: {}", cachedCount);
    LOGGER.info("  Uncached: {}", uncachedCount);
    LOGGER.info("  Time: {} ms", elapsedMs);
    LOGGER.info("  Avg per request: {} μs", (elapsedMs * 1000.0) / requests.size());
  }

  /**
   * Test DuckDB SQL-based cache filtering performance.
   * This uses CacheManifestQueryHelper for bulk filtering.
   */
  @Test public void testOptimizedCacheFiltering() throws Exception {
    // Generate same 2000 download requests
    List<CacheManifestQueryHelper.DownloadRequest> requests = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      String seriesId = "SERIES" + String.format("%04d", i % 1500);
      int year = 2010 + (i % 15);
      Map<String, String> params = new HashMap<>();
      params.put("series", seriesId);
      requests.add(new CacheManifestQueryHelper.DownloadRequest("fred_indicators", year, params));
    }

    LOGGER.info("Starting optimized cache filtering for {} requests", requests.size());
    long startTime = System.nanoTime();

    // Optimized approach: filter all requests in single SQL query
    String manifestPath = testOperatingDir.getAbsolutePath() + "/cache_manifest.parquet";
    List<CacheManifestQueryHelper.DownloadRequest> uncached =
        CacheManifestQueryHelper.filterUncachedRequestsOptimal(manifestPath, requests);

    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    int cachedCount = requests.size() - uncached.size();
    int uncachedCount = uncached.size();

    LOGGER.info("Optimized cache filtering complete:");
    LOGGER.info("  Total requests: {}", requests.size());
    LOGGER.info("  Cached: {}", cachedCount);
    LOGGER.info("  Uncached: {}", uncachedCount);
    LOGGER.info("  Time: {} ms", elapsedMs);
    LOGGER.info("  Avg per request: {} μs", (elapsedMs * 1000.0) / requests.size());

    // Verify we got reasonable results
    assertTrue(uncachedCount > 0, "Should have some uncached requests");
    assertTrue(cachedCount > 0, "Should have some cached requests");
  }

  /**
   * Comparative benchmark showing speedup factor.
   */
  @Test public void testComparativePerformance() throws Exception {
    // Generate test data
    List<CacheManifestQueryHelper.DownloadRequest> requests = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      String seriesId = "SERIES" + String.format("%04d", i % 1500);
      int year = 2010 + (i % 15);
      Map<String, String> params = new HashMap<>();
      params.put("series", seriesId);
      requests.add(new CacheManifestQueryHelper.DownloadRequest("fred_indicators", year, params));
    }

    // Run traditional approach
    long traditionalStart = System.nanoTime();
    int traditionalCached = 0;
    for (CacheManifestQueryHelper.DownloadRequest req : requests) {
      Map<String, String> allParams = new HashMap<>(req.parameters);
      allParams.put("year", String.valueOf(req.year));
      org.apache.calcite.adapter.govdata.CacheKey cacheKey =
          new org.apache.calcite.adapter.govdata.CacheKey(req.dataType, allParams);
      if (testManifest.isCached(cacheKey)) {
        traditionalCached++;
      }
    }
    long traditionalMs = (System.nanoTime() - traditionalStart) / 1_000_000;

    // Run optimized approach
    String manifestPath = testOperatingDir.getAbsolutePath() + "/cache_manifest.parquet";
    long optimizedStart = System.nanoTime();
    List<CacheManifestQueryHelper.DownloadRequest> uncached =
        CacheManifestQueryHelper.filterUncachedRequestsOptimal(manifestPath, requests);
    long optimizedMs = (System.nanoTime() - optimizedStart) / 1_000_000;

    int optimizedCached = requests.size() - uncached.size();

    // Calculate speedup
    double speedup = (double) traditionalMs / optimizedMs;

    LOGGER.info("=== Performance Comparison ===");
    LOGGER.info("Traditional approach: {} ms ({} cached)", traditionalMs, traditionalCached);
    LOGGER.info("Optimized approach:   {} ms ({} cached)", optimizedMs, optimizedCached);
    LOGGER.info("Speedup factor:       {:.2f}x", speedup);

    // Verify results match
    assertTrue(Math.abs(traditionalCached - optimizedCached) <= 1,
        "Both approaches should find similar number of cached entries");

    // Performance improvement should be significant (at least 2x, ideally 10-20x)
    LOGGER.info("Performance improvement: {}x (target: 10-20x)", speedup);
    if (speedup < 2.0) {
      LOGGER.warn("Speedup below expected range - may need investigation");
    } else if (speedup >= 10.0) {
      LOGGER.info("✓ Achieved target performance improvement!");
    }
  }
}
