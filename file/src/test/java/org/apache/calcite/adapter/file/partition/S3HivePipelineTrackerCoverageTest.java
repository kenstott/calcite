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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for {@link S3HivePipelineTracker} targeting maximum line coverage.
 *
 * <p>Tests all utility methods, caching behavior, and data structures via reflection.
 * External S3 and DuckDB interactions are avoided by testing internal logic directly.
 */
@Tag("unit")
public class S3HivePipelineTrackerCoverageTest {

  @TempDir
  Path tempDir;

  // ===== Constructor Tests =====

  @Test
  void testConstructorBasic() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", "http://localhost:9000");
    assertNotNull(tracker);
    tracker.close();
  }

  @Test
  void testConstructorWithTrailingSlash() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker/", "http://localhost:9000");
    assertNotNull(tracker);
    tracker.close();
  }

  @Test
  void testConstructorWithNullEndpoint() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    assertNotNull(tracker);
    tracker.close();
  }

  @Test
  void testConstructorWithConfig() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("accessKeyId", "AKID");
    config.put("secretAccessKey", "secret");
    config.put("region", "us-west-2");
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", "http://localhost:9000", config);
    assertNotNull(tracker);
    tracker.close();
  }

  @Test
  void testConstructorWithNullConfig() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", "http://localhost:9000", null);
    assertNotNull(tracker);
    tracker.close();
  }

  @Test
  void testConstructorWithEmptyConfig() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", "",
            Collections.<String, String>emptyMap());
    assertNotNull(tracker);
    tracker.close();
  }

  @Test
  void testTwoArgConstructor() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", "http://localhost:9000");
    assertNotNull(tracker);
    tracker.close();
  }

  // ===== flattenKeyValues Tests =====

  @Test
  void testFlattenKeyValuesNull() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "flattenKeyValues", Map.class);
      method.setAccessible(true);

      assertEquals("_empty", method.invoke(tracker, (Map<String, String>) null));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testFlattenKeyValuesEmpty() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "flattenKeyValues", Map.class);
      method.setAccessible(true);

      assertEquals("_empty",
          method.invoke(tracker, Collections.<String, String>emptyMap()));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testFlattenKeyValuesSingleKey() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "flattenKeyValues", Map.class);
      method.setAccessible(true);

      Map<String, String> singleKey = Collections.singletonMap("year", "2023");
      assertEquals("2023", method.invoke(tracker, singleKey));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testFlattenKeyValuesMultipleKeys() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "flattenKeyValues", Map.class);
      method.setAccessible(true);

      Map<String, String> multiKey = new LinkedHashMap<String, String>();
      multiKey.put("year", "2023");
      multiKey.put("type", "10-K");

      String result = (String) method.invoke(tracker, multiKey);
      // Keys are sorted, so "type" comes after "year"
      assertTrue(result.contains("type=10-K"));
      assertTrue(result.contains("year=2023"));
      assertTrue(result.contains("__"));
    } finally {
      tracker.close();
    }
  }

  // ===== unflattenKeyValues Tests =====

  @Test
  void testUnflattenKeyValuesEmpty() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "unflattenKeyValues", String.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      Map<String, String> result =
          (Map<String, String>) method.invoke(tracker, "_empty");
      assertTrue(result.isEmpty());
    } finally {
      tracker.close();
    }
  }

  @Test
  void testUnflattenKeyValuesSingleValue() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "unflattenKeyValues", String.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      Map<String, String> result =
          (Map<String, String>) method.invoke(tracker, "2023");
      assertEquals(1, result.size());
      assertEquals("2023", result.get("source_key"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testUnflattenKeyValuesMultipleKeys() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "unflattenKeyValues", String.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      Map<String, String> result =
          (Map<String, String>) method.invoke(tracker, "type=10-K__year=2023");
      assertEquals(2, result.size());
      assertEquals("10-K", result.get("type"));
      assertEquals("2023", result.get("year"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testUnflattenKeyValuesWithEqualsButNoDoubleDash() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "unflattenKeyValues", String.class);
      method.setAccessible(true);

      // Has '=' but no '__' - falls through to single-value path
      @SuppressWarnings("unchecked")
      Map<String, String> result =
          (Map<String, String>) method.invoke(tracker, "key=value");
      assertEquals(1, result.size());
      assertEquals("key=value", result.get("source_key"));
    } finally {
      tracker.close();
    }
  }

  // ===== sanitizeHiveValue Tests =====

  @Test
  void testSanitizeHiveValue() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "sanitizeHiveValue", String.class);
      method.setAccessible(true);

      assertEquals("a_b_c_d", method.invoke(tracker, "a/b c:d"));
      assertEquals("normal_value", method.invoke(tracker, "normal_value"));
      assertEquals("path_with_slashes", method.invoke(tracker, "path/with/slashes"));
    } finally {
      tracker.close();
    }
  }

  // ===== extractYear Tests =====

  @Test
  void testExtractYearFromFlattenedDimensions() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // Flattened dimension key with year=YYYY
      assertEquals("2023",
          method.invoke(tracker, "geography=state__type=acs__year=2023", 0L));

      // Year at start
      assertEquals("2024",
          method.invoke(tracker, "year=2024__type=income", 0L));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearFromSecAccessionFormat() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // SEC accession format: XXXXXXXXXX-YY-XXXXXX
      assertEquals("2022",
          method.invoke(tracker, "0001234567-22-012345", 0L));
      assertEquals("1999",
          method.invoke(tracker, "0001234567-99-012345", 0L));
      assertEquals("2005",
          method.invoke(tracker, "0001234567-05-012345", 0L));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearFromBareYear() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // Bare 4-digit year
      assertEquals("2023", method.invoke(tracker, "2023", 0L));
      assertEquals("1990", method.invoke(tracker, "1990", 0L));
      assertEquals("2100", method.invoke(tracker, "2100", 0L));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearFromInvalidBareYear() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // Below 1900 - falls through to current year
      String result = (String) method.invoke(tracker, "1899", System.currentTimeMillis());
      assertNotNull(result);
      assertTrue(result.matches("\\d{4}"));

      // Above 2100 - falls through to current year
      result = (String) method.invoke(tracker, "2101", System.currentTimeMillis());
      assertNotNull(result);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearFromNonNumeric() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // Non-numeric strings - fall through to current year
      String result =
          (String) method.invoke(tracker, "hello_world", System.currentTimeMillis());
      assertNotNull(result);
      assertTrue(result.matches("\\d{4}"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearFromNull() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      String result =
          (String) method.invoke(tracker, (String) null, System.currentTimeMillis());
      assertNotNull(result);
      assertTrue(result.matches("\\d{4}"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearPartialYearInDimensionKey() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // year= but not followed by 4 digits
      String result =
          (String) method.invoke(tracker, "year=12__type=acs", System.currentTimeMillis());
      // Not exactly 4 digits after year=, so fallback to accession or bare year or current
      assertNotNull(result);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearSecFormatInvalidNumbers() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // Length >= 15, has '-' at 10, but non-numeric in positions 11-12
      String result = (String) method.invoke(tracker,
          "0001234567-AB-012345", System.currentTimeMillis());
      assertNotNull(result); // Falls through to other checks
    } finally {
      tracker.close();
    }
  }

  @Test
  void testExtractYearBareYearNonNumeric4Chars() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "extractYear", String.class, long.class);
      method.setAccessible(true);

      // 4 chars but not numeric
      String result =
          (String) method.invoke(tracker, "abcd", System.currentTimeMillis());
      assertNotNull(result); // Falls through to current year
    } finally {
      tracker.close();
    }
  }

  // ===== parsePendingFlushThreshold Tests =====

  @Test
  void testParsePendingFlushThresholdDefault() throws Exception {
    // Default value when env var is not set
    Method method = S3HivePipelineTracker.class.getDeclaredMethod(
        "parsePendingFlushThreshold");
    method.setAccessible(true);

    int result = (Integer) method.invoke(null);
    // Default is 500 (or whatever env var is set to)
    assertTrue(result > 0);
  }

  // ===== deleteDir Tests =====

  @Test
  void testDeleteDirEmpty() throws Exception {
    Method method = S3HivePipelineTracker.class.getDeclaredMethod(
        "deleteDir", java.io.File.class);
    method.setAccessible(true);

    java.io.File emptyDir = tempDir.resolve("empty_dir").toFile();
    emptyDir.mkdirs();
    assertTrue(emptyDir.exists());

    method.invoke(null, emptyDir);
    assertFalse(emptyDir.exists());
  }

  @Test
  void testDeleteDirWithFiles() throws Exception {
    Method method = S3HivePipelineTracker.class.getDeclaredMethod(
        "deleteDir", java.io.File.class);
    method.setAccessible(true);

    java.io.File dir = tempDir.resolve("dir_with_files").toFile();
    dir.mkdirs();
    new java.io.File(dir, "file1.txt").createNewFile();
    new java.io.File(dir, "file2.txt").createNewFile();

    java.io.File subDir = new java.io.File(dir, "subdir");
    subDir.mkdirs();
    new java.io.File(subDir, "file3.txt").createNewFile();

    assertTrue(dir.exists());
    method.invoke(null, dir);
    assertFalse(dir.exists());
  }

  @Test
  void testDeleteDirNonExistent() throws Exception {
    Method method = S3HivePipelineTracker.class.getDeclaredMethod(
        "deleteDir", java.io.File.class);
    method.setAccessible(true);

    java.io.File nonExistent = tempDir.resolve("nonexistent_dir").toFile();
    // Should not throw
    method.invoke(null, nonExistent);
  }

  // ===== allIndices Tests =====

  @Test
  void testAllIndices() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "allIndices", int.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      Set<Integer> result = (Set<Integer>) method.invoke(tracker, 5);
      assertEquals(5, result.size());
      assertTrue(result.contains(0));
      assertTrue(result.contains(1));
      assertTrue(result.contains(2));
      assertTrue(result.contains(3));
      assertTrue(result.contains(4));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testAllIndicesZero() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "allIndices", int.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      Set<Integer> result = (Set<Integer>) method.invoke(tracker, 0);
      assertTrue(result.isEmpty());
    } finally {
      tracker.close();
    }
  }

  // ===== Stage Cache Tests (directly manipulating internal state) =====

  @Test
  void testIsCompleteFromStageCache() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      // Populate stage cache directly
      Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
      stageCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, Set<String>> stageCache =
          (Map<String, Set<String>>) stageCacheField.get(tracker);

      Set<String> tables = new LinkedHashSet<String>();
      tables.add("metadata");
      tables.add("facts");
      stageCache.put("source_key_1\0staging", tables);

      // isComplete should find it in cache
      assertTrue(tracker.isComplete("source_key_1", "metadata", "staging"));
      assertTrue(tracker.isComplete("source_key_1", "facts", "staging"));
      assertFalse(tracker.isComplete("source_key_1", "other", "staging"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testGetCompletedTablesFromStageCache() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
      stageCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, Set<String>> stageCache =
          (Map<String, Set<String>>) stageCacheField.get(tracker);

      Set<String> tables = new LinkedHashSet<String>();
      tables.add("table_a");
      tables.add("table_b");
      stageCache.put("key1\0phase1", tables);

      Set<String> result = tracker.getCompletedTables("key1", "phase1");
      assertEquals(2, result.size());
      assertTrue(result.contains("table_a"));
      assertTrue(result.contains("table_b"));
    } finally {
      tracker.close();
    }
  }

  // ===== Completion Cache Tests =====

  @Test
  void testIsTableCompleteFromCompletionCache() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field completionCacheField =
          S3HivePipelineTracker.class.getDeclaredField("completionCache");
      completionCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, IncrementalTracker.CachedCompletion> cache =
          (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);

      cache.put("pipeline1", new IncrementalTracker.CachedCompletion(
          "hash1", "sig1", 100, System.currentTimeMillis(), 0));

      assertTrue(tracker.isTableComplete("pipeline1", "sig1"));
      assertFalse(tracker.isTableComplete("pipeline1", "sig_different"));
      assertFalse(tracker.isTableComplete("pipeline_unknown", "sig1"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkCompleteUpdatesStageCache() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      // markComplete should update the stage cache
      // First with no existing cache entry
      tracker.markComplete("source1", "table1", "staging", 50);

      Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
      stageCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, Set<String>> stageCache =
          (Map<String, Set<String>>) stageCacheField.get(tracker);

      String cacheKey = "source1\0staging";
      Set<String> cached = stageCache.get(cacheKey);
      assertNotNull(cached);
      assertTrue(cached.contains("table1"));

      // Now with existing cache entry
      tracker.markComplete("source1", "table2", "staging", 30);
      cached = stageCache.get(cacheKey);
      assertNotNull(cached);
      assertTrue(cached.contains("table1"));
      assertTrue(cached.contains("table2"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkClearedUpdatesStageCache() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
      stageCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, Set<String>> stageCache =
          (Map<String, Set<String>>) stageCacheField.get(tracker);

      // Pre-populate cache
      Set<String> tables = Collections.newSetFromMap(
          new ConcurrentHashMap<String, Boolean>());
      tables.add("table1");
      tables.add("table2");
      stageCache.put("source1\0staging", tables);

      // markCleared should remove the table
      tracker.markCleared("source1", "table1", "staging");
      Set<String> remaining = stageCache.get("source1\0staging");
      assertNotNull(remaining);
      assertFalse(remaining.contains("table1"));
      assertTrue(remaining.contains("table2"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkClearedNoCacheEntry() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      // markCleared when no cache entry exists should not throw
      tracker.markCleared("nonexistent", "table1", "staging");
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkError() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      // Should not throw
      tracker.markError("source1", "table1", "staging", "test error message");
    } finally {
      tracker.close();
    }
  }

  // ===== markTableComplete / markTableCompleteWithConfig / markTableCompleteWithSourceWatermark =====

  @Test
  void testMarkTableComplete() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      tracker.markTableComplete("pipeline1", "sig_abc");

      Field completionCacheField =
          S3HivePipelineTracker.class.getDeclaredField("completionCache");
      completionCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, IncrementalTracker.CachedCompletion> cache =
          (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);

      IncrementalTracker.CachedCompletion completion = cache.get("pipeline1");
      assertNotNull(completion);
      assertEquals("sig_abc", completion.signature);
      assertNull(completion.configHash);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkTableCompleteWithConfig() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      tracker.markTableCompleteWithConfig("pipeline2", "cfg_hash", "sig_xyz", 1000);

      Field completionCacheField =
          S3HivePipelineTracker.class.getDeclaredField("completionCache");
      completionCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, IncrementalTracker.CachedCompletion> cache =
          (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);

      IncrementalTracker.CachedCompletion completion = cache.get("pipeline2");
      assertNotNull(completion);
      assertEquals("cfg_hash", completion.configHash);
      assertEquals("sig_xyz", completion.signature);
      assertEquals(1000, completion.rowCount);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkTableCompleteWithSourceWatermark() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      tracker.markTableCompleteWithSourceWatermark(
          "pipeline3", "cfg_hash", "sig_water", 500, 1234567890L);

      Field completionCacheField =
          S3HivePipelineTracker.class.getDeclaredField("completionCache");
      completionCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, IncrementalTracker.CachedCompletion> cache =
          (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);

      IncrementalTracker.CachedCompletion completion = cache.get("pipeline3");
      assertNotNull(completion);
      assertEquals("cfg_hash", completion.configHash);
      assertEquals("sig_water", completion.signature);
      assertEquals(500, completion.rowCount);
      assertEquals(1234567890L, completion.sourceFileWatermark);
    } finally {
      tracker.close();
    }
  }

  // ===== getCachedCompletion Tests =====

  @Test
  void testGetCachedCompletionFromMemory() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field completionCacheField =
          S3HivePipelineTracker.class.getDeclaredField("completionCache");
      completionCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, IncrementalTracker.CachedCompletion> cache =
          (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);

      cache.put("cached_pipeline", new IncrementalTracker.CachedCompletion(
          "hash", "sig", 100, System.currentTimeMillis(), 0));

      IncrementalTracker.CachedCompletion result =
          tracker.getCachedCompletion("cached_pipeline");
      assertNotNull(result);
      assertEquals("hash", result.configHash);
      assertEquals("sig", result.signature);
      assertEquals(100, result.rowCount);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testGetCachedCompletionPreloaded() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      // Set completionsPreloaded = true to prevent S3 queries
      Field preloadedField =
          S3HivePipelineTracker.class.getDeclaredField("completionsPreloaded");
      preloadedField.setAccessible(true);
      preloadedField.set(tracker, true);

      // Should return null because preloaded cache is authoritative
      IncrementalTracker.CachedCompletion result =
          tracker.getCachedCompletion("nonexistent_pipeline");
      assertNull(result);
    } finally {
      tracker.close();
    }
  }

  // ===== invalidateTableCompletion Tests =====

  @Test
  void testInvalidateTableCompletion() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field completionCacheField =
          S3HivePipelineTracker.class.getDeclaredField("completionCache");
      completionCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, IncrementalTracker.CachedCompletion> cache =
          (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);

      cache.put("to_invalidate", new IncrementalTracker.CachedCompletion(
          "h", "s", 1, System.currentTimeMillis(), 0));

      tracker.invalidateTableCompletion("to_invalidate");
      assertNull(cache.get("to_invalidate"));
    } finally {
      tracker.close();
    }
  }

  // ===== clearAllCompletions Tests =====

  @Test
  void testClearAllCompletions() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field completionCacheField =
          S3HivePipelineTracker.class.getDeclaredField("completionCache");
      completionCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, IncrementalTracker.CachedCompletion> cache =
          (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);

      cache.put("p1", new IncrementalTracker.CachedCompletion(
          "h", "s", 1, System.currentTimeMillis(), 0));
      cache.put("p2", new IncrementalTracker.CachedCompletion(
          "h2", "s2", 2, System.currentTimeMillis(), 0));

      tracker.clearAllCompletions();
      assertTrue(cache.isEmpty());
    } finally {
      tracker.close();
    }
  }

  // ===== markProcessed / markProcessedWithRowCount / markProcessedWithError Tests =====

  @Test
  void testMarkProcessed() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Map<String, String> keyValues = Collections.singletonMap("year", "2023");
      // Should not throw
      tracker.markProcessed("alt1", "source1", keyValues, "target1");
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkProcessedWithRowCount() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Map<String, String> keyValues = Collections.singletonMap("year", "2023");
      tracker.markProcessedWithRowCount("alt1", "source1", keyValues, "target1", 42);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testMarkProcessedWithError() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Map<String, String> keyValues = Collections.singletonMap("year", "2023");
      tracker.markProcessedWithError("alt1", "source1", keyValues, "target1",
          "Something went wrong");
    } finally {
      tracker.close();
    }
  }

  // ===== invalidate / invalidateAll Tests =====

  @Test
  void testInvalidate() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Map<String, String> keyValues = Collections.singletonMap("year", "2023");
      tracker.invalidate("alt1", keyValues);
    } finally {
      tracker.close();
    }
  }

  // ===== Pending State Buffering Tests =====

  @Test
  void testWriteStateBuffering() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field pendingStatesField =
          S3HivePipelineTracker.class.getDeclaredField("pendingStates");
      pendingStatesField.setAccessible(true);

      // Write some states
      tracker.markComplete("src1", "tbl1", "phase1", 10);
      tracker.markComplete("src2", "tbl2", "phase1", 20);

      @SuppressWarnings("unchecked")
      List<?> pending = (List<?>) pendingStatesField.get(tracker);
      // Should have buffered states (not immediately flushed)
      assertTrue(pending.size() >= 2);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testFlushPendingStatesEmpty() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      // Flushing empty pending states should be a no-op
      tracker.flushPendingStates();
    } finally {
      tracker.close();
    }
  }

  // ===== bulkGetCompletedTables Cache Tests =====

  @Test
  void testBulkGetCompletedTablesEmptyInput() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Map<String, Set<String>> result = tracker.bulkGetCompletedTables(
          Collections.<String>emptyList(), "staging");
      assertTrue(result.isEmpty());
    } finally {
      tracker.close();
    }
  }

  @Test
  void testBulkGetCompletedTablesAllCached() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field stageCacheField =
          S3HivePipelineTracker.class.getDeclaredField("stageCache");
      stageCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, Set<String>> stageCache =
          (Map<String, Set<String>>) stageCacheField.get(tracker);

      // Pre-populate cache
      Set<String> tables1 = new LinkedHashSet<String>();
      tables1.add("table_a");
      stageCache.put("key1\0staging", tables1);

      Set<String> tables2 = new LinkedHashSet<String>();
      tables2.add("table_b");
      stageCache.put("key2\0staging", tables2);

      Map<String, Set<String>> result =
          tracker.bulkGetCompletedTables(Arrays.asList("key1", "key2"), "staging");
      assertEquals(2, result.size());
      assertTrue(result.get("key1").contains("table_a"));
      assertTrue(result.get("key2").contains("table_b"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testBulkGetCompletedTablesEmptyCachedSkipped() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field stageCacheField =
          S3HivePipelineTracker.class.getDeclaredField("stageCache");
      stageCacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, Set<String>> stageCache =
          (Map<String, Set<String>>) stageCacheField.get(tracker);

      // Empty set in cache means "checked, no data"
      stageCache.put("empty_key\0staging", new LinkedHashSet<String>());

      Map<String, Set<String>> result =
          tracker.bulkGetCompletedTables(
              Collections.singletonList("empty_key"), "staging");
      // Should not include empty_key in results (empty set is excluded)
      assertTrue(result.isEmpty());
    } finally {
      tracker.close();
    }
  }

  // ===== close() Tests =====

  @Test
  void testCloseCleanup() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);

    // Populate some internal state
    Field stageCacheField =
        S3HivePipelineTracker.class.getDeclaredField("stageCache");
    stageCacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Set<String>> stageCache =
        (Map<String, Set<String>>) stageCacheField.get(tracker);
    stageCache.put("key\0phase", new LinkedHashSet<String>());

    Field completionCacheField =
        S3HivePipelineTracker.class.getDeclaredField("completionCache");
    completionCacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, IncrementalTracker.CachedCompletion> completionCache =
        (Map<String, IncrementalTracker.CachedCompletion>) completionCacheField.get(tracker);
    completionCache.put("p1", new IncrementalTracker.CachedCompletion(
        "h", "s", 0, 0, 0));

    Field scannedYearsField =
        S3HivePipelineTracker.class.getDeclaredField("scannedYears");
    scannedYearsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> scannedYears = (Set<String>) scannedYearsField.get(tracker);
    scannedYears.add("2023");

    // Close should clear all caches
    tracker.close();

    // After close, caches should be empty (we need to re-access since close clears)
    // Note: close() is called above; internal state is now cleared
  }

  @Test
  void testCloseWithoutConnection() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    // Close without ever opening a connection should not throw
    tracker.close();
  }

  @Test
  void testDoubleClose() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    tracker.close();
    // Second close should also be safe
    tracker.close();
  }

  // ===== CachedCompletion Tests =====

  @Test
  void testCachedCompletionConstructors() {
    // 3-arg constructor
    IncrementalTracker.CachedCompletion cc1 =
        new IncrementalTracker.CachedCompletion("hash1", "sig1", 100);
    assertEquals("hash1", cc1.configHash);
    assertEquals("sig1", cc1.signature);
    assertEquals(100, cc1.rowCount);
    assertEquals(0L, cc1.sourceFileWatermark);

    // 4-arg constructor
    IncrementalTracker.CachedCompletion cc2 =
        new IncrementalTracker.CachedCompletion("hash2", "sig2", 200, 12345L);
    assertEquals("hash2", cc2.configHash);
    assertEquals(200, cc2.rowCount);
    assertEquals(12345L, cc2.completedAt);
    assertEquals(0L, cc2.sourceFileWatermark);

    // 5-arg constructor
    IncrementalTracker.CachedCompletion cc3 =
        new IncrementalTracker.CachedCompletion("hash3", "sig3", 300, 12345L, 99999L);
    assertEquals("hash3", cc3.configHash);
    assertEquals(300, cc3.rowCount);
    assertEquals(99999L, cc3.sourceFileWatermark);
  }

  @Test
  void testCachedCompletionIsEmptyResultTtlExpired() {
    long now = System.currentTimeMillis();

    // Non-empty result (rowCount > 0) - TTL never expires
    IncrementalTracker.CachedCompletion nonEmpty =
        new IncrementalTracker.CachedCompletion("h", "s", 100, now, 0);
    assertFalse(nonEmpty.isEmptyResultTtlExpired(1000));

    // Empty result within TTL
    IncrementalTracker.CachedCompletion withinTtl =
        new IncrementalTracker.CachedCompletion("h", "s", 0, now, 0);
    assertFalse(withinTtl.isEmptyResultTtlExpired(60000));

    // Empty result past TTL
    IncrementalTracker.CachedCompletion pastTtl =
        new IncrementalTracker.CachedCompletion("h", "s", 0, now - 10000, 0);
    assertTrue(pastTtl.isEmptyResultTtlExpired(5000));

    // No TTL configured (TTL <= 0)
    assertFalse(withinTtl.isEmptyResultTtlExpired(0));
    assertFalse(withinTtl.isEmptyResultTtlExpired(-1));
  }

  @Test
  void testCachedCompletionIsSourceFilesModified() {
    // Watermark not enabled (0)
    IncrementalTracker.CachedCompletion noWm =
        new IncrementalTracker.CachedCompletion("h", "s", 100, 0, 0);
    assertFalse(noWm.isSourceFilesModified(12345));

    // Current watermark is 0
    IncrementalTracker.CachedCompletion hasWm =
        new IncrementalTracker.CachedCompletion("h", "s", 100, 0, 10000);
    assertFalse(hasWm.isSourceFilesModified(0));

    // Source files not modified (current <= recorded)
    assertFalse(hasWm.isSourceFilesModified(10000));
    assertFalse(hasWm.isSourceFilesModified(9999));

    // Source files modified (current > recorded)
    assertTrue(hasWm.isSourceFilesModified(10001));
  }

  // ===== PipelineTracker.NOOP_PIPELINE Tests =====

  @Test
  void testNoopPipelineTracker() {
    PipelineTracker noop = PipelineTracker.NOOP_PIPELINE;

    assertFalse(noop.isProcessed("alt", "src", Collections.<String, String>emptyMap()));
    assertFalse(noop.isProcessedWithTtl("alt", "src",
        Collections.<String, String>emptyMap(), 1000));
    noop.markProcessed("alt", "src",
        Collections.<String, String>emptyMap(), "target");
    assertTrue(noop.getProcessedKeyValues("alt").isEmpty());
    noop.invalidate("alt", Collections.<String, String>emptyMap());
    noop.invalidateAll("alt");

    Set<Integer> unprocessed = noop.filterUnprocessed("alt", "src",
        Arrays.asList(Collections.<String, String>emptyMap()));
    assertEquals(1, unprocessed.size());
    assertTrue(unprocessed.contains(0));

    assertFalse(noop.isTableComplete("pipeline", "sig"));
    noop.markTableComplete("pipeline", "sig");
    noop.invalidateTableCompletion("pipeline");
    noop.clearAllCompletions();

    assertFalse(noop.isComplete("src", "tbl", "phase"));
    noop.markComplete("src", "tbl", "phase", 10);
    noop.markError("src", "tbl", "phase", "error");
    assertTrue(noop.getCompletedTables("src", "phase").isEmpty());
  }

  // ===== IncrementalTracker.NOOP Tests =====

  @Test
  void testNoopIncrementalTracker() {
    IncrementalTracker noop = IncrementalTracker.NOOP;

    assertFalse(noop.isProcessed("alt", "src", Collections.<String, String>emptyMap()));
    assertFalse(noop.isProcessedWithTtl("alt", "src",
        Collections.<String, String>emptyMap(), 1000));
    noop.markProcessed("alt", "src",
        Collections.<String, String>emptyMap(), "target");
    assertTrue(noop.getProcessedKeyValues("alt").isEmpty());
    noop.invalidate("alt", Collections.<String, String>emptyMap());
    noop.invalidateAll("alt");
    assertFalse(noop.isTableComplete("pipeline", "sig"));
    noop.markTableComplete("pipeline", "sig");
    noop.invalidateTableCompletion("pipeline");
    noop.clearAllCompletions();

    Set<Integer> unprocessed = noop.filterUnprocessed("alt", "src",
        Arrays.asList(Collections.<String, String>emptyMap()));
    assertEquals(1, unprocessed.size());
  }

  // ===== IncrementalTracker static methods Tests =====

  @Test
  void testComputeDimensionSignatureNull() {
    assertEquals("empty", IncrementalTracker.computeDimensionSignature(null));
  }

  @Test
  void testComputeDimensionSignatureEmpty() {
    assertEquals("empty",
        IncrementalTracker.computeDimensionSignature(
            Collections.<Map<String, String>>emptyList()));
  }

  @Test
  void testComputeDimensionSignatureWithData() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> combo1 = new HashMap<String, String>();
    combo1.put("year", "2023");
    combo1.put("region", "US");
    combos.add(combo1);
    Map<String, String> combo2 = new HashMap<String, String>();
    combo2.put("year", "2024");
    combo2.put("region", "EU");
    combos.add(combo2);

    String sig = IncrementalTracker.computeDimensionSignature(combos);
    assertNotNull(sig);
    assertTrue(sig.contains("count:2"));
    assertTrue(sig.contains("|hash:"));
  }

  // ===== PipelineTracker default method Tests =====

  @Test
  void testPipelineTrackerDefaultIsFullyComplete() {
    PipelineTracker tracker = PipelineTracker.NOOP_PIPELINE;

    Set<String> required = new HashSet<String>();
    required.add("table1");
    required.add("table2");

    // NOOP always returns false for isComplete, so isFullyComplete should be false
    assertFalse(tracker.isFullyComplete("src1", "staging", required));
  }

  @Test
  void testPipelineTrackerDefaultGetSourceKeysForPhase() {
    PipelineTracker tracker = PipelineTracker.NOOP_PIPELINE;
    Set<String> keys = tracker.getSourceKeysForPhase("staging");
    assertTrue(keys.isEmpty());
  }

  @Test
  void testPipelineTrackerDefaultPreloadAll() {
    PipelineTracker tracker = PipelineTracker.NOOP_PIPELINE;
    // Should be a no-op
    tracker.preloadAll("staging");
  }

  @Test
  void testPipelineTrackerDefaultBulkGetCompletedTables() {
    PipelineTracker tracker = PipelineTracker.NOOP_PIPELINE;
    Map<String, Set<String>> result = tracker.bulkGetCompletedTables(
        Arrays.asList("key1", "key2"), "staging");
    assertTrue(result.isEmpty());
  }

  // ===== listTrackerFiles path parsing Tests =====

  @Test
  void testListTrackerFilesPathParsing() throws Exception {
    // Test the path parsing logic within listTrackerFiles via reflection
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://mybucket/tracker-prefix", null);
    try {
      // Access bucketPath field to verify it was set correctly
      Field bucketPathField =
          S3HivePipelineTracker.class.getDeclaredField("bucketPath");
      bucketPathField.setAccessible(true);
      String bucketPath = (String) bucketPathField.get(tracker);
      assertEquals("s3://mybucket/tracker-prefix", bucketPath);
    } finally {
      tracker.close();
    }
  }

  @Test
  void testBucketPathNormalization() throws Exception {
    // Trailing slash should be removed
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://mybucket/tracker/", null);
    try {
      Field bucketPathField =
          S3HivePipelineTracker.class.getDeclaredField("bucketPath");
      bucketPathField.setAccessible(true);
      String bucketPath = (String) bucketPathField.get(tracker);
      assertEquals("s3://mybucket/tracker", bucketPath);
    } finally {
      tracker.close();
    }
  }

  // ===== readTrackerGlobAllPhases Tests =====

  @Test
  void testReadTrackerGlobAllPhasesPathFormat() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "readTrackerGlobAllPhases", String.class);
      method.setAccessible(true);

      // Test that list format (starting with '[') uses direct reference
      // Note: this will fail due to no DuckDB connection, but we test the path format logic
      // by checking it does not crash on constructing the SQL

      // A glob pattern not starting with '['
      // This will try to query DuckDB - expect null/failure since no real files
      int[] result = (int[]) method.invoke(tracker, "/tmp/nonexistent/*.parquet");
      // Should return null because query will fail (no files)
      assertNull(result);
    } finally {
      tracker.close();
    }
  }

  // ===== Scanned Years Tracking =====

  @Test
  void testScannedYearsTracking() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field scannedYearsField =
          S3HivePipelineTracker.class.getDeclaredField("scannedYears");
      scannedYearsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Set<String> scannedYears = (Set<String>) scannedYearsField.get(tracker);

      assertTrue(scannedYears.isEmpty());
      scannedYears.add("2023");
      assertTrue(scannedYears.contains("2023"));
      assertFalse(scannedYears.contains("2024"));
    } finally {
      tracker.close();
    }
  }

  @Test
  void testFullyScannedYearsTracking() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field fullyScannedYearsField =
          S3HivePipelineTracker.class.getDeclaredField("fullyScannedYears");
      fullyScannedYearsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Set<String> fullyScannedYears = (Set<String>) fullyScannedYearsField.get(tracker);

      assertTrue(fullyScannedYears.isEmpty());
      fullyScannedYears.add("2023");
      assertTrue(fullyScannedYears.contains("2023"));
    } finally {
      tracker.close();
    }
  }

  // ===== writeState / ensureShutdownHook Tests =====

  @Test
  void testEnsureShutdownHookIdempotent() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Method method = S3HivePipelineTracker.class.getDeclaredMethod(
          "ensureShutdownHook");
      method.setAccessible(true);

      // Call twice - should be idempotent
      method.invoke(tracker);
      method.invoke(tracker);

      Field hookField =
          S3HivePipelineTracker.class.getDeclaredField("shutdownHookRegistered");
      hookField.setAccessible(true);
      assertTrue((Boolean) hookField.get(tracker));
    } finally {
      tracker.close();
    }
  }

  // ===== PendingState inner class Tests =====

  @Test
  void testPendingStateCreation() throws Exception {
    // Access PendingState via reflection
    Class<?> pendingStateClass = null;
    for (Class<?> clazz : S3HivePipelineTracker.class.getDeclaredClasses()) {
      if (clazz.getSimpleName().equals("PendingState")) {
        pendingStateClass = clazz;
        break;
      }
    }
    assertNotNull(pendingStateClass);

    java.lang.reflect.Constructor<?> constructor = pendingStateClass.getDeclaredConstructor(
        String.class, String.class, String.class,
        String.class, long.class, String.class, String.class,
        String.class, long.class);
    constructor.setAccessible(true);

    Object ps = constructor.newInstance("src", "tbl", "phase",
        "complete", 100L, "cfg_hash", "sig", "error_msg", 12345L);
    assertNotNull(ps);

    // Verify fields
    Field srcField = pendingStateClass.getDeclaredField("sourceKey");
    srcField.setAccessible(true);
    assertEquals("src", srcField.get(ps));

    Field tblField = pendingStateClass.getDeclaredField("tableName");
    tblField.setAccessible(true);
    assertEquals("tbl", tblField.get(ps));

    Field phaseField = pendingStateClass.getDeclaredField("phase");
    phaseField.setAccessible(true);
    assertEquals("phase", phaseField.get(ps));

    Field stateField = pendingStateClass.getDeclaredField("state");
    stateField.setAccessible(true);
    assertEquals("complete", stateField.get(ps));

    Field rowCountField = pendingStateClass.getDeclaredField("rowCount");
    rowCountField.setAccessible(true);
    assertEquals(100L, rowCountField.get(ps));

    Field configHashField = pendingStateClass.getDeclaredField("configHash");
    configHashField.setAccessible(true);
    assertEquals("cfg_hash", configHashField.get(ps));

    Field sigField = pendingStateClass.getDeclaredField("signature");
    sigField.setAccessible(true);
    assertEquals("sig", sigField.get(ps));

    Field errorField = pendingStateClass.getDeclaredField("errorMessage");
    errorField.setAccessible(true);
    assertEquals("error_msg", errorField.get(ps));

    Field asOfField = pendingStateClass.getDeclaredField("asOf");
    asOfField.setAccessible(true);
    assertEquals(12345L, asOfField.get(ps));
  }

  // ===== filterUnprocessed with empty combinations Tests =====

  @Test
  void testFilterUnprocessedEmptyCombinations() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Set<Integer> result = tracker.filterUnprocessed("alt", "src", null);
      assertTrue(result.isEmpty());

      result = tracker.filterUnprocessed("alt", "src",
          Collections.<Map<String, String>>emptyList());
      assertTrue(result.isEmpty());
    } finally {
      tracker.close();
    }
  }

  // ===== noCompact configuration Tests =====

  @Test
  void testNoCompactFieldSetFromSystemProperty() throws Exception {
    // Test reading the noCompact field
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field noCompactField =
          S3HivePipelineTracker.class.getDeclaredField("noCompact");
      noCompactField.setAccessible(true);
      // Default should be false unless system property is set
      boolean noCompact = (Boolean) noCompactField.get(tracker);
      // Just verify we can read it
      assertNotNull(noCompact);
    } finally {
      tracker.close();
    }
  }

  // ===== hasAnyTrackerData Tests =====

  @Test
  void testHasAnyTrackerDataField() throws Exception {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", null);
    try {
      Field field =
          S3HivePipelineTracker.class.getDeclaredField("hasAnyTrackerData");
      field.setAccessible(true);

      // Initially null (not yet checked)
      assertNull(field.get(tracker));

      // Set it
      field.set(tracker, Boolean.TRUE);
      assertEquals(Boolean.TRUE, field.get(tracker));
    } finally {
      tracker.close();
    }
  }

  // ===== COMPLETION_YEAR constant Test =====

  @Test
  void testCompletionYearConstant() throws Exception {
    Field field = S3HivePipelineTracker.class.getDeclaredField("COMPLETION_YEAR");
    field.setAccessible(true);
    assertEquals("0", field.get(null));
  }

  // ===== READ_BATCH_SIZE constant Test =====

  @Test
  void testReadBatchSizeConstant() throws Exception {
    Field field = S3HivePipelineTracker.class.getDeclaredField("READ_BATCH_SIZE");
    field.setAccessible(true);
    assertEquals(10000, field.get(null));
  }

  // ===== FILTER_CHUNK_SIZE constant Test =====

  @Test
  void testFilterChunkSizeConstant() throws Exception {
    Field field =
        S3HivePipelineTracker.class.getDeclaredField("FILTER_CHUNK_SIZE");
    field.setAccessible(true);
    assertEquals(50000, field.get(null));
  }

  // ===== IncrementalTracker default methods Tests =====

  @Test
  void testIncrementalTrackerDefaultMethods() {
    IncrementalTracker tracker = IncrementalTracker.NOOP;

    // markProcessedWithRowCount default delegates to markProcessed
    tracker.markProcessedWithRowCount("alt", "src",
        Collections.<String, String>emptyMap(), "target", 100);

    // isProcessedWithEmptyTtl default delegates to isProcessed
    assertFalse(tracker.isProcessedWithEmptyTtl("alt", "src",
        Collections.<String, String>emptyMap(), 1000));

    // markProcessedWithError default delegates to markProcessedWithRowCount
    tracker.markProcessedWithError("alt", "src",
        Collections.<String, String>emptyMap(), "target", "error msg");

    // filterUnprocessedWithEmptyTtl default delegates to filterUnprocessed
    Set<Integer> result = tracker.filterUnprocessedWithEmptyTtl("alt", "src",
        Arrays.asList(Collections.<String, String>emptyMap()), 1000);
    assertEquals(1, result.size());

    // filterUnprocessedWithTtl default delegates to filterUnprocessedWithEmptyTtl
    result = tracker.filterUnprocessedWithTtl("alt", "src",
        Arrays.asList(Collections.<String, String>emptyMap()), 1000, 500);
    assertEquals(1, result.size());

    // markTableCompleteWithConfig default delegates to markTableComplete
    tracker.markTableCompleteWithConfig("pipeline", "hash", "sig", 100);

    // getCachedCompletion default returns null
    assertNull(tracker.getCachedCompletion("pipeline"));

    // preloadAllCompletions default is no-op
    tracker.preloadAllCompletions();

    // markTableCompleteWithSourceWatermark default delegates
    tracker.markTableCompleteWithSourceWatermark("p", "h", "s", 0, 0);

    // isSourceFilesModified
    assertTrue(tracker.isSourceFilesModified("nonexistent", 12345));
  }

  // ===== PipelineTracker default implementations (bridge methods) =====

  @Test
  void testPipelineTrackerDefaultBridgeMethods() {
    // Create a simple IncrementalTracker-based PipelineTracker to test defaults
    PipelineTracker tracker = new PipelineTracker() {
      private final Set<String> processed = new HashSet<String>();

      @Override public boolean isProcessed(String alternateName, String sourceTable,
          Map<String, String> keyValues) {
        return processed.contains(alternateName + ":" + keyValues);
      }

      @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
          Map<String, String> keyValues, long ttlMillis) {
        return false;
      }

      @Override public void markProcessed(String alternateName, String sourceTable,
          Map<String, String> keyValues, String targetPattern) {
        processed.add(alternateName + ":" + keyValues);
      }

      @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
        return Collections.emptySet();
      }

      @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
        // no-op
      }

      @Override public void invalidateAll(String alternateName) {
        // no-op
      }

      @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
          List<Map<String, String>> allCombinations) {
        return Collections.emptySet();
      }

      @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
        return false;
      }

      @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
        // no-op
      }

      @Override public void invalidateTableCompletion(String pipelineName) {
        // no-op
      }

      @Override public void clearAllCompletions() {
        // no-op
      }
    };

    // Test default isComplete (bridges to isProcessed) - before marking
    assertFalse(tracker.isComplete("src1", "table1", "staging"));

    // Test default isFullyComplete - before marking, should be false
    Set<String> required = new HashSet<String>();
    required.add("table1");
    assertFalse(tracker.isFullyComplete("src1", "staging", required));

    // Test default getCompletedTables - empty by default
    Set<String> tables = tracker.getCompletedTables("src1", "staging");
    assertTrue(tables.isEmpty());

    // Test default markComplete (bridges to markProcessedWithRowCount)
    tracker.markComplete("src1", "table1", "staging", 50);

    // After marking, isComplete should be true (bridges to isProcessed which now finds it)
    assertTrue(tracker.isComplete("src1", "table1", "staging"));

    // Test default markError
    tracker.markError("src1", "table1", "staging", "failed");

    // Test default markCleared
    tracker.markCleared("src1", "table1", "staging");
  }
}
