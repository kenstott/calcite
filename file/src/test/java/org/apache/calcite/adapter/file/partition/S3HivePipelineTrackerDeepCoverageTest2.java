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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for {@link S3HivePipelineTracker} targeting 215 missed lines.
 * Focuses on utility methods (extractYear, flattenKeyValues, unflattenKeyValues,
 * sanitizeHiveValue), cache operations (stageCache, completionCache), and
 * the PendingState/CachedCompletion inner classes via reflection.
 * Does NOT require actual S3 connectivity.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class S3HivePipelineTrackerDeepCoverageTest2 {

  // ========== extractYear ==========

  @Test
  void testExtractYearFromFlattenedKey() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "extractYear", String.class, long.class);
    m.setAccessible(true);

    // Flattened key with year=YYYY
    assertEquals("2023", m.invoke(tracker, "geography=state__type=acs__year=2023", 0L));
    assertEquals("2020", m.invoke(tracker, "year=2020", 0L));
    assertEquals("2024", m.invoke(tracker, "geo=US__year=2024__type=income", 0L));
  }

  @Test
  void testExtractYearFromSecAccession() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "extractYear", String.class, long.class);
    m.setAccessible(true);

    // SEC accession format: 0000123456-YY-012345
    assertEquals("2024", m.invoke(tracker, "0000123456-24-012345", 0L));
    assertEquals("1999", m.invoke(tracker, "0000123456-99-012345", 0L)); // 90+ => 1900+
    assertEquals("1995", m.invoke(tracker, "0000123456-95-999999", 0L));
  }

  @Test
  void testExtractYearBare4DigitYear() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "extractYear", String.class, long.class);
    m.setAccessible(true);

    assertEquals("2023", m.invoke(tracker, "2023", 0L));
    assertEquals("1999", m.invoke(tracker, "1999", 0L));
    assertEquals("2100", m.invoke(tracker, "2100", 0L));
  }

  @Test
  void testExtractYearFallbackToCurrentYear() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "extractYear", String.class, long.class);
    m.setAccessible(true);

    long now = System.currentTimeMillis();
    java.util.Calendar cal = java.util.Calendar.getInstance();
    cal.setTimeInMillis(now);
    String expectedYear = String.valueOf(cal.get(java.util.Calendar.YEAR));

    // Non-matching pattern falls back to current year
    assertEquals(expectedYear, m.invoke(tracker, "random_key_no_year", now));
    assertEquals(expectedYear, m.invoke(tracker, (String) null, now));
  }

  @Test
  void testExtractYearNotValidYear() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "extractYear", String.class, long.class);
    m.setAccessible(true);

    // 4-digit but out of range
    long now = System.currentTimeMillis();
    java.util.Calendar cal = java.util.Calendar.getInstance();
    cal.setTimeInMillis(now);
    String expectedYear = String.valueOf(cal.get(java.util.Calendar.YEAR));
    assertEquals(expectedYear, m.invoke(tracker, "0001", now));
    assertEquals(expectedYear, m.invoke(tracker, "9999", now));
  }

  @Test
  void testExtractYearNonNumeric4Digit() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "extractYear", String.class, long.class);
    m.setAccessible(true);

    long now = System.currentTimeMillis();
    java.util.Calendar cal = java.util.Calendar.getInstance();
    cal.setTimeInMillis(now);
    String expectedYear = String.valueOf(cal.get(java.util.Calendar.YEAR));
    assertEquals(expectedYear, m.invoke(tracker, "abcd", now));
  }

  // ========== flattenKeyValues ==========

  @Test
  void testFlattenKeyValuesEmpty() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "flattenKeyValues", Map.class);
    m.setAccessible(true);

    assertEquals("_empty", m.invoke(tracker, (Map<String, String>) null));
    assertEquals("_empty", m.invoke(tracker, Collections.emptyMap()));
  }

  @Test
  void testFlattenKeyValuesSingleEntry() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "flattenKeyValues", Map.class);
    m.setAccessible(true);

    Map<String, String> single = Collections.singletonMap("year", "2023");
    assertEquals("2023", m.invoke(tracker, single));
  }

  @Test
  void testFlattenKeyValuesMultiEntry() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "flattenKeyValues", Map.class);
    m.setAccessible(true);

    Map<String, String> multi = new LinkedHashMap<String, String>();
    multi.put("year", "2023");
    multi.put("geo", "STATE");
    String result = (String) m.invoke(tracker, multi);
    // Keys are sorted: geo before year
    assertTrue(result.contains("geo=STATE"), "Should contain geo: " + result);
    assertTrue(result.contains("year=2023"), "Should contain year: " + result);
    assertTrue(result.contains("__"), "Should use __ separator: " + result);
  }

  // ========== unflattenKeyValues ==========

  @Test
  void testUnflattenKeyValuesEmpty() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "unflattenKeyValues", String.class);
    m.setAccessible(true);

    Map<String, String> result = (Map<String, String>) m.invoke(tracker, "_empty");
    assertTrue(result.isEmpty());
  }

  @Test
  void testUnflattenKeyValuesMultiKey() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "unflattenKeyValues", String.class);
    m.setAccessible(true);

    Map<String, String> result =
        (Map<String, String>) m.invoke(tracker, "geo=STATE__year=2023");
    assertEquals("STATE", result.get("geo"));
    assertEquals("2023", result.get("year"));
  }

  @Test
  void testUnflattenKeyValuesSingleValue() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "unflattenKeyValues", String.class);
    m.setAccessible(true);

    // Single value without = or __
    Map<String, String> result = (Map<String, String>) m.invoke(tracker, "2023");
    assertEquals("2023", result.get("source_key"));
  }

  @Test
  void testUnflattenKeyValuesSingleKeyValue() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "unflattenKeyValues", String.class);
    m.setAccessible(true);

    // Has = but no __ => single value fallback
    Map<String, String> result = (Map<String, String>) m.invoke(tracker, "year=2023");
    assertEquals("year=2023", result.get("source_key"));
  }

  // ========== sanitizeHiveValue ==========

  @Test
  void testSanitizeHiveValue() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "sanitizeHiveValue", String.class);
    m.setAccessible(true);

    assertEquals("path_to_file", m.invoke(tracker, "path/to/file"));
    assertEquals("hello_world", m.invoke(tracker, "hello world"));
    assertEquals("key_value", m.invoke(tracker, "key:value"));
    assertEquals("a_b_c_d", m.invoke(tracker, "a/b c:d"));
  }

  // ========== stageCache operations ==========

  @Test
  void testStageCacheDirectManipulation() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
    stageCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> stageCache =
        (Map<String, Set<String>>) stageCacheField.get(tracker);

    // Manually populate stageCache
    Set<String> tables = new LinkedHashSet<String>();
    tables.add("table_a");
    tables.add("table_b");
    stageCache.put("sourceKey\0download", tables);

    // isComplete should find it in cache
    assertTrue(tracker.isComplete("sourceKey", "table_a", "download"));
    assertTrue(tracker.isComplete("sourceKey", "table_b", "download"));
  }

  // ========== completionCache operations ==========

  @Test
  void testCompletionCacheDirectManipulation() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field completionCacheField =
        S3HivePipelineTracker.class.getDeclaredField("completionCache");
    completionCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, PipelineTracker.CachedCompletion> completionCache =
        (Map<String, PipelineTracker.CachedCompletion>) completionCacheField.get(tracker);

    PipelineTracker.CachedCompletion cached =
        new PipelineTracker.CachedCompletion("hash123", "sig456", 1000L,
            System.currentTimeMillis(), 0);
    completionCache.put("my_pipeline", cached);

    assertTrue(tracker.isTableComplete("my_pipeline", "sig456"));
    assertFalse(tracker.isTableComplete("my_pipeline", "wrong_sig"));
    assertFalse(tracker.isTableComplete("other_pipeline", "sig456"));
  }

  @Test
  void testCompletionsPreloadedFlag() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field preloadedField =
        S3HivePipelineTracker.class.getDeclaredField("completionsPreloaded");
    preloadedField.setAccessible(true);
    preloadedField.setBoolean(tracker, true);

    // getCachedCompletion should return null without hitting S3 when preloaded
    assertNull(tracker.getCachedCompletion("nonexistent_pipeline"));
  }

  // ========== markComplete updates stageCache ==========

  @Test
  void testMarkCompleteNewEntry() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
    stageCacheField.setAccessible(true);

    // markComplete should update stageCache and buffer writes
    tracker.markComplete("sk1", "tbl1", "phase1", 100L);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> stageCache =
        (Map<String, Set<String>>) stageCacheField.get(tracker);
    Set<String> tables = stageCache.get("sk1\0phase1");
    assertNotNull(tables, "Stage cache should have entry for sk1+phase1");
    assertTrue(tables.contains("tbl1"));
  }

  @Test
  void testMarkCompleteExistingEntry() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
    stageCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> stageCache =
        (Map<String, Set<String>>) stageCacheField.get(tracker);

    Set<String> existing = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    existing.add("tbl_existing");
    stageCache.put("sk2\0phase2", existing);

    tracker.markComplete("sk2", "tbl_new", "phase2", 50L);
    assertTrue(existing.contains("tbl_new"));
    assertTrue(existing.contains("tbl_existing"));
  }

  // ========== markCleared removes from stageCache ==========

  @Test
  void testMarkClearedRemovesFromCache() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
    stageCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> stageCache =
        (Map<String, Set<String>>) stageCacheField.get(tracker);

    Set<String> tables = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    tables.add("to_clear");
    tables.add("to_keep");
    stageCache.put("sk3\0phase3", tables);

    tracker.markCleared("sk3", "to_clear", "phase3");
    assertFalse(tables.contains("to_clear"));
    assertTrue(tables.contains("to_keep"));
  }

  @Test
  void testMarkClearedNonExistentCache() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    // Should not throw when cache entry doesn't exist
    tracker.markCleared("nonexistent", "tbl", "phase");
  }

  // ========== markTableComplete and markTableCompleteWithConfig ==========

  @Test
  void testMarkTableCompleteUpdatesCache() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    tracker.markTableComplete("pipeline1", "dim_sig_1");

    Field completionCacheField =
        S3HivePipelineTracker.class.getDeclaredField("completionCache");
    completionCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, PipelineTracker.CachedCompletion> cache =
        (Map<String, PipelineTracker.CachedCompletion>) completionCacheField.get(tracker);
    assertNotNull(cache.get("pipeline1"));
    assertEquals("dim_sig_1", cache.get("pipeline1").signature);
  }

  @Test
  void testMarkTableCompleteWithConfigUpdatesCache() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    tracker.markTableCompleteWithConfig("pipeline2", "confHash", "dimSig2", 5000L);

    Field completionCacheField =
        S3HivePipelineTracker.class.getDeclaredField("completionCache");
    completionCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, PipelineTracker.CachedCompletion> cache =
        (Map<String, PipelineTracker.CachedCompletion>) completionCacheField.get(tracker);
    PipelineTracker.CachedCompletion c = cache.get("pipeline2");
    assertNotNull(c);
    assertEquals("confHash", c.configHash);
    assertEquals("dimSig2", c.signature);
    assertEquals(5000L, c.rowCount);
  }

  @Test
  void testMarkTableCompleteWithSourceWatermark() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    tracker.markTableCompleteWithSourceWatermark("pipeline3", "confHash3",
        "dimSig3", 3000L, 1700000000000L);

    Field completionCacheField =
        S3HivePipelineTracker.class.getDeclaredField("completionCache");
    completionCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, PipelineTracker.CachedCompletion> cache =
        (Map<String, PipelineTracker.CachedCompletion>) completionCacheField.get(tracker);
    PipelineTracker.CachedCompletion c = cache.get("pipeline3");
    assertNotNull(c);
    assertEquals("confHash3", c.configHash);
    assertEquals(1700000000000L, c.sourceFileWatermark);
  }

  // ========== invalidateTableCompletion ==========

  @Test
  void testInvalidateTableCompletion() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    tracker.markTableComplete("to_invalidate", "sig_x");

    Field completionCacheField =
        S3HivePipelineTracker.class.getDeclaredField("completionCache");
    completionCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, PipelineTracker.CachedCompletion> cache =
        (Map<String, PipelineTracker.CachedCompletion>) completionCacheField.get(tracker);
    assertNotNull(cache.get("to_invalidate"));

    tracker.invalidateTableCompletion("to_invalidate");
    assertNull(cache.get("to_invalidate"));
  }

  // ========== PendingState inner class ==========

  @Test
  void testPendingStateInnerClass() throws Exception {
    Class<?> psClass = null;
    for (Class<?> inner : S3HivePipelineTracker.class.getDeclaredClasses()) {
      if (inner.getSimpleName().equals("PendingState")) {
        psClass = inner;
        break;
      }
    }
    assertNotNull(psClass, "PendingState inner class should exist");

    java.lang.reflect.Constructor<?> ctor = psClass.getDeclaredConstructors()[0];
    ctor.setAccessible(true);

    Object ps = ctor.newInstance("sk", "tbl", "phase", "complete",
        100L, "hash", "sig", "none", 123456789L);
    assertNotNull(ps);

    Field sourceKeyF = psClass.getDeclaredField("sourceKey");
    sourceKeyF.setAccessible(true);
    assertEquals("sk", sourceKeyF.get(ps));

    Field rowCountF = psClass.getDeclaredField("rowCount");
    rowCountF.setAccessible(true);
    assertEquals(100L, rowCountF.get(ps));

    Field asOfF = psClass.getDeclaredField("asOf");
    asOfF.setAccessible(true);
    assertEquals(123456789L, asOfF.get(ps));
  }

  // ========== allIndices ==========

  @Test
  void testAllIndices() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod("allIndices", int.class);
    m.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(tracker, 5);
    assertEquals(5, result.size());
    for (int i = 0; i < 5; i++) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  void testAllIndicesEmpty() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Method m = S3HivePipelineTracker.class.getDeclaredMethod("allIndices", int.class);
    m.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(tracker, 0);
    assertTrue(result.isEmpty());
  }

  // ========== parsePendingFlushThreshold ==========

  @Test
  void testParsePendingFlushThresholdDefault() throws Exception {
    Method m = S3HivePipelineTracker.class.getDeclaredMethod("parsePendingFlushThreshold");
    m.setAccessible(true);
    int result = (Integer) m.invoke(null);
    // Default is 500 when env var not set
    assertEquals(500, result);
  }

  // ========== close ==========

  @Test
  void testCloseWithNullConnection() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    // connection is null initially, close should not throw
    tracker.close();
  }

  // ========== bulkGetCompletedTables with empty input ==========

  @Test
  void testBulkGetCompletedTablesEmpty() {
    S3HivePipelineTracker tracker = createTracker();
    Map<String, Set<String>> result = tracker.bulkGetCompletedTables(
        Collections.<String>emptyList(), "phase");
    assertTrue(result.isEmpty());
  }

  @Test
  void testBulkGetCompletedTablesAllCached() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
    stageCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> stageCache =
        (Map<String, Set<String>>) stageCacheField.get(tracker);

    Set<String> tables1 = new LinkedHashSet<String>();
    tables1.add("t1");
    stageCache.put("sk1\0p1", tables1);

    Set<String> tables2 = new LinkedHashSet<String>();
    tables2.add("t2");
    stageCache.put("sk2\0p1", tables2);

    Map<String, Set<String>> result = tracker.bulkGetCompletedTables(
        Arrays.asList("sk1", "sk2"), "p1");
    assertEquals(2, result.size());
    assertTrue(result.get("sk1").contains("t1"));
    assertTrue(result.get("sk2").contains("t2"));
  }

  @Test
  void testBulkGetCompletedTablesEmptyCachedSkipped() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    Field stageCacheField = S3HivePipelineTracker.class.getDeclaredField("stageCache");
    stageCacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> stageCache =
        (Map<String, Set<String>>) stageCacheField.get(tracker);

    // Empty set means cached but no data
    stageCache.put("sk_empty\0p1", new LinkedHashSet<String>());

    Map<String, Set<String>> result = tracker.bulkGetCompletedTables(
        Collections.singletonList("sk_empty"), "p1");
    assertTrue(result.isEmpty(), "Empty cached set should not be included in results");
  }

  // ========== markError ==========

  @Test
  void testMarkError() throws Exception {
    S3HivePipelineTracker tracker = createTracker();
    // Should buffer the write without throwing
    tracker.markError("sk_err", "tbl_err", "phase_err", "Something went wrong");

    // Verify pendingStates has the entry
    Field pendingField = S3HivePipelineTracker.class.getDeclaredField("pendingStates");
    pendingField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<?> pending = (List<?>) pendingField.get(tracker);
    assertFalse(pending.isEmpty());
  }

  // ========== Constructor variants ==========

  @Test
  void testConstructorWithTrailingSlash() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker/", null);
    // Should strip trailing slash
    Field bucketPathField;
    try {
      bucketPathField = S3HivePipelineTracker.class.getDeclaredField("bucketPath");
      bucketPathField.setAccessible(true);
      assertEquals("s3://bucket/tracker", bucketPathField.get(tracker));
    } catch (Exception e) {
      fail("Should be able to access bucketPath field: " + e.getMessage());
    }
  }

  @Test
  void testConstructorWithNullConfig() {
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker("s3://bucket/tracker", "http://localhost:9000", null);
    assertNotNull(tracker);
  }

  // ========== deleteDir static utility ==========

  @Test
  void testDeleteDir() throws Exception {
    // Create a temp directory structure
    java.io.File root = java.io.File.createTempFile("tracker-test-", "");
    root.delete();
    root.mkdirs();
    java.io.File sub = new java.io.File(root, "sub");
    sub.mkdirs();
    new java.io.File(sub, "file.txt").createNewFile();
    new java.io.File(root, "root_file.txt").createNewFile();

    assertTrue(root.exists());

    Method m = S3HivePipelineTracker.class.getDeclaredMethod("deleteDir", java.io.File.class);
    m.setAccessible(true);
    m.invoke(null, root);

    assertFalse(root.exists());
  }

  @Test
  void testDeleteDirNull() throws Exception {
    Method m = S3HivePipelineTracker.class.getDeclaredMethod("deleteDir", java.io.File.class);
    m.setAccessible(true);
    // Should handle gracefully when listFiles returns null
    java.io.File nonExistent = new java.io.File("/nonexistent_dir_for_test");
    m.invoke(null, nonExistent); // Should not throw
  }

  // ========== Helper ==========

  private S3HivePipelineTracker createTracker() {
    return new S3HivePipelineTracker("s3://test-bucket/tracker", null,
        Collections.<String, String>emptyMap());
  }
}
