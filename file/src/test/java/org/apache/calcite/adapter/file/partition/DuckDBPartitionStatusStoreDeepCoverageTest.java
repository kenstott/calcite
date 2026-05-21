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

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link DuckDBPartitionStatusStore}.
 * Covers JSON utilities, full DuckDB-based tracker operations, and bulk filtering.
 */
@Tag("unit")
class DuckDBPartitionStatusStoreDeepCoverageTest {

  @TempDir
  Path tempDir;

  private DuckDBPartitionStatusStore store;

  @BeforeEach
  void setUp() {
    // Use a unique subdirectory per test to ensure data isolation and avoid
    // interfering with stores from concurrently-running test classes.
    String baseDir =
        new File(tempDir.toFile(), "store_" + System.nanoTime()).getAbsolutePath();
    new File(baseDir).mkdirs();
    store = DuckDBPartitionStatusStore.getInstance(baseDir);
  }

  @AfterEach
  void tearDown() {
    // Only close our own store instance — do NOT clear the global OPEN_STORES map,
    // as that would destroy connections used by concurrently-running test classes.
    if (store != null) {
      try {
        store.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  // ===== JSON Utilities via Reflection =====

  @Test void testMapToJsonViaReflection() throws Exception {
    Method mapToJson = DuckDBPartitionStatusStore.class.getDeclaredMethod("mapToJson", Map.class);
    mapToJson.setAccessible(true);

    Map<String, String> map = new LinkedHashMap<String, String>();
    map.put("year", "2020");
    map.put("geo", "STATE");

    String json = (String) mapToJson.invoke(store, map);
    assertNotNull(json);
    assertTrue(json.contains("\"year\":\"2020\""));
    assertTrue(json.contains("\"geo\":\"STATE\""));
  }

  @Test void testMapToJsonNullMap() throws Exception {
    Method mapToJson = DuckDBPartitionStatusStore.class.getDeclaredMethod("mapToJson", Map.class);
    mapToJson.setAccessible(true);

    String json = (String) mapToJson.invoke(store, (Object) null);
    assertEquals("{}", json);
  }

  @Test void testMapToJsonEmptyMap() throws Exception {
    Method mapToJson = DuckDBPartitionStatusStore.class.getDeclaredMethod("mapToJson", Map.class);
    mapToJson.setAccessible(true);

    String json = (String) mapToJson.invoke(store, Collections.emptyMap());
    assertEquals("{}", json);
  }

  @Test void testJsonToMapViaReflection() throws Exception {
    Method jsonToMap = DuckDBPartitionStatusStore.class.getDeclaredMethod("jsonToMap", String.class);
    jsonToMap.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result =
        (Map<String, String>) jsonToMap.invoke(store, "{\"year\":\"2020\",\"geo\":\"STATE\"}");
    assertEquals("2020", result.get("year"));
    assertEquals("STATE", result.get("geo"));
  }

  @Test void testJsonToMapNull() throws Exception {
    Method jsonToMap = DuckDBPartitionStatusStore.class.getDeclaredMethod("jsonToMap", String.class);
    jsonToMap.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) jsonToMap.invoke(store, (Object) null);
    assertTrue(result.isEmpty());
  }

  @Test void testJsonToMapEmpty() throws Exception {
    Method jsonToMap = DuckDBPartitionStatusStore.class.getDeclaredMethod("jsonToMap", String.class);
    jsonToMap.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) jsonToMap.invoke(store, "{}");
    assertTrue(result.isEmpty());
  }

  @Test void testJsonToMapEmptyString() throws Exception {
    Method jsonToMap = DuckDBPartitionStatusStore.class.getDeclaredMethod("jsonToMap", String.class);
    jsonToMap.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) jsonToMap.invoke(store, "");
    assertTrue(result.isEmpty());
  }

  // ===== escapeJson via Reflection =====

  @Test void testEscapeJsonViaReflection() throws Exception {
    Method escapeJson = DuckDBPartitionStatusStore.class.getDeclaredMethod("escapeJson", String.class);
    escapeJson.setAccessible(true);

    assertEquals("hello", escapeJson.invoke(store, "hello"));
    assertEquals("\\\"quoted\\\"", escapeJson.invoke(store, "\"quoted\""));
    assertEquals("back\\\\slash", escapeJson.invoke(store, "back\\slash"));
    assertEquals("new\\nline", escapeJson.invoke(store, "new\nline"));
    assertEquals("tab\\there", escapeJson.invoke(store, "tab\there"));
    assertEquals("cr\\rhere", escapeJson.invoke(store, "cr\rhere"));
    assertEquals("", escapeJson.invoke(store, (Object) null));
  }

  // ===== unescapeJson via Reflection =====

  @Test void testUnescapeJsonViaReflection() throws Exception {
    Method unescape = DuckDBPartitionStatusStore.class.getDeclaredMethod("unescapeJson", String.class);
    unescape.setAccessible(true);

    assertEquals("hello", unescape.invoke(store, "hello"));
    assertEquals("\"quoted\"", unescape.invoke(store, "\\\"quoted\\\""));
    assertEquals("back\\slash", unescape.invoke(store, "back\\\\slash"));
    assertEquals("new\nline", unescape.invoke(store, "new\\nline"));
    assertEquals("tab\there", unescape.invoke(store, "tab\\there"));
    assertEquals("cr\rhere", unescape.invoke(store, "cr\\rhere"));
    assertEquals("", unescape.invoke(store, (Object) null));
  }

  // ===== findClosingQuote via Reflection =====

  @Test void testFindClosingQuoteViaReflection() throws Exception {
    Method findClosing =
        DuckDBPartitionStatusStore.class.getDeclaredMethod("findClosingQuote", String.class, int.class);
    findClosing.setAccessible(true);

    assertEquals(5, findClosing.invoke(store, "hello\"", 0));
    // "esc\\\"d\"" in Java source = esc\"d" as runtime string
    // findClosingQuote scans from index 0: e(0) s(1) c(2) \(3)->skip "(4) d(5) "(6)->return 6
    assertEquals(6, findClosing.invoke(store, "esc\\\"d\"", 0));
    assertEquals(-1, findClosing.invoke(store, "no closing", 0));
  }

  // ===== JSON roundtrip =====

  @Test void testJsonRoundtrip() throws Exception {
    Method mapToJson = DuckDBPartitionStatusStore.class.getDeclaredMethod("mapToJson", Map.class);
    mapToJson.setAccessible(true);
    Method jsonToMap = DuckDBPartitionStatusStore.class.getDeclaredMethod("jsonToMap", String.class);
    jsonToMap.setAccessible(true);

    Map<String, String> original = new LinkedHashMap<String, String>();
    original.put("year", "2020");
    original.put("geo", "STATE");
    original.put("special", "val\"with\"quotes");

    String json = (String) mapToJson.invoke(store, original);

    @SuppressWarnings("unchecked")
    Map<String, String> restored = (Map<String, String>) jsonToMap.invoke(store, json);
    assertEquals("2020", restored.get("year"));
    assertEquals("STATE", restored.get("geo"));
    assertEquals("val\"with\"quotes", restored.get("special"));
  }

  // ===== Full DuckDB operations =====

  @Test void testIsProcessedNotFound() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");
    assertFalse(store.isProcessed("alt1", "src1", keys));
  }

  @Test void testMarkProcessedAndCheck() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");

    store.markProcessed("alt1", "src1", keys, "target_pattern");
    assertTrue(store.isProcessed("alt1", "src1", keys));
  }

  @Test void testMarkProcessedWithRowCount() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2021");

    store.markProcessedWithRowCount("alt1", "src1", keys, "target", 500);
    assertTrue(store.isProcessed("alt1", "src1", keys));
  }

  @Test void testMarkProcessedWithNegativeRowCount() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2022");

    store.markProcessedWithRowCount("alt1", "src1", keys, "target", -1);
    assertTrue(store.isProcessed("alt1", "src1", keys));
  }

  @Test void testIsProcessedWithTtl() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2023");

    store.markProcessed("alt1", "src1", keys, "target");
    // Just marked, should be within a very large TTL
    assertTrue(store.isProcessedWithTtl("alt1", "src1", keys, 60000L));
    // With 0 TTL, should be expired
    assertFalse(store.isProcessedWithTtl("alt1", "src1", keys, 0L));
  }

  @Test void testIsProcessedWithTtlNotFound() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "9999");
    assertFalse(store.isProcessedWithTtl("alt_none", "src", keys, 60000L));
  }

  @Test void testIsProcessedWithEmptyTtl() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2024");

    // Mark with 0 rows (empty result)
    store.markProcessedWithRowCount("alt1", "src1", keys, "target", 0);
    // Should be within large TTL
    assertTrue(store.isProcessedWithEmptyTtl("alt1", "src1", keys, 60000L));
    // Should be expired with 0 TTL
    assertFalse(store.isProcessedWithEmptyTtl("alt1", "src1", keys, 0L));
  }

  @Test void testIsProcessedWithEmptyTtlNonEmpty() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2025");

    // Mark with rows (non-empty result)
    store.markProcessedWithRowCount("alt1", "src1", keys, "target", 100);
    // Permanently processed (non-empty)
    assertTrue(store.isProcessedWithEmptyTtl("alt1", "src1", keys, 0L));
  }

  @Test void testIsProcessedWithEmptyTtlNotFound() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "9998");
    assertFalse(store.isProcessedWithEmptyTtl("alt_none", "src", keys, 60000L));
  }

  @Test void testMarkProcessedWithError() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");

    store.markProcessedWithError("alt1", "src1", keys, "target", "some error message");
    // Error entries count as processed
    assertTrue(store.isProcessed("alt1", "src1", keys));
  }

  @Test void testMarkProcessedWithLongErrorMessage() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");

    // Create a very long error message (>1000 chars)
    StringBuilder longMsg = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      longMsg.append("error ");
    }

    store.markProcessedWithError("alt1", "src1", keys, "target", longMsg.toString());
    assertTrue(store.isProcessed("alt1", "src1", keys));
  }

  @Test void testMarkProcessedWithNullError() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");

    store.markProcessedWithError("alt_err", "src1", keys, "target", null);
    assertTrue(store.isProcessed("alt_err", "src1", keys));
  }

  @Test void testGetProcessedKeyValues() {
    Map<String, String> keys1 = new HashMap<String, String>();
    keys1.put("year", "2020");
    Map<String, String> keys2 = new HashMap<String, String>();
    keys2.put("year", "2021");

    store.markProcessed("alt2", "src", keys1, "target");
    store.markProcessed("alt2", "src", keys2, "target");

    Set<Map<String, String>> result = store.getProcessedKeyValues("alt2");
    assertEquals(2, result.size());
  }

  @Test void testGetProcessedKeyValuesEmpty() {
    Set<Map<String, String>> result = store.getProcessedKeyValues("nonexistent");
    assertTrue(result.isEmpty());
  }

  @Test void testInvalidate() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");

    store.markProcessed("alt3", "src", keys, "target");
    assertTrue(store.isProcessed("alt3", "src", keys));

    store.invalidate("alt3", keys);
    assertFalse(store.isProcessed("alt3", "src", keys));
  }

  @Test void testInvalidateNonexistent() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "9999");
    // Should not throw
    store.invalidate("nonexistent", keys);
  }

  @Test void testInvalidateAll() {
    Map<String, String> keys1 = new HashMap<String, String>();
    keys1.put("year", "2020");
    Map<String, String> keys2 = new HashMap<String, String>();
    keys2.put("year", "2021");

    store.markProcessed("alt4", "src", keys1, "target");
    store.markProcessed("alt4", "src", keys2, "target");

    store.invalidateAll("alt4");
    assertFalse(store.isProcessed("alt4", "src", keys1));
    assertFalse(store.isProcessed("alt4", "src", keys2));
  }

  // ===== Bulk Filtering =====

  @Test void testFilterUnprocessed() {
    Map<String, String> keys1 = new HashMap<String, String>();
    keys1.put("year", "2020");
    Map<String, String> keys2 = new HashMap<String, String>();
    keys2.put("year", "2021");
    Map<String, String> keys3 = new HashMap<String, String>();
    keys3.put("year", "2022");

    store.markProcessed("alt5", "src", keys1, "target");
    // keys2 and keys3 are NOT processed

    List<Map<String, String>> all = Arrays.asList(keys1, keys2, keys3);
    Set<Integer> unprocessed = store.filterUnprocessed("alt5", "src", all);

    assertFalse(unprocessed.contains(0)); // keys1 is processed
    assertTrue(unprocessed.contains(1));  // keys2 is not
    assertTrue(unprocessed.contains(2));  // keys3 is not
  }

  @Test void testFilterUnprocessedEmpty() {
    Set<Integer> result = store.filterUnprocessed("alt", "src", Collections.<Map<String, String>>emptyList());
    assertTrue(result.isEmpty());
  }

  @Test void testFilterUnprocessedNull() {
    Set<Integer> result = store.filterUnprocessed("alt", "src", null);
    assertTrue(result.isEmpty());
  }

  @Test void testFilterUnprocessedWithEmptyTtl() {
    Map<String, String> keys1 = new HashMap<String, String>();
    keys1.put("year", "2020");

    store.markProcessedWithRowCount("alt6", "src", keys1, "target", 0);

    List<Map<String, String>> all = Arrays.asList(keys1);

    // With large TTL, should be considered processed
    Set<Integer> resultLargeTtl = store.filterUnprocessedWithEmptyTtl("alt6", "src", all, 60000L);
    assertTrue(resultLargeTtl.isEmpty());

    // With 0 TTL, should need processing
    Set<Integer> resultZeroTtl = store.filterUnprocessedWithEmptyTtl("alt6", "src", all, 0L);
    assertTrue(resultZeroTtl.contains(0));
  }

  @Test void testFilterUnprocessedWithEmptyTtlNull() {
    Set<Integer> result = store.filterUnprocessedWithEmptyTtl("alt", "src", null, 60000L);
    assertTrue(result.isEmpty());
  }

  @Test void testFilterUnprocessedWithTtl() {
    Map<String, String> keys1 = new HashMap<String, String>();
    keys1.put("year", "2020");

    store.markProcessedWithRowCount("alt7", "src", keys1, "target", 0);

    List<Map<String, String>> all = Arrays.asList(keys1);
    Set<Integer> result = store.filterUnprocessedWithTtl("alt7", "src", all, 60000L, 30000L);
    // Should be within TTL
    assertTrue(result.isEmpty());
  }

  @Test void testFilterUnprocessedWithTtlNull() {
    Set<Integer> result = store.filterUnprocessedWithTtl("alt", "src", null, 60000L, 30000L);
    assertTrue(result.isEmpty());
  }

  // ===== Upsert behavior (ON CONFLICT) =====

  @Test void testMarkProcessedUpsert() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");

    store.markProcessedWithRowCount("alt8", "src", keys, "target1", 100);
    assertTrue(store.isProcessed("alt8", "src", keys));

    // Re-mark with different row count
    store.markProcessedWithRowCount("alt8", "src", keys, "target2", 200);
    assertTrue(store.isProcessed("alt8", "src", keys));
  }
}
