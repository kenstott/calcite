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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for DuckDBPartitionStatusStore covering CRUD operations,
 * partition tracking, bulk filtering, and JSON utilities.
 */
@Tag("unit")
public class DuckDBPartitionStatusStoreCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  private DuckDBPartitionStatusStore store;
  private String baseDir;

  @BeforeEach
  void setUp() throws Exception {
    // Use a unique subdirectory for each test to avoid data leakage
    // and avoid closing stores from other concurrently-running test classes.
    baseDir = new File(tempDir.toFile(), "store_" + System.nanoTime()).getAbsolutePath();
    new File(baseDir).mkdirs();
    store = DuckDBPartitionStatusStore.getInstance(baseDir);
    assertNotNull(store);
  }

  @AfterEach
  void tearDown() throws Exception {
    // Only close our own store instance — do NOT clear the global OPEN_STORES map,
    // as that would destroy connections used by concurrently-running test classes.
    closeCurrentStore();
  }

  /**
   * Close only the current test's store instance and remove it from OPEN_STORES.
   */
  private void closeCurrentStore() {
    if (store != null) {
      try {
        store.close();
      } catch (Exception ignored) {
        // ignore
      }
      store = null;
    }
  }

  /**
   * Get a fresh store instance with a new unique directory to avoid data leakage.
   */
  private DuckDBPartitionStatusStore freshStore() throws Exception {
    closeCurrentStore();
    baseDir = new java.io.File(tempDir.toFile(), "store_" + System.nanoTime()).getAbsolutePath();
    new java.io.File(baseDir).mkdirs();
    return DuckDBPartitionStatusStore.getInstance(baseDir);
  }

  @Test void testMarkProcessedAndCheck() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = new LinkedHashMap<>();
    keyValues.put("year", "2024");
    keyValues.put("state", "CA");

    assertFalse(store.isProcessed("alt1", "source1", keyValues));

    store.markProcessed("alt1", "source1", keyValues, "/path/to/file.parquet");

    assertTrue(store.isProcessed("alt1", "source1", keyValues));
  }

  @Test void testMarkProcessedWithRowCount() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "100");

    store.markProcessedWithRowCount("alt2", "source2", keyValues, "/output.parquet", 42);

    assertTrue(store.isProcessed("alt2", "source2", keyValues));
  }

  @Test void testMarkProcessedWithError() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "200");

    store.markProcessedWithError("alt3", "source3", keyValues,
        "/output.parquet", "Connection timeout");

    // Should still be considered processed (error state is tracked)
    assertTrue(store.isProcessed("alt3", "source3", keyValues));
  }

  @Test void testIsProcessedReturnsFalseForUnknown() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "999");
    assertFalse(store.isProcessed("unknown_alt", "unknown_src", keyValues));
  }

  @Test void testIsProcessedWithTtl() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "300");

    store.markProcessed("alt_ttl", "source_ttl", keyValues, "/output.parquet");

    // Within TTL should return true
    assertTrue(
        store.isProcessedWithTtl("alt_ttl", "source_ttl",
        keyValues, 60000)); // 60 second TTL

    // Very short TTL that has already expired
    Thread.sleep(50);
    assertFalse(
        store.isProcessedWithTtl("alt_ttl", "source_ttl",
        keyValues, 1)); // 1ms TTL - should be expired
  }

  @Test void testIsProcessedWithEmptyTtl() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "400");

    // Mark as processed with 0 rows (empty result)
    store.markProcessedWithRowCount("alt_empty", "source_empty",
        keyValues, "/output.parquet", 0);

    // Within TTL should return true
    assertTrue(
        store.isProcessedWithEmptyTtl("alt_empty", "source_empty",
        keyValues, 60000));

    // With very short TTL
    Thread.sleep(50);
    assertFalse(
        store.isProcessedWithEmptyTtl("alt_empty", "source_empty",
        keyValues, 1));
  }

  @Test void testIsProcessedWithEmptyTtlNonEmptyResult() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "401");

    // Mark as processed with non-zero rows
    store.markProcessedWithRowCount("alt_nonempty", "source_nonempty",
        keyValues, "/output.parquet", 100);

    // Non-empty results should always be considered processed regardless of TTL
    assertTrue(
        store.isProcessedWithEmptyTtl("alt_nonempty", "source_nonempty",
        keyValues, 1));
  }

  @Test void testGetProcessedKeyValues() throws Exception {
    store = freshStore();
    Map<String, String> kv1 = new LinkedHashMap<>();
    kv1.put("year", "2023");

    store.markProcessed("alt_keys_gpkv", "source_keys", kv1, "/out1.parquet");

    // Verify at least one processed key value is found
    Set<Map<String, String>> processed = store.getProcessedKeyValues("alt_keys_gpkv");
    assertNotNull(processed);
    assertTrue(processed.size() >= 1, "Should find at least 1 processed key: " + processed);
  }

  @Test void testInvalidate() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "500");

    store.markProcessed("alt_inv", "source_inv", keyValues, "/output.parquet");
    assertTrue(store.isProcessed("alt_inv", "source_inv", keyValues));

    store.invalidate("alt_inv", keyValues);
    assertFalse(store.isProcessed("alt_inv", "source_inv", keyValues));
  }

  @Test void testInvalidateAll() throws Exception {
    store = freshStore();
    Map<String, String> kv1 = Collections.singletonMap("id", "601");
    Map<String, String> kv2 = Collections.singletonMap("id", "602");

    store.markProcessed("alt_all", "source_all", kv1, "/out1.parquet");
    store.markProcessed("alt_all", "source_all", kv2, "/out2.parquet");

    assertTrue(store.isProcessed("alt_all", "source_all", kv1));
    assertTrue(store.isProcessed("alt_all", "source_all", kv2));

    store.invalidateAll("alt_all");

    assertFalse(store.isProcessed("alt_all", "source_all", kv1));
    assertFalse(store.isProcessed("alt_all", "source_all", kv2));
  }

  @Test void testFilterUnprocessed() throws Exception {
    store = freshStore();
    List<Map<String, String>> combinations = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Map<String, String> kv = Collections.singletonMap("id", String.valueOf(i));
      combinations.add(kv);
    }

    // Mark some as processed
    store.markProcessed("alt_filter", "source_filter",
        combinations.get(1), "/out.parquet");
    store.markProcessed("alt_filter", "source_filter",
        combinations.get(3), "/out.parquet");

    Set<Integer> unprocessed =
        store.filterUnprocessed("alt_filter", "source_filter", combinations);

    assertNotNull(unprocessed);
    // Indices 0, 2, 4 should be unprocessed
    assertEquals(3, unprocessed.size());
    assertTrue(unprocessed.contains(0));
    assertTrue(unprocessed.contains(2));
    assertTrue(unprocessed.contains(4));
    assertFalse(unprocessed.contains(1));
    assertFalse(unprocessed.contains(3));
  }

  @Test void testFilterUnprocessedEmptyList() {
    Set<Integer> result =
        store.filterUnprocessed("alt_empty_list", "source", Collections.emptyList());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testFilterUnprocessedNullList() {
    Set<Integer> result =
        store.filterUnprocessed("alt_null_list", "source", null);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testUpdateExistingEntry() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "700");

    // Mark as processed
    store.markProcessedWithRowCount("alt_update", "source_update",
        keyValues, "/out1.parquet", 10);
    assertTrue(store.isProcessed("alt_update", "source_update", keyValues));

    // Update with new data
    store.markProcessedWithRowCount("alt_update", "source_update",
        keyValues, "/out2.parquet", 20);
    assertTrue(store.isProcessed("alt_update", "source_update", keyValues));
  }

  @Test void testEmptyKeyValues() throws Exception {
    store = freshStore();
    Map<String, String> emptyKv = Collections.emptyMap();

    store.markProcessed("alt_empty_kv", "source", emptyKv, "/out.parquet");
    assertTrue(store.isProcessed("alt_empty_kv", "source", emptyKv));
  }

  @Test void testSpecialCharactersInKeyValues() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = new LinkedHashMap<>();
    keyValues.put("name", "O'Brien");
    keyValues.put("path", "/data/file test.csv");

    store.markProcessed("alt_special", "source_special",
        keyValues, "/out.parquet");
    assertTrue(store.isProcessed("alt_special", "source_special", keyValues));
  }

  @Test void testGetInstanceReturnsSameStore() throws Exception {
    String dir = new File(tempDir.toFile(), "singleton_" + System.nanoTime()).getAbsolutePath();
    new File(dir).mkdirs();

    DuckDBPartitionStatusStore store1 = DuckDBPartitionStatusStore.getInstance(dir);
    DuckDBPartitionStatusStore store2 = DuckDBPartitionStatusStore.getInstance(dir);

    // Should be the same instance
    assertTrue(store1 == store2);

    // Clean up the store we just created
    store1.close();
  }

  @Test void testPipelineTrackerMethods() throws Exception {
    store = freshStore();

    // Test PipelineTracker default methods
    store.markComplete("accession123", "metadata", "staging", 50);
    assertTrue(store.isComplete("accession123", "metadata", "staging"));

    store.markError("accession456", "facts", "materialized", "Parse error");
    // isComplete excludes errored entries (SQL: AND COALESCE(error_status, FALSE) = FALSE)
    assertFalse(store.isComplete("accession456", "facts", "materialized"));

    store.markCleared("accession123", "metadata", "staging");
    assertFalse(store.isComplete("accession123", "metadata", "staging"));
  }

  @Test void testLongErrorMessage() throws Exception {
    store = freshStore();
    Map<String, String> keyValues = Collections.singletonMap("id", "800");

    // Create an error message longer than 1000 characters
    StringBuilder longMsg = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      longMsg.append("Error detail ").append(i).append(". ");
    }

    store.markProcessedWithError("alt_long_err", "source",
        keyValues, "/out.parquet", longMsg.toString());

    // Should still be processed
    assertTrue(store.isProcessed("alt_long_err", "source", keyValues));
  }
}
