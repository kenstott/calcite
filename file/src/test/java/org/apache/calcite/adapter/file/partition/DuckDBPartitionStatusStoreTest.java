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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DuckDBPartitionStatusStore}.
 */
@Tag("unit")
public class DuckDBPartitionStatusStoreTest {

  @TempDir
  Path tempDir;

  private DuckDBPartitionStatusStore store;
  private String baseDir;

  @BeforeEach
  void setUp() throws Exception {
    // Use a unique subdirectory per test to ensure data isolation and avoid
    // interfering with stores from concurrently-running test classes.
    baseDir = new File(tempDir.toFile(), "store_" + System.nanoTime()).getAbsolutePath();
    new File(baseDir).mkdirs();
    store = DuckDBPartitionStatusStore.getInstance(baseDir);
  }

  @AfterEach
  void tearDown() throws Exception {
    // Only close our own store instance — do NOT clear the global OPEN_STORES map,
    // as that would destroy connections used by concurrently-running test classes.
    if (store != null) {
      try {
        store.close();
      } catch (Exception ignored) {
        // ignore
      }
    }
  }

  // ===== Basic IncrementalTracker operations =====

  @Test void testIsProcessedFalseWhenNotTracked() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    assertFalse(store.isProcessed("alt1", "source_table", keyValues));
  }

  @Test void testMarkAndIsProcessed() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    store.markProcessed("alt1", "source_table", keyValues, "target/pattern");

    assertTrue(store.isProcessed("alt1", "source_table", keyValues));
  }

  @Test void testMarkProcessedMultipleKeys() {
    Map<String, String> keyValues = new LinkedHashMap<String, String>();
    keyValues.put("year", "2023");
    keyValues.put("geo", "US");
    store.markProcessed("alt1", "source_table", keyValues, "target/pattern");

    assertTrue(store.isProcessed("alt1", "source_table", keyValues));
  }

  @Test void testDifferentAlternatesAreSeparate() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    store.markProcessed("alt1", "source_table", keyValues, "target1");

    assertTrue(store.isProcessed("alt1", "source_table", keyValues));
    assertFalse(store.isProcessed("alt2", "source_table", keyValues));
  }

  // ===== Invalidation =====

  @Test void testInvalidateSingle() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    store.markProcessed("alt1", "source_table", keyValues, "target");

    assertTrue(store.isProcessed("alt1", "source_table", keyValues));

    store.invalidate("alt1", keyValues);

    assertFalse(store.isProcessed("alt1", "source_table", keyValues));
  }

  @Test void testInvalidateAll() {
    store.markProcessed("alt1", "source", Collections.singletonMap("year", "2020"), "t");
    store.markProcessed("alt1", "source", Collections.singletonMap("year", "2021"), "t");

    store.invalidateAll("alt1");

    assertFalse(store.isProcessed("alt1", "source", Collections.singletonMap("year", "2020")));
    assertFalse(store.isProcessed("alt1", "source", Collections.singletonMap("year", "2021")));
  }

  // ===== getProcessedKeyValues =====

  @Test void testGetProcessedKeyValues() {
    store.markProcessed("alt1", "source", Collections.singletonMap("year", "2020"), "t");
    store.markProcessed("alt1", "source", Collections.singletonMap("year", "2021"), "t");

    Set<Map<String, String>> processed = store.getProcessedKeyValues("alt1");

    assertNotNull(processed);
    assertEquals(2, processed.size());
  }

  @Test void testGetProcessedKeyValuesEmpty() {
    Set<Map<String, String>> processed = store.getProcessedKeyValues("nonexistent");

    assertNotNull(processed);
    assertTrue(processed.isEmpty());
  }

  // ===== filterUnprocessed =====

  @Test void testFilterUnprocessed() {
    store.markProcessed("alt1", "source", Collections.singletonMap("year", "2020"), "t");

    List<Map<String, String>> all = new ArrayList<Map<String, String>>();
    all.add(Collections.singletonMap("year", "2020"));
    all.add(Collections.singletonMap("year", "2021"));
    all.add(Collections.singletonMap("year", "2022"));

    Set<Integer> unprocessed = store.filterUnprocessed("alt1", "source", all);

    assertEquals(2, unprocessed.size());
    assertFalse(unprocessed.contains(0)); // 2020 already processed
    assertTrue(unprocessed.contains(1));
    assertTrue(unprocessed.contains(2));
  }

  @Test void testFilterUnprocessedEmptyList() {
    Set<Integer> unprocessed =
        store.filterUnprocessed("alt1", "source", Collections.<Map<String, String>>emptyList());

    assertNotNull(unprocessed);
    assertTrue(unprocessed.isEmpty());
  }

  // ===== TTL-based processing =====

  @Test void testIsProcessedWithTtlNotExpired() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    store.markProcessed("alt1", "source", keyValues, "t");

    // Just marked, should be within TTL of 1 hour
    assertTrue(store.isProcessedWithTtl("alt1", "source", keyValues, 3600000));
  }

  @Test void testIsProcessedWithTtlNotProcessed() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    assertFalse(store.isProcessedWithTtl("alt1", "source", keyValues, 3600000));
  }

  // ===== Table completion =====

  @Test void testTableCompletionNotComplete() {
    assertFalse(store.isTableComplete("pipeline1", "sig123"));
  }

  @Test void testMarkAndCheckTableComplete() {
    store.markTableComplete("pipeline1", "sig123");

    assertTrue(store.isTableComplete("pipeline1", "sig123"));
    // Different signature should not match
    assertFalse(store.isTableComplete("pipeline1", "different_sig"));
  }

  @Test void testInvalidateTableCompletion() {
    store.markTableComplete("pipeline1", "sig123");
    assertTrue(store.isTableComplete("pipeline1", "sig123"));

    store.invalidateTableCompletion("pipeline1");

    assertFalse(store.isTableComplete("pipeline1", "sig123"));
  }

  @Test void testMarkTableCompleteWithConfig() {
    store.markTableCompleteWithConfig("pipeline1", "cfg:abc", "sig123", 1000);

    assertTrue(store.isTableComplete("pipeline1", "sig123"));

    IncrementalTracker.CachedCompletion cc = store.getCachedCompletion("pipeline1");
    assertNotNull(cc);
    assertEquals("cfg:abc", cc.configHash);
    assertEquals("sig123", cc.signature);
    assertEquals(1000, cc.rowCount);
  }

  @Test void testGetCachedCompletionNotFound() {
    assertNull(store.getCachedCompletion("nonexistent"));
  }

  @Test void testMarkTableCompleteWithSourceWatermark() {
    long watermark = System.currentTimeMillis();
    store.markTableCompleteWithSourceWatermark(
        "pipeline1", "cfg:abc", "sig123", 1000, watermark);

    IncrementalTracker.CachedCompletion cc = store.getCachedCompletion("pipeline1");
    assertNotNull(cc);
    assertEquals(watermark, cc.sourceFileWatermark);
  }

  // ===== clearAllCompletions =====

  @Test void testClearAllCompletions() {
    store.markProcessed("alt1", "source", Collections.singletonMap("year", "2023"), "t");
    store.markTableComplete("pipeline1", "sig123");

    store.clearAllCompletions();

    assertFalse(store.isProcessed("alt1", "source", Collections.singletonMap("year", "2023")));
    assertFalse(store.isTableComplete("pipeline1", "sig123"));
  }

  // ===== markProcessedWithRowCount =====

  @Test void testMarkProcessedWithRowCount() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    store.markProcessedWithRowCount("alt1", "source", keyValues, "target", 500);

    assertTrue(store.isProcessed("alt1", "source", keyValues));
  }

  @Test void testMarkProcessedWithError() {
    Map<String, String> keyValues = Collections.singletonMap("year", "2023");
    store.markProcessedWithError("alt1", "source", keyValues, "target", "some error");

    // Errors are recorded but may or may not count as processed depending on impl
    // The important thing is it does not throw
  }

  // ===== PipelineTracker operations =====

  @Test void testPipelineTrackerIsComplete() {
    assertFalse(store.isComplete("key1", "metadata", "staging"));

    store.markComplete("key1", "metadata", "staging", 100);

    assertTrue(store.isComplete("key1", "metadata", "staging"));
  }

  @Test void testPipelineTrackerMarkError() {
    store.markError("key1", "metadata", "staging", "download failed");

    // Error entries should not count as complete
    assertFalse(store.isComplete("key1", "metadata", "staging"));
  }

  @Test void testPipelineTrackerMarkCleared() {
    store.markComplete("key1", "metadata", "staging", 100);
    assertTrue(store.isComplete("key1", "metadata", "staging"));

    store.markCleared("key1", "metadata", "staging");

    assertFalse(store.isComplete("key1", "metadata", "staging"));
  }

  @Test void testPipelineTrackerGetCompletedTables() {
    store.markComplete("key1", "metadata", "staging", 100);
    store.markComplete("key1", "facts", "staging", 200);
    store.markComplete("key1", "other", "download", 50); // different phase

    Set<String> completed = store.getCompletedTables("key1", "staging");

    assertEquals(2, completed.size());
    assertTrue(completed.contains("metadata"));
    assertTrue(completed.contains("facts"));
  }

  @Test void testPipelineTrackerGetSourceKeysForPhaseDefaultEmpty() {
    // DuckDB store does not override getSourceKeysForPhase; default returns empty
    store.markComplete("key1", "metadata", "staging", 100);
    store.markComplete("key2", "metadata", "staging", 200);

    Set<String> keys = store.getSourceKeysForPhase("staging");

    assertNotNull(keys);
    // Default implementation returns empty set
    assertTrue(keys.isEmpty());
  }
}
