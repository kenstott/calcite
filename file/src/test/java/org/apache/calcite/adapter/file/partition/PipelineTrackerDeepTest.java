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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for PipelineTracker interface and its NoopPipelineTracker
 * implementation, covering all default methods and bulk operations.
 */
@Tag("unit")
public class PipelineTrackerDeepTest {

  // ===== NoopPipelineTracker =====

  @Test void testNoopIsComplete() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isComplete("key1", "table1", "staging"));
  }

  @Test void testNoopMarkCompleteDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markComplete("key1", "table1", "staging", 100);
  }

  @Test void testNoopMarkErrorDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markError("key1", "table1", "staging", "error msg");
  }

  @Test void testNoopGetCompletedTablesEmpty() {
    Set<String> tables = PipelineTracker.NOOP_PIPELINE.getCompletedTables("key1", "staging");
    assertTrue(tables.isEmpty());
  }

  @Test void testNoopIsProcessed() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isProcessed("alt", "src",
        Collections.<String, String>emptyMap()));
  }

  @Test void testNoopIsProcessedWithTtl() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isProcessedWithTtl("alt", "src",
        Collections.<String, String>emptyMap(), 1000));
  }

  @Test void testNoopMarkProcessedDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markProcessed("alt", "src",
        Collections.<String, String>emptyMap(), "pattern");
  }

  @Test void testNoopGetProcessedKeyValues() {
    assertTrue(PipelineTracker.NOOP_PIPELINE.getProcessedKeyValues("alt").isEmpty());
  }

  @Test void testNoopInvalidate() {
    PipelineTracker.NOOP_PIPELINE.invalidate("alt", Collections.<String, String>emptyMap());
  }

  @Test void testNoopInvalidateAll() {
    PipelineTracker.NOOP_PIPELINE.invalidateAll("alt");
  }

  @Test void testNoopFilterUnprocessedReturnsAll() {
    java.util.List<Map<String, String>> combos = new java.util.ArrayList<>();
    combos.add(Collections.singletonMap("k", "v1"));
    combos.add(Collections.singletonMap("k", "v2"));

    Set<Integer> result = PipelineTracker.NOOP_PIPELINE.filterUnprocessed("alt", "src", combos);
    assertEquals(2, result.size());
    assertTrue(result.contains(0));
    assertTrue(result.contains(1));
  }

  @Test void testNoopIsTableComplete() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isTableComplete("pipe", "sig"));
  }

  @Test void testNoopMarkTableCompleteDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markTableComplete("pipe", "sig");
  }

  @Test void testNoopInvalidateTableCompletion() {
    PipelineTracker.NOOP_PIPELINE.invalidateTableCompletion("pipe");
  }

  @Test void testNoopClearAllCompletions() {
    PipelineTracker.NOOP_PIPELINE.clearAllCompletions();
  }

  // ===== Default method: isFullyComplete =====

  @Test void testIsFullyCompleteAllTrue() {
    // Create a tracker that says everything is complete
    PipelineTracker tracker = new PipelineTracker.NoopPipelineTracker() {
      @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
        return true;
      }
    };

    Set<String> required = new HashSet<>(Arrays.asList("table1", "table2", "table3"));
    assertTrue(tracker.isFullyComplete("key1", "staging", required));
  }

  @Test void testIsFullyCompletePartialFails() {
    // Create a tracker that only completes "table1"
    PipelineTracker tracker = new PipelineTracker.NoopPipelineTracker() {
      @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
        return "table1".equals(tableName);
      }
    };

    Set<String> required = new HashSet<>(Arrays.asList("table1", "table2"));
    assertFalse(tracker.isFullyComplete("key1", "staging", required));
  }

  @Test void testIsFullyCompleteEmptyRequired() {
    assertTrue(PipelineTracker.NOOP_PIPELINE.isFullyComplete("key1", "staging",
        Collections.<String>emptySet()));
  }

  // ===== Default method: getSourceKeysForPhase =====

  @Test void testGetSourceKeysForPhaseDefault() {
    Set<String> keys = PipelineTracker.NOOP_PIPELINE.getSourceKeysForPhase("staging");
    assertTrue(keys.isEmpty());
  }

  // ===== Default method: preloadAll =====

  @Test void testPreloadAllDefault() {
    // Should not throw
    PipelineTracker.NOOP_PIPELINE.preloadAll("staging");
  }

  // ===== Default method: bulkGetCompletedTables =====

  @Test void testBulkGetCompletedTablesDefault() {
    Map<String, Set<String>> result = PipelineTracker.NOOP_PIPELINE.bulkGetCompletedTables(
        Arrays.asList("key1", "key2", "key3"), "staging");
    assertTrue(result.isEmpty()); // NOOP returns empty sets
  }

  @Test void testBulkGetCompletedTablesWithData() {
    // Create a tracker that returns data for some keys
    PipelineTracker tracker = new PipelineTracker.NoopPipelineTracker() {
      @Override public Set<String> getCompletedTables(String sourceKey, String phase) {
        if ("key1".equals(sourceKey)) {
          return new HashSet<>(Arrays.asList("table1", "table2"));
        }
        return Collections.emptySet();
      }
    };

    Map<String, Set<String>> result =
        tracker.bulkGetCompletedTables(Arrays.asList("key1", "key2"), "staging");
    assertEquals(1, result.size());
    assertTrue(result.containsKey("key1"));
    assertEquals(2, result.get("key1").size());
    assertFalse(result.containsKey("key2")); // Empty = absent
  }

  // ===== Default method: markCleared =====

  @Test void testMarkClearedDefault() {
    // Should not throw
    PipelineTracker.NOOP_PIPELINE.markCleared("key1", "table1", "staging");
  }
}
