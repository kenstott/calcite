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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PipelineTracker} interface default methods and NoopPipelineTracker.
 */
@Tag("unit")
public class PipelineTrackerTest {

  // ===== NOOP_PIPELINE =====

  @Test void testNoopPipelineIsCompleteReturnsFalse() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isComplete("key1", "table1", "staging"));
  }

  @Test void testNoopPipelineMarkCompleteDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markComplete("key1", "table1", "staging", 100);
  }

  @Test void testNoopPipelineMarkErrorDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markError("key1", "table1", "staging", "error msg");
  }

  @Test void testNoopPipelineGetCompletedTablesReturnsEmpty() {
    Set<String> result = PipelineTracker.NOOP_PIPELINE.getCompletedTables("key1", "staging");
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testNoopPipelineIsProcessedReturnsFalse() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isProcessed(
        "alt", "source", Collections.singletonMap("year", "2023")));
  }

  @Test void testNoopPipelineIsProcessedWithTtlReturnsFalse() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isProcessedWithTtl(
        "alt", "source", Collections.singletonMap("year", "2023"), 60000));
  }

  @Test void testNoopPipelineFilterUnprocessedReturnsAll() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(Collections.singletonMap("year", "2020"));
    combos.add(Collections.singletonMap("year", "2021"));

    Set<Integer> result =
        PipelineTracker.NOOP_PIPELINE.filterUnprocessed("alt", "source", combos);

    assertEquals(2, result.size());
    assertTrue(result.contains(0));
    assertTrue(result.contains(1));
  }

  @Test void testNoopPipelineIsTableCompleteReturnsFalse() {
    assertFalse(PipelineTracker.NOOP_PIPELINE.isTableComplete("pipeline", "sig"));
  }

  @Test void testNoopPipelineMarkTableCompleteDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markTableComplete("pipeline", "sig");
  }

  @Test void testNoopPipelineInvalidateTableCompletionDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.invalidateTableCompletion("pipeline");
  }

  @Test void testNoopPipelineClearAllCompletionsDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.clearAllCompletions();
  }

  @Test void testNoopPipelineGetProcessedKeyValuesReturnsEmpty() {
    Set<Map<String, String>> result =
        PipelineTracker.NOOP_PIPELINE.getProcessedKeyValues("alt");
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testNoopPipelineInvalidateDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.invalidate("alt", Collections.singletonMap("year", "2023"));
  }

  @Test void testNoopPipelineInvalidateAllDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.invalidateAll("alt");
  }

  @Test void testNoopPipelineMarkProcessedDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markProcessed(
        "alt", "source", Collections.singletonMap("year", "2023"), "target");
  }

  // ===== Default methods on interface =====

  @Test void testDefaultIsFullyCompleteAllFalse() {
    // NOOP always returns false for isComplete, so isFullyComplete should be false
    Set<String> requiredTables = new HashSet<String>(Arrays.asList("t1", "t2", "t3"));
    assertFalse(PipelineTracker.NOOP_PIPELINE.isFullyComplete("key1", "staging", requiredTables));
  }

  @Test void testDefaultGetSourceKeysForPhaseReturnsEmpty() {
    Set<String> keys = PipelineTracker.NOOP_PIPELINE.getSourceKeysForPhase("staging");
    assertNotNull(keys);
    assertTrue(keys.isEmpty());
  }

  @Test void testDefaultPreloadAllDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.preloadAll("staging");
  }

  @Test void testDefaultBulkGetCompletedTables() {
    List<String> sourceKeys = Arrays.asList("key1", "key2", "key3");
    Map<String, Set<String>> result =
        PipelineTracker.NOOP_PIPELINE.bulkGetCompletedTables(sourceKeys, "staging");

    assertNotNull(result);
    // NOOP returns empty sets, so the result map should be empty
    assertTrue(result.isEmpty());
  }

  @Test void testDefaultMarkClearedDoesNotThrow() {
    PipelineTracker.NOOP_PIPELINE.markCleared("key1", "table1", "staging");
  }

  // ===== Test isFullyComplete with a custom tracker =====

  @Test void testIsFullyCompleteWithAllComplete() {
    // Create a tracker that returns true for specific combinations
    PipelineTracker tracker = new PipelineTracker.NoopPipelineTracker() {
      @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
        return "staging".equals(phase) && "key1".equals(sourceKey);
      }
    };

    Set<String> required = new HashSet<String>(Arrays.asList("t1", "t2"));
    assertTrue(tracker.isFullyComplete("key1", "staging", required));
  }

  @Test void testIsFullyCompleteWithOneMissing() {
    PipelineTracker tracker = new PipelineTracker.NoopPipelineTracker() {
      @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
        return "t1".equals(tableName);  // Only t1 is complete
      }
    };

    Set<String> required = new HashSet<String>(Arrays.asList("t1", "t2"));
    assertFalse(tracker.isFullyComplete("key1", "staging", required));
  }

  @Test void testIsFullyCompleteEmptyRequired() {
    Set<String> required = new HashSet<String>();
    // Empty required = all are trivially complete
    assertTrue(PipelineTracker.NOOP_PIPELINE.isFullyComplete("key1", "staging", required));
  }

  // ===== Test bulkGetCompletedTables with custom tracker =====

  @Test void testBulkGetCompletedTablesWithData() {
    PipelineTracker tracker = new PipelineTracker.NoopPipelineTracker() {
      @Override public Set<String> getCompletedTables(String sourceKey, String phase) {
        if ("key1".equals(sourceKey)) {
          Set<String> tables = new HashSet<String>();
          tables.add("metadata");
          tables.add("facts");
          return tables;
        }
        return Collections.emptySet();
      }
    };

    List<String> sourceKeys = Arrays.asList("key1", "key2");
    Map<String, Set<String>> result =
        tracker.bulkGetCompletedTables(sourceKeys, "staging");

    assertEquals(1, result.size());
    assertTrue(result.containsKey("key1"));
    assertEquals(2, result.get("key1").size());
    assertFalse(result.containsKey("key2"));
  }
}
