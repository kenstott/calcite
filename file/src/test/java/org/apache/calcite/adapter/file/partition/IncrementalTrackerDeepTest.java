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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for IncrementalTracker interface, including
 * NOOP implementation, default methods, CachedCompletion, and static methods.
 */
@Tag("unit")
public class IncrementalTrackerDeepTest {

  // ===== NOOP implementation =====

  @Test void testNoopIsProcessed() {
    assertFalse(IncrementalTracker.NOOP.isProcessed("alt", "src", Collections.<String, String>emptyMap()));
  }

  @Test void testNoopIsProcessedWithTtl() {
    assertFalse(
        IncrementalTracker.NOOP.isProcessedWithTtl("alt", "src",
        Collections.<String, String>emptyMap(), 1000));
  }

  @Test void testNoopMarkProcessedDoesNotThrow() {
    IncrementalTracker.NOOP.markProcessed("alt", "src",
        Collections.<String, String>emptyMap(), "pattern");
  }

  @Test void testNoopGetProcessedKeyValues() {
    assertTrue(IncrementalTracker.NOOP.getProcessedKeyValues("alt").isEmpty());
  }

  @Test void testNoopInvalidate() {
    // Should not throw
    IncrementalTracker.NOOP.invalidate("alt", Collections.<String, String>emptyMap());
  }

  @Test void testNoopInvalidateAll() {
    // Should not throw
    IncrementalTracker.NOOP.invalidateAll("alt");
  }

  @Test void testNoopFilterUnprocessedReturnsAllIndices() {
    List<Map<String, String>> combos = new ArrayList<>();
    combos.add(Collections.singletonMap("year", "2020"));
    combos.add(Collections.singletonMap("year", "2021"));
    combos.add(Collections.singletonMap("year", "2022"));

    Set<Integer> result = IncrementalTracker.NOOP.filterUnprocessed("alt", "src", combos);
    assertEquals(3, result.size());
    assertTrue(result.contains(0));
    assertTrue(result.contains(1));
    assertTrue(result.contains(2));
  }

  @Test void testNoopIsTableComplete() {
    assertFalse(IncrementalTracker.NOOP.isTableComplete("pipeline", "sig"));
  }

  @Test void testNoopMarkTableComplete() {
    // Should not throw
    IncrementalTracker.NOOP.markTableComplete("pipeline", "sig");
  }

  @Test void testNoopInvalidateTableCompletion() {
    // Should not throw
    IncrementalTracker.NOOP.invalidateTableCompletion("pipeline");
  }

  @Test void testNoopClearAllCompletions() {
    // Should not throw
    IncrementalTracker.NOOP.clearAllCompletions();
  }

  // ===== Default methods =====

  @Test void testDefaultMarkProcessedWithRowCount() {
    // Default delegates to markProcessed
    final boolean[] called = {false};
    IncrementalTracker tracker = createMinimalTracker(called);
    tracker.markProcessedWithRowCount("alt", "src",
        Collections.<String, String>emptyMap(), "pattern", 100);
    assertTrue(called[0]);
  }

  @Test void testDefaultIsProcessedWithEmptyTtl() {
    // Default falls back to isProcessed
    IncrementalTracker tracker = createMinimalTracker(new boolean[]{false});
    boolean result =
        tracker.isProcessedWithEmptyTtl("alt", "src", Collections.<String, String>emptyMap(), 1000);
    // NOOP-based returns false
    assertFalse(result);
  }

  @Test void testDefaultFilterUnprocessedWithEmptyTtl() {
    List<Map<String, String>> combos = new ArrayList<>();
    combos.add(Collections.singletonMap("year", "2020"));
    // Default falls back to filterUnprocessed
    Set<Integer> result =
        IncrementalTracker.NOOP.filterUnprocessedWithEmptyTtl("alt", "src", combos, 1000);
    assertEquals(1, result.size());
  }

  @Test void testDefaultMarkProcessedWithError() {
    final boolean[] called = {false};
    IncrementalTracker tracker = createMinimalTracker(called);
    // Default delegates to markProcessedWithRowCount with 0 rows
    tracker.markProcessedWithError("alt", "src",
        Collections.<String, String>emptyMap(), "pattern", "error msg");
    assertTrue(called[0]);
  }

  @Test void testDefaultFilterUnprocessedWithTtl() {
    List<Map<String, String>> combos = new ArrayList<>();
    combos.add(Collections.singletonMap("year", "2020"));
    // Default falls back to filterUnprocessedWithEmptyTtl
    Set<Integer> result =
        IncrementalTracker.NOOP.filterUnprocessedWithTtl("alt", "src", combos, 1000, 500);
    assertEquals(1, result.size());
  }

  @Test void testDefaultMarkTableCompleteWithConfig() {
    // Default delegates to markTableComplete
    IncrementalTracker.NOOP.markTableCompleteWithConfig("pipe", "cfg", "sig", 100);
    // Just verify it doesn't throw
  }

  @Test void testDefaultGetCachedCompletion() {
    // Default returns null
    assertNull(IncrementalTracker.NOOP.getCachedCompletion("pipeline"));
  }

  @Test void testDefaultPreloadAllCompletions() {
    // Default is no-op
    IncrementalTracker.NOOP.preloadAllCompletions();
  }

  @Test void testDefaultMarkTableCompleteWithSourceWatermark() {
    // Default delegates to markTableCompleteWithConfig
    IncrementalTracker.NOOP.markTableCompleteWithSourceWatermark(
        "pipe", "cfg", "sig", 100, 999L);
  }

  @Test void testDefaultIsSourceFilesModified() {
    // Default calls getCachedCompletion which returns null
    // null -> return true (never processed)
    assertTrue(IncrementalTracker.NOOP.isSourceFilesModified("pipe", 1000));
  }

  // ===== CachedCompletion =====

  @Test void testCachedCompletionSimpleConstructor() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100);
    assertEquals("cfg", cc.configHash);
    assertEquals("sig", cc.signature);
    assertEquals(100, cc.rowCount);
    assertTrue(cc.completedAt > 0);
    assertEquals(0, cc.sourceFileWatermark);
  }

  @Test void testCachedCompletionWithTimestamp() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 5000L);
    assertEquals(5000L, cc.completedAt);
    assertEquals(0, cc.sourceFileWatermark);
  }

  @Test void testCachedCompletionWithWatermark() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 5000L, 7000L);
    assertEquals(5000L, cc.completedAt);
    assertEquals(7000L, cc.sourceFileWatermark);
  }

  @Test void testIsEmptyResultTtlExpiredNonEmpty() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100);
    assertFalse(cc.isEmptyResultTtlExpired(1000)); // rowCount > 0, never expires
  }

  @Test void testIsEmptyResultTtlExpiredNoTtl() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 0);
    assertFalse(cc.isEmptyResultTtlExpired(0)); // ttl <= 0
    assertFalse(cc.isEmptyResultTtlExpired(-1)); // ttl <= 0
  }

  @Test void testIsEmptyResultTtlExpiredRecent() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 0,
            System.currentTimeMillis());
    // Very recent, large TTL -> not expired
    assertFalse(cc.isEmptyResultTtlExpired(86400000));
  }

  @Test void testIsEmptyResultTtlExpiredOld() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 0, 1000L);
    // Very old, any TTL -> expired
    assertTrue(cc.isEmptyResultTtlExpired(1));
  }

  @Test void testIsSourceFilesModifiedNoWatermark() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 5000L, 0);
    assertFalse(cc.isSourceFilesModified(9999L)); // watermark disabled
  }

  @Test void testIsSourceFilesModifiedNoCurrentWatermark() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 5000L, 7000L);
    assertFalse(cc.isSourceFilesModified(0)); // current=0 -> disabled
  }

  @Test void testIsSourceFilesModifiedYes() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 5000L, 7000L);
    assertTrue(cc.isSourceFilesModified(8000L)); // 8000 > 7000
  }

  @Test void testIsSourceFilesModifiedNo() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 5000L, 7000L);
    assertFalse(cc.isSourceFilesModified(6000L)); // 6000 <= 7000
  }

  // ===== computeDimensionSignature =====

  @Test void testComputeDimensionSignatureNull() {
    assertEquals("empty", IncrementalTracker.computeDimensionSignature(null));
  }

  @Test void testComputeDimensionSignatureEmpty() {
    assertEquals("empty", IncrementalTracker.computeDimensionSignature(Collections.<Map<String, String>>emptyList()));
  }

  @Test void testComputeDimensionSignatureSingleCombo() {
    List<Map<String, String>> combos = new ArrayList<>();
    Map<String, String> combo = new LinkedHashMap<>();
    combo.put("year", "2020");
    combos.add(combo);

    String sig = IncrementalTracker.computeDimensionSignature(combos);
    assertNotNull(sig);
    assertTrue(sig.startsWith("count:1"));
    assertTrue(sig.contains("|year"));
    assertTrue(sig.contains("|hash:"));
  }

  @Test void testComputeDimensionSignatureMultipleCombos() {
    List<Map<String, String>> combos = new ArrayList<>();
    Map<String, String> c1 = new LinkedHashMap<>();
    c1.put("year", "2020");
    c1.put("month", "01");
    combos.add(c1);
    Map<String, String> c2 = new LinkedHashMap<>();
    c2.put("year", "2020");
    c2.put("month", "02");
    combos.add(c2);

    String sig = IncrementalTracker.computeDimensionSignature(combos);
    assertTrue(sig.startsWith("count:2"));
  }

  @Test void testComputeDimensionSignatureDifferentValuesDifferentSig() {
    List<Map<String, String>> combos1 = new ArrayList<>();
    combos1.add(Collections.singletonMap("year", "2020"));

    List<Map<String, String>> combos2 = new ArrayList<>();
    combos2.add(Collections.singletonMap("year", "2021"));

    String sig1 = IncrementalTracker.computeDimensionSignature(combos1);
    String sig2 = IncrementalTracker.computeDimensionSignature(combos2);
    assertNotEquals(sig1, sig2);
  }

  // ===== Helper =====

  private IncrementalTracker createMinimalTracker(final boolean[] markProcessedCalled) {
    return new IncrementalTracker() {
      @Override public boolean isProcessed(String alternateName, String sourceTable,
          Map<String, String> keyValues) {
        return false;
      }

      @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
          Map<String, String> keyValues, long ttlMillis) {
        return false;
      }

      @Override public void markProcessed(String alternateName, String sourceTable,
          Map<String, String> keyValues, String targetPattern) {
        markProcessedCalled[0] = true;
      }

      @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
        return Collections.emptySet();
      }

      @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
      }

      @Override public void invalidateAll(String alternateName) {
      }

      @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
          List<Map<String, String>> allCombinations) {
        return Collections.emptySet();
      }

      @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
        return false;
      }

      @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
      }

      @Override public void invalidateTableCompletion(String pipelineName) {
      }

      @Override public void clearAllCompletions() {
      }
    };
  }
}
