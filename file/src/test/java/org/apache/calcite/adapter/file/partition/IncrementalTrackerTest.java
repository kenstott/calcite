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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link IncrementalTracker} interface, NOOP instance, default methods,
 * and {@link IncrementalTracker.CachedCompletion}.
 */
@Tag("unit")
public class IncrementalTrackerTest {

  // ===== NOOP tracker =====

  @Test void testNoopIsProcessedReturnsFalse() {
    assertFalse(
        IncrementalTracker.NOOP.isProcessed(
        "alt", "source", Collections.singletonMap("year", "2023")));
  }

  @Test void testNoopIsProcessedWithTtlReturnsFalse() {
    assertFalse(
        IncrementalTracker.NOOP.isProcessedWithTtl(
        "alt", "source", Collections.singletonMap("year", "2023"), 60000));
  }

  @Test void testNoopMarkProcessedDoesNotThrow() {
    IncrementalTracker.NOOP.markProcessed(
        "alt", "source", Collections.singletonMap("year", "2023"), "target");
  }

  @Test void testNoopGetProcessedKeyValuesReturnsEmpty() {
    Set<Map<String, String>> result =
        IncrementalTracker.NOOP.getProcessedKeyValues("alt");
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testNoopInvalidateDoesNotThrow() {
    IncrementalTracker.NOOP.invalidate(
        "alt", Collections.singletonMap("year", "2023"));
  }

  @Test void testNoopInvalidateAllDoesNotThrow() {
    IncrementalTracker.NOOP.invalidateAll("alt");
  }

  @Test void testNoopFilterUnprocessedReturnsAllIndices() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(Collections.singletonMap("year", "2020"));
    combos.add(Collections.singletonMap("year", "2021"));
    combos.add(Collections.singletonMap("year", "2022"));

    Set<Integer> result =
        IncrementalTracker.NOOP.filterUnprocessed("alt", "source", combos);

    assertEquals(3, result.size());
    assertTrue(result.contains(0));
    assertTrue(result.contains(1));
    assertTrue(result.contains(2));
  }

  @Test void testNoopIsTableCompleteReturnsFalse() {
    assertFalse(IncrementalTracker.NOOP.isTableComplete("pipeline", "sig123"));
  }

  @Test void testNoopMarkTableCompleteDoesNotThrow() {
    IncrementalTracker.NOOP.markTableComplete("pipeline", "sig123");
  }

  @Test void testNoopInvalidateTableCompletionDoesNotThrow() {
    IncrementalTracker.NOOP.invalidateTableCompletion("pipeline");
  }

  @Test void testNoopClearAllCompletionsDoesNotThrow() {
    IncrementalTracker.NOOP.clearAllCompletions();
  }

  // ===== Per-period completion markers (fix #2) =====

  @Test void testPeriodCompletionKeyYearOnlyNaFills() {
    String key = IncrementalTracker.periodCompletionKey(
        "patents_patent_grants", Collections.singletonMap("year", "2022"));
    // canonical slots are year_quarter_month_week_day_day_of_week, then the pipeline name
    assertEquals("2022_NA_NA_NA_NA_NA_patents_patent_grants", key);
  }

  @Test void testPeriodCompletionKeyYearAndQuarter() {
    Map<String, String> combo = new LinkedHashMap<String, String>();
    combo.put("year", "2026");
    combo.put("quarter", "2026Q2");
    combo.put("geography", "CA"); // non-period dim must be ignored
    assertEquals("2026_2026Q2_NA_NA_NA_NA_t", IncrementalTracker.periodCompletionKey("t", combo));
  }

  @Test void testPeriodCompletionKeyNoPeriodAllNa() {
    assertEquals("NA_NA_NA_NA_NA_NA_t",
        IncrementalTracker.periodCompletionKey("t", Collections.<String, String>emptyMap()));
    assertEquals("NA_NA_NA_NA_NA_NA_t", IncrementalTracker.periodCompletionKey("t", null));
  }

  @Test void testPeriodCompletionKeyDistinctPerPeriod() {
    // The whole point of fix #2: different years => different keys => no cross-period skip.
    String y2025 = IncrementalTracker.periodCompletionKey("p", Collections.singletonMap("year", "2025"));
    String y2022 = IncrementalTracker.periodCompletionKey("p", Collections.singletonMap("year", "2022"));
    assertFalse(y2025.equals(y2022));
  }

  @Test void testHasCanonicalPeriodTrueForYear() {
    assertTrue(IncrementalTracker.hasCanonicalPeriod(Collections.singletonMap("year", "2022")));
  }

  @Test void testHasCanonicalPeriodTrueForQuarter() {
    assertTrue(IncrementalTracker.hasCanonicalPeriod(Collections.singletonMap("quarter", "2026Q2")));
  }

  @Test void testHasCanonicalPeriodFalseForNonPeriodOnly() {
    Map<String, String> combo = new LinkedHashMap<String, String>();
    combo.put("geography", "CA");
    combo.put("frequency", "annual");
    assertFalse(IncrementalTracker.hasCanonicalPeriod(combo));
    assertFalse(IncrementalTracker.hasCanonicalPeriod(null));
    assertFalse(IncrementalTracker.hasCanonicalPeriod(Collections.<String, String>emptyMap()));
  }

  @Test void testHasCanonicalPeriodFalseForEmptyPeriodValue() {
    assertFalse(IncrementalTracker.hasCanonicalPeriod(Collections.singletonMap("year", "")));
  }

  @Test void testNoopPeriodMarkersAreSafeNoOps() {
    Map<String, String> combo = Collections.singletonMap("year", "2099");
    assertFalse(IncrementalTracker.NOOP.isPeriodComplete("p", combo));
    IncrementalTracker.NOOP.markPeriodComplete("p", combo); // must not throw
    IncrementalTracker.NOOP.invalidatePeriod("p", combo);   // must not throw
    // Non-persistent tracker still cannot prove completion after a mark.
    assertFalse(IncrementalTracker.NOOP.isPeriodComplete("p", combo));
  }

  // ===== Default methods =====

  @Test void testDefaultMarkProcessedWithRowCount() {
    // NOOP delegates to markProcessed
    IncrementalTracker.NOOP.markProcessedWithRowCount(
        "alt", "source", Collections.singletonMap("year", "2023"), "target", 100);
    // Should not throw
  }

  @Test void testDefaultIsProcessedWithEmptyTtl() {
    // Default delegates to isProcessed
    boolean result =
        IncrementalTracker.NOOP.isProcessedWithEmptyTtl("alt", "source", Collections.singletonMap("year", "2023"), 60000);
    assertFalse(result);
  }

  @Test void testDefaultFilterUnprocessedWithEmptyTtl() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(Collections.singletonMap("year", "2020"));
    combos.add(Collections.singletonMap("year", "2021"));

    // Default delegates to filterUnprocessed
    Set<Integer> result =
        IncrementalTracker.NOOP.filterUnprocessedWithEmptyTtl("alt", "source", combos, 60000);

    assertEquals(2, result.size());
  }

  @Test void testDefaultMarkProcessedWithError() {
    // Default delegates to markProcessedWithRowCount with 0
    IncrementalTracker.NOOP.markProcessedWithError(
        "alt", "source", Collections.singletonMap("year", "2023"), "target", "error msg");
  }

  @Test void testDefaultFilterUnprocessedWithTtl() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(Collections.singletonMap("year", "2020"));

    // Default delegates to filterUnprocessedWithEmptyTtl
    Set<Integer> result =
        IncrementalTracker.NOOP.filterUnprocessedWithTtl("alt", "source", combos, 60000, 30000);

    assertEquals(1, result.size());
  }

  @Test void testDefaultMarkTableCompleteWithConfig() {
    // Default delegates to markTableComplete
    IncrementalTracker.NOOP.markTableCompleteWithConfig(
        "pipeline", "cfg:abc", "sig123", 1000);
  }

  @Test void testDefaultGetCachedCompletionReturnsNull() {
    assertNull(IncrementalTracker.NOOP.getCachedCompletion("pipeline"));
  }

  @Test void testDefaultPreloadAllCompletionsDoesNotThrow() {
    IncrementalTracker.NOOP.preloadAllCompletions();
  }

  @Test void testDefaultMarkTableCompleteWithSourceWatermark() {
    IncrementalTracker.NOOP.markTableCompleteWithSourceWatermark(
        "pipeline", "cfg:abc", "sig123", 1000, System.currentTimeMillis());
  }

  @Test void testDefaultIsSourceFilesModifiedNeverProcessed() {
    // NOOP returns null for getCachedCompletion, so isSourceFilesModified should return true
    assertTrue(IncrementalTracker.NOOP.isSourceFilesModified("pipeline", 1000L));
  }

  // ===== CachedCompletion =====

  @Test void testCachedCompletionBasicConstructor() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg:abc", "sig123", 1000);

    assertEquals("cfg:abc", cc.configHash);
    assertEquals("sig123", cc.signature);
    assertEquals(1000, cc.rowCount);
    assertTrue(cc.completedAt > 0);
    assertEquals(0L, cc.sourceFileWatermark);
  }

  @Test void testCachedCompletionWithCompletedAt() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg:abc", "sig123", 1000, 999L);

    assertEquals(999L, cc.completedAt);
    assertEquals(0L, cc.sourceFileWatermark);
  }

  @Test void testCachedCompletionFull() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg:abc", "sig123", 1000, 999L, 888L);

    assertEquals("cfg:abc", cc.configHash);
    assertEquals("sig123", cc.signature);
    assertEquals(1000, cc.rowCount);
    assertEquals(999L, cc.completedAt);
    assertEquals(888L, cc.sourceFileWatermark);
  }

  @Test void testCachedCompletionEmptyResultTtlNotExpired() {
    long now = System.currentTimeMillis();
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 0, now);

    // TTL of 1 hour, just completed -> not expired
    assertFalse(cc.isEmptyResultTtlExpired(3600000));
  }

  @Test void testCachedCompletionEmptyResultTtlExpired() {
    long oneHourAgo = System.currentTimeMillis() - 3600001;
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 0, oneHourAgo);

    // TTL of 1 hour, completed > 1 hour ago -> expired
    assertTrue(cc.isEmptyResultTtlExpired(3600000));
  }

  @Test void testCachedCompletionNonEmptyNotExpired() {
    long longAgo = 1000L;
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, longAgo);

    // rowCount > 0, so TTL never expires
    assertFalse(cc.isEmptyResultTtlExpired(1));
  }

  @Test void testCachedCompletionTtlDisabled() {
    long longAgo = 1000L;
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 0, longAgo);

    // TTL <= 0 means disabled
    assertFalse(cc.isEmptyResultTtlExpired(0));
    assertFalse(cc.isEmptyResultTtlExpired(-1));
  }

  @Test void testCachedCompletionSourceFilesModified() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 999L, 500L);

    // Current watermark > stored watermark -> modified
    assertTrue(cc.isSourceFilesModified(600L));
  }

  @Test void testCachedCompletionSourceFilesNotModified() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 999L, 500L);

    // Current watermark == stored watermark -> not modified
    assertFalse(cc.isSourceFilesModified(500L));
    // Current watermark < stored watermark -> not modified
    assertFalse(cc.isSourceFilesModified(400L));
  }

  @Test void testCachedCompletionSourceFilesWatermarkDisabled() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 999L, 0L);

    // sourceFileWatermark == 0 means disabled
    assertFalse(cc.isSourceFilesModified(999L));
  }

  @Test void testCachedCompletionSourceFilesCurrentWatermarkZero() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("cfg", "sig", 100, 999L, 500L);

    // currentSourceWatermark == 0 means disabled
    assertFalse(cc.isSourceFilesModified(0L));
  }

  // ===== Freshness token default methods =====

  @Test void testNoopGetFreshnessTokenReturnsNull() {
    // Default interface method: NOOP has no persistence, so every pipeline is "first run"
    assertNull(IncrementalTracker.NOOP.getFreshnessToken("any_pipeline"));
  }

  @Test void testNoopPutFreshnessTokenIsNoOp() {
    // Must not throw; token not retained (NOOP)
    IncrementalTracker.NOOP.putFreshnessToken("any_pipeline", "token-xyz");
    assertNull(IncrementalTracker.NOOP.getFreshnessToken("any_pipeline"),
        "NOOP tracker must not retain tokens");
  }

  // ===== Static utility methods =====

  @Test void testComputeDimensionSignatureEmptyList() {
    String sig =
        IncrementalTracker.computeDimensionSignature(Collections.<Map<String, String>>emptyList());
    assertEquals("empty", sig);
  }

  @Test void testComputeDimensionSignatureNull() {
    String sig = IncrementalTracker.computeDimensionSignature(null);
    assertEquals("empty", sig);
  }

  @Test void testComputeDimensionSignatureWithData() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> combo1 = new LinkedHashMap<String, String>();
    combo1.put("year", "2020");
    combo1.put("geo", "US");
    combos.add(combo1);

    Map<String, String> combo2 = new LinkedHashMap<String, String>();
    combo2.put("year", "2021");
    combo2.put("geo", "UK");
    combos.add(combo2);

    String sig = IncrementalTracker.computeDimensionSignature(combos);

    assertNotNull(sig);
    assertTrue(sig.startsWith("count:2"));
    assertTrue(sig.contains("|geo"));
    assertTrue(sig.contains("|year"));
    assertTrue(sig.contains("|hash:"));
  }

  @Test void testComputeDimensionSignatureDeterministic() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> combo = new LinkedHashMap<String, String>();
    combo.put("year", "2020");
    combos.add(combo);

    String sig1 = IncrementalTracker.computeDimensionSignature(combos);
    String sig2 = IncrementalTracker.computeDimensionSignature(combos);

    assertEquals(sig1, sig2);
  }

  @Test void testComputeDimensionSignatureChangesWithData() {
    List<Map<String, String>> combos1 = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new LinkedHashMap<String, String>();
    c1.put("year", "2020");
    combos1.add(c1);

    List<Map<String, String>> combos2 = new ArrayList<Map<String, String>>();
    Map<String, String> c2 = new LinkedHashMap<String, String>();
    c2.put("year", "2021");
    combos2.add(c2);

    String sig1 = IncrementalTracker.computeDimensionSignature(combos1);
    String sig2 = IncrementalTracker.computeDimensionSignature(combos2);

    // Different data should produce different signatures
    assertFalse(sig1.equals(sig2));
  }
}
