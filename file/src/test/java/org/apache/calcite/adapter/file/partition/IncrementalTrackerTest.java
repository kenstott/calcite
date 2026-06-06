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
import java.util.HashMap;
import java.util.HashSet;
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

  @Test void testNoopClearPeriodCompletionsIsNoOp() {
    // Must not throw and must not affect NOOP's always-false isPeriodComplete
    IncrementalTracker.NOOP.clearPeriodCompletions("myschema");
    assertFalse(IncrementalTracker.NOOP.isPeriodComplete("myschema_table",
        Collections.singletonMap("year", "2022")));
  }

  @Test void testNoopClearProcessedKeysIsNoOp() {
    // Must not throw; NOOP.isProcessed always returns false so clearing has no observable effect
    IncrementalTracker.NOOP.clearProcessedKeys("myschema");
    assertFalse(IncrementalTracker.NOOP.isProcessed("myschema_table", "myschema_table",
        Collections.singletonMap("year", "2022")));
  }

  // ===== clearPeriodCompletions semantics (in-memory stub) =====

  /**
   * Minimal in-memory IncrementalTracker that implements per-period completion with
   * schema-scoped clearing semantics — mirrors the S3 implementation's latest-wins
   * logic without any I/O, so tests are fast and hermetic.
   */
  private static class InMemoryPeriodTracker implements IncrementalTracker {
    /** periodKey -> as_of of the latest "complete" mark (Long.MIN_VALUE = invalidated). */
    private final Map<String, Long> periodCompleteAsOf = new HashMap<String, Long>();
    /** schemaName -> as_of of the "cleared" sentinel. */
    private final Map<String, Long> clearedAsOf = new HashMap<String, Long>();

    @Override public boolean isProcessed(String alt, String src, Map<String, String> kv) {
      return false;
    }
    @Override public boolean isProcessedWithTtl(String alt, String src,
        Map<String, String> kv, long ttl) {
      return false;
    }
    @Override public void markProcessed(String alt, String src,
        Map<String, String> kv, String pat) {}
    @Override public Set<Map<String, String>> getProcessedKeyValues(String alt) {
      return Collections.emptySet();
    }
    @Override public void invalidate(String alt, Map<String, String> kv) {}
    @Override public void invalidateAll(String alt) {}
    @Override public Set<Integer> filterUnprocessed(String alt, String src,
        List<Map<String, String>> combos) {
      Set<Integer> all = new HashSet<Integer>();
      for (int i = 0; i < combos.size(); i++) {
        all.add(i);
      }
      return all;
    }
    @Override public boolean isTableComplete(String pipe, String sig) { return false; }
    @Override public void markTableComplete(String pipe, String sig) {}
    @Override public void invalidateTableCompletion(String pipe) {}
    @Override public void clearAllCompletions() {}

    @Override public boolean isPeriodComplete(String pipelineName,
        Map<String, String> periodValues) {
      String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, periodValues);
      Long completedAt = periodCompleteAsOf.get(periodKey);
      if (completedAt == null || completedAt == Long.MIN_VALUE) {
        return false;
      }
      // Find the longest matching schema prefix
      long clearedAt = getMatchingClearedAsOf(pipelineName);
      if (clearedAt > 0 && completedAt <= clearedAt) {
        return false;
      }
      return true;
    }

    @Override public void markPeriodComplete(String pipelineName,
        Map<String, String> periodValues) {
      String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, periodValues);
      periodCompleteAsOf.put(periodKey, System.currentTimeMillis());
    }

    @Override public void invalidatePeriod(String pipelineName,
        Map<String, String> periodValues) {
      String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, periodValues);
      periodCompleteAsOf.put(periodKey, Long.MIN_VALUE);
    }

    @Override public void clearPeriodCompletions(String schemaName) {
      clearedAsOf.put(schemaName, System.currentTimeMillis());
    }

    private long getMatchingClearedAsOf(String pipelineName) {
      String bestMatch = null;
      int bestLen = -1;
      for (String schemaName : clearedAsOf.keySet()) {
        String prefix = schemaName + "_";
        if (pipelineName.startsWith(prefix) && schemaName.length() > bestLen) {
          bestMatch = schemaName;
          bestLen = schemaName.length();
        }
      }
      return bestMatch != null ? clearedAsOf.get(bestMatch) : 0L;
    }
  }

  @Test void testClearPeriodCompletionsMakesPriorCompleteInvisible() throws InterruptedException {
    InMemoryPeriodTracker tracker = new InMemoryPeriodTracker();
    Map<String, String> period = Collections.singletonMap("year", "2022");
    String pipeline = "schemaA_table1";

    // Mark period complete
    tracker.markPeriodComplete(pipeline, period);
    assertTrue(tracker.isPeriodComplete(pipeline, period),
        "should be complete before clear");

    // Ensure at least 1ms passes so the cleared sentinel is strictly newer
    Thread.sleep(2);

    // Clear schema A's period completions
    tracker.clearPeriodCompletions("schemaA");
    assertFalse(tracker.isPeriodComplete(pipeline, period),
        "after clearPeriodCompletions, prior mark should be invisible");
  }

  @Test void testClearPeriodCompletionsDoesNotAffectOtherSchema() throws InterruptedException {
    InMemoryPeriodTracker tracker = new InMemoryPeriodTracker();
    Map<String, String> period = Collections.singletonMap("year", "2022");
    String pipelineA = "schemaA_table1";
    String pipelineB = "schemaB_table1";

    tracker.markPeriodComplete(pipelineA, period);
    tracker.markPeriodComplete(pipelineB, period);

    Thread.sleep(2);

    // Clear only schema A
    tracker.clearPeriodCompletions("schemaA");

    assertFalse(tracker.isPeriodComplete(pipelineA, period),
        "schemaA period should be invisible after clear");
    assertTrue(tracker.isPeriodComplete(pipelineB, period),
        "schemaB period should be unaffected by clearing schemaA");
  }

  @Test void testClearPeriodCompletionsThenRemark() throws InterruptedException {
    InMemoryPeriodTracker tracker = new InMemoryPeriodTracker();
    Map<String, String> period = Collections.singletonMap("year", "2023");
    String pipeline = "schemaA_table2";

    tracker.markPeriodComplete(pipeline, period);
    Thread.sleep(2);
    tracker.clearPeriodCompletions("schemaA");
    assertFalse(tracker.isPeriodComplete(pipeline, period), "should be gone after clear");

    // Re-mark after the clear: the new mark is newer than the cleared sentinel
    Thread.sleep(2);
    tracker.markPeriodComplete(pipeline, period);
    assertTrue(tracker.isPeriodComplete(pipeline, period),
        "re-mark after clear should be visible (newer asOf)");
  }

  @Test void testClearPeriodCompletionsLongestPrefixMatch() throws InterruptedException {
    InMemoryPeriodTracker tracker = new InMemoryPeriodTracker();
    Map<String, String> period = Collections.singletonMap("year", "2024");
    String pipelineEcon = "econ_table1";
    String pipelineEconRef = "econ_reference_table1";

    tracker.markPeriodComplete(pipelineEcon, period);
    tracker.markPeriodComplete(pipelineEconRef, period);

    Thread.sleep(2);

    // Clear "econ_reference" only — must NOT affect "econ" pipelines
    tracker.clearPeriodCompletions("econ_reference");

    assertTrue(tracker.isPeriodComplete(pipelineEcon, period),
        "econ_table1 should NOT be affected by clearing econ_reference");
    assertFalse(tracker.isPeriodComplete(pipelineEconRef, period),
        "econ_reference_table1 SHOULD be affected by clearing econ_reference");
  }

  // ===== clearProcessedKeys semantics (in-memory stub) =====

  /**
   * Minimal in-memory IncrementalTracker that implements per-combo processed-key tracking with
   * schema-scoped clearing semantics — mirrors the S3 implementation's sentinel logic
   * without any I/O so tests are fast and hermetic.
   */
  private static class InMemoryProcessedTracker implements IncrementalTracker {
    /** flatKey(combo, alternateName) -> as_of of latest "complete" mark (0 = not processed). */
    private final Map<String, Long> processedAsOf = new HashMap<String, Long>();
    /** schemaName -> as_of of the "_processed_cleared" sentinel. */
    private final Map<String, Long> clearedAsOf = new HashMap<String, Long>();

    @Override public boolean isProcessed(String alt, String src, Map<String, String> kv) {
      String key = alt + "\0" + kv.toString();
      Long markedAt = processedAsOf.get(key);
      if (markedAt == null || markedAt == 0L) {
        return false;
      }
      long clearedAt = getClearedForPipeline(alt);
      if (clearedAt > 0 && markedAt <= clearedAt) {
        return false;
      }
      return true;
    }

    @Override public boolean isProcessedWithTtl(String alt, String src,
        Map<String, String> kv, long ttl) {
      return false;
    }

    @Override public void markProcessed(String alt, String src,
        Map<String, String> kv, String pat) {
      String key = alt + "\0" + kv.toString();
      processedAsOf.put(key, System.currentTimeMillis());
    }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String alt) {
      return Collections.emptySet();
    }

    @Override public void invalidate(String alt, Map<String, String> kv) {
      String key = alt + "\0" + kv.toString();
      processedAsOf.remove(key);
    }

    @Override public void invalidateAll(String alt) {}

    @Override public Set<Integer> filterUnprocessed(String alt, String src,
        List<Map<String, String>> combos) {
      Set<Integer> all = new HashSet<Integer>();
      for (int i = 0; i < combos.size(); i++) {
        all.add(i);
      }
      return all;
    }

    @Override public boolean isTableComplete(String pipe, String sig) { return false; }
    @Override public void markTableComplete(String pipe, String sig) {}
    @Override public void invalidateTableCompletion(String pipe) {}
    @Override public void clearAllCompletions() {}

    @Override public void clearProcessedKeys(String schemaName) {
      clearedAsOf.put(schemaName, System.currentTimeMillis());
    }

    private long getClearedForPipeline(String pipelineName) {
      // Longest-prefix match on schema name
      String bestMatch = null;
      int bestLen = -1;
      for (String schemaName : clearedAsOf.keySet()) {
        String prefix = schemaName + "_";
        if (pipelineName.startsWith(prefix) && schemaName.length() > bestLen) {
          bestMatch = schemaName;
          bestLen = schemaName.length();
        }
      }
      return bestMatch != null ? clearedAsOf.get(bestMatch) : 0L;
    }
  }

  @Test void testClearProcessedKeysMakesPriorMarkInvisible() throws InterruptedException {
    InMemoryProcessedTracker tracker = new InMemoryProcessedTracker();
    Map<String, String> combo = Collections.singletonMap("year", "2024");
    String pipeline = "energy_eia_coal_mines";

    // Record a processed mark for a combo in schemaA's pipeline
    tracker.markProcessed(pipeline, pipeline, combo, null);
    assertTrue(tracker.isProcessed(pipeline, pipeline, combo),
        "should be processed before clear");

    // Ensure at least 1ms so sentinel is strictly newer
    Thread.sleep(2);

    // Clear schema "energy" processed keys
    tracker.clearProcessedKeys("energy");

    // Prior mark should now be invisible
    assertFalse(tracker.isProcessed(pipeline, pipeline, combo),
        "after clearProcessedKeys, prior mark should be invisible");
  }

  @Test void testClearProcessedKeysThenRemark() throws InterruptedException {
    InMemoryProcessedTracker tracker = new InMemoryProcessedTracker();
    Map<String, String> combo = Collections.singletonMap("year", "2024");
    String pipeline = "energy_eia_coal_mines";

    tracker.markProcessed(pipeline, pipeline, combo, null);
    Thread.sleep(2);
    tracker.clearProcessedKeys("energy");
    assertFalse(tracker.isProcessed(pipeline, pipeline, combo),
        "should not be processed after clear");

    // Re-mark after the sentinel: new mark is newer than sentinel
    Thread.sleep(2);
    tracker.markProcessed(pipeline, pipeline, combo, null);
    assertTrue(tracker.isProcessed(pipeline, pipeline, combo),
        "re-mark after clearProcessedKeys should be visible");
  }

  @Test void testClearProcessedKeysDoesNotAffectOtherSchema() throws InterruptedException {
    InMemoryProcessedTracker tracker = new InMemoryProcessedTracker();
    Map<String, String> combo = Collections.singletonMap("year", "2024");
    String pipelineA = "energy_eia_coal_mines";
    String pipelineB = "econ_gdp";

    tracker.markProcessed(pipelineA, pipelineA, combo, null);
    tracker.markProcessed(pipelineB, pipelineB, combo, null);
    Thread.sleep(2);

    // Clear only "energy" schema
    tracker.clearProcessedKeys("energy");

    assertFalse(tracker.isProcessed(pipelineA, pipelineA, combo),
        "energy pipeline combo should be invisible after clearing energy");
    assertTrue(tracker.isProcessed(pipelineB, pipelineB, combo),
        "econ pipeline combo should be unaffected by clearing energy");
  }

  @Test void testClearProcessedKeysLongestPrefixMatch() throws InterruptedException {
    InMemoryProcessedTracker tracker = new InMemoryProcessedTracker();
    Map<String, String> combo = Collections.singletonMap("year", "2024");
    String pipelineEcon = "econ_gdp";
    String pipelineEconRef = "econ_reference_codes";

    tracker.markProcessed(pipelineEcon, pipelineEcon, combo, null);
    tracker.markProcessed(pipelineEconRef, pipelineEconRef, combo, null);
    Thread.sleep(2);

    // Clear "econ_reference" only — must NOT affect "econ" pipelines
    tracker.clearProcessedKeys("econ_reference");

    assertTrue(tracker.isProcessed(pipelineEcon, pipelineEcon, combo),
        "econ_gdp should NOT be affected by clearing econ_reference");
    assertFalse(tracker.isProcessed(pipelineEconRef, pipelineEconRef, combo),
        "econ_reference_codes SHOULD be affected by clearing econ_reference");
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
