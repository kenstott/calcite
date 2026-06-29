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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionIterator;
import org.apache.calcite.adapter.file.etl.DimensionType;
import org.apache.calcite.adapter.file.etl.HttpSource;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-083 / FILE-084 / FILE-140 — exact-assertion recode of weak file-adapter ETL,
 * dimension, and tracker tests.
 *
 * <p>Each requirement pins behavior that the prior {@code contains()}/smoke tests only
 * brushed against:
 * <ul>
 *   <li>FILE-083 — {@link DimensionType#fromString} parsing for every supported type, plus an
 *       inclusive RANGE expansion with the default step of 1 (verified end-to-end through
 *       {@link DimensionIterator#expand}).</li>
 *   <li>FILE-084 — {@link IncrementalTracker} per-period completion keying and
 *       markProcessed/isProcessed semantics, mirroring {@code IncrementalTrackerTest}'s
 *       in-memory tracker construction.</li>
 *   <li>FILE-140 — {@link HttpSource} circuit-breaker / retry constants
 *       ({@code CIRCUIT_503_THRESHOLD == 5}, {@code RETRY_AFTER_CAP_MS == 300000}) read via
 *       reflection, plus {@code shouldRetry} treating 503 and 429 as retryable.</li>
 * </ul>
 */
@Tag("unit")
public class EtlTrackerRequirementsTest {

  // ============================================================ FILE-083 ======================
  // Dimension-type parsing + inclusive RANGE expansion with default step.

  @Test @Tag("FILE-083") void dimensionTypesParseToExactEnumConstants() {
    // The six supported dimension types named in the requirement, each parsed exactly.
    assertEquals(DimensionType.LIST, DimensionType.fromString("list"));
    assertEquals(DimensionType.RANGE, DimensionType.fromString("range"));
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("yearRange"));
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("year_range"));
    assertEquals(DimensionType.QUERY, DimensionType.fromString("query"));
    assertEquals(DimensionType.JSON_CATALOG, DimensionType.fromString("json_catalog"));
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("custom"));
    // Unknown / null / empty all fall back to LIST (documented default).
    assertEquals(DimensionType.LIST, DimensionType.fromString("unknown_type"));
    assertEquals(DimensionType.LIST, DimensionType.fromString(null));
    assertEquals(DimensionType.LIST, DimensionType.fromString(""));
  }

  @Test @Tag("FILE-083") void rangeDefaultsToStepOneWhenStepOmitted() {
    // step is absent from the map: DimensionConfig must default it to 1.
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("type", "range");
    map.put("start", 2020);
    map.put("end", 2024);
    DimensionConfig config = DimensionConfig.fromMap("year", map);

    assertEquals(DimensionType.RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertEquals(Integer.valueOf(2024), config.getEnd());
    assertEquals(Integer.valueOf(1), config.getStep(), "step defaults to 1 when omitted");
  }

  @Test @Tag("FILE-083") void rangeExpansionIsInclusiveWithDefaultStep() {
    // start=2020, end=2024, default step 1 => inclusive [2020..2024] (both endpoints present).
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("type", "range");
    map.put("start", 2020);
    map.put("end", 2024);

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.fromMap("year", map));

    List<Map<String, String>> combos = DimensionIterator.create().expand(dims);

    List<String> years = new ArrayList<String>();
    for (Map<String, String> combo : combos) {
      years.add(combo.get("year"));
    }
    // 2020,2021,2022,2023,2024 — inclusive of both start and end, step 1.
    assertEquals(Arrays.asList("2020", "2021", "2022", "2023", "2024"), years);
    assertTrue(years.contains("2020"), "start is inclusive");
    assertTrue(years.contains("2024"), "end is inclusive");
  }

  @Test @Tag("FILE-083") void rangeExpansionHonorsExplicitStep() {
    // start=2020, end=2024, step=2 => [2020, 2022, 2024]; end remains inclusive when on-step.
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("type", "range");
    map.put("start", 2020);
    map.put("end", 2024);
    map.put("step", 2);

    DimensionConfig config = DimensionConfig.fromMap("year", map);
    assertEquals(Integer.valueOf(2), config.getStep());

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", config);

    List<Map<String, String>> combos = DimensionIterator.create().expand(dims);
    List<String> years = new ArrayList<String>();
    for (Map<String, String> combo : combos) {
      years.add(combo.get("year"));
    }
    assertEquals(Arrays.asList("2020", "2022", "2024"), years);
  }

  // ============================================================ FILE-084 ======================
  // IncrementalTracker per-period completion keying + markProcessed/isProcessed semantics.

  /**
   * Minimal in-memory IncrementalTracker — mirrors the construction used by
   * {@code IncrementalTrackerTest.InMemoryProcessedTracker}, reduced to just the
   * processed-key behavior this requirement asserts.
   */
  private static final class InMemoryTracker implements IncrementalTracker {
    private final Map<String, Long> processedAsOf = new HashMap<String, Long>();

    @Override public boolean isProcessed(String alt, String src, Map<String, String> kv) {
      Long markedAt = processedAsOf.get(alt + "\0" + kv.toString());
      return markedAt != null && markedAt != 0L;
    }

    @Override public boolean isProcessedWithTtl(String alt, String src,
        Map<String, String> kv, long ttl) {
      return false;
    }

    @Override public void markProcessed(String alt, String src,
        Map<String, String> kv, String pat) {
      processedAsOf.put(alt + "\0" + kv.toString(), System.currentTimeMillis());
    }

    @Override public java.util.Set<Map<String, String>> getProcessedKeyValues(String alt) {
      return Collections.emptySet();
    }

    @Override public void invalidate(String alt, Map<String, String> kv) {
      processedAsOf.remove(alt + "\0" + kv.toString());
    }

    @Override public void invalidateAll(String alt) {
      processedAsOf.clear();
    }

    @Override public java.util.Set<Integer> filterUnprocessed(String alt, String src,
        List<Map<String, String>> combos) {
      java.util.Set<Integer> all = new java.util.HashSet<Integer>();
      for (int i = 0; i < combos.size(); i++) {
        all.add(i);
      }
      return all;
    }

    @Override public boolean isTableComplete(String pipe, String sig) {
      return false;
    }

    @Override public void markTableComplete(String pipe, String sig) {
    }

    @Override public void invalidateTableCompletion(String pipe) {
    }

    @Override public void clearAllCompletions() {
    }
  }

  @Test @Tag("FILE-084") void periodCompletionKeyFillsCanonicalSlotsThenPipelineName() {
    // Canonical slots are year_quarter_month_week_day_day_of_week, then the pipeline name.
    String key = IncrementalTracker.periodCompletionKey(
        "patents_patent_grants", Collections.singletonMap("year", "2022"));
    assertEquals("2022_NA_NA_NA_NA_NA_patents_patent_grants", key);
  }

  @Test @Tag("FILE-084") void periodCompletionKeyIgnoresNonPeriodDimensions() {
    Map<String, String> combo = new LinkedHashMap<String, String>();
    combo.put("year", "2026");
    combo.put("quarter", "2026Q2");
    combo.put("geography", "CA"); // non-period dim must not affect the key
    assertEquals("2026_2026Q2_NA_NA_NA_NA_t", IncrementalTracker.periodCompletionKey("t", combo));
  }

  @Test @Tag("FILE-084") void periodCompletionKeyIsDistinctPerPeriod() {
    // Different periods => different keys => no cross-period skip.
    String y2025 =
        IncrementalTracker.periodCompletionKey("p", Collections.singletonMap("year", "2025"));
    String y2022 =
        IncrementalTracker.periodCompletionKey("p", Collections.singletonMap("year", "2022"));
    assertFalse(y2025.equals(y2022));
  }

  @Test @Tag("FILE-084") void markProcessedThenIsProcessedReportsProcessed() {
    InMemoryTracker tracker = new InMemoryTracker();
    Map<String, String> combo = Collections.singletonMap("year", "2024");
    String pipeline = "energy_eia_coal_mines";

    // Before marking, the key is not processed.
    assertFalse(tracker.isProcessed(pipeline, pipeline, combo),
        "an unmarked key must report unprocessed");

    tracker.markProcessed(pipeline, pipeline, combo, null);

    // After marking, the same key reports processed.
    assertTrue(tracker.isProcessed(pipeline, pipeline, combo),
        "a marked key must report processed");
  }

  @Test @Tag("FILE-084") void isProcessedIsKeyedPerComboAndPipeline() {
    InMemoryTracker tracker = new InMemoryTracker();
    Map<String, String> combo2024 = Collections.singletonMap("year", "2024");
    Map<String, String> combo2023 = Collections.singletonMap("year", "2023");
    String pipeline = "energy_eia_coal_mines";

    tracker.markProcessed(pipeline, pipeline, combo2024, null);

    // A different combo on the same pipeline is independent.
    assertFalse(tracker.isProcessed(pipeline, pipeline, combo2023),
        "a different period must remain unprocessed");
    // A different pipeline on the same combo is independent.
    assertFalse(tracker.isProcessed("econ_gdp", "econ_gdp", combo2024),
        "a different pipeline must remain unprocessed");
  }

  @Test @Tag("FILE-084") void noopIsProcessedAlwaysFalseEvenAfterMark() {
    // NOOP is non-persistent: it cannot prove completion after a mark.
    Map<String, String> combo = Collections.singletonMap("year", "2023");
    IncrementalTracker.NOOP.markProcessed("alt", "source", combo, "target");
    assertFalse(IncrementalTracker.NOOP.isProcessed("alt", "source", combo));
  }

  // ============================================================ FILE-140 ======================
  // HttpSource circuit-breaker / retry constants + 503/429 retryability.

  private static HttpSource newHttpSource() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    return new HttpSource(config);
  }

  @Test @Tag("FILE-140") void circuit503ThresholdConstantIsFive() throws Exception {
    Field f = HttpSource.class.getDeclaredField("CIRCUIT_503_THRESHOLD");
    f.setAccessible(true);
    assertEquals(5, f.getInt(null), "CIRCUIT_503_THRESHOLD must be 5");
  }

  @Test @Tag("FILE-140") void retryAfterCapConstantIsFiveMinutes() throws Exception {
    Field f = HttpSource.class.getDeclaredField("RETRY_AFTER_CAP_MS");
    f.setAccessible(true);
    assertEquals(300000L, f.getLong(null), "RETRY_AFTER_CAP_MS must be 300000 ms (5 min)");
  }

  @Test @Tag("FILE-140") void shouldRetryTreats503AndA429AsRetryable() throws Exception {
    HttpSource source = newHttpSource();
    Method method = HttpSource.class.getDeclaredMethod(
        "shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();

    assertTrue((boolean) method.invoke(source,
        new IOException("HTTP 503: Service Unavailable"), rateLimit),
        "503 must be retryable");
    assertTrue((boolean) method.invoke(source,
        new IOException("HTTP 429: Too Many Requests"), rateLimit),
        "429 must be retryable");
  }

  @Test @Tag("FILE-140") void shouldRetryRejectsNonRetryableStatuses() throws Exception {
    HttpSource source = newHttpSource();
    Method method = HttpSource.class.getDeclaredMethod(
        "shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();

    // A 404 is not retryable.
    assertFalse((boolean) method.invoke(source,
        new IOException("HTTP 404: Not Found"), rateLimit),
        "404 must not be retryable");
    // A null message is not retryable.
    assertFalse((boolean) method.invoke(source,
        new IOException((String) null), rateLimit),
        "a null message must not be retryable");
  }
}
