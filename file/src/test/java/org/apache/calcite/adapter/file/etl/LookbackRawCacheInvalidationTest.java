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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a lookback reopen records the reopened periods' completion keys, which is what
 * lets {@code processSingleBatch} drop a reopened unit's raw cache entry before fetching.
 *
 * <p>A raw cache entry is validated by existence alone, so a reopened period served from cache
 * would be handed back the very bytes the reopen exists to refresh — the head period freezes at
 * its first fetch. The reopened-period key set is the pipeline's record of which units must
 * reach the source instead.
 */
@Tag("unit")
public class LookbackRawCacheInvalidationTest {

  private static final String PIPE = "disasters.disaster_declarations";

  private static EtlPipeline pipeline() {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year").type(DimensionType.YEAR_RANGE).start(2026).end(2026).build());
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name(PIPE)
        .source(HttpSourceConfig.builder().url("https://example.invalid/data").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();
    return new EtlPipeline(config, new LocalFileStorageProvider(), "/tmp/out");
  }

  private static Map<String, String> period(String year) {
    Map<String, String> p = new LinkedHashMap<String, String>();
    p.put("year", year);
    return p;
  }

  @SuppressWarnings("unchecked")
  private static Set<String> reopenedKeys(EtlPipeline pipeline) throws Exception {
    Field f = EtlPipeline.class.getDeclaredField("lookbackReopenedPeriodKeys");
    f.setAccessible(true);
    return (Set<String>) f.get(pipeline);
  }

  private static void invokeReopen(EtlPipeline pipeline, List<Map<String, String>> periods,
      List<Map<String, String>> combinations, Set<Integer> unprocessed) throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod("reopenLookbackPeriods",
        String.class, List.class, List.class, Set.class);
    m.setAccessible(true);
    m.invoke(pipeline, PIPE, periods, combinations, unprocessed);
  }

  @Test void recordsTheReopenedPeriodKey() throws Exception {
    EtlPipeline pipeline = pipeline();
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(period("2026"));
    Set<Integer> unprocessed = new HashSet<Integer>();

    invokeReopen(pipeline, java.util.Collections.singletonList(period("2026")), combos,
        unprocessed);

    String key = IncrementalTracker.periodCompletionKey(PIPE, period("2026"));
    assertTrue(reopenedKeys(pipeline).contains(key),
        "the reopened period's completion key must be recorded");
    assertTrue(unprocessed.contains(0), "the combo is re-added for reprocessing");
  }

  @Test void recordsNothingWhenTheLookbackSelectsNoPeriod() throws Exception {
    EtlPipeline pipeline = pipeline();
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(period("2026"));
    Set<Integer> unprocessed = new HashSet<Integer>();

    invokeReopen(pipeline, java.util.Collections.<Map<String, String>>emptyList(), combos,
        unprocessed);

    assertTrue(reopenedKeys(pipeline).isEmpty(),
        "no lookback selection means no unit needs its cache dropped");
    assertTrue(unprocessed.isEmpty(), "nothing is re-added");
  }

  @Test void recordsOnlyPeriodsTheLookbackSelected() throws Exception {
    EtlPipeline pipeline = pipeline();
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(period("2025"));
    combos.add(period("2026"));
    Set<Integer> unprocessed = new HashSet<Integer>();

    invokeReopen(pipeline, java.util.Collections.singletonList(period("2026")), combos,
        unprocessed);

    Set<String> keys = reopenedKeys(pipeline);
    assertEquals(1, keys.size(), "only the selected period is recorded");
    assertTrue(keys.contains(IncrementalTracker.periodCompletionKey(PIPE, period("2026"))));
    assertFalse(keys.contains(IncrementalTracker.periodCompletionKey(PIPE, period("2025"))),
        "an unselected sibling period keeps its cache entry");
  }
}
