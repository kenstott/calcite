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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * How a {@code computed_delta} unit that writes no rows is recorded: an unchanged source is a
 * normal completion, while rows lost before or after the high-water filter are a suspect empty.
 */
@Tag("unit")
class EtlPipelineComputedDeltaUnchangedTest {

  private static final String HWM_KEY = "delta_test::computed_delta_hwm::year=2024";

  @TempDir
  Path tempDir;

  /** Drops every row. */
  public static final class DropAllTransformer implements RowTransformer {
    @Override public List<Map<String, Object>> transform(Map<String, Object> row,
        RowContext context) {
      return Collections.emptyList();
    }
  }

  /** Tracker recording completion and suspect-empty marks, with freshness-token round trips. */
  static final class MarkTracker implements IncrementalTracker {
    final Map<String, String> tokens = new HashMap<String, String>();
    final List<Long> completedRowCounts = new ArrayList<Long>();
    int suspectEmpty;

    @Override public String getFreshnessToken(String pipelineName) {
      return tokens.get(pipelineName);
    }

    @Override public void putFreshnessToken(String pipelineName, String token) {
      tokens.put(pipelineName, token);
    }

    @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern, long rowCount) {
      completedRowCounts.add(rowCount);
    }

    @Override public void markProcessedSuspectEmpty(String alternateName, String sourceTable,
        Map<String, String> keyValues, long scanned, long filtered, String reason) {
      suspectEmpty++;
    }

    @Override public boolean isProcessed(String an, String st, Map<String, String> kv) {
      return false;
    }

    @Override public boolean isProcessedWithTtl(String an, String st,
        Map<String, String> kv, long ttl) {
      return false;
    }

    @Override public void markProcessed(String an, String st,
        Map<String, String> kv, String tp) { }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String an) {
      return Collections.emptySet();
    }

    @Override public void invalidate(String an, Map<String, String> kv) { }

    @Override public void invalidateAll(String an) { }

    @Override public Set<Integer> filterUnprocessed(String an, String st,
        List<Map<String, String>> combos) {
      Set<Integer> all = new HashSet<Integer>();
      for (int i = 0; i < combos.size(); i++) {
        all.add(i);
      }
      return all;
    }

    @Override public boolean isTableComplete(String p, String sig) {
      return false;
    }

    @Override public void markTableComplete(String p, String sig) { }

    @Override public void invalidateTableCompletion(String p) { }

    @Override public void clearAllCompletions() { }
  }

  /** Consumes every row and reports {@code written} as the count, or the number consumed. */
  private static DataWriter writer(final Long written) {
    return new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data, Map<String, String> variables) {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return written != null ? written : count;
      }
    };
  }

  private EtlPipelineConfig config(HooksConfig hooks) {
    Map<String, Object> incremental = new HashMap<String, Object>();
    incremental.put("dateField", "modified");
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2024)
        .end(2024)
        .build());
    EtlPipelineConfig.Builder builder = EtlPipelineConfig.builder()
        .name("delta_test")
        .source(HttpSourceConfig.builder()
            .url("https://example.invalid/api")
            .incremental(HttpSourceConfig.IncrementalConfig.fromMap(incremental))
            .build())
        .dimensions(dims)
        .datasetType("computed_delta")
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build());
    if (hooks != null) {
      builder.hooks(hooks);
    }
    return builder.build();
  }

  private void run(MarkTracker tracker, HooksConfig hooks, DataWriter writer) throws IOException {
    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        Map<String, Object> first = new LinkedHashMap<String, Object>();
        first.put("modified", "2024-01-15");
        Map<String, Object> second = new LinkedHashMap<String, Object>();
        second.put("modified", "2024-03-01");
        return Arrays.asList(first, second).iterator();
      }
    };
    new EtlPipeline(config(hooks), new LocalFileStorageProvider(), tempDir.toString(), null,
        tracker, provider, writer).execute();
  }

  @Test void unchangedPullIsACompletionNotASuspectEmpty() throws IOException {
    MarkTracker tracker = new MarkTracker();
    run(tracker, null, writer(null));
    run(tracker, null, writer(null));

    assertEquals(0, tracker.suspectEmpty);
    assertEquals(Arrays.asList(2L, 0L), tracker.completedRowCounts);
    assertEquals("2024-03-01", tracker.tokens.get(HWM_KEY));
  }

  @Test void transformerDroppingEveryRowIsSuspectEmpty() throws IOException {
    MarkTracker tracker = new MarkTracker();
    tracker.tokens.put(HWM_KEY, "2024-03-01");
    HooksConfig hooks = HooksConfig.builder()
        .rowTransformers(Collections.singletonList(
            HooksConfig.TransformerConfig.ofClass(DropAllTransformer.class.getName())))
        .build();
    run(tracker, hooks, writer(null));

    assertEquals(1, tracker.suspectEmpty);
    assertEquals(Collections.<Long>emptyList(), tracker.completedRowCounts);
  }

  @Test void writerDroppingChangedRowsIsSuspectEmpty() throws IOException {
    MarkTracker tracker = new MarkTracker();
    run(tracker, null, writer(0L));

    assertEquals(1, tracker.suspectEmpty);
    assertEquals(Collections.<Long>emptyList(), tracker.completedRowCounts);
  }
}
