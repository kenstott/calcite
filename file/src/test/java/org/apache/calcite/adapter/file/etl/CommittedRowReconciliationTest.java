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

import java.io.File;
import java.io.IOException;
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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The committed-row reconciliation in a pipeline that has a {@link DataWriter} hook, which is
 * every schema run: the hook returns -1 to hand a unit to the standard writer. Rows the standard
 * writer took must match the rows in its commits; rows the hook wrote itself are not its to
 * account for.
 */
@Tag("unit")
class CommittedRowReconciliationTest {

  @TempDir
  File tempDir;

  /** Records combo and table completion marks. */
  static final class RecordingTracker implements IncrementalTracker {
    final List<Map<String, String>> comboMarks = new CopyOnWriteArrayList<Map<String, String>>();
    final List<String> tableMarks = new CopyOnWriteArrayList<String>();

    @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern, long rowCount) {
      comboMarks.add(new LinkedHashMap<String, String>(keyValues));
    }

    @Override public void markTableCompleteWithConfig(String pipelineName, String configHash,
        String signature, long rowCount) {
      tableMarks.add(pipelineName);
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

  private static final DataWriter DEFER_TO_STANDARD_WRITER = (config, data, variables) -> -1;

  @Test void reconciledRunWithDeferringHookIsMarkedComplete() throws IOException {
    RecordingTracker tracker = new RecordingTracker();

    EtlResult result = run(config("deferring", true, null), tracker,
        (config, variables) -> rows(variables, 5), DEFER_TO_STANDARD_WRITER);

    assertEquals(10L, result.getTotalRows());
    assertEquals(1, tracker.tableMarks.size());
    assertEquals(2, tracker.comboMarks.size());
  }

  /** Units the hook wrote itself never reach the standard writer, so they are not expected. */
  @Test void hookWrittenUnitsAreExcludedFromTheExpectedCount() throws IOException {
    RecordingTracker tracker = new RecordingTracker();
    DataWriter hookTakes2024 = (config, data, variables) -> {
      if (!"2024".equals(variables.get("year"))) {
        return -1;
      }
      long count = 0;
      while (data.hasNext()) {
        data.next();
        count++;
      }
      return count;
    };

    EtlResult result = run(config("mixed", true, null), tracker,
        (config, variables) -> rows(variables, 5), hookTakes2024);

    assertEquals(10L, result.getTotalRows());
    assertEquals(1, tracker.tableMarks.size());
  }

  @Test void rowsDroppedByColumnPolicyAreNotAMismatch() throws IOException {
    RecordingTracker tracker = new RecordingTracker();
    DataProvider oneBadRowPerUnit = (config, variables) -> {
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
      rows(variables, 3).forEachRemaining(out::add);
      out.get(0).put("id", "not-a-number");
      return out.iterator();
    };

    run(config("dropping", true, "DROP"), tracker, oneBadRowPerUnit, DEFER_TO_STANDARD_WRITER);

    assertEquals(1, tracker.tableMarks.size());
  }

  /**
   * A unit that dies part-way in append mode leaves rows in the commit that the pipeline never
   * counted. The run must fail, and record neither the table nor any combo as complete.
   */
  @Test void mismatchFailsTheRunAndLeavesNoCompletionMarks() {
    RecordingTracker tracker = new RecordingTracker();
    DataProvider secondUnitDiesMidStream = (config, variables) -> {
      final Iterator<Map<String, Object>> all = rows(variables, 5);
      if (!"2024".equals(variables.get("year"))) {
        return all;
      }
      return new Iterator<Map<String, Object>>() {
        private int served;

        @Override public boolean hasNext() {
          return all.hasNext();
        }

        @Override public Map<String, Object> next() {
          if (served++ == 3) {
            CommittedRowReconciliationTest.<RuntimeException>sneakyThrow(
                new SkippedBatchException("skipped mid-stream"));
          }
          return all.next();
        }
      };
    };

    String failure = failureOf(config("mismatch", false, null), tracker, secondUnitDiesMidStream);

    assertTrue(failure.contains("registered in the Iceberg commit"), failure);
    assertTrue(tracker.tableMarks.isEmpty(), "table marked complete: " + tracker.tableMarks);
    assertTrue(tracker.comboMarks.isEmpty(), "combos marked complete: " + tracker.comboMarks);
  }

  private String failureOf(EtlPipelineConfig config, RecordingTracker tracker,
      DataProvider provider) {
    try {
      EtlResult result = run(config, tracker, provider, DEFER_TO_STANDARD_WRITER);
      assertTrue(result.isFailed(), "run reported success: " + result.getTotalRows() + " rows");
      return String.valueOf(result.getErrors()) + result.getFailureMessage();
    } catch (IOException e) {
      return String.valueOf(e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }

  private EtlResult run(EtlPipelineConfig config, IncrementalTracker tracker,
      DataProvider provider, DataWriter writer) throws IOException {
    return new EtlPipeline(config, new LocalFileStorageProvider(), tempDir.getAbsolutePath(),
        null, tracker, provider, writer).execute();
  }

  private static Iterator<Map<String, Object>> rows(Map<String, String> variables, int count) {
    int year = Integer.parseInt(variables.get("year"));
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", year * 100 + i);
      row.put("year", year);
      rows.add(row);
    }
    return rows.iterator();
  }

  private EtlPipelineConfig config(String name, boolean overwritePartitions,
      String idCoercionPolicy) {
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER")
            .onCoercionFailure(idCoercionPolicy).build(),
        ColumnConfig.builder().name("year").type("INTEGER").build());
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2023)
        .end(2024)
        .build());
    return EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name(name)
            .targetTableId(name)
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            .options(MaterializeOptionsConfig.builder().batchSize(2).build())
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(tempDir.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(overwritePartitions)
                .build())
            .build())
        .build();
  }
}
