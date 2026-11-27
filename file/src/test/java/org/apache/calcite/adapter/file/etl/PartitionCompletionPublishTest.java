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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A table partitioned by year but fetched by (year, region) publishes each year once all its
 * regions are written, when it declares {@code commitPerPartition}.
 */
@Tag("unit")
@Isolated
class PartitionCompletionPublishTest {

  private static final String TABLE = "partition_completion_test";

  @TempDir
  File tempDir;

  /** A schema run always hands the pipeline a writer hook, so the tests do too. */
  private final DataWriter dataWriter = decliningDataWriter();

  @Test void failedUnitStillPublishesTheCompletePartitions() throws Exception {
    EtlPipelineCommitFailureTrackerTest.RecordingTracker tracker =
        new EtlPipelineCommitFailureTrackerTest.RecordingTracker();

    EtlResult result = run(true, "2024/b", tracker);

    assertTrue(result.isFailed(), "the failed unit must still fail the table");
    assertEquals(Arrays.asList("2025/a", "2025/b"), committed(),
        "the complete year is committed; the year missing a region is not");
    assertEquals(Arrays.asList("2025/a", "2025/b"), marked(tracker, "2024/b"),
        "only combos of the committed year are marked complete");
  }

  @Test void withoutTheOptionNothingIsCommitted() throws Exception {
    EtlPipelineCommitFailureTrackerTest.RecordingTracker tracker =
        new EtlPipelineCommitFailureTrackerTest.RecordingTracker();

    EtlResult result = run(false, "2024/b", tracker);

    assertTrue(result.isFailed());
    assertEquals(Collections.<String>emptyList(), committed());
    assertEquals(Collections.<String>emptyList(), marked(tracker, "2024/b"));
  }

  @Test void cleanRunCommitsAndMarksEveryUnitOnce() throws Exception {
    EtlPipelineCommitFailureTrackerTest.RecordingTracker tracker =
        new EtlPipelineCommitFailureTrackerTest.RecordingTracker();

    EtlResult result = run(true, null, tracker);

    assertTrue(result.isCompleteSuccess(), String.valueOf(result.getFailureMessage()));
    List<String> all = Arrays.asList("2024/a", "2024/b", "2025/a", "2025/b");
    assertEquals(all, committed());
    assertEquals(all, marked(tracker, null));
  }

  @Test void failedRunKeepsThePreviousRowsOfTheIncompletePartition() throws Exception {
    run(true, null, new EtlPipelineCommitFailureTrackerTest.RecordingTracker());

    run(true, "2024/b", new EtlPipelineCommitFailureTrackerTest.RecordingTracker());

    assertEquals(Arrays.asList("2024/a", "2024/b", "2025/a", "2025/b"), committed());
  }

  private EtlResult run(boolean commitPerPartition, final String failingUnit,
      IncrementalTracker tracker) throws IOException {
    File warehouse = new File(tempDir, "warehouse");
    warehouse.mkdirs();
    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        String unit = variables.get("year") + "/" + variables.get("region");
        if (unit.equals(failingUnit)) {
          throw new IllegalStateException("source refused " + unit);
        }
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("year", Integer.parseInt(variables.get("year")));
        row.put("region", variables.get("region"));
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        rows.add(row);
        return rows.iterator();
      }
    };
    return new EtlPipeline(config(warehouse, commitPerPartition), new LocalFileStorageProvider(),
        warehouse.getAbsolutePath(), null, tracker, provider, dataWriter).execute();
  }

  /** Declines every unit, as the schema lifecycle's writer hook does by default. */
  private static DataWriter decliningDataWriter() {
    return new DataWriter() {
      @Override public long write(EtlPipelineConfig config, Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        return -1;
      }
    };
  }

  private static EtlPipelineConfig config(File warehouse, boolean commitPerPartition) {
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("year").type("INTEGER").build(),
        ColumnConfig.builder().name("region").type("VARCHAR").build());
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year").type(DimensionType.YEAR_RANGE).start(2024).end(2025).build());
    dims.put("region", DimensionConfig.builder()
        .name("region").type(DimensionType.LIST).values(Arrays.asList("a", "b")).build());
    return EtlPipelineConfig.builder()
        .name(TABLE)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name(TABLE)
            .targetTableId(TABLE)
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            .partition(MaterializePartitionConfig.builder()
                .columns(Collections.singletonList("year"))
                .build())
            .options(MaterializeOptionsConfig.builder()
                .commitPerPartition(commitPerPartition)
                .build())
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(warehouse.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(true)
                .build())
            .build())
        .build();
  }

  /**
   * The combos marked complete. The tracker's default error mark records the failing combo through
   * the same method, so that one is left out.
   */
  private static List<String> marked(EtlPipelineCommitFailureTrackerTest.RecordingTracker tracker,
      String failingUnit) {
    List<String> units = new ArrayList<String>();
    for (Map<String, String> call : tracker.processedCalls) {
      String unit = call.get("year") + "/" + call.get("region");
      if (!unit.equals(failingUnit)) {
        units.add(unit);
      }
    }
    Collections.sort(units);
    return units;
  }

  /** The committed rows as year/region, sorted; empty when nothing has been committed. */
  private List<String> committed() throws IOException {
    File location = new File(tempDir, "warehouse/" + TABLE);
    HadoopTables tables = new HadoopTables(new Configuration());
    if (!tables.exists(location.getAbsolutePath())) {
      return Collections.emptyList();
    }
    Table table = tables.load(location.getAbsolutePath());
    TreeSet<String> rows = new TreeSet<String>();
    int count = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(record.getField("year") + "/" + record.getField("region"));
        count++;
      }
    }
    assertEquals(rows.size(), count, "no row may be committed twice");
    return new ArrayList<String>(rows);
  }
}
