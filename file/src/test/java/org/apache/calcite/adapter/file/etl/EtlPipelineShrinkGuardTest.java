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

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A short load of a replace-partitions table with {@code failOnPartitionShrink} fails the run,
 * keeps the committed rows and records nothing as processed.
 */
@Tag("unit")
class EtlPipelineShrinkGuardTest {

  @TempDir
  File tempDir;

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private EtlPipelineConfig config() {
    Map<String, Object> iceberg = new HashMap<String, Object>();
    iceberg.put("catalogType", "hadoop");
    iceberg.put("warehousePath", tempDir.getAbsolutePath());
    iceberg.put("overwritePartitions", true);
    iceberg.put("failOnPartitionShrink", true);
    iceberg.put("maxPartitionShrinkFraction", 0.001);
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2024)
        .end(2024)
        .build());
    return EtlPipelineConfig.builder()
        .name("shrink_guard_test")
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name("shrink_guard_test")
            .targetTableId("shrink_guard_test")
            .output(MaterializeOutputConfig.builder().build())
            .columns(
                Arrays.asList(ColumnConfig.builder().name("id").type("INTEGER").build(),
                    ColumnConfig.builder().name("year").type("INTEGER").build()))
            .iceberg(MaterializeConfig.IcebergConfig.fromMap(iceberg))
            .build())
        .build();
  }

  private static DataProvider rows(final int count) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < count; i++) {
          Map<String, Object> row = new HashMap<String, Object>();
          row.put("id", i);
          row.put("year", 2024);
          rows.add(row);
        }
        return rows.iterator();
      }
    };
  }

  private EtlResult run(int count, EtlPipelineCommitFailureTrackerTest.RecordingTracker tracker)
      throws IOException {
    return new EtlPipeline(config(), new LocalFileStorageProvider(), tempDir.getAbsolutePath(),
        null, tracker, rows(count), null).execute();
  }

  private long committedRows() throws IOException {
    IcebergCatalogManager.clearCache();
    Map<String, Object> catalog = new HashMap<String, Object>();
    catalog.put("catalogType", "hadoop");
    catalog.put("warehousePath", tempDir.getAbsolutePath());
    Table table = IcebergCatalogManager.loadTable(catalog, "shrink_guard_test");
    long count = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        count += task.file().recordCount();
      }
    }
    return count;
  }

  @Test void shortLoadFailsTheRunAndKeepsTheTable() throws IOException {
    EtlPipelineCommitFailureTrackerTest.RecordingTracker tracker =
        new EtlPipelineCommitFailureTrackerTest.RecordingTracker();
    assertTrue(!run(5000, tracker).isFailed());
    assertEquals(5000, committedRows());
    tracker.processedCalls.clear();

    EtlResult result = run(635, tracker);

    assertTrue(result.isFailed());
    assertTrue(result.getFailureMessage().contains("Bulk commit failed"),
        result.getFailureMessage());
    assertEquals(5000, committedRows());
    assertTrue(tracker.processedCalls.isEmpty());
  }
}
