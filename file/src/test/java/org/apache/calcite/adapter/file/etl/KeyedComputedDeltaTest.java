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
 * A computed_delta table with {@code incremental.keyFields} appends exactly the record versions it
 * does not hold, whatever the tracker remembers and in whatever order the source publishes them.
 */
@Tag("unit")
class KeyedComputedDeltaTest {

  private static final String NAME = "keyed_delta_test";

  @TempDir
  File tempDir;

  @Test void appendsOnlyVersionsTheTableLacksWithoutAnyTrackerState() throws Exception {
    assertEquals(2L, run(keyed(), row("a", "2026-01-02T10:00:00+02:00"),
        row("b", "2026-01-02T09:00:00Z")));

    // Each run gets an empty tracker. "c" is stamped earlier than everything already appended.
    assertEquals(2L, run(keyed(), row("a", "2026-01-02T10:00:00+02:00"),
        row("b", "2026-01-02T09:00:00Z"),
        row("a", "2026-01-03T00:00:00Z"),
        row("c", "2025-12-31T00:00:00Z")));

    assertEquals(0L, run(keyed(), row("c", "2025-12-31T00:00:00Z")));
  }

  @Test void appendsARepeatedSourceRowOnce() throws Exception {
    assertEquals(1L, run(keyed(), row("a", "2026-01-01"), row("a", "2026-01-01")));
  }

  @Test void failsOnAnEmptyKeyValue() throws Exception {
    EtlResult result = execute(keyed(), row("a", ""));
    assertTrue(result.isFailed() || result.getFailedBatches() > 0, "empty key was accepted");
    assertTrue(String.valueOf(result.getErrors()).contains("is empty in a source row"),
        String.valueOf(result.getErrors()));
  }

  @Test void failsWhenAHighWaterMarkFieldIsAlsoDeclared() throws Exception {
    Map<String, Object> incremental = new HashMap<String, Object>();
    incremental.put("keyFields", Arrays.asList("id", "modified"));
    incremental.put("dateField", "modified");
    EtlResult result = execute(config(incremental), row("a", "2026-01-01"));
    assertTrue(String.valueOf(result.getErrors()).contains("use one"),
        String.valueOf(result.getErrors()));
  }

  private long run(EtlPipelineConfig config, Map<String, Object>... rows) throws Exception {
    EtlResult result = execute(config, rows);
    assertFalse(result.isFailed(), String.valueOf(result.getFailureMessage()));
    assertEquals(0, result.getFailedBatches(), String.valueOf(result.getErrors()));
    return result.getTotalRows();
  }

  private EtlResult execute(EtlPipelineConfig config, Map<String, Object>... rows)
      throws Exception {
    final List<Map<String, Object>> source = Arrays.asList(rows);
    DataProvider provider = (c, variables) -> new ArrayList<Map<String, Object>>(source).iterator();
    return new EtlPipeline(config, new LocalFileStorageProvider(), tempDir.getAbsolutePath(),
        null, IncrementalTracker.NOOP, provider, null).execute();
  }

  private static Map<String, Object> row(String id, String modified) {
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("Record.Id", id);
    row.put("modified", modified);
    row.put("type", "t");
    return row;
  }

  private EtlPipelineConfig keyed() {
    return config(
        Collections.<String, Object>singletonMap("keyFields", Arrays.asList("id", "modified")));
  }

  private EtlPipelineConfig config(Map<String, Object> incremental) {
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("VARCHAR").source("Record.Id").build(),
        ColumnConfig.builder().name("modified").type("VARCHAR").build(),
        ColumnConfig.builder().name("type").type("VARCHAR").build());
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("type", DimensionConfig.builder()
        .name("type")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("t"))
        .build());
    return EtlPipelineConfig.builder()
        .name(NAME)
        .datasetType("computed_delta")
        .columns(columns)
        .source(HttpSourceConfig.builder()
            .url("https://example.invalid/api")
            .incremental(HttpSourceConfig.IncrementalConfig.fromMap(incremental))
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name(NAME)
            .targetTableId(NAME)
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            .partition(MaterializePartitionConfig.builder()
                .columns(Collections.singletonList("type"))
                .build())
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(tempDir.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(false)
                .build())
            .build())
        .build();
  }
}
