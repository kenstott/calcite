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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.partition.PartitionDetector;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for PartitionedParquetTable focusing on
 * partition column management and edge cases.
 */
@Tag("unit")
class PartitionedParquetTableDeepTest {

  private PartitionedParquetTable createTable(List<String> filePaths,
      List<String> partitionColumns) {
    Map<String, String> partitionValues = new HashMap<String, String>();
    for (String col : partitionColumns) {
      partitionValues.put(col, "test");
    }
    PartitionDetector.PartitionInfo partInfo =
        new PartitionDetector.PartitionInfo(partitionValues, partitionColumns, true);
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("linq4j", 1024);
    return new PartitionedParquetTable(filePaths, partInfo, engineConfig);
  }

  @Test void testPartitionedParquetTableCreation() {
    PartitionedParquetTable table = createTable(
        Collections.singletonList("/data/year=2020/data.parquet"),
        Arrays.asList("year", "month"));
    assertNotNull(table);
  }

  @Test void testGetPartitionColumns() {
    PartitionedParquetTable table = createTable(
        Collections.singletonList("/data/year=2020/month=01/data.parquet"),
        Arrays.asList("year", "month"));
    List<String> partCols = table.getPartitionColumns();
    assertNotNull(partCols);
    assertEquals(2, partCols.size());
    assertTrue(partCols.contains("year"));
    assertTrue(partCols.contains("month"));
  }

  @Test void testIsPartitionColumn() {
    PartitionedParquetTable table = createTable(
        Collections.singletonList("/data/year=2020/data.parquet"),
        Arrays.asList("year", "month"));
    assertTrue(table.isPartitionColumn("year"));
    assertTrue(table.isPartitionColumn("month"));
    assertFalse(table.isPartitionColumn("day"));
    assertFalse(table.isPartitionColumn(null));
  }

  @Test void testGetDistinctPartitionValuesNonPartitionColumn() {
    PartitionedParquetTable table = createTable(
        Collections.singletonList("/data/year=2020/data.parquet"),
        Arrays.asList("year"));
    // Non-partition column should return empty
    List<String> values = table.getDistinctPartitionValues("nonexistent");
    assertNotNull(values);
    assertTrue(values.isEmpty());
  }

  @Test void testGetPartitionColumnsEmpty() {
    Map<String, String> partitionValues = new HashMap<String, String>();
    PartitionDetector.PartitionInfo partInfo =
        new PartitionDetector.PartitionInfo(partitionValues,
            Collections.<String>emptyList(), true);
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("linq4j", 1024);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/data/data.parquet"), partInfo, engineConfig);
    List<String> partCols = table.getPartitionColumns();
    assertNotNull(partCols);
    assertTrue(partCols.isEmpty());
  }

  @Test void testNullPartitionInfo() {
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("linq4j", 1024);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/data/data.parquet"), null, engineConfig);
    assertNotNull(table);
    List<String> partCols = table.getPartitionColumns();
    assertNotNull(partCols);
    assertTrue(partCols.isEmpty());
  }
}
