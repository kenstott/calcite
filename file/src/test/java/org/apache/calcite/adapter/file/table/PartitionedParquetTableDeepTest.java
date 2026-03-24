/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
