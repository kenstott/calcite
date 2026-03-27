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
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for PartitionedParquetTable focusing on constructors,
 * getRowType, getFilePaths, comments, and partition handling.
 */
@Tag("unit")
public class PartitionedParquetTableCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // Tests for constructors (various overloads)
  // ====================================================================

  @Test
  void testThreeArgConstructor() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedParquetTable table = new PartitionedParquetTable(files, null, engineConfig);
    assertNotNull(table);
    assertNotNull(table.getFilePaths());
    assertTrue(table.getFilePaths().isEmpty());
  }

  @Test
  void testFourArgConstructor() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);
    Map<String, String> types = new HashMap<String, String>();
    types.put("year", "INTEGER");

    PartitionedParquetTable table = new PartitionedParquetTable(files, null, engineConfig, types);
    assertNotNull(table);
  }

  @Test
  void testSixArgConstructor() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null);
    assertNotNull(table);
  }

  @Test
  void testSevenArgConstructor() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    Map<String, Object> constraintConfig = new HashMap<String, Object>();
    constraintConfig.put("primaryKey", Arrays.asList("id"));

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null, constraintConfig);
    assertNotNull(table);
  }

  @Test
  void testNineArgConstructor() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null, null, "test_schema", "test_table");
    assertNotNull(table);
  }

  @Test
  void testTenArgConstructor() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null, null,
        "test_schema", "test_table", null);
    assertNotNull(table);
  }

  @Test
  void testFullConstructorWithPartitionInfo() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    List<String> partitionCols = new ArrayList<String>();
    partitionCols.add("year");
    partitionCols.add("month");
    PartitionDetector.PartitionInfo partInfo = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), partitionCols, true);

    Map<String, String> colTypes = new HashMap<String, String>();
    colTypes.put("year", "INTEGER");
    colTypes.put("month", "VARCHAR");

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, partInfo, engineConfig, colTypes, null, null, null,
        null, "test_table", null, null);
    assertNotNull(table);
  }

  // ====================================================================
  // Tests for getFilePaths
  // ====================================================================

  @Test
  void testGetFilePathsReturnsOriginalList() {
    List<String> files = new ArrayList<String>();
    files.add("/path/file1.parquet");
    files.add("/path/file2.parquet");

    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedParquetTable table = new PartitionedParquetTable(files, null, engineConfig);
    assertEquals(2, table.getFilePaths().size());
    assertEquals("/path/file1.parquet", table.getFilePaths().get(0));
  }

  // ====================================================================
  // Tests for CommentableTable methods
  // ====================================================================

  @Test
  void testGetTableCommentFromConfig() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedTableConfig config = mock_config("Test table comment",
        Collections.singletonMap("col1", "Column 1 description"));

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null, null,
        null, "test_table", null, config);

    assertEquals("Test table comment", table.getTableComment());
    assertEquals("Column 1 description", table.getColumnComment("col1"));
    assertNull(table.getColumnComment("nonexistent"));
  }

  @Test
  void testGetTableCommentNoConfig() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null, null,
        null, "test_table", null, null);

    assertNull(table.getTableComment());
    assertNull(table.getColumnComment("col1"));
  }

  // ====================================================================
  // Tests for empty file list
  // ====================================================================

  @Test
  void testEmptyFileListHandling() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null, null,
        null, null, null, null);
    assertNotNull(table);
    assertTrue(table.getFilePaths().isEmpty());
  }

  // ====================================================================
  // Tests for partition columns with types
  // ====================================================================

  @Test
  void testPartitionColumnTypes() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    List<String> partCols = new ArrayList<String>();
    partCols.add("year");
    PartitionDetector.PartitionInfo partInfo = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), partCols, true);

    Map<String, String> colTypes = new HashMap<String, String>();
    colTypes.put("year", "INTEGER");

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, partInfo, engineConfig, colTypes, null, null, null,
        "schema", "table", null, null);
    assertNotNull(table);
  }

  // ====================================================================
  // Tests with PartitionedTableConfig with comments
  // ====================================================================

  @Test
  void testConfigWithColumnComments() {
    List<String> files = Collections.emptyList();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    Map<String, String> colComments = new HashMap<String, String>();
    colComments.put("id", "Primary key");
    colComments.put("name", "User full name");

    PartitionedTableConfig config = mock_config(null, colComments);

    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, engineConfig, null, null, null, null,
        null, "test", null, config);

    assertEquals("Primary key", table.getColumnComment("id"));
    assertEquals("User full name", table.getColumnComment("name"));
  }

  // ====================================================================
  // Helper to create PartitionedTableConfig stubs
  // ====================================================================

  private PartitionedTableConfig mock_config(String tableComment,
      Map<String, String> columnComments) {
    // Use a real PartitionedTableConfig via fromMap
    // column_comments must be a list of {name, comment} maps
    Map<String, Object> configMap = new HashMap<String, Object>();
    if (tableComment != null) {
      configMap.put("comment", tableComment);
    }
    if (columnComments != null) {
      List<Map<String, String>> colCommentsList = new ArrayList<Map<String, String>>();
      for (Map.Entry<String, String> entry : columnComments.entrySet()) {
        Map<String, String> item = new HashMap<String, String>();
        item.put("name", entry.getKey());
        item.put("comment", entry.getValue());
        colCommentsList.add(item);
      }
      configMap.put("column_comments", colCommentsList);
    }
    return PartitionedTableConfig.fromMap(configMap);
  }
}
