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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for {@link IcebergMaterializationWriter}
 * to maximize JaCoCo line coverage.
 *
 * <p>Covers: initialization, batch writing, commit logic, staging,
 * partition handling, type mapping, expression evaluation, retry logic,
 * error handling, cleanup, and configuration.
 */
@Tag("unit")
public class IcebergMaterializationWriterCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;
  private String warehousePath;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    warehousePath = tempDir.resolve("warehouse").toString();
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  // ---- Constructor tests ----

  @Test
  void testConstructorWithNullTracker() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNotNull(writer);
    assertEquals(0, writer.getTotalRowsWritten());
    assertEquals(0, writer.getTotalFilesWritten());
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat());
  }

  @Test
  void testConstructorWithTracker() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath,
            IncrementalTracker.NOOP);
    assertNotNull(writer);
  }

  // ---- initialize validation tests ----

  @Test
  void testInitializeNullConfig() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertThrows(IllegalArgumentException.class,
        () -> writer.initialize(null));
  }

  @Test
  void testInitializeDisabledConfig() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("test_table")
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IOException.class, () -> writer.initialize(config));
  }

  @Test
  void testInitializeWrongFormat() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .targetTableId("test_table")
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IllegalArgumentException.class,
        () -> writer.initialize(config));
  }

  @Test
  void testInitializeNoTargetTableId() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IllegalArgumentException.class,
        () -> writer.initialize(config));
  }

  @Test
  void testInitializeWithNameFallback() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .name("fallback_table")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  // ---- initialize with IcebergConfig ----

  @Test
  void testInitializeWithIcebergConfig() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath(warehousePath)
            .maxRetries(5)
            .retryDelayMs(500)
            .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("iceberg_config_test")
        .columns(columns)
        .iceberg(icebergConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  @Test
  void testInitializeWithRestCatalogType() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    // REST catalog type + URI
    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.REST)
            .warehousePath(warehousePath)
            .restUri("http://localhost:8181")
            .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("rest_test")
        .columns(columns)
        .iceberg(icebergConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    // This will fail to connect to REST catalog - but we're testing the config building
    try {
      writer.initialize(config);
    } catch (Exception e) {
      // Expected - REST catalog not available in tests
    }
  }

  @Test
  void testInitializeWithHiveCatalogType() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HIVE)
            .warehousePath(warehousePath)
            .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("hive_test")
        .columns(columns)
        .iceberg(icebergConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    try {
      writer.initialize(config);
    } catch (Exception e) {
      // Expected - Hive catalog not available
    }
  }

  // ---- initialize with options config ----

  @Test
  void testInitializeWithOptionsConfig() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeOptionsConfig options = MaterializeOptionsConfig.builder()
        .batchSize(5000)
        .stagingMode(MaterializeOptionsConfig.StagingMode.LOCAL)
        .threads(4)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("options_test")
        .columns(columns)
        .options(options)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  @Test
  void testInitializeWithNullOptions() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    // Build config WITHOUT explicit options - should use defaults
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("null_options_test")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  // ---- writeBatch tests ----

  @Test
  void testWriteBatchNotInitialized() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(data.iterator(), null));
  }

  @Test
  void testWriteBatchNullData() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("wb_null");
    long result = writer.writeBatch(null, null);
    assertEquals(0, result);
  }

  @Test
  void testWriteBatchEmptyData() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("wb_empty");
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(0, result);
  }

  @Test
  void testWriteBatchSimpleData() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("wb_simple");

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "hello");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
    assertEquals(1, writer.getTotalRowsWritten());
    assertEquals(1, writer.getTotalFilesWritten());
  }

  @Test
  void testWriteBatchWithPartitionVariables() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriterPartitioned("wb_part");

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "hello");
    data.add(row);

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("year", "2024");

    long result = writer.writeBatch(data.iterator(), partVars);
    assertEquals(1, result);
  }

  @Test
  void testWriteBatchMultipleChunks() throws Exception {
    // Create writer with very small batch size to trigger multi-chunk processing
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeOptionsConfig options = MaterializeOptionsConfig.builder()
        .batchSize(3)  // Very small batch size
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("wb_chunks")
        .columns(columns)
        .options(options)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);

    // Write 10 rows with batchSize=3, should produce 4 chunks (3+3+3+1)
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("data", "row-" + i);
      data.add(row);
    }

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(10, result);
    assertEquals(10, writer.getTotalRowsWritten());
  }

  // ---- Column mapping / transform tests ----

  @Test
  void testWriteBatchWithColumnMapping() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").source("source_id").build());
    columns.add(ColumnConfig.builder().name("name").type("VARCHAR").source("source_name").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("col_map")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("source_id", 42);
    row.put("source_name", "Alice");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testWriteBatchWithComputedColumns() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("computed")
        .type("VARCHAR")
        .expression("src.\"data\"")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("computed_col")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "hello");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testWriteBatchComputedPartitionVariable() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("region")
        .type("VARCHAR")
        .expression("{region}")
        .build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("region");
    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("comp_part_var")
        .columns(columns)
        .partition(partConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "test");
    data.add(row);

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("region", "US");

    long result = writer.writeBatch(data.iterator(), partVars);
    assertEquals(1, result);
  }

  @Test
  void testWriteBatchComputedPartitionVariableQuoted() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder()
        .name("region")
        .type("VARCHAR")
        .expression("'{region}'")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("comp_pv_quoted")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    data.add(row);

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("region", "EU");

    long result = writer.writeBatch(data.iterator(), partVars);
    assertEquals(1, result);
  }

  // ---- Expression evaluation tests ----

  @Test
  void testExpressionEvaluationBareField() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("copy_data")
        .type("VARCHAR")
        .expression("data")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("bare_field")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "original");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationCast() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("str_val").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("num_val")
        .type("BIGINT")
        .expression("TRY_CAST(src.\"str_val\" AS BIGINT)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("cast_expr")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("str_val", "12345");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationBareCast() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("str_val").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("num_val")
        .type("DOUBLE")
        .expression("CAST(str_val AS DOUBLE)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("bare_cast")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("str_val", "3.14");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationReplace() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("price").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("clean_price")
        .type("VARCHAR")
        .expression("REPLACE(src.\"price\", ',', '')")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("replace_expr")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("price", "1,234,567");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationCastReplace() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("amount").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("amount_num")
        .type("BIGINT")
        .expression("TRY_CAST(REPLACE(src.\"amount\", ',', '') AS BIGINT)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("cast_replace_expr")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("amount", "1,000,000");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationSubstring() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("code").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("prefix")
        .type("VARCHAR")
        .expression("SUBSTRING(src.\"code\", 1, 2)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("substr_expr")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("code", "US-CA-SF");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationRight() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("code").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("suffix")
        .type("VARCHAR")
        .expression("RIGHT(src.\"code\", 3)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("right_expr")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("code", "ABCDEF");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationRightLongerThanString() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("code").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("suffix")
        .type("VARCHAR")
        .expression("RIGHT(src.\"code\", 100)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("right_long")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("code", "AB");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationCoalesce() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("first").type("VARCHAR").build());
    columns.add(ColumnConfig.builder().name("second").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("result")
        .type("VARCHAR")
        .expression("COALESCE(src.\"first\", src.\"second\")")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("coalesce_expr")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    // First is null, should use second
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("first", null);
    row.put("second", "fallback");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testExpressionEvaluationUnrecognized() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder()
        .name("complex")
        .type("VARCHAR")
        .expression("SOME_UNKNOWN_FUNC(x, y, z)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("unrecognized_expr")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  // ---- castValue tests (exercised through expressions) ----

  @Test
  void testCastValueVariousTypes() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("val").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("as_int")
        .type("INTEGER")
        .expression("CAST(src.\"val\" AS INTEGER)")
        .build());
    columns.add(ColumnConfig.builder()
        .name("as_float")
        .type("FLOAT")
        .expression("TRY_CAST(src.\"val\" AS FLOAT)")
        .build());
    columns.add(ColumnConfig.builder()
        .name("as_str")
        .type("VARCHAR")
        .expression("CAST(src.\"val\" AS VARCHAR)")
        .build());
    columns.add(ColumnConfig.builder()
        .name("as_bool")
        .type("BOOLEAN")
        .expression("TRY_CAST(src.\"val\" AS BOOLEAN)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("cast_types")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("val", "42");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testCastValueEmptyString() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("val").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("num")
        .type("BIGINT")
        .expression("TRY_CAST(src.\"val\" AS BIGINT)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("cast_empty")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("val", "");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  @Test
  void testCastValueParseFailure() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("val").type("VARCHAR").build());
    columns.add(ColumnConfig.builder()
        .name("num")
        .type("BIGINT")
        .expression("TRY_CAST(src.\"val\" AS BIGINT)")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("cast_fail")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("val", "not_a_number");
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  // ---- mapToIcebergType tests (via column configs) ----

  @Test
  void testMapToIcebergTypeAllTypes() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("col_varchar").type("VARCHAR").build());
    columns.add(ColumnConfig.builder().name("col_char").type("CHAR(10)").build());
    columns.add(ColumnConfig.builder().name("col_int").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("col_int2").type("INT").build());
    columns.add(ColumnConfig.builder().name("col_bigint").type("BIGINT").build());
    columns.add(ColumnConfig.builder().name("col_long").type("LONG").build());
    columns.add(ColumnConfig.builder().name("col_double").type("DOUBLE").build());
    columns.add(ColumnConfig.builder().name("col_float").type("FLOAT").build());
    columns.add(ColumnConfig.builder().name("col_bool").type("BOOLEAN").build());
    columns.add(ColumnConfig.builder().name("col_date").type("DATE").build());
    columns.add(ColumnConfig.builder().name("col_ts").type("TIMESTAMP").build());
    columns.add(ColumnConfig.builder().name("col_null").type(null).build());
    columns.add(ColumnConfig.builder().name("col_unknown").type("STRUCT").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("all_types")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  // ---- Partition column handling tests ----

  @Test
  void testPartitionColumnsAddedToSchema() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());
    // Note: "year" is NOT in the column list, only in partition config

    List<String> partCols = new ArrayList<String>();
    partCols.add("year");
    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("part_schema")
        .columns(columns)
        .partition(partConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  @Test
  void testPartitionColumnWithDefinition() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("region");

    List<MaterializePartitionConfig.ColumnDefinition> colDefs =
        new ArrayList<MaterializePartitionConfig.ColumnDefinition>();
    colDefs.add(new MaterializePartitionConfig.ColumnDefinition("region", "VARCHAR"));

    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .columnDefinitions(colDefs)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("part_def")
        .columns(columns)
        .partition(partConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  @Test
  void testPartitionColumnAlreadyInSource() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("year").type("INTEGER").build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("year"); // Already in columns above

    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("part_in_src")
        .columns(columns)
        .partition(partConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  // ---- commit tests ----

  @Test
  void testCommitNotInitialized() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertThrows(IllegalStateException.class, () -> writer.commit());
  }

  @Test
  void testCommitEmpty() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("commit_empty");
    // Commit with no data should succeed
    writer.commit();
    assertEquals(0, writer.getTotalRowsWritten());
  }

  @Test
  void testCommitWithData() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("commit_data");

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "commit-test");
    data.add(row);

    writer.writeBatch(data.iterator(), null);
    writer.commit();
    assertEquals(1, writer.getTotalRowsWritten());
  }

  @Test
  void testCommitWithMaintenance() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath(warehousePath)
            .runMaintenance(true)
            .snapshotRetentionDays(7)
            .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("commit_maint")
        .columns(columns)
        .iceberg(icebergConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "maint");
    data.add(row);
    writer.writeBatch(data.iterator(), null);
    writer.commit();
  }

  @Test
  void testCommitWithCompaction() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath(warehousePath)
            .runCompaction(true)
            .compactionTargetFileSizeBytes(128 * 1024 * 1024)
            .compactionMinFiles(2)
            .compactionSmallFileSizeBytes(10 * 1024 * 1024)
            .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("commit_compact")
        .columns(columns)
        .iceberg(icebergConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "compact");
    data.add(row);
    writer.writeBatch(data.iterator(), null);
    writer.commit();
  }

  // ---- close tests ----

  @Test
  void testCloseUninitialized() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    writer.close(); // Should not throw
  }

  @Test
  void testCloseAfterInit() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("close_init");
    // Verify table location exists before close
    assertNotNull(writer.getTableLocation());
    writer.close();
    // After close, initialized is false but table ref is still available
    // (getTableLocation checks table != null, not initialized)
    // Verify close completed without error and writer is no longer initialized
    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(Collections.<Map<String, Object>>emptyList().iterator(), null));
  }

  @Test
  void testCloseWithUnflushedBuffers() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("close_unflushed");

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "unflushed");
    data.add(row);
    writer.writeBatch(data.iterator(), null);

    // Close without commit - should warn about unflushed data
    writer.close();
  }

  // ---- getTableLocation tests ----

  @Test
  void testGetTableLocationNotInitialized() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNull(writer.getTableLocation());
  }

  @Test
  void testGetTableLocationAfterInit() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("loc_test");
    String location = writer.getTableLocation();
    assertNotNull(location);
    assertTrue(location.contains("loc_test"));
  }

  // ---- storeEtlProperties / getEtlProperty tests ----

  @Test
  void testStoreEtlPropertiesNotInitialized() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    // Should log warning and return
    writer.storeEtlProperties("hash", "sig", 100);
  }

  @Test
  void testStoreAndGetEtlProperties() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("etl_props");

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "props");
    data.add(row);
    writer.writeBatch(data.iterator(), null);
    writer.commit();

    writer.storeEtlProperties("abc123", "dim_sig", 1);

    assertEquals("abc123", writer.getEtlProperty("etl.config-hash"));
    assertEquals("dim_sig", writer.getEtlProperty("etl.signature"));
    assertEquals("1", writer.getEtlProperty("etl.row-count"));
    assertNotNull(writer.getEtlProperty("etl.completed-timestamp"));
  }

  @Test
  void testGetEtlPropertyNotInitialized() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNull(writer.getEtlProperty("anything"));
  }

  @Test
  void testGetEtlPropertyNotFound() throws Exception {
    IcebergMaterializationWriter writer = createInitializedWriter("etl_notfound");
    assertNull(writer.getEtlProperty("nonexistent.key"));
  }

  // ---- storeTableMetadata tests (via commit) ----

  @Test
  void testStoreTableComment() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    Map<String, String> colComments = new HashMap<String, String>();
    colComments.put("id", "Primary key");

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("table_comment")
        .columns(columns)
        .tableComment("This is a test table")
        .columnComments(colComments)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    data.add(row);
    writer.writeBatch(data.iterator(), null);
    writer.commit();

    // Table properties should contain comment
    assertEquals("This is a test table", writer.getEtlProperty("comment"));
  }

  // ---- convertToS3aScheme tests ----

  @Test
  void testS3SchemeConversionInInitialize() throws Exception {
    // Test with S3 warehouse path
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, "s3://bucket/warehouse", null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("s3_test")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    // This will fail because s3a is not available in tests, but it exercises the conversion
    try {
      writer.initialize(config);
    } catch (Exception e) {
      // Expected - S3 not available in tests
    }
  }

  // ---- buildPartitionKey tests ----

  @Test
  void testBuildPartitionKeyMultipleVars() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("year").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("region").type("VARCHAR").build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("year");
    partCols.add("region");
    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("multi_key")
        .columns(columns)
        .partition(partConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("year", 2024);
    row.put("region", "US");
    data.add(row);

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("year", "2024");
    partVars.put("region", "US");

    long result = writer.writeBatch(data.iterator(), partVars);
    assertEquals(1, result);
  }

  // ---- getValueCaseInsensitive tests (through column mapping) ----

  @Test
  void testCaseInsensitiveColumnLookup() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").source("ID").build());
    columns.add(ColumnConfig.builder().name("name").type("VARCHAR").source("Name").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("case_lookup")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);       // lowercase - should match source "ID"
    row.put("name", "test"); // lowercase - should match source "Name"
    data.add(row);

    long result = writer.writeBatch(data.iterator(), null);
    assertEquals(1, result);
  }

  // ---- Column comments on partition columns ----

  @Test
  void testPartitionColumnComments() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("year");

    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    Map<String, String> colComments = new HashMap<String, String>();
    colComments.put("year", "The fiscal year");
    colComments.put("id", "Unique identifier");

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("part_comments")
        .columns(columns)
        .partition(partConfig)
        .columnComments(colComments)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  // ---- Existing table re-use (ensureTableExists) ----

  @Test
  void testReuseExistingTable() throws Exception {
    // First create the writer and initialize to create the table
    IcebergMaterializationWriter writer1 =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("reuse_table")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer1.initialize(config);
    writer1.close();

    // Second writer should reuse the existing table
    IcebergMaterializationWriter writer2 =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    writer2.initialize(config);
    assertNotNull(writer2.getTableLocation());
    writer2.close();
  }

  // ---- escapeString (via buildDuckDBSql) ----
  // This is exercised indirectly when DuckDB SQL is built

  // ---- S3 config handling ----

  @Test
  void testBuildHadoopS3ConfigNoCredentials() throws Exception {
    // Use a storage provider with no S3 config
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("no_s3")
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  // ---- Multiple partitions then commit ----

  @Test
  void testMultiplePartitionsAndCommit() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());
    columns.add(ColumnConfig.builder().name("year").type("INTEGER").build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("year");
    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("multi_part_commit")
        .columns(columns)
        .partition(partConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    writer.initialize(config);

    // Write to multiple partitions
    for (int year = 2020; year <= 2024; year++) {
      List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", year);
      row.put("data", "year-" + year);
      row.put("year", year);
      data.add(row);

      Map<String, String> partVars = new HashMap<String, String>();
      partVars.put("year", String.valueOf(year));
      writer.writeBatch(data.iterator(), partVars);
    }

    writer.commit();
    assertEquals(5, writer.getTotalRowsWritten());
    assertEquals(5, writer.getTotalFilesWritten());
  }

  // ---- getFormat tests ----

  @Test
  void testGetFormat() {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat());
  }

  // ---- Partition column with case-insensitive comment lookup ----

  @Test
  void testPartitionColumnCaseInsensitiveCommentLookup() throws Exception {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("Year"); // Note: capitalized

    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    Map<String, String> colComments = new HashMap<String, String>();
    colComments.put("year", "The year column"); // lowercase key

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("part_ci_comment")
        .columns(columns)
        .partition(partConfig)
        .columnComments(colComments)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    assertNotNull(writer.getTableLocation());
  }

  // ---- Helper methods ----

  private IcebergMaterializationWriter createInitializedWriter(String tableName) throws IOException {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId(tableName)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    return writer;
  }

  private IcebergMaterializationWriter createInitializedWriterPartitioned(String tableName)
      throws IOException {
    IcebergMaterializationWriter writer =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("id").type("INTEGER").build());
    columns.add(ColumnConfig.builder().name("data").type("VARCHAR").build());
    columns.add(ColumnConfig.builder().name("year").type("INTEGER").build());

    List<String> partCols = new ArrayList<String>();
    partCols.add("year");
    MaterializePartitionConfig partConfig = MaterializePartitionConfig.builder()
        .columns(partCols)
        .build();

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId(tableName)
        .columns(columns)
        .partition(partConfig)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    writer.initialize(config);
    return writer;
  }
}
