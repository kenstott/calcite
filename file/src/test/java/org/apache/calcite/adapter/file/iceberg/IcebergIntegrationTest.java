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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Iceberg components using a real local Hadoop catalog.
 *
 * <p>Tests exercise end-to-end Iceberg operations including table creation,
 * record writing, reading, compaction, maintenance, and catalog management
 * against a temporary local warehouse directory.
 */
@Tag("integration")
public class IcebergIntegrationTest {

  @TempDir
  Path tempDir;

  private String warehousePath;
  private Map<String, Object> catalogConfig;
  private StorageProvider storageProvider;
  private HadoopCatalog hadoopCatalog;

  @BeforeEach
  void setUp() {
    warehousePath = tempDir.resolve("warehouse").toString();
    storageProvider = new LocalFileStorageProvider();

    catalogConfig = new HashMap<String, Object>();
    catalogConfig.put("catalog", "hadoop");
    catalogConfig.put("warehouse", warehousePath);
    catalogConfig.put("warehousePath", warehousePath);

    // Create a direct HadoopCatalog for verification reads
    Configuration conf = new Configuration();
    hadoopCatalog = new HadoopCatalog(conf, warehousePath);
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
    try {
      hadoopCatalog.close();
    } catch (Exception ignored) {
      // Ignore close errors
    }
  }

  // ==========================================================================
  // IcebergCatalogManager integration tests
  // ==========================================================================

  @Test
  void testCreateAndLoadTableViaHadoopCatalog() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "create_load_test", schema, PartitionSpec.unpartitioned());
    assertNotNull(table);

    // Load same table -- should return the existing one
    Table loaded = IcebergCatalogManager.loadTable(catalogConfig, "create_load_test");
    assertNotNull(loaded);
    assertEquals(table.location(), loaded.location());
    assertEquals(2, loaded.schema().columns().size());
  }

  @Test
  void testCreateTableFromColumnsWithPartitioning() {
    List<IcebergCatalogManager.ColumnDef> columns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("name", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("year", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("active", "BOOLEAN"));

    List<String> partCols = new ArrayList<String>();
    partCols.add("year");

    Table table = IcebergCatalogManager.createTableFromColumns(
        catalogConfig, "from_cols_test", columns, partCols);
    assertNotNull(table);
    assertEquals(4, table.schema().columns().size());
    assertEquals(1, table.spec().fields().size());
    assertEquals("year", table.spec().fields().get(0).name());
  }

  @Test
  void testCreateTableFromColumnsWithDocumentation() {
    List<IcebergCatalogManager.ColumnDef> columns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER", "Primary key"));
    columns.add(new IcebergCatalogManager.ColumnDef("description", "VARCHAR",
        "Human-readable description"));
    columns.add(new IcebergCatalogManager.ColumnDef("score", "DOUBLE", null));

    Table table = IcebergCatalogManager.createTableFromColumns(
        catalogConfig, "doc_cols_test", columns, Collections.<String>emptyList());
    assertNotNull(table);
    assertEquals(3, table.schema().columns().size());
  }

  @Test
  void testTableExistsAndDrop() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    IcebergCatalogManager.createTable(
        catalogConfig, "exists_test", schema, PartitionSpec.unpartitioned());

    assertTrue(IcebergCatalogManager.tableExists(catalogConfig, "exists_test"));

    boolean dropped = IcebergCatalogManager.dropTable(
        catalogConfig, "exists_test", true);
    assertTrue(dropped);
    assertFalse(IcebergCatalogManager.tableExists(catalogConfig, "exists_test"));
  }

  @Test
  void testDropNonExistentTableReturnsFalse() {
    boolean dropped = IcebergCatalogManager.dropTable(
        catalogConfig, "nonexistent_table", false);
    assertFalse(dropped);
  }

  @Test
  void testCreateTableAlreadyExistsReturnsExisting() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Table first = IcebergCatalogManager.createTable(
        catalogConfig, "idempotent_create", schema, PartitionSpec.unpartitioned());
    Table second = IcebergCatalogManager.createTable(
        catalogConfig, "idempotent_create", schema, PartitionSpec.unpartitioned());
    assertEquals(first.location(), second.location());
  }

  @Test
  void testLoadTableWithNamespaceDotNotation() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    TableIdentifier ti = TableIdentifier.of("testns", "dot_table");
    hadoopCatalog.createTable(ti, schema);

    Map<String, Object> config = new HashMap<String, Object>(catalogConfig);
    Table loaded = IcebergCatalogManager.loadTable(config, "testns.dot_table");
    assertNotNull(loaded);
    assertEquals(1, loaded.schema().columns().size());
  }

  @Test
  void testLoadTableWithNamespaceConfig() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "val", Types.StringType.get()));
    TableIdentifier ti = TableIdentifier.of("mynamespace", "ns_cfg_table");
    hadoopCatalog.createTable(ti, schema);

    Map<String, Object> config = new HashMap<String, Object>(catalogConfig);
    config.put("namespace", "mynamespace");
    Table loaded = IcebergCatalogManager.loadTable(config, "ns_cfg_table");
    assertNotNull(loaded);
  }

  @Test
  void testListAlternateTables() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Map<String, Object> config = new HashMap<String, Object>(catalogConfig);
    config.put("namespace", "default");

    IcebergCatalogManager.createTable(config, "default.normal_table", schema,
        PartitionSpec.unpartitioned());
    IcebergCatalogManager.createTable(config, "default._mv_test1", schema,
        PartitionSpec.unpartitioned());
    IcebergCatalogManager.createTable(config, "default._mv_test2", schema,
        PartitionSpec.unpartitioned());

    List<TableIdentifier> alternates =
        IcebergCatalogManager.listAlternateTables(config);
    assertEquals(2, alternates.size());
    for (TableIdentifier id : alternates) {
      assertTrue(id.name().startsWith("_mv_"));
    }
  }

  @Test
  void testListAlternatesForSourceReturnsAll() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Map<String, Object> config = new HashMap<String, Object>(catalogConfig);
    config.put("namespace", "default");

    IcebergCatalogManager.createTable(config, "default._mv_src1", schema,
        PartitionSpec.unpartitioned());

    List<TableIdentifier> result =
        IcebergCatalogManager.listAlternatesForSource(config, "any_source");
    assertEquals(1, result.size());
  }

  @Test
  void testGenerateAlternateName() {
    String name1 = IcebergCatalogManager.generateAlternateName();
    String name2 = IcebergCatalogManager.generateAlternateName();
    assertTrue(name1.startsWith("_mv_"));
    assertTrue(name2.startsWith("_mv_"));
    assertEquals(36, name1.length());
    assertFalse(name1.equals(name2));
  }

  @Test
  void testIsAlternateName() {
    assertTrue(IcebergCatalogManager.isAlternateName("_mv_something"));
    assertFalse(IcebergCatalogManager.isAlternateName("normal_table"));
    assertFalse(IcebergCatalogManager.isAlternateName(null));
    assertFalse(IcebergCatalogManager.isAlternateName(""));
  }

  @Test
  void testColumnDefConstructors() {
    IcebergCatalogManager.ColumnDef withDoc =
        new IcebergCatalogManager.ColumnDef("col1", "INTEGER", "my doc");
    assertEquals("col1", withDoc.getName());
    assertEquals("INTEGER", withDoc.getType());
    assertEquals("my doc", withDoc.getDoc());

    IcebergCatalogManager.ColumnDef noDoc =
        new IcebergCatalogManager.ColumnDef("col2", "VARCHAR");
    assertEquals("VARCHAR", noDoc.getType());
    assertNull(noDoc.getDoc());

    IcebergCatalogManager.ColumnDef nullType =
        new IcebergCatalogManager.ColumnDef("col3", null);
    assertEquals("VARCHAR", nullType.getType());
  }

  @Test
  void testCatalogCacheClearAndReuse() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "x", Types.IntegerType.get()));
    IcebergCatalogManager.createTable(catalogConfig, "cache_test", schema,
        PartitionSpec.unpartitioned());
    IcebergCatalogManager.clearCache();
    Table loaded = IcebergCatalogManager.loadTable(catalogConfig, "cache_test");
    assertNotNull(loaded);
  }

  @Test
  void testTypeMappingAllTypes() {
    List<IcebergCatalogManager.ColumnDef> columns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("c_int", "INT"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_integer", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_bigint", "BIGINT"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_long", "LONG"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_double", "DOUBLE"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_float8", "FLOAT8"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_float", "FLOAT"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_real", "REAL"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_bool", "BOOLEAN"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_bool2", "BOOL"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_date", "DATE"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_ts", "TIMESTAMP"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_tstz", "TIMESTAMPTZ"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_decimal", "DECIMAL"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_binary", "BINARY"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_bytes", "BYTES"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_string", "STRING"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_text", "TEXT"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_varchar", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("c_array", "array<double>"));

    Table table = IcebergCatalogManager.createTableFromColumns(
        catalogConfig, "all_types_test", columns, Collections.<String>emptyList());
    assertNotNull(table);
    assertEquals(20, table.schema().columns().size());
  }

  // ==========================================================================
  // IcebergTableWriter integration tests - write and read back
  // ==========================================================================

  @Test
  void testWriteAndReadUnpartitionedRecords() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "score", Types.DoubleType.get()));

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "write_read_test", schema, PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("name", "item_" + i);
      row.put("score", i * 1.5);
      records.add(row);
    }
    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(10, df.recordCount());

    List<DataFile> files = new ArrayList<DataFile>();
    files.add(df);
    writer.commitDataFiles(files, null);

    int count = countRecords(table);
    assertEquals(10, count);
  }

  @Test
  void testWriteAndReadPartitionedRecords() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "partitioned_rw", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records2024 = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 5; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("data", "data_2024_" + i);
      row.put("year", 2024);
      records2024.add(row);
    }
    Map<String, String> pv2024 = new HashMap<String, String>();
    pv2024.put("year", "2024");
    DataFile df2024 = writer.writeRecords(records2024, pv2024);
    assertNotNull(df2024);

    List<Map<String, Object>> records2025 = new ArrayList<Map<String, Object>>();
    for (int i = 5; i < 8; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("data", "data_2025_" + i);
      row.put("year", 2025);
      records2025.add(row);
    }
    Map<String, String> pv2025 = new HashMap<String, String>();
    pv2025.put("year", "2025");
    DataFile df2025 = writer.writeRecords(records2025, pv2025);
    assertNotNull(df2025);

    List<DataFile> allFiles = new ArrayList<DataFile>();
    allFiles.add(df2024);
    allFiles.add(df2025);
    writer.commitDataFiles(allFiles, null);

    int total = countRecords(table);
    assertEquals(8, total);
  }

  @Test
  void testBulkCommitMultiplePartitions() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "region", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("region").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "bulk_commit_test", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<DataFile> allFiles = new ArrayList<DataFile>();
    String[] regions = {"US", "EU", "APAC"};
    for (String region : regions) {
      List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", 1);
      row.put("region", region);
      records.add(row);
      Map<String, String> partVals = new HashMap<String, String>();
      partVals.put("region", region);
      DataFile df = writer.writeRecords(records, partVals);
      assertNotNull(df);
      allFiles.add(df);
    }

    writer.bulkCommitDataFiles(allFiles);
    assertEquals(3, countRecords(table));
  }

  @Test
  void testDeletePartitionIntegration() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "del_part_int", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("year", 2024);
    records.add(row);
    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2024");
    DataFile df = writer.writeRecords(records, partVals);
    writer.commitDataFiles(Collections.singletonList(df), null);
    assertEquals(1, countRecords(table));

    Map<String, Object> filter = new HashMap<String, Object>();
    filter.put("year", 2024);
    writer.deletePartition(filter);
    assertEquals(0, countRecords(table));
  }

  @Test
  void testWriteRecordsWithAllTypeCoercions() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "int_col", Types.IntegerType.get()),
        Types.NestedField.optional(2, "long_col", Types.LongType.get()),
        Types.NestedField.optional(3, "float_col", Types.FloatType.get()),
        Types.NestedField.optional(4, "double_col", Types.DoubleType.get()),
        Types.NestedField.optional(5, "bool_col", Types.BooleanType.get()),
        Types.NestedField.optional(6, "string_col", Types.StringType.get()));

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "type_coerce_int", schema, PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

    Map<String, Object> row1 = new HashMap<String, Object>();
    row1.put("int_col", "42");
    row1.put("long_col", "9999999999");
    row1.put("float_col", "3.14");
    row1.put("double_col", "2.71828");
    row1.put("bool_col", "true");
    row1.put("string_col", "hello");
    records.add(row1);

    Map<String, Object> row2 = new HashMap<String, Object>();
    row2.put("int_col", 100.7);
    row2.put("long_col", 200);
    row2.put("float_col", 1.23);
    row2.put("double_col", 4.56f);
    row2.put("bool_col", false);
    row2.put("string_col", 999);
    records.add(row2);

    Map<String, Object> row3 = new HashMap<String, Object>();
    row3.put("int_col", null);
    row3.put("long_col", "");
    row3.put("float_col", "-");
    row3.put("double_col", "not_a_number");
    row3.put("bool_col", null);
    row3.put("string_col", null);
    records.add(row3);

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(3, df.recordCount());

    writer.commitDataFiles(Collections.singletonList(df), null);
    assertEquals(3, countRecords(table));
  }

  @Test
  void testWriteRecordsWithListType() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "values",
            Types.ListType.ofOptional(3, Types.DoubleType.get())));

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "list_type_int", schema, PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

    Map<String, Object> row1 = new HashMap<String, Object>();
    row1.put("name", "from_list");
    List<Double> list1 = new ArrayList<Double>();
    list1.add(1.0);
    list1.add(2.0);
    list1.add(3.0);
    row1.put("values", list1);
    records.add(row1);

    Map<String, Object> row2 = new HashMap<String, Object>();
    row2.put("name", "from_array");
    row2.put("values", new Double[]{4.0, 5.0});
    records.add(row2);

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(2, df.recordCount());
    writer.commitDataFiles(Collections.singletonList(df), null);
  }

  @Test
  void testWriteRecordsCaseInsensitiveFields() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "case_insensitive_int", schema,
        PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("ID", 1);
    row.put("DATA", "test");
    records.add(row);

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(1, df.recordCount());
    writer.commitDataFiles(Collections.singletonList(df), null);
  }

  @Test
  void testWriteRecordsPartitionFallback() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "part_fallback_int", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "fallback test");
    records.add(row);

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2024");
    DataFile df = writer.writeRecords(records, partVals);
    assertNotNull(df);
    writer.commitDataFiles(Collections.singletonList(df), null);
    assertEquals(1, countRecords(table));
  }

  @Test
  void testMultiplePartitionKeyWrite() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "year", Types.IntegerType.get()),
        Types.NestedField.optional(3, "region", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year")
        .identity("region")
        .build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "multi_part_int", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("year", 2024);
    row.put("region", "US");
    records.add(row);

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2024");
    partVals.put("region", "US");
    DataFile df = writer.writeRecords(records, partVals);
    assertNotNull(df);
    assertTrue(df.path().toString().contains("year=2024"));
    assertTrue(df.path().toString().contains("region=US"));
    writer.commitDataFiles(Collections.singletonList(df), null);
  }

  // ==========================================================================
  // Maintenance and compaction integration tests
  // ==========================================================================

  @Test
  void testRunMaintenanceOnTableWithData() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "maint_int", schema, PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    for (int batch = 0; batch < 3; batch++) {
      List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", batch);
      row.put("name", "batch_" + batch);
      records.add(row);
      DataFile df = writer.writeRecords(records, null);
      writer.commitDataFiles(Collections.singletonList(df), null);
    }

    writer.runMaintenance(0, 0);
    assertTrue(countRecords(table) >= 3);
  }

  @Test
  void testRunMaintenanceSkipsOrphanDetectionForLargeThreshold()
      throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "maint_skip_orphan_int", schema,
        PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writer.runMaintenance(7, 365);
  }

  @Test
  void testCompactSmallFilesEmptyTable() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "compact_empty_int", schema,
        PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    int result = writer.compactSmallFiles(
        128 * 1024 * 1024, 3, 10 * 1024 * 1024);
    assertEquals(0, result);
  }

  @Test
  void testCompactSmallFilesBelowMinFiles() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "compact_below_int", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    for (int i = 0; i < 2; i++) {
      List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("year", 2024);
      records.add(row);
      Map<String, String> partVals = new HashMap<String, String>();
      partVals.put("year", "2024");
      DataFile df = writer.writeRecords(records, partVals);
      writer.commitDataFiles(Collections.singletonList(df), null);
    }

    int result = writer.compactSmallFiles(
        128 * 1024 * 1024, 10, 10 * 1024 * 1024);
    assertEquals(0, result);
    assertEquals(2, countRecords(table));
  }

  @Test
  void testCompactSmallFilesTriggered() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "compact_trigger_int", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    int totalRows = 0;
    for (int i = 0; i < 5; i++) {
      List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
      for (int j = 0; j < 10; j++) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", i * 10 + j);
        row.put("year", 2024);
        records.add(row);
        totalRows++;
      }
      Map<String, String> partVals = new HashMap<String, String>();
      partVals.put("year", "2024");
      DataFile df = writer.writeRecords(records, partVals);
      writer.commitDataFiles(Collections.singletonList(df), null);
    }

    int result = writer.compactSmallFiles(
        128 * 1024 * 1024, 3, 1024 * 1024 * 1024);
    assertTrue(result >= 1,
        "Expected at least 1 partition to be compacted");
    int afterCount = countRecords(table);
    assertEquals(totalRows, afterCount);
    int fileCount = countFiles(table);
    assertTrue(fileCount < 5,
        "Expected fewer files after compaction, got " + fileCount);
  }

  @Test
  void testConstructorWithCustomHadoopConfig() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "custom_conf_int", schema,
        PartitionSpec.unpartitioned());

    IcebergTableWriter writer =
        new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 42);
    records.add(row);
    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    writer.commitDataFiles(Collections.singletonList(df), null);
  }

  // ==========================================================================
  // IcebergStorageProvider integration tests
  // ==========================================================================

  @Test
  void testStorageProviderListFiles() throws Exception {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    spConfig.put("catalogType", "hadoop");
    spConfig.put("warehouse", warehousePath);
    spConfig.put("warehousePath", warehousePath);

    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    TableIdentifier ti = TableIdentifier.of("spns", "sp_table");
    hadoopCatalog.createTable(ti, schema);

    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);
    List<StorageProvider.FileEntry> entries = sp.listFiles("/spns", false);
    assertFalse(entries.isEmpty());
    assertEquals("sp_table", entries.get(0).getName());
  }

  @Test
  void testStorageProviderExists() throws Exception {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    spConfig.put("catalogType", "hadoop");
    spConfig.put("warehouse", warehousePath);
    spConfig.put("warehousePath", warehousePath);

    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    TableIdentifier ti = TableIdentifier.of("existns", "exist_table");
    hadoopCatalog.createTable(ti, schema);

    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);
    assertTrue(sp.exists("existns/exist_table"));
    assertFalse(sp.exists("existns/nonexistent"));
  }

  @Test
  void testStorageProviderMetadata() throws Exception {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    spConfig.put("catalogType", "hadoop");
    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);

    StorageProvider.FileMetadata meta = sp.getMetadata("/some/path");
    assertNotNull(meta);
    assertEquals("application/x-iceberg-table", meta.getContentType());
  }

  @Test
  void testStorageProviderIsDirectory() throws Exception {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    spConfig.put("catalogType", "hadoop");
    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);

    assertTrue(sp.isDirectory("namespace_only"));
    assertFalse(sp.isDirectory("namespace/table"));
  }

  @Test
  void testStorageProviderResolvePath() {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);

    assertEquals("/abs/path", sp.resolvePath("/base", "/abs/path"));
    assertEquals("/base/rel", sp.resolvePath("/base", "rel"));
    assertEquals("/base/rel", sp.resolvePath("/base/", "rel"));
  }

  @Test
  void testStorageProviderGetType() {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);
    assertEquals("iceberg", sp.getStorageType());
  }

  @Test
  void testStorageProviderOpenStreamReturnsEmpty() throws Exception {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);

    java.io.InputStream is = sp.openInputStream("/some/table");
    assertNotNull(is);
    assertEquals(-1, is.read());
    is.close();

    java.io.Reader reader = sp.openReader("/some/table");
    assertNotNull(reader);
    assertEquals(-1, reader.read());
    reader.close();
  }

  @Test
  void testStorageProviderListFilesRootNamespaces() throws Exception {
    Map<String, Object> spConfig = new HashMap<String, Object>();
    spConfig.put("catalogType", "hadoop");
    spConfig.put("warehouse", warehousePath);
    spConfig.put("warehousePath", warehousePath);

    // Create a table in a namespace so the catalog has namespaces to list
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    TableIdentifier ti = TableIdentifier.of("rootns", "root_table");
    hadoopCatalog.createTable(ti, schema);

    IcebergStorageProvider sp = new IcebergStorageProvider(spConfig);
    List<StorageProvider.FileEntry> entries = sp.listFiles("/", false);
    assertNotNull(entries);
    assertFalse(entries.isEmpty());
  }
  // ==========================================================================
  // Staging and commit integration tests
  // ==========================================================================

  @Test
  void testStageFilesWithEmptyDirectory() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "stage_empty_int", schema,
        PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Path emptyDir = tempDir.resolve("empty_staging");
    Files.createDirectories(emptyDir);
    List<DataFile> result = writer.stageFiles(emptyDir.toString());
    assertTrue(result.isEmpty());
  }

  @Test
  void testStageFilesWithNonParquetFiles() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "stage_nonpq", schema, PartitionSpec.unpartitioned());
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Path staging = tempDir.resolve("non_parquet_staging");
    Files.createDirectories(staging);
    Files.write(staging.resolve("readme.txt"), "hello".getBytes());
    Files.write(staging.resolve("data.csv"), "1,2,3".getBytes());

    List<DataFile> result = writer.stageFiles(staging.toString());
    assertTrue(result.isEmpty());
  }

  // ==========================================================================
  // Large batch test
  // ==========================================================================

  @Test
  void testWriteLargeBatch() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "large_batch_int", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 500; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("name", "record_" + i);
      row.put("year", 2024);
      records.add(row);
    }
    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2024");
    DataFile df = writer.writeRecords(records, partVals);
    assertNotNull(df);
    assertEquals(500, df.recordCount());

    writer.commitDataFiles(Collections.singletonList(df), null);
    assertEquals(500, countRecords(table));
  }

  // ==========================================================================
  // End-to-end: write, compact, verify
  // ==========================================================================

  @Test
  void testWriteCompactVerify() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "category", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("category").build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "e2e_compact", schema, spec);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    int totalRows = 0;
    for (int batch = 0; batch < 4; batch++) {
      List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
      for (int j = 0; j < 5; j++) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", batch * 5 + j);
        row.put("category", "test_cat");
        records.add(row);
        totalRows++;
      }
      Map<String, String> partVals = new HashMap<String, String>();
      partVals.put("category", "test_cat");
      DataFile df = writer.writeRecords(records, partVals);
      writer.commitDataFiles(Collections.singletonList(df), null);
    }

    assertEquals(totalRows, countRecords(table));
    assertEquals(4, countFiles(table));

    int compacted = writer.compactSmallFiles(
        128 * 1024 * 1024, 3, 1024 * 1024 * 1024);
    assertTrue(compacted >= 1);

    assertEquals(totalRows, countRecords(table));
    assertTrue(countFiles(table) < 4);
  }

  // ==========================================================================
  // Helpers
  // ==========================================================================

  private int countRecords(Table table) throws IOException {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        count += task.file().recordCount();
      }
    }
    return count;
  }

  private int countFiles(Table table) throws IOException {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        count++;
      }
    }
    return count;
  }
}
