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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
/**
 * Tests for IcebergCatalogManager.
 */
@Tag("unit")
public class IcebergCatalogManagerTest {

  @TempDir
  Path tempDir;

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  @Test public void testCreateHadoopCatalog() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", tempDir.toString());

    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    assertNotNull(catalog);
  }

  @Test public void testLoadTableFromHadoopCatalog() throws Exception {
    // Setup
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    // Create catalog and table
    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.StringType.get()));

    TableIdentifier tableId = TableIdentifier.of("test_table");
    Table createdTable = catalog.createTable(tableId, schema);
    assertNotNull(createdTable);

    // Load table using catalog manager
    config.put("tablePath", "test_table");
    Table loadedTable = IcebergCatalogManager.loadTable(config, "test_table");
    assertNotNull(loadedTable);
    // Table name may include default namespace prefix from Iceberg
    assertTrue(loadedTable.name().equals("test_table") || loadedTable.name().equals("hadoop.test_table"));
  }

  @Test public void testDirectPathLoading() throws Exception {
    // Setup - create a table first
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    TableIdentifier tableId = TableIdentifier.of("direct_table");
    Table createdTable = catalog.createTable(tableId, schema);
    String tablePath = createdTable.location();

    // Load table directly by path - need warehousePath for catalog creation
    Map<String, Object> directConfig = new HashMap<>();
    directConfig.put("catalogType", "hadoop");
    directConfig.put("warehousePath", warehousePath);
    Table directTable = IcebergCatalogManager.loadTable(directConfig, tablePath);
    assertNotNull(directTable);
    assertEquals(tablePath, directTable.location());
  }

  @Test public void testCatalogCaching() {
    Map<String, Object> config1 = new HashMap<>();
    config1.put("catalogType", "hadoop");
    config1.put("warehousePath", tempDir.resolve("warehouse1").toString());

    Map<String, Object> config2 = new HashMap<>();
    config2.put("catalogType", "hadoop");
    config2.put("warehousePath", tempDir.resolve("warehouse2").toString());

    // Get catalogs
    Catalog catalog1 = IcebergCatalogManager.getCatalogForProvider("hadoop", config1);
    Catalog catalog2 = IcebergCatalogManager.getCatalogForProvider("hadoop", config2);

    assertNotNull(catalog1);
    assertNotNull(catalog2);

    // Get same catalog again - should be cached (same identity)
    Catalog catalog1Again = IcebergCatalogManager.getCatalogForProvider("hadoop", config1);
    assertTrue(catalog1 == catalog1Again, "Cached catalog should be same instance");
  }

  @Test public void testRestCatalogCreation() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "rest");
    config.put("uri", "http://localhost:8181");

    // Just test that we can create the catalog object without connecting
    try {
      Catalog catalog = IcebergCatalogManager.getCatalogForProvider("rest", config);
      assertNotNull(catalog);
      // Don't test actual connectivity since there's no real REST server
    } catch (Exception e) {
      // Expected - REST catalog creation may fail without a real server
      assertTrue(e.getMessage().contains("Connection refused") ||
                 e.getMessage().contains("refused") ||
                 e.getCause() instanceof java.net.ConnectException);
    }
  }

  @Test public void testInvalidCatalogType() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "invalid");

    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      IcebergCatalogManager.getCatalogForProvider("invalid", config);
    });

    assertTrue(exception.getMessage().contains("Unknown catalog type"));
  }

  @Test public void testMissingWarehousePathForHadoop() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    // Missing warehousePath

    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    });

    assertTrue(exception.getMessage().contains("warehouse"));
  }

  @Test public void testParseTablePath() throws Exception {
    // Setup catalog
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);

    // Create tables in different namespaces
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    // Single level table
    catalog.createTable(TableIdentifier.of("simple_table"), schema);

    // Namespace.table format
    catalog.createTable(TableIdentifier.of("ns1", "table1"), schema);

    // Load tables with different path formats
    config.put("tablePath", "simple_table");
    Table table1 = IcebergCatalogManager.loadTable(config, "simple_table");
    assertNotNull(table1);

    config.put("tablePath", "ns1.table1");
    Table table2 = IcebergCatalogManager.loadTable(config, "ns1.table1");
    assertNotNull(table2);
  }

  @Test public void testCreateTableWithSchema() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    // Create schema with partition column
    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year")
        .build();

    Table table = IcebergCatalogManager.createTable(config, "test_partitioned", schema, spec);

    assertNotNull(table);
    assertTrue(table.name().contains("test_partitioned"));
    assertEquals(1, table.spec().fields().size());
    assertEquals("year", table.spec().fields().get(0).name());
  }

  @Test public void testCreateTableFromColumns() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("name", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("geo", "STRING"));
    columns.add(new IcebergCatalogManager.ColumnDef("amount", "DOUBLE"));

    List<String> partitionColumns = new ArrayList<>();
    partitionColumns.add("geo");

    Table table =
        IcebergCatalogManager.createTableFromColumns(config, "from_columns", columns, partitionColumns);

    assertNotNull(table);
    assertEquals(4, table.schema().columns().size());
    assertEquals(1, table.spec().fields().size());
  }

  @Test public void testDropTable() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    // Create a table first
    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    IcebergCatalogManager.createTable(config, "to_drop", schema, spec);
    assertTrue(IcebergCatalogManager.tableExists(config, "to_drop"));

    // Drop it
    boolean dropped = IcebergCatalogManager.dropTable(config, "to_drop", true);
    assertTrue(dropped);
    assertFalse(IcebergCatalogManager.tableExists(config, "to_drop"));

    // Try to drop non-existent table
    boolean droppedAgain = IcebergCatalogManager.dropTable(config, "to_drop", true);
    assertFalse(droppedAgain);
  }

  @Test public void testTableExists() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    assertFalse(IcebergCatalogManager.tableExists(config, "nonexistent"));

    // Create a table
    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    IcebergCatalogManager.createTable(config, "exists_test",
        schema, PartitionSpec.unpartitioned());

    assertTrue(IcebergCatalogManager.tableExists(config, "exists_test"));
  }

  @Test public void testGenerateAlternateName() {
    String name1 = IcebergCatalogManager.generateAlternateName();
    String name2 = IcebergCatalogManager.generateAlternateName();

    assertNotNull(name1);
    assertNotNull(name2);
    assertTrue(name1.startsWith("_mv_"));
    assertTrue(name2.startsWith("_mv_"));
    assertEquals(36, name1.length()); // "_mv_" + 32 chars
    assertFalse(name1.equals(name2)); // Should be unique
  }

  @Test public void testIsAlternateName() {
    assertTrue(IcebergCatalogManager.isAlternateName("_mv_abc123"));
    assertTrue(IcebergCatalogManager.isAlternateName("_mv_"));
    assertFalse(IcebergCatalogManager.isAlternateName("regular_table"));
    assertFalse(IcebergCatalogManager.isAlternateName("mv_no_underscore"));
    assertFalse(IcebergCatalogManager.isAlternateName(null));
  }

  @Test public void testListAlternateTables() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    config.put("namespace", "db");  // Use a namespace for HadoopCatalog

    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    // Create regular and alternate tables in the namespace
    IcebergCatalogManager.createTable(config, "db.regular_table", schema, spec);
    IcebergCatalogManager.createTable(config, "db._mv_alternate1", schema, spec);
    IcebergCatalogManager.createTable(config, "db._mv_alternate2", schema, spec);

    List<TableIdentifier> alternates = IcebergCatalogManager.listAlternateTables(config);

    assertEquals(2, alternates.size());
    assertTrue(alternates.stream().anyMatch(t -> t.name().equals("_mv_alternate1")));
    assertTrue(alternates.stream().anyMatch(t -> t.name().equals("_mv_alternate2")));
    assertFalse(alternates.stream().anyMatch(t -> t.name().equals("regular_table")));
  }

  @Test public void testCreateTableIdempotent() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    // Create twice - should not throw
    Table table1 = IcebergCatalogManager.createTable(config, "idempotent_test", schema, spec);
    Table table2 = IcebergCatalogManager.createTable(config, "idempotent_test", schema, spec);

    assertNotNull(table1);
    assertNotNull(table2);
    assertEquals(table1.location(), table2.location());
  }

  @Test public void testTypeMapping() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);

    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<>();
    columns.add(new IcebergCatalogManager.ColumnDef("int_col", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("long_col", "BIGINT"));
    columns.add(new IcebergCatalogManager.ColumnDef("double_col", "DOUBLE"));
    columns.add(new IcebergCatalogManager.ColumnDef("float_col", "FLOAT"));
    columns.add(new IcebergCatalogManager.ColumnDef("bool_col", "BOOLEAN"));
    columns.add(new IcebergCatalogManager.ColumnDef("date_col", "DATE"));
    columns.add(new IcebergCatalogManager.ColumnDef("ts_col", "TIMESTAMP"));
    columns.add(new IcebergCatalogManager.ColumnDef("str_col", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("binary_col", "BINARY"));

    List<String> partitionColumns = new ArrayList<>();

    Table table =
        IcebergCatalogManager.createTableFromColumns(config, "type_mapping_test", columns, partitionColumns);

    assertNotNull(table);
    assertEquals(9, table.schema().columns().size());
  }
}
