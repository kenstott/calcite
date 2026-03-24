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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for IcebergCatalogManager using real Hadoop catalog on local filesystem.
 */
@Tag("integration")
public class IcebergCatalogManagerIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergCatalogManagerIntegrationTest.class);

  @TempDir
  Path tempDir;

  private Map<String, Object> hadoopConfig;

  @BeforeEach
  public void setUp() {
    hadoopConfig = new HashMap<>();
    hadoopConfig.put("catalog", "hadoop");
    hadoopConfig.put("warehouse", tempDir.resolve("warehouse").toString());
    hadoopConfig.put("namespace", "test_ns");

    IcebergCatalogManager.clearCache();
  }

  @AfterEach
  public void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  @Test public void testCreateAndLoadTable() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "value", Types.DoubleType.get())
    );

    Table table = IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.my_table", schema, PartitionSpec.unpartitioned());

    assertNotNull(table);
    assertNotNull(table.location());
    assertEquals(3, table.schema().columns().size());
  }

  @Test public void testCreateTableWithPartitionSpec() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "region", Types.StringType.get()),
        Types.NestedField.optional(3, "amount", Types.DoubleType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("region")
        .build();

    Table table = IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.partitioned_table", schema, spec);

    assertNotNull(table);
    assertFalse(table.spec().isUnpartitioned());
    assertEquals(1, table.spec().fields().size());
  }

  @Test public void testTableExists() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.exists_test", schema, PartitionSpec.unpartitioned());

    assertTrue(IcebergCatalogManager.tableExists(hadoopConfig, "test_ns.exists_test"));
    assertFalse(IcebergCatalogManager.tableExists(hadoopConfig, "test_ns.nonexistent"));
  }

  @Test public void testDropTable() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.drop_me", schema, PartitionSpec.unpartitioned());

    assertTrue(IcebergCatalogManager.tableExists(hadoopConfig, "test_ns.drop_me"));

    boolean dropped = IcebergCatalogManager.dropTable(hadoopConfig, "test_ns.drop_me", true);
    assertTrue(dropped);

    assertFalse(IcebergCatalogManager.tableExists(hadoopConfig, "test_ns.drop_me"));
  }

  @Test public void testDropNonExistentTable() {
    boolean dropped = IcebergCatalogManager.dropTable(hadoopConfig, "test_ns.no_such_table", false);
    assertFalse(dropped);
  }

  @Test public void testCreateTableFromColumns() {
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("name", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("score", "DOUBLE"));
    columns.add(new IcebergCatalogManager.ColumnDef("active", "BOOLEAN"));
    columns.add(new IcebergCatalogManager.ColumnDef("created", "TIMESTAMP"));

    List<String> partitionColumns = new ArrayList<>();
    // No partitions

    Table table = IcebergCatalogManager.createTableFromColumns(
        hadoopConfig, "test_ns.columns_table", columns, partitionColumns);

    assertNotNull(table);
    assertEquals(5, table.schema().columns().size());
  }

  @Test public void testCreateTableFromColumnsWithPartitions() {
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("region", "STRING"));
    columns.add(new IcebergCatalogManager.ColumnDef("value", "DOUBLE"));

    List<String> partitionColumns = new ArrayList<>();
    partitionColumns.add("region");

    Table table = IcebergCatalogManager.createTableFromColumns(
        hadoopConfig, "test_ns.partitioned_cols", columns, partitionColumns);

    assertNotNull(table);
    assertFalse(table.spec().isUnpartitioned());
  }

  @Test public void testCreateTableFromColumnsAllTypes() {
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<>();
    columns.add(new IcebergCatalogManager.ColumnDef("col_int", "INT"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_integer", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_bigint", "BIGINT"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_long", "LONG"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_double", "DOUBLE"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_float8", "FLOAT8"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_float", "FLOAT"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_real", "REAL"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_boolean", "BOOLEAN"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_bool", "BOOL"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_date", "DATE"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_ts", "TIMESTAMP"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_tstz", "TIMESTAMPTZ"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_decimal", "DECIMAL"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_binary", "BINARY"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_bytes", "BYTES"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_varchar", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_string", "STRING"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_text", "TEXT"));
    columns.add(new IcebergCatalogManager.ColumnDef("col_unknown", "UNKNOWN_TYPE"));

    List<String> partitionColumns = new ArrayList<>();

    Table table = IcebergCatalogManager.createTableFromColumns(
        hadoopConfig, "test_ns.all_types", columns, partitionColumns);

    assertNotNull(table);
    assertEquals(20, table.schema().columns().size());
  }

  @Test public void testCreateTableFromColumnsWithDoc() {
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER", "Primary key"));
    columns.add(new IcebergCatalogManager.ColumnDef("name", "STRING", "User name"));
    columns.add(new IcebergCatalogManager.ColumnDef("email", "STRING", null));
    columns.add(new IcebergCatalogManager.ColumnDef("score", "DOUBLE", ""));

    Table table = IcebergCatalogManager.createTableFromColumns(
        hadoopConfig, "test_ns.doc_table", columns, new ArrayList<>());

    assertNotNull(table);
    assertEquals(4, table.schema().columns().size());
  }

  @Test public void testCreateTableFromColumnsWithArrayType() {
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("tags", "array<string>"));

    Table table = IcebergCatalogManager.createTableFromColumns(
        hadoopConfig, "test_ns.array_table", columns, new ArrayList<>());

    assertNotNull(table);
    assertEquals(2, table.schema().columns().size());
  }

  @Test public void testCreateExistingTableReturnsExisting() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    Table table1 = IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.idempotent", schema, PartitionSpec.unpartitioned());

    // Creating again should return existing table
    Table table2 = IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.idempotent", schema, PartitionSpec.unpartitioned());

    assertEquals(table1.location(), table2.location());
  }

  @Test public void testLoadTableFromDirectPath() {
    // Create a table via HadoopCatalog first
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get())
    );

    Table created = IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.direct_path", schema, PartitionSpec.unpartitioned());

    String location = created.location();

    // Load via direct path
    Table loaded = IcebergCatalogManager.loadTable(hadoopConfig, location);

    assertNotNull(loaded);
    assertEquals(2, loaded.schema().columns().size());
  }

  @Test public void testGetCatalogForProvider() {
    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", hadoopConfig);
    assertNotNull(catalog);
  }

  @Test public void testCatalogCaching() {
    Catalog c1 = IcebergCatalogManager.getCatalogForProvider("hadoop", hadoopConfig);
    Catalog c2 = IcebergCatalogManager.getCatalogForProvider("hadoop", hadoopConfig);
    // Should return the same cached instance
    assertTrue(c1 == c2);
  }

  @Test public void testClearCache() {
    IcebergCatalogManager.getCatalogForProvider("hadoop", hadoopConfig);
    IcebergCatalogManager.clearCache();
    // After clear, should create a new instance
    Catalog c = IcebergCatalogManager.getCatalogForProvider("hadoop", hadoopConfig);
    assertNotNull(c);
  }

  @Test public void testHiveCatalogUnsupported() {
    Map<String, Object> hiveConfig = new HashMap<>();
    hiveConfig.put("catalog", "hive");
    hiveConfig.put("warehouse", tempDir.toString());

    assertThrows(UnsupportedOperationException.class,
        () -> IcebergCatalogManager.getCatalogForProvider("hive", hiveConfig));
  }

  @Test public void testUnknownCatalogType() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalog", "unknown");

    assertThrows(IllegalArgumentException.class,
        () -> IcebergCatalogManager.getCatalogForProvider("unknown", config));
  }

  @Test public void testHadoopCatalogRequiresWarehouse() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalog", "hadoop");
    // No warehouse

    assertThrows(IllegalArgumentException.class,
        () -> IcebergCatalogManager.getCatalogForProvider("hadoop", config));
  }

  @Test public void testHadoopCatalogAcceptsWarehousePath() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalog", "hadoop");
    config.put("warehousePath", tempDir.resolve("alt-wh").toString());

    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    assertNotNull(catalog);
  }

  @Test public void testGenerateAlternateName() {
    String name = IcebergCatalogManager.generateAlternateName();
    assertNotNull(name);
    assertTrue(name.startsWith("_mv_"));
    assertEquals(36, name.length()); // "_mv_" (4) + 32 random chars

    // Generate multiple names and ensure uniqueness
    String name2 = IcebergCatalogManager.generateAlternateName();
    assertFalse(name.equals(name2));
  }

  @Test public void testIsAlternateName() {
    assertTrue(IcebergCatalogManager.isAlternateName("_mv_abcdef123456"));
    assertFalse(IcebergCatalogManager.isAlternateName("regular_table"));
    assertFalse(IcebergCatalogManager.isAlternateName(null));
    assertFalse(IcebergCatalogManager.isAlternateName(""));
  }

  @Test public void testListAlternateTables() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    // Create regular and alternate tables
    IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns.regular_table", schema, PartitionSpec.unpartitioned());

    String altName = IcebergCatalogManager.generateAlternateName();
    IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns." + altName, schema, PartitionSpec.unpartitioned());

    List<TableIdentifier> alternates = IcebergCatalogManager.listAlternateTables(hadoopConfig);

    boolean foundAlternate = false;
    for (TableIdentifier tid : alternates) {
      if (tid.name().equals(altName)) {
        foundAlternate = true;
      }
      assertTrue(IcebergCatalogManager.isAlternateName(tid.name()));
    }
    assertTrue(foundAlternate);
  }

  @Test public void testListAlternateTablesEmptyNamespace() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalog", "hadoop");
    config.put("warehouse", tempDir.resolve("empty-wh").toString());
    // No namespace - should use "default"

    List<TableIdentifier> alternates = IcebergCatalogManager.listAlternateTables(config);
    assertNotNull(alternates);
    // May be empty if namespace doesn't exist yet
  }

  @Test public void testListAlternatesForSource() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    String altName = IcebergCatalogManager.generateAlternateName();
    IcebergCatalogManager.createTable(
        hadoopConfig, "test_ns." + altName, schema, PartitionSpec.unpartitioned());

    List<TableIdentifier> alternates =
        IcebergCatalogManager.listAlternatesForSource(hadoopConfig, "source_table");
    assertNotNull(alternates);
  }

  @Test public void testColumnDefGetters() {
    IcebergCatalogManager.ColumnDef col =
        new IcebergCatalogManager.ColumnDef("name", "VARCHAR", "doc text");
    assertEquals("name", col.getName());
    assertEquals("VARCHAR", col.getType());
    assertEquals("doc text", col.getDoc());
  }

  @Test public void testColumnDefNullType() {
    IcebergCatalogManager.ColumnDef col =
        new IcebergCatalogManager.ColumnDef("col", null);
    assertEquals("VARCHAR", col.getType());
    assertNull(col.getDoc());
  }

  @Test public void testColumnDefTwoArgConstructor() {
    IcebergCatalogManager.ColumnDef col =
        new IcebergCatalogManager.ColumnDef("col", "INT");
    assertEquals("col", col.getName());
    assertEquals("INT", col.getType());
    assertNull(col.getDoc());
  }

  @Test public void testDefaultCatalogTypeIsHadoop() {
    // When catalog type not specified, should default to hadoop
    Map<String, Object> config = new HashMap<>();
    config.put("warehouse", tempDir.resolve("default-catalog").toString());
    config.put("namespace", "ns");

    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    Table table = IcebergCatalogManager.createTable(
        config, "ns.default_type_table", schema, PartitionSpec.unpartitioned());
    assertNotNull(table);
  }

  @Test public void testParseTableIdentifierWithDot() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    // Table path with dot notation: namespace.tableName
    Table table = IcebergCatalogManager.createTable(
        hadoopConfig, "custom_ns.dotted_table", schema, PartitionSpec.unpartitioned());
    assertNotNull(table);
  }

  @Test public void testParseTableIdentifierSingleLevel() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalog", "hadoop");
    config.put("warehouse", tempDir.resolve("single-level").toString());
    // No namespace in config

    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    // Single level table name
    Table table = IcebergCatalogManager.createTable(
        config, "single_table", schema, PartitionSpec.unpartitioned());
    assertNotNull(table);
  }

  @Test public void testRestCatalogRequiresUri() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalog", "rest");
    // No URI

    assertThrows(IllegalArgumentException.class,
        () -> IcebergCatalogManager.getCatalogForProvider("rest", config));
  }
}
