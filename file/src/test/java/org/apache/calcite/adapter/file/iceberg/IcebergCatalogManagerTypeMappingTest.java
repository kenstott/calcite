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

import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for type mapping and ColumnDef in {@link IcebergCatalogManager}.
 * Exercises the private {@code mapToIcebergType} method indirectly via
 * {@link IcebergCatalogManager#createTableFromColumns}.
 */
@Tag("unit")
public class IcebergCatalogManagerTypeMappingTest {

  @TempDir
  Path tempDir;

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private Map<String, Object> config() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", tempDir.resolve("warehouse").toString());
    return config;
  }

  private Table createWithColumn(String tableName, String typeName) {
    List<IcebergCatalogManager.ColumnDef> columns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("col", typeName));
    List<String> partitionColumns = Collections.emptyList();
    return IcebergCatalogManager.createTableFromColumns(
        config(), tableName, columns, partitionColumns);
  }

  @Test public void testMapIntegerType() {
    Table table = createWithColumn("test_int", "INTEGER");
    assertEquals(Types.IntegerType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapIntType() {
    Table table = createWithColumn("test_int2", "INT");
    assertEquals(Types.IntegerType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapBigintType() {
    Table table = createWithColumn("test_bigint", "BIGINT");
    assertEquals(Types.LongType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapLongType() {
    Table table = createWithColumn("test_long", "LONG");
    assertEquals(Types.LongType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapDoubleType() {
    Table table = createWithColumn("test_double", "DOUBLE");
    assertEquals(Types.DoubleType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapFloat8Type() {
    Table table = createWithColumn("test_float8", "FLOAT8");
    assertEquals(Types.DoubleType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapFloatType() {
    Table table = createWithColumn("test_float", "FLOAT");
    assertEquals(Types.FloatType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapRealType() {
    Table table = createWithColumn("test_real", "REAL");
    assertEquals(Types.FloatType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapBooleanType() {
    Table table = createWithColumn("test_bool", "BOOLEAN");
    assertEquals(Types.BooleanType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapBoolType() {
    Table table = createWithColumn("test_bool2", "BOOL");
    assertEquals(Types.BooleanType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapDateType() {
    Table table = createWithColumn("test_date", "DATE");
    assertEquals(Types.DateType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapTimestampType() {
    Table table = createWithColumn("test_ts", "TIMESTAMP");
    assertEquals(Types.TimestampType.withoutZone(),
        table.schema().findField("col").type());
  }

  @Test public void testMapTimestamptzType() {
    Table table = createWithColumn("test_tstz", "TIMESTAMPTZ");
    assertEquals(Types.TimestampType.withZone(),
        table.schema().findField("col").type());
  }

  @Test public void testMapTimestampWithTimeZoneType() {
    Table table = createWithColumn("test_tstz2", "TIMESTAMP WITH TIME ZONE");
    assertEquals(Types.TimestampType.withZone(),
        table.schema().findField("col").type());
  }

  @Test public void testMapDecimalType() {
    Table table = createWithColumn("test_decimal", "DECIMAL");
    Types.DecimalType decimalType = (Types.DecimalType)
        table.schema().findField("col").type();
    assertEquals(38, decimalType.precision());
    assertEquals(9, decimalType.scale());
  }

  @Test public void testMapBinaryType() {
    Table table = createWithColumn("test_binary", "BINARY");
    assertEquals(Types.BinaryType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapBytesType() {
    Table table = createWithColumn("test_bytes", "BYTES");
    assertEquals(Types.BinaryType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapVarcharType() {
    Table table = createWithColumn("test_varchar", "VARCHAR");
    assertEquals(Types.StringType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapStringType() {
    Table table = createWithColumn("test_string", "STRING");
    assertEquals(Types.StringType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapTextType() {
    Table table = createWithColumn("test_text", "TEXT");
    assertEquals(Types.StringType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapNullTypeDefaultsToString() {
    Table table = createWithColumn("test_null_type", null);
    // ColumnDef constructor defaults null to "VARCHAR"
    assertEquals(Types.StringType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapUnknownTypeDefaultsToString() {
    Table table = createWithColumn("test_unknown", "FOOBAR");
    assertEquals(Types.StringType.get(),
        table.schema().findField("col").type());
  }

  @Test public void testMapArrayType() {
    Table table = createWithColumn("test_array", "array<double>");
    assertTrue(table.schema().findField("col").type() instanceof Types.ListType);
  }

  @Test public void testMapArrayTypeUpperCase() {
    Table table = createWithColumn("test_array_upper", "ARRAY<INTEGER>");
    assertTrue(table.schema().findField("col").type() instanceof Types.ListType);
  }

  @Test public void testMapCaseInsensitiveType() {
    Table table1 = createWithColumn("test_case1", "integer");
    assertEquals(Types.IntegerType.get(),
        table1.schema().findField("col").type());

    Table table2 = createWithColumn("test_case2", "Boolean");
    assertEquals(Types.BooleanType.get(),
        table2.schema().findField("col").type());
  }

  @Test public void testColumnDefWithDoc() {
    IcebergCatalogManager.ColumnDef col =
        new IcebergCatalogManager.ColumnDef("myCol", "INTEGER", "A documented column");

    assertEquals("myCol", col.getName());
    assertEquals("INTEGER", col.getType());
    assertEquals("A documented column", col.getDoc());
  }

  @Test public void testColumnDefWithoutDoc() {
    IcebergCatalogManager.ColumnDef col =
        new IcebergCatalogManager.ColumnDef("myCol", "INTEGER");

    assertEquals("myCol", col.getName());
    assertEquals("INTEGER", col.getType());
    assertNull(col.getDoc());
  }

  @Test public void testColumnDefNullTypeDefaultsToVarchar() {
    IcebergCatalogManager.ColumnDef col =
        new IcebergCatalogManager.ColumnDef("myCol", null);

    assertEquals("VARCHAR", col.getType());
  }

  @Test public void testColumnDefDocOnCreatedField() {
    List<IcebergCatalogManager.ColumnDef> columns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER", "Primary key"));
    columns.add(new IcebergCatalogManager.ColumnDef("name", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("desc", "STRING", ""));

    List<String> partitionColumns = Collections.emptyList();
    Table table =
        IcebergCatalogManager.createTableFromColumns(config(), "test_doc_fields", columns, partitionColumns);

    assertNotNull(table);
    // Verify the doc was set on the first field
    assertEquals("Primary key", table.schema().findField("id").doc());
    // Second field should have no doc
    assertNull(table.schema().findField("name").doc());
  }

  @Test public void testCreateTableFromColumnsMultiplePartitions() {
    List<IcebergCatalogManager.ColumnDef> columns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("year", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("month", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("data", "STRING"));

    List<String> partitionColumns = new ArrayList<String>();
    partitionColumns.add("year");
    partitionColumns.add("month");

    Table table =
        IcebergCatalogManager.createTableFromColumns(config(), "test_multi_partition", columns, partitionColumns);

    assertNotNull(table);
    assertEquals(4, table.schema().columns().size());
    assertEquals(2, table.spec().fields().size());
  }

  @Test public void testCreateTableFromColumnsNoPartition() {
    List<IcebergCatalogManager.ColumnDef> columns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("id", "INTEGER"));
    columns.add(new IcebergCatalogManager.ColumnDef("data", "STRING"));

    List<String> partitionColumns = Collections.emptyList();

    Table table =
        IcebergCatalogManager.createTableFromColumns(config(), "test_no_partition", columns, partitionColumns);

    assertNotNull(table);
    assertTrue(table.spec().isUnpartitioned());
  }

  @Test public void testHiveCatalogThrows() {
    final Map<String, Object> cfg = config();
    Exception ex =
        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
        new org.junit.jupiter.api.function.Executable() {
          @Override public void execute() {
            IcebergCatalogManager.getCatalogForProvider("hive", cfg);
          }
        });
    assertTrue(ex.getMessage().contains("Hive"));
  }

  @Test public void testClearCacheDoesNotThrow() {
    // Just ensure it works without error
    IcebergCatalogManager.clearCache();
    IcebergCatalogManager.clearCache();
  }

  @Test public void testListAlternatesForSource() {
    Map<String, Object> cfg = config();
    cfg.put("namespace", "ns");

    // listAlternatesForSource returns all alternates (per implementation note)
    List<org.apache.iceberg.catalog.TableIdentifier> result =
        IcebergCatalogManager.listAlternatesForSource(cfg, "any_table");
    assertNotNull(result);
    // Should return empty for non-existing namespace (no tables created)
    assertEquals(0, result.size());
  }

  @Test public void testParseTableIdentifierWithConfigNamespace() {
    Map<String, Object> cfg = config();
    cfg.put("namespace", "myns");

    // Create a table using namespace from config
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));

    // Create namespace table using getCatalogForProvider + direct catalog create
    org.apache.iceberg.catalog.Catalog cat =
        IcebergCatalogManager.getCatalogForProvider("hadoop", cfg);
    cat.createTable(
        org.apache.iceberg.catalog.TableIdentifier.of("myns", "ns_table"), schema);

    // Now load it using config namespace
    Table loaded = IcebergCatalogManager.loadTable(cfg, "ns_table");
    assertNotNull(loaded);
  }
}
