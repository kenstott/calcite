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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests Iceberg-to-Calcite type mapping, statistics, and CommentableTable in
 * {@link IcebergTable}.
 */
@Tag("unit")
public class IcebergTableTypeMappingTest {

  @TempDir
  Path tempDir;

  private RelDataTypeFactory typeFactory;
  private HadoopCatalog catalog;
  private String warehousePath;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    warehousePath = tempDir.resolve("warehouse").toString();
    Configuration conf = new Configuration();
    catalog = new HadoopCatalog(conf, warehousePath);
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  @Test public void testBooleanTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "flag", Types.BooleanType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_boolean"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.BOOLEAN,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testIntegerTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "num", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_int"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.INTEGER,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testLongTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "big_num", Types.LongType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_long"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.BIGINT,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testFloatTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "flt", Types.FloatType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_float"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.REAL,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testDoubleTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "dbl", Types.DoubleType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_double"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.DOUBLE,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testStringTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "str", Types.StringType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_string"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.VARCHAR,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testDateTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "dt", Types.DateType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_date"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.DATE,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testTimestampWithoutZoneTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_ts"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.TIMESTAMP,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testTimestampWithZoneTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "tstz", Types.TimestampType.withZone()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_tstz"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testDecimalTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "dec", Types.DecimalType.of(18, 4)));

    Table table =
        catalog.createTable(TableIdentifier.of("test_decimal"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    RelDataType decType = rowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.DECIMAL, decType.getSqlTypeName());
    assertEquals(18, decType.getPrecision());
    assertEquals(4, decType.getScale());
  }

  @Test public void testBinaryTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "bin", Types.BinaryType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_binary"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.VARBINARY,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testUuidTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "uid", Types.UUIDType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_uuid"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.VARCHAR,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test public void testListTypeMapping() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "tags",
            Types.ListType.ofOptional(100, Types.StringType.get())));

    Table table =
        catalog.createTable(TableIdentifier.of("test_list"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    RelDataType listType = rowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.ARRAY, listType.getSqlTypeName());
  }

  @Test public void testMapTypeMapping() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "props",
            Types.MapType.ofOptional(100, 101,
                Types.StringType.get(), Types.IntegerType.get())));

    Table table =
        catalog.createTable(TableIdentifier.of("test_map"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    RelDataType mapType = rowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.MAP, mapType.getSqlTypeName());
  }

  @Test public void testStructTypeMapping() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "address",
            Types.StructType.of(
                Types.NestedField.optional(100, "street", Types.StringType.get()),
                Types.NestedField.optional(101, "city", Types.StringType.get()))));

    Table table =
        catalog.createTable(TableIdentifier.of("test_struct"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    RelDataType structType = rowType.getFieldList().get(0).getType();
    assertEquals(2, structType.getFieldCount());
    assertTrue(structType.getFieldNames().contains("street"));
    assertTrue(structType.getFieldNames().contains("city"));
  }

  @Test public void testMultiColumnSchema() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "amount", Types.DoubleType.get()),
        Types.NestedField.optional(4, "active", Types.BooleanType.get()),
        Types.NestedField.optional(5, "created", Types.TimestampType.withoutZone()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_multi"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(5, rowType.getFieldCount());

    List<String> fieldNames = rowType.getFieldNames();
    assertEquals("id", fieldNames.get(0));
    assertEquals("name", fieldNames.get(1));
    assertEquals("amount", fieldNames.get(2));
    assertEquals("active", fieldNames.get(3));
    assertEquals("created", fieldNames.get(4));
  }

  @Test public void testStatisticsEmptyTable() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_stats_empty"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    Statistic stat = icebergTable.getStatistic();
    assertNotNull(stat);
    // Empty table should have 0 rows
    assertEquals(0.0, stat.getRowCount());
  }

  @Test public void testStatisticsPopulatedTable() throws Exception {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_stats_pop"), schema, PartitionSpec.unpartitioned());

    // Add some data
    OutputFile outputFile =
        table.io().newOutputFile(table.location() + "/data/file-" + UUID.randomUUID() + ".parquet");

    DataWriter<Record> writer = Parquet.writeData(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", i);
      writer.write(record);
    }
    writer.close();
    table.newAppend().appendFile(writer.toDataFile()).commit();

    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    Statistic stat = icebergTable.getStatistic();
    assertNotNull(stat);
    assertEquals(5.0, stat.getRowCount());
  }

  @Test public void testGetIcebergTable() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_get_table"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    assertNotNull(icebergTable.getIcebergTable());
    assertEquals(table, icebergTable.getIcebergTable());
  }

  @Test public void testTableCommentNull() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_no_comment"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    assertNull(icebergTable.getTableComment());
  }

  @Test public void testColumnCommentFromFieldDoc() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get(), "The primary key"));

    Table table =
        catalog.createTable(TableIdentifier.of("test_col_comment"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    assertEquals("The primary key", icebergTable.getColumnComment("id"));
  }

  @Test public void testColumnCommentCaseInsensitive() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "MyColumn", Types.IntegerType.get(), "A column"));

    Table table =
        catalog.createTable(TableIdentifier.of("test_col_case"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    assertEquals("A column", icebergTable.getColumnComment("mycolumn"));
    assertEquals("A column", icebergTable.getColumnComment("MYCOLUMN"));
  }

  @Test public void testColumnCommentNullForNonexistent() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_col_null"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    assertNull(icebergTable.getColumnComment("nonexistent"));
  }

  @Test public void testColumnCommentNullInput() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_col_null_input"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    assertNull(icebergTable.getColumnComment(null));
  }

  @Test public void testRowTypeCaching() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(TableIdentifier.of("test_cache"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType1 = icebergTable.getRowType(typeFactory);
    RelDataType rowType2 = icebergTable.getRowType(typeFactory);
    // Should return cached instance
    assertTrue(rowType1 == rowType2);
  }

  @Test public void testFixedTypeMapping() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "fixed_col", Types.FixedType.ofLength(16)));

    Table table =
        catalog.createTable(TableIdentifier.of("test_fixed"), schema, PartitionSpec.unpartitioned());
    IcebergTable icebergTable = new IcebergTable(table, Sources.of(tempDir.toFile()));

    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertEquals(SqlTypeName.VARBINARY,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }
}
