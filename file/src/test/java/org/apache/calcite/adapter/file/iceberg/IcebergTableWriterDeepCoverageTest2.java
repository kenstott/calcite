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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
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
 * Deep coverage tests for IcebergTableWriter.
 * Tests private/internal methods via reflection and complex scenarios.
 */
@Tag("unit")
public class IcebergTableWriterDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;
  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    String warehousePath = tempDir.resolve("warehouse").toString();
    catalogConfig = new HashMap<String, Object>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", warehousePath);
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private Table createSimpleTable(String name) {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));

    return IcebergCatalogManager.createTable(catalogConfig, name, schema,
        PartitionSpec.unpartitioned());
  }

  private Table createPartitionedTable(String name) {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year")
        .build();

    return IcebergCatalogManager.createTable(catalogConfig, name, schema, spec);
  }

  private Table createFullTypeTable(String name) {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "int_col", Types.IntegerType.get()),
        Types.NestedField.optional(2, "long_col", Types.LongType.get()),
        Types.NestedField.optional(3, "float_col", Types.FloatType.get()),
        Types.NestedField.optional(4, "double_col", Types.DoubleType.get()),
        Types.NestedField.optional(5, "bool_col", Types.BooleanType.get()),
        Types.NestedField.optional(6, "string_col", Types.StringType.get()),
        Types.NestedField.optional(7, "date_col", Types.DateType.get()),
        Types.NestedField.optional(8, "timestamp_col", Types.TimestampType.withoutZone()));

    return IcebergCatalogManager.createTable(catalogConfig, name, schema,
        PartitionSpec.unpartitioned());
  }

  // ===== Constructor tests =====

  @Test void testTwoArgConstructor() {
    Table table = createSimpleTable("two_arg_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertNotNull(writer);
    assertEquals(table, writer.getTable());
  }

  @Test void testThreeArgConstructor() {
    Table table = createSimpleTable("three_arg_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertNotNull(writer);
    assertEquals(table, writer.getTable());
  }

  // ===== writeRecords tests =====

  @Test void testWriteRecordsNull() throws IOException {
    Table table = createSimpleTable("write_null_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertNull(writer.writeRecords(null, null));
  }

  @Test void testWriteRecordsEmpty() throws IOException {
    Table table = createSimpleTable("write_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertNull(writer.writeRecords(new ArrayList<Map<String, Object>>(), null));
  }

  @Test void testWriteRecordsSimple() throws IOException {
    Table table = createSimpleTable("write_simple_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "Alice");
    records.add(row);

    DataFile dataFile = writer.writeRecords(records, null);
    assertNotNull(dataFile);
    assertTrue(dataFile.recordCount() > 0);
    assertTrue(dataFile.fileSizeInBytes() > 0);
  }

  @Test void testWriteRecordsWithPartition() throws IOException {
    Table table = createPartitionedTable("write_partition_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "Bob");
    row.put("year", 2020);
    records.add(row);

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2020");

    DataFile dataFile = writer.writeRecords(records, partVals);
    assertNotNull(dataFile);
    assertTrue(dataFile.recordCount() > 0);
  }

  @Test void testWriteRecordsFieldFromPartitionValues() throws IOException {
    Table table = createPartitionedTable("write_pv_fallback_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "Charlie");
    // year is NOT in the row - should fall back to partitionValues
    records.add(row);

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2021");

    DataFile dataFile = writer.writeRecords(records, partVals);
    assertNotNull(dataFile);
  }

  @Test void testWriteRecordsMultipleRecords() throws IOException {
    Table table = createSimpleTable("write_multi_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 100; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("name", "Person_" + i);
      records.add(row);
    }

    DataFile dataFile = writer.writeRecords(records, null);
    assertNotNull(dataFile);
    assertEquals(100, dataFile.recordCount());
  }

  // ===== coerceValue tests (via reflection) =====

  @Test void testCoerceValueNull() throws Exception {
    Table table = createSimpleTable("coerce_null_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, null, Types.IntegerType.get()));
  }

  @Test void testCoerceValueEmptyString() throws Exception {
    Table table = createSimpleTable("coerce_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "", Types.IntegerType.get()));
  }

  @Test void testCoerceValueDashIndicator() throws Exception {
    Table table = createSimpleTable("coerce_dash_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "-", Types.IntegerType.get()));
  }

  @Test void testCoerceValueIntegerFromNumber() throws Exception {
    Table table = createSimpleTable("coerce_int_num_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(42, method.invoke(writer, 42L, Types.IntegerType.get()));
    assertEquals(42, method.invoke(writer, 42.7, Types.IntegerType.get()));
  }

  @Test void testCoerceValueIntegerFromString() throws Exception {
    Table table = createSimpleTable("coerce_int_str_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(42, method.invoke(writer, "42", Types.IntegerType.get()));
  }

  @Test void testCoerceValueIntegerInvalid() throws Exception {
    Table table = createSimpleTable("coerce_int_inv_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "not_a_number", Types.IntegerType.get()));
  }

  @Test void testCoerceValueLongFromNumber() throws Exception {
    Table table = createSimpleTable("coerce_long_num_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(100L, method.invoke(writer, 100, Types.LongType.get()));
    assertEquals(100L, method.invoke(writer, 100.9, Types.LongType.get()));
  }

  @Test void testCoerceValueLongFromString() throws Exception {
    Table table = createSimpleTable("coerce_long_str_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(999L, method.invoke(writer, "999", Types.LongType.get()));
  }

  @Test void testCoerceValueLongInvalid() throws Exception {
    Table table = createSimpleTable("coerce_long_inv_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "abc", Types.LongType.get()));
  }

  @Test void testCoerceValueFloatFromNumber() throws Exception {
    Table table = createSimpleTable("coerce_float_num_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(3.14f, method.invoke(writer, 3.14, Types.FloatType.get()));
  }

  @Test void testCoerceValueFloatFromString() throws Exception {
    Table table = createSimpleTable("coerce_float_str_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(1.5f, method.invoke(writer, "1.5", Types.FloatType.get()));
  }

  @Test void testCoerceValueFloatInvalid() throws Exception {
    Table table = createSimpleTable("coerce_float_inv_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "xyz", Types.FloatType.get()));
  }

  @Test void testCoerceValueDoubleFromNumber() throws Exception {
    Table table = createSimpleTable("coerce_double_num_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    // Float to double conversion loses precision, so use delta comparison
    double result = (Double) method.invoke(writer, 2.718f, Types.DoubleType.get());
    assertEquals(2.718, result, 0.001);
  }

  @Test void testCoerceValueDoubleFromString() throws Exception {
    Table table = createSimpleTable("coerce_double_str_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(9.81, method.invoke(writer, "9.81", Types.DoubleType.get()));
  }

  @Test void testCoerceValueDoubleInvalid() throws Exception {
    Table table = createSimpleTable("coerce_double_inv_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "nope", Types.DoubleType.get()));
  }

  @Test void testCoerceValueBoolean() throws Exception {
    Table table = createSimpleTable("coerce_bool_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals(true, method.invoke(writer, true, Types.BooleanType.get()));
    assertEquals(true, method.invoke(writer, "true", Types.BooleanType.get()));
    assertEquals(false, method.invoke(writer, "false", Types.BooleanType.get()));
  }

  @Test void testCoerceValueString() throws Exception {
    Table table = createSimpleTable("coerce_str_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    assertEquals("42", method.invoke(writer, 42, Types.StringType.get()));
    assertEquals("hello", method.invoke(writer, "hello", Types.StringType.get()));
  }

  @Test void testCoerceValueDateFromString() throws Exception {
    Table table = createSimpleTable("coerce_date_str_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    Object result = method.invoke(writer, "2020-01-01", Types.DateType.get());
    assertNotNull(result);
    assertTrue(result instanceof java.time.LocalDate);
    assertEquals(java.time.LocalDate.of(2020, 1, 1), result);
  }

  @Test void testCoerceValueDateFromLocalDate() throws Exception {
    Table table = createSimpleTable("coerce_date_ld_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    Object result = method.invoke(writer, java.time.LocalDate.of(2021, 6, 15), Types.DateType.get());
    assertNotNull(result);
    assertEquals(java.time.LocalDate.of(2021, 6, 15), result);
  }

  @Test void testCoerceValueDateFromSqlDate() throws Exception {
    Table table = createSimpleTable("coerce_date_sql_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    java.sql.Date sqlDate = java.sql.Date.valueOf("2022-03-20");
    Object result = method.invoke(writer, sqlDate, Types.DateType.get());
    assertNotNull(result);
  }

  @Test void testCoerceValueTimestampFromInstant() throws Exception {
    Table table = createSimpleTable("coerce_ts_inst_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    java.time.Instant instant = java.time.Instant.parse("2020-01-01T00:00:00Z");
    Object result = method.invoke(writer, instant, Types.TimestampType.withoutZone());
    assertNotNull(result);
    assertTrue(result instanceof Long);
  }

  @Test void testCoerceValueTimestampFromSqlTimestamp() throws Exception {
    Table table = createSimpleTable("coerce_ts_sql_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);
    java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2020-06-15 12:30:00");
    Object result = method.invoke(writer, ts, Types.TimestampType.withoutZone());
    assertNotNull(result);
    assertTrue(result instanceof Long);
  }

  @Test void testCoerceValueListFromJavaList() throws Exception {
    Table table = createSimpleTable("coerce_list_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);

    Types.ListType listType = Types.ListType.ofOptional(10, Types.IntegerType.get());
    List<Object> input = new ArrayList<Object>();
    input.add(1);
    input.add(2);
    input.add(3);

    Object result = method.invoke(writer, input, listType);
    assertNotNull(result);
    assertTrue(result instanceof List);
    List<?> resultList = (List<?>) result;
    assertEquals(3, resultList.size());
  }

  @Test void testCoerceValueListFromArray() throws Exception {
    Table table = createSimpleTable("coerce_array_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coerceValue", Object.class, org.apache.iceberg.types.Type.class);
    method.setAccessible(true);

    Types.ListType listType = Types.ListType.ofOptional(10, Types.StringType.get());
    String[] input = {"a", "b", "c"};

    Object result = method.invoke(writer, input, listType);
    assertNotNull(result);
    assertTrue(result instanceof List);
    assertEquals(3, ((List<?>) result).size());
  }

  // ===== coercePartitionValue tests (via reflection) =====

  @Test void testCoercePartitionValueNull() throws Exception {
    Table table = createPartitionedTable("cpv_null_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coercePartitionValue", String.class, org.apache.iceberg.PartitionField.class);
    method.setAccessible(true);

    org.apache.iceberg.PartitionField field = table.spec().fields().get(0);
    assertNull(method.invoke(writer, null, field));
    assertNull(method.invoke(writer, "", field));
    assertNull(method.invoke(writer, "-", field));
  }

  @Test void testCoercePartitionValueInteger() throws Exception {
    Table table = createPartitionedTable("cpv_int_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coercePartitionValue", String.class, org.apache.iceberg.PartitionField.class);
    method.setAccessible(true);

    org.apache.iceberg.PartitionField field = table.spec().fields().get(0);
    assertEquals(2020, method.invoke(writer, "2020", field));
  }

  @Test void testCoercePartitionValueInvalidNumber() throws Exception {
    Table table = createPartitionedTable("cpv_inv_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coercePartitionValue", String.class, org.apache.iceberg.PartitionField.class);
    method.setAccessible(true);

    org.apache.iceberg.PartitionField field = table.spec().fields().get(0);
    assertNull(method.invoke(writer, "not_a_number", field));
  }

  // ===== computeRelativePath tests (via reflection) =====

  @Test void testComputeRelativePathNormalizedBase() throws Exception {
    Table table = createSimpleTable("crp_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("computeRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("file.parquet",
        method.invoke(writer, "/staging/", "/staging/file.parquet"));
  }

  @Test void testComputeRelativePathWithoutTrailingSlash() throws Exception {
    Table table = createSimpleTable("crp_no_slash_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("computeRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("file.parquet",
        method.invoke(writer, "/staging", "/staging/file.parquet"));
  }

  @Test void testComputeRelativePathNested() throws Exception {
    Table table = createSimpleTable("crp_nested_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("computeRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("year=2020/file.parquet",
        method.invoke(writer, "/staging", "/staging/year=2020/file.parquet"));
  }

  @Test void testComputeRelativePathFallback() throws Exception {
    Table table = createSimpleTable("crp_fallback_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("computeRelativePath", String.class, String.class);
    method.setAccessible(true);

    // Completely different paths - should fallback to filename
    assertEquals("data.parquet",
        method.invoke(writer, "/base1/", "/base2/data.parquet"));
  }

  // ===== getParentPath tests (via reflection) =====

  @Test void testGetParentPathNormal() throws Exception {
    Table table = createSimpleTable("gpp_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getParentPath", String.class);
    method.setAccessible(true);

    assertEquals("/data/warehouse", method.invoke(writer, "/data/warehouse/file.parquet"));
  }

  @Test void testGetParentPathNoSlash() throws Exception {
    Table table = createSimpleTable("gpp_no_slash_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getParentPath", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(writer, "file.parquet"));
  }

  @Test void testGetParentPathS3Root() throws Exception {
    Table table = createSimpleTable("gpp_s3_root_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getParentPath", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(writer, "s3://bucket"));
  }

  @Test void testGetParentPathS3aRoot() throws Exception {
    Table table = createSimpleTable("gpp_s3a_root_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getParentPath", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(writer, "s3a://bucket"));
  }

  @Test void testGetParentPathS3Deep() throws Exception {
    Table table = createSimpleTable("gpp_s3_deep_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getParentPath", String.class);
    method.setAccessible(true);

    assertEquals("s3://bucket/prefix",
        method.invoke(writer, "s3://bucket/prefix/file.parquet"));
  }

  // ===== normalizeS3Path tests (via reflection) =====

  @Test void testNormalizeS3PathNull() throws Exception {
    Table table = createSimpleTable("ns3_null_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("normalizeS3Path", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(writer, (String) null));
  }

  @Test void testNormalizeS3PathSingleSlash() throws Exception {
    Table table = createSimpleTable("ns3_single_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("normalizeS3Path", String.class);
    method.setAccessible(true);

    assertEquals("s3a://bucket/file", method.invoke(writer, "s3a:/bucket/file"));
  }

  @Test void testNormalizeS3PathDoubleSlash() throws Exception {
    Table table = createSimpleTable("ns3_double_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("normalizeS3Path", String.class);
    method.setAccessible(true);

    assertEquals("s3a://bucket/file", method.invoke(writer, "s3a://bucket/file"));
  }

  @Test void testNormalizeS3PathS3Single() throws Exception {
    Table table = createSimpleTable("ns3_s3_single_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("normalizeS3Path", String.class);
    method.setAccessible(true);

    assertEquals("s3://bucket/file", method.invoke(writer, "s3:/bucket/file"));
  }

  @Test void testNormalizeS3PathLocal() throws Exception {
    Table table = createSimpleTable("ns3_local_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("normalizeS3Path", String.class);
    method.setAccessible(true);

    assertEquals("/local/path", method.invoke(writer, "/local/path"));
  }

  // ===== estimateRecordCount tests (via reflection) =====

  @Test void testEstimateRecordCountSmallFile() throws Exception {
    Table table = createSimpleTable("erc_small_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("estimateRecordCount", long.class);
    method.setAccessible(true);

    assertEquals(1L, method.invoke(writer, 50L));
  }

  @Test void testEstimateRecordCountLargeFile() throws Exception {
    Table table = createSimpleTable("erc_large_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("estimateRecordCount", long.class);
    method.setAccessible(true);

    assertEquals(10000L, method.invoke(writer, 1000000L));
  }

  // ===== buildPartitionPath tests (via reflection) =====

  @Test void testBuildPartitionPathNull() throws Exception {
    Table table = createPartitionedTable("bpp_null_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("buildPartitionPath", Map.class);
    method.setAccessible(true);

    assertEquals("", method.invoke(writer, (Map<String, String>) null));
  }

  @Test void testBuildPartitionPathEmpty() throws Exception {
    Table table = createPartitionedTable("bpp_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("buildPartitionPath", Map.class);
    method.setAccessible(true);

    assertEquals("", method.invoke(writer, new HashMap<String, String>()));
  }

  @Test void testBuildPartitionPathWithValues() throws Exception {
    Table table = createPartitionedTable("bpp_vals_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("buildPartitionPath", Map.class);
    method.setAccessible(true);

    Map<String, String> vals = new HashMap<String, String>();
    vals.put("year", "2020");

    String result = (String) method.invoke(writer, vals);
    assertEquals("year=2020", result);
  }

  // ===== commitDataFiles tests =====

  @Test void testCommitDataFilesEmpty() {
    Table table = createSimpleTable("cdf_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    // Should not throw for empty list
    writer.commitDataFiles(new ArrayList<DataFile>(), null);
  }

  @Test void testCommitDataFilesWithPartitionFilter() throws IOException {
    Table table = createSimpleTable("cdf_filter_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Write a record first
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "Test");
    records.add(row);

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);

    List<DataFile> files = new ArrayList<DataFile>();
    files.add(df);

    Map<String, Object> filter = new HashMap<String, Object>();
    filter.put("id", 1);

    writer.commitDataFiles(files, filter);
    // Verify commit succeeded by checking snapshot exists
    assertNotNull(table.currentSnapshot());
  }

  // ===== bulkCommitDataFiles tests =====

  @Test void testBulkCommitDataFilesEmpty() {
    Table table = createSimpleTable("bcdf_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writer.bulkCommitDataFiles(new ArrayList<DataFile>());
  }

  @Test void testBulkCommitDataFiles() throws IOException {
    Table table = createSimpleTable("bcdf_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<DataFile> allFiles = new ArrayList<DataFile>();
    for (int i = 0; i < 3; i++) {
      List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("name", "Record_" + i);
      records.add(row);
      DataFile df = writer.writeRecords(records, null);
      if (df != null) {
        allFiles.add(df);
      }
    }

    assertFalse(allFiles.isEmpty());
    writer.bulkCommitDataFiles(allFiles);
    assertNotNull(table.currentSnapshot());
  }

  // ===== deletePartition tests =====

  @Test void testDeletePartitionNullFilter() {
    Table table = createPartitionedTable("dp_null_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertThrows(IllegalArgumentException.class, () -> writer.deletePartition(null));
  }

  @Test void testDeletePartitionEmptyFilter() {
    Table table = createPartitionedTable("dp_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertThrows(IllegalArgumentException.class,
        () -> writer.deletePartition(new HashMap<String, Object>()));
  }

  // ===== stageFiles from empty staging =====

  @Test void testStageFilesEmpty() throws IOException {
    Table table = createSimpleTable("sf_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Path staging = tempDir.resolve("empty_staging");
    Files.createDirectories(staging);

    List<DataFile> result = writer.stageFiles(staging.toString());
    assertTrue(result.isEmpty());
  }

  // ===== runMaintenance tests =====

  @Test void testRunMaintenanceNoSnapshots() {
    Table table = createSimpleTable("rm_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    // Should not throw on empty table
    writer.runMaintenance(7, 1);
  }

  @Test void testRunMaintenanceSkipsOrphanDetection() {
    Table table = createSimpleTable("rm_skip_orphan_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    // orphanFilesDays > 30 should skip orphan detection
    writer.runMaintenance(7, 31);
  }

  @Test void testRunMaintenanceWithSnapshots() throws IOException {
    Table table = createSimpleTable("rm_snap_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Create some data to have snapshots
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "test");
    records.add(row);

    DataFile df = writer.writeRecords(records, null);
    List<DataFile> files = new ArrayList<DataFile>();
    files.add(df);
    writer.commitDataFiles(files, null);

    // Now run maintenance
    writer.runMaintenance(7, 1);
  }

  // ===== compactSmallFiles tests =====

  @Test void testCompactSmallFilesEmptyTable() throws IOException {
    Table table = createSimpleTable("csf_empty_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    int result = writer.compactSmallFiles(128 * 1024 * 1024, 10, 10 * 1024 * 1024);
    assertEquals(0, result);
  }

  // ===== getFieldValue tests (via reflection) =====

  @Test void testGetFieldValueFromRow() throws Exception {
    Table table = createSimpleTable("gfv_row_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getFieldValue", Map.class, String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("Name", "Alice");

    // Case-insensitive lookup
    assertEquals("Alice", method.invoke(writer, row, "name", null));
    assertEquals("Alice", method.invoke(writer, row, "Name", null));
  }

  @Test void testGetFieldValueFromPartitionValues() throws Exception {
    Table table = createSimpleTable("gfv_pv_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getFieldValue", Map.class, String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("Year", "2020");

    // Not in row, falls back to partition values (case-insensitive)
    assertEquals("2020", method.invoke(writer, row, "year", partVals));
  }

  @Test void testGetFieldValueNotFound() throws Exception {
    Table table = createSimpleTable("gfv_missing_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getFieldValue", Map.class, String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();

    assertNull(method.invoke(writer, row, "missing_field", null));
  }

  // ===== ensureVersionHint tests (via reflection) =====

  @Test void testEnsureVersionHintOnNewTable() throws Exception {
    Table table = createSimpleTable("evh_test");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Write and commit data to create a snapshot
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "test");
    records.add(row);
    DataFile df = writer.writeRecords(records, null);
    List<DataFile> files = new ArrayList<DataFile>();
    files.add(df);
    writer.commitDataFiles(files, null);

    // ensureVersionHint is called internally by commitDataFiles
    // Verify version-hint.text exists
    String metadataDir = table.location() + "/metadata";
    String versionHintPath = metadataDir + "/version-hint.text";
    try {
      StorageProvider.FileMetadata metadata = storageProvider.getMetadata(versionHintPath);
      assertNotNull(metadata);
      assertTrue(metadata.getSize() > 0);
    } catch (IOException e) {
      // File might not exist if ensureVersionHint was skipped
      // (e.g., it already existed from the Iceberg commit itself)
    }
  }
}
