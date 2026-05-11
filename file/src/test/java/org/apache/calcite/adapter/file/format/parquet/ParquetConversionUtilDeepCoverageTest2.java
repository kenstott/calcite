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
package org.apache.calcite.adapter.file.format.parquet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for ParquetConversionUtil focusing on uncovered branches:
 * getParquetCacheDir variants, getCachedParquetFile, needsConversion,
 * isS3Path, getHadoopPath, isNullRepresentation, and createParquetFieldFromCalciteType.
 */
@Tag("unit")
public class ParquetConversionUtilDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  // ====== getParquetCacheDir tests ======

  @Test
  void testGetParquetCacheDirBaseOnly() {
    File baseDir = tempDir.toFile();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir);
    assertNotNull(result);
    assertTrue(result.getPath().endsWith(".parquet_cache"));
  }

  @Test
  void testGetParquetCacheDirWithCustomDir() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toAbsolutePath().toString();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir);
    assertNotNull(result);
    assertEquals(customDir, result.getPath());
  }

  @Test
  void testGetParquetCacheDirWithCustomDirAndSchema() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toAbsolutePath().toString();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "econ");
    assertNotNull(result);
    assertTrue(result.getPath().contains("schema_econ"));
  }

  @Test
  void testGetParquetCacheDirWithNullCustomDir() {
    File baseDir = tempDir.toFile();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir, null, "econ");
    assertNotNull(result);
    assertTrue(result.getPath().endsWith(".parquet_cache"));
  }

  @Test
  void testGetParquetCacheDirWithEmptyCustomDir() {
    File baseDir = tempDir.toFile();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir, "", "econ");
    assertNotNull(result);
    assertTrue(result.getPath().endsWith(".parquet_cache"));
  }

  @Test
  void testGetParquetCacheDirWithNullSchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toAbsolutePath().toString();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, null);
    assertNotNull(result);
    // No schema_xxx appended when schemaName is null
    assertFalse(result.getPath().contains("schema_"));
  }

  @Test
  void testGetParquetCacheDirWithEmptySchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toAbsolutePath().toString();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "");
    assertNotNull(result);
    // Empty schema name - no schema_ prefix appended
    assertFalse(result.getPath().contains("schema_"));
  }

  @Test
  void testGetParquetCacheDirCreatesDirectory() {
    File baseDir = tempDir.resolve("newdir").toFile();
    baseDir.mkdirs();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir);
    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.isDirectory());
  }

  // ====== getCachedParquetFile tests ======

  @Test
  void testGetCachedParquetFileBasicCsv() {
    File sourceFile = new File(tempDir.toFile(), "data.csv");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "SMART_CASING");
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  @Test
  void testGetCachedParquetFileNoExtension() {
    File sourceFile = new File(tempDir.toFile(), "datafile");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, false, "SMART_CASING");
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  @Test
  void testGetCachedParquetFileMultipleDots() {
    File sourceFile = new File(tempDir.toFile(), "data.2023.01.csv");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, true, "SMART_CASING");
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  @Test
  void testGetCachedParquetFileWithTypeInference() {
    File sourceFile = new File(tempDir.toFile(), "test_data.json");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, true, "SMART_CASING");
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
  }

  // ====== needsConversion(File, File) tests ======

  @Test
  void testNeedsConversionParquetDoesNotExist() throws Exception {
    File sourceFile = tempDir.resolve("source.csv").toFile();
    sourceFile.createNewFile();
    File parquetFile = tempDir.resolve("target.parquet").toFile();
    // target does not exist
    assertTrue(ParquetConversionUtil.needsConversion(sourceFile, parquetFile));
  }

  @Test
  void testNeedsConversionSourceNewer() throws Exception {
    File sourceFile = tempDir.resolve("source.csv").toFile();
    sourceFile.createNewFile();
    File parquetFile = tempDir.resolve("target.parquet").toFile();
    parquetFile.createNewFile();

    // Make source newer
    long futureTime = System.currentTimeMillis() + 10000;
    sourceFile.setLastModified(futureTime);
    parquetFile.setLastModified(futureTime - 5000);

    assertTrue(ParquetConversionUtil.needsConversion(sourceFile, parquetFile));
  }

  @Test
  void testNeedsConversionParquetNewer() throws Exception {
    File sourceFile = tempDir.resolve("source.csv").toFile();
    sourceFile.createNewFile();
    File parquetFile = tempDir.resolve("target.parquet").toFile();
    parquetFile.createNewFile();

    // Make parquet newer
    long now = System.currentTimeMillis();
    sourceFile.setLastModified(now - 5000);
    parquetFile.setLastModified(now);

    assertFalse(ParquetConversionUtil.needsConversion(sourceFile, parquetFile));
  }

  // ====== isS3Path tests ======

  @Test
  void testIsS3Path() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "s3://my-bucket/data/file.parquet"));
    assertFalse((Boolean) method.invoke(null, "/local/path/file.parquet"));
    assertFalse((Boolean) method.invoke(null, (String) null));
    assertFalse((Boolean) method.invoke(null, "hdfs://cluster/data"));
    assertFalse((Boolean) method.invoke(null, "http://example.com/data"));
  }

  // ====== getHadoopPath tests ======

  @Test
  void testGetHadoopPathS3() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("getHadoopPath", String.class);
    method.setAccessible(true);
    assertEquals("s3a://my-bucket/data", method.invoke(null, "s3://my-bucket/data"));
  }

  @Test
  void testGetHadoopPathLocalPath() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("getHadoopPath", String.class);
    method.setAccessible(true);
    assertEquals("/local/path", method.invoke(null, "/local/path"));
  }

  @Test
  void testGetHadoopPathNull() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("getHadoopPath", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, (String) null));
  }

  // ====== isNullRepresentation tests ======

  @Test
  void testIsNullRepresentationDefault() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("isNullRepresentation", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "NULL"));
    assertTrue((Boolean) method.invoke(null, "null"));
    assertTrue((Boolean) method.invoke(null, "NA"));
    assertTrue((Boolean) method.invoke(null, "N/A"));
    assertFalse((Boolean) method.invoke(null, "value"));
    // Empty string IS a null representation per NullEquivalents (both in the default set
    // and because trimmed empty strings return true)
    assertTrue((Boolean) method.invoke(null, ""));
  }

  @Test
  void testIsNullRepresentationWithCustomSet() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod(
        "isNullRepresentation", String.class, java.util.Set.class);
    method.setAccessible(true);

    java.util.Set<String> customNulls = new java.util.HashSet<>();
    customNulls.add("MISSING");
    customNulls.add("EMPTY");

    assertTrue((Boolean) method.invoke(null, "MISSING", customNulls));
    assertTrue((Boolean) method.invoke(null, "EMPTY", customNulls));
    assertFalse((Boolean) method.invoke(null, "NULL", customNulls));
    assertFalse((Boolean) method.invoke(null, "value", customNulls));
  }

  // ====== createParquetFieldFromCalciteType tests ======

  @Test
  void testCreateParquetFieldBoolean() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType boolType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BOOLEAN);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("flag", 0, boolType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "flag", org.apache.calcite.sql.type.SqlTypeName.BOOLEAN, field);

    assertNotNull(result);
    assertEquals("flag", result.getName());
  }

  @Test
  void testCreateParquetFieldInteger() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType intType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("count", 0, intType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "count", org.apache.calcite.sql.type.SqlTypeName.INTEGER, field);

    assertNotNull(result);
    assertEquals("count", result.getName());
  }

  @Test
  void testCreateParquetFieldSmallint() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType smallintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.SMALLINT);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("small_val", 0, smallintType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "small_val", org.apache.calcite.sql.type.SqlTypeName.SMALLINT, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldTinyint() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType tinyintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.TINYINT);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("tiny_val", 0, tinyintType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "tiny_val", org.apache.calcite.sql.type.SqlTypeName.TINYINT, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldBigint() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("big_val", 0, bigintType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "big_val", org.apache.calcite.sql.type.SqlTypeName.BIGINT, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldFloat() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType floatType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.FLOAT);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("float_val", 0, floatType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "float_val", org.apache.calcite.sql.type.SqlTypeName.FLOAT, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldReal() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType realType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.REAL);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("real_val", 0, realType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "real_val", org.apache.calcite.sql.type.SqlTypeName.REAL, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldDouble() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType doubleType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("dbl_val", 0, doubleType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "dbl_val", org.apache.calcite.sql.type.SqlTypeName.DOUBLE, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldDecimal() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType decimalType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 10, 2);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("amount", 0, decimalType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "amount", org.apache.calcite.sql.type.SqlTypeName.DECIMAL, field);

    assertNotNull(result);
    assertEquals("amount", result.getName());
  }

  @Test
  void testCreateParquetFieldDecimalZeroPrecision() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    // Use DECIMAL without explicit precision - defaults to max
    org.apache.calcite.rel.type.RelDataType decimalType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("val", 0, decimalType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "val", org.apache.calcite.sql.type.SqlTypeName.DECIMAL, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldDate() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType dateType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DATE);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("dt", 0, dateType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "dt", org.apache.calcite.sql.type.SqlTypeName.DATE, field);

    assertNotNull(result);
    assertEquals("dt", result.getName());
  }

  @Test
  void testCreateParquetFieldTime() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType timeType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.TIME);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("tm", 0, timeType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "tm", org.apache.calcite.sql.type.SqlTypeName.TIME, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldTimestamp() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType tsType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("ts", 0, tsType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "ts", org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldTimestampWithTZ() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType tsTZType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("ts_tz", 0, tsTZType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "ts_tz",
            org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, field);

    assertNotNull(result);
  }

  @Test
  void testCreateParquetFieldVarchar() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType varcharType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("name", 0, varcharType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR, field);

    assertNotNull(result);
    // VARCHAR should be OPTIONAL
    assertEquals(org.apache.parquet.schema.Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  void testCreateParquetFieldChar() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType charType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.CHAR);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("code", 0, charType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "code", org.apache.calcite.sql.type.SqlTypeName.CHAR, field);

    assertNotNull(result);
    assertEquals(org.apache.parquet.schema.Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  void testCreateParquetFieldNonNullableInteger() throws Exception {
    Method method = getCreateParquetFieldMethod();
    method.setAccessible(true);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    org.apache.calcite.rel.type.RelDataType intType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), false);
    org.apache.calcite.rel.type.RelDataTypeField field =
        new org.apache.calcite.rel.type.RelDataTypeFieldImpl("id", 0, intType);

    org.apache.parquet.schema.Type result = (org.apache.parquet.schema.Type)
        method.invoke(null, "id", org.apache.calcite.sql.type.SqlTypeName.INTEGER, field);

    assertNotNull(result);
    assertEquals(org.apache.parquet.schema.Type.Repetition.REQUIRED, result.getRepetition());
  }

  private Method getCreateParquetFieldMethod() throws Exception {
    return ParquetConversionUtil.class.getDeclaredMethod(
        "createParquetFieldFromCalciteType", String.class,
        org.apache.calcite.sql.type.SqlTypeName.class,
        org.apache.calcite.rel.type.RelDataTypeField.class);
  }
}
