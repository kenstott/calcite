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
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for IcebergMaterializationWriter focusing on
 * expression evaluation, type casting, partition management, and error paths.
 */
@Tag("unit")
public class IcebergMaterializationWriterDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;
  private String warehousePath;
  private IcebergMaterializationWriter writer;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    warehousePath = tempDir.resolve("warehouse").toString();
  }

  @AfterEach
  void tearDown() {
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException ignored) {
        // cleanup
      }
    }
    IcebergCatalogManager.clearCache();
  }

  // ====================================================================
  // Tests for getEnvInt static method
  // ====================================================================

  @Test void testGetEnvIntDefaultValue() throws Exception {
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("getEnvInt", String.class, int.class);
    method.setAccessible(true);
    int result = (int) method.invoke(null, "NON_EXISTENT_ENV_VAR_12345", 42);
    assertEquals(42, result);
  }

  // ====================================================================
  // Tests for mapToIcebergType
  // ====================================================================

  @Test void testMapToIcebergTypeCoversAllBranches() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("mapToIcebergType", String.class);
    method.setAccessible(true);

    assertEquals("STRING", method.invoke(writer, (Object) null));
    assertEquals("STRING", method.invoke(writer, "VARCHAR(255)"));
    assertEquals("STRING", method.invoke(writer, "CHAR(10)"));
    assertEquals("INT", method.invoke(writer, "INTEGER"));
    assertEquals("INT", method.invoke(writer, "INT"));
    assertEquals("LONG", method.invoke(writer, "BIGINT"));
    assertEquals("LONG", method.invoke(writer, "LONG"));
    assertEquals("DOUBLE", method.invoke(writer, "DOUBLE"));
    assertEquals("DOUBLE", method.invoke(writer, "FLOAT"));
    assertEquals("BOOLEAN", method.invoke(writer, "BOOLEAN"));
    assertEquals("DATE", method.invoke(writer, "DATE"));
    assertEquals("TIMESTAMP", method.invoke(writer, "TIMESTAMP"));
    assertEquals("STRING", method.invoke(writer, "UNKNOWN_TYPE"));
  }

  // ====================================================================
  // Tests for mapToDuckDBType
  // ====================================================================

  @Test void testMapToDuckDBTypeCoversAllBranches() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("mapToDuckDBType", String.class);
    method.setAccessible(true);

    assertEquals("VARCHAR", method.invoke(writer, (Object) null));
    assertEquals("VARCHAR", method.invoke(writer, "VARCHAR(255)"));
    assertEquals("VARCHAR", method.invoke(writer, "CHAR(10)"));
    assertEquals("VARCHAR", method.invoke(writer, "STRING"));
    assertEquals("INTEGER", method.invoke(writer, "INTEGER"));
    assertEquals("INTEGER", method.invoke(writer, "INT"));
    assertEquals("BIGINT", method.invoke(writer, "BIGINT"));
    assertEquals("BIGINT", method.invoke(writer, "LONG"));
    assertEquals("DOUBLE", method.invoke(writer, "DOUBLE"));
    assertEquals("DOUBLE", method.invoke(writer, "FLOAT"));
    assertEquals("BOOLEAN", method.invoke(writer, "BOOLEAN"));
    assertEquals("DATE", method.invoke(writer, "DATE"));
    assertEquals("TIMESTAMP", method.invoke(writer, "TIMESTAMP"));
    assertEquals("VARCHAR", method.invoke(writer, "UNKNOWN_TYPE"));
  }

  // ====================================================================
  // Tests for castValue
  // ====================================================================

  @Test void testCastValueAllTypes() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("castValue", Object.class, String.class);
    method.setAccessible(true);

    assertNull(method.invoke(writer, null, "BIGINT"));
    assertNull(method.invoke(writer, "", "BIGINT"));
    assertNull(method.invoke(writer, "   ", "BIGINT"));

    assertEquals(42L, method.invoke(writer, "42", "BIGINT"));
    assertEquals(42L, method.invoke(writer, "42", "INT64"));
    assertEquals(42L, method.invoke(writer, "42", "LONG"));
    assertEquals(42, method.invoke(writer, "42", "INTEGER"));
    assertEquals(42, method.invoke(writer, "42", "INT"));
    assertEquals(42, method.invoke(writer, "42", "INT32"));
    assertEquals(3.14, method.invoke(writer, "3.14", "DOUBLE"));
    assertEquals(3.14, method.invoke(writer, "3.14", "FLOAT8"));
    assertEquals(3.14f, method.invoke(writer, "3.14", "FLOAT"));
    assertEquals(3.14f, method.invoke(writer, "3.14", "FLOAT4"));
    assertEquals(3.14f, method.invoke(writer, "3.14", "REAL"));
    assertEquals("hello", method.invoke(writer, "hello", "VARCHAR"));
    assertEquals("hello", method.invoke(writer, "hello", "STRING"));
    assertEquals("hello", method.invoke(writer, "hello", "TEXT"));
    assertEquals(true, method.invoke(writer, "true", "BOOLEAN"));
    assertEquals(false, method.invoke(writer, "false", "BOOL"));
    assertEquals("test", method.invoke(writer, "test", "SOME_UNKNOWN"));
    assertNull(method.invoke(writer, "not_a_number", "BIGINT"));
    assertNull(method.invoke(writer, "abc", "INTEGER"));
    assertNull(method.invoke(writer, "xyz", "DOUBLE"));
    assertNull(method.invoke(writer, "nope", "FLOAT"));
  }

  // ====================================================================
  // Tests for evaluateExpression
  // ====================================================================

  @Test void testEvaluateExpressionSrcFieldPattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("FIELDNAME", "value123");

    assertEquals("value123", method.invoke(writer, "src.\"FIELDNAME\"", row));
    assertEquals("value123", method.invoke(writer, "src.FIELDNAME", row));
    assertNull(method.invoke(writer, null, row));
    assertNull(method.invoke(writer, "", row));
  }

  @Test void testEvaluateExpressionBareFieldPattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("table_name", "my_table");
    assertEquals("my_table", method.invoke(writer, "table_name", row));
  }

  @Test void testEvaluateExpressionCastPattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("VALUE", "42");

    assertEquals(42L, method.invoke(writer, "TRY_CAST(src.\"VALUE\" AS BIGINT)", row));
    assertEquals(42, method.invoke(writer, "CAST(src.VALUE AS INTEGER)", row));
  }

  @Test void testEvaluateExpressionBareCastPattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("VALUE", "3.14");
    assertEquals(3.14, method.invoke(writer, "CAST(VALUE AS DOUBLE)", row));
  }

  @Test void testEvaluateExpressionReplacePattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("AMOUNT", "1,234,567");
    assertEquals("1234567", method.invoke(writer, "REPLACE(src.\"AMOUNT\", ',', '')", row));

    Map<String, Object> emptyRow = new HashMap<String, Object>();
    assertNull(method.invoke(writer, "REPLACE(src.\"AMOUNT\", ',', '')", emptyRow));
  }

  @Test void testEvaluateExpressionCastReplacePattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("AMOUNT", "1,234");
    assertEquals(
        1234L, method.invoke(writer,
        "TRY_CAST(REPLACE(src.\"AMOUNT\", ',', '') AS BIGINT)", row));
    assertNull(
        method.invoke(writer,
        "TRY_CAST(REPLACE(src.\"MISSING\", ',', '') AS BIGINT)", row));
  }

  @Test void testEvaluateExpressionSubstringPattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("CODE", "ABCDE");
    assertEquals("ABC", method.invoke(writer, "SUBSTRING(src.\"CODE\", 1, 3)", row));

    row.put("SHORT", "AB");
    assertNull(method.invoke(writer, "SUBSTRING(src.\"SHORT\", 5, 3)", row));
    assertNull(method.invoke(writer, "SUBSTRING(src.\"MISSING\", 1, 3)", row));
  }

  @Test void testEvaluateExpressionRightPattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("CODE", "ABCDE");
    assertEquals("CDE", method.invoke(writer, "RIGHT(src.\"CODE\", 3)", row));
    assertEquals("ABCDE", method.invoke(writer, "RIGHT(src.\"CODE\", 10)", row));
    assertNull(method.invoke(writer, "RIGHT(src.\"MISSING\", 3)", row));
  }

  @Test void testEvaluateExpressionCoalescePattern() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("FIELD2", "backup_value");
    assertEquals(
        "backup_value", method.invoke(writer,
        "COALESCE(src.\"FIELD1\", src.\"FIELD2\")", row));

    Map<String, Object> emptyRow = new HashMap<String, Object>();
    assertNull(
        method.invoke(writer,
        "COALESCE(src.\"FIELD1\", src.\"FIELD2\")", emptyRow));
  }

  @Test void testEvaluateExpressionUnrecognizedExpression() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    assertNull(method.invoke(writer, "COMPLEX_FUNCTION(a, b, c)", row));
  }

  // ====================================================================
  // Tests for getValueCaseInsensitive
  // ====================================================================

  @Test void testGetValueCaseInsensitive() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("getValueCaseInsensitive", Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("MyField", "value1");

    assertEquals("value1", method.invoke(writer, map, "MyField"));
    assertEquals("value1", method.invoke(writer, map, "myfield"));
    assertEquals("value1", method.invoke(writer, map, "MYFIELD"));
    assertNull(method.invoke(writer, map, "NoSuchField"));
    assertNull(method.invoke(writer, null, "key"));
    assertNull(method.invoke(writer, map, null));
  }

  // ====================================================================
  // Tests for convertToS3aScheme
  // ====================================================================

  @Test void testConvertToS3aScheme() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("convertToS3aScheme", String.class);
    method.setAccessible(true);

    assertEquals("s3a://bucket/path", method.invoke(writer, "s3://bucket/path"));
    assertEquals("/local/path", method.invoke(writer, "/local/path"));
    assertNull(method.invoke(writer, (Object) null));
  }

  // ====================================================================
  // Tests for buildPartitionKey
  // ====================================================================

  @Test void testBuildPartitionKey() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildPartitionKey", Map.class);
    method.setAccessible(true);

    assertEquals("", method.invoke(writer, (Object) null));
    assertEquals("", method.invoke(writer, new HashMap<String, String>()));

    Map<String, String> single = new HashMap<String, String>();
    single.put("year", "2020");
    assertEquals("year=2020", method.invoke(writer, single));

    Map<String, String> multi = new HashMap<String, String>();
    multi.put("year", "2020");
    multi.put("month", "01");
    assertEquals("month=01|year=2020", method.invoke(writer, multi));
  }

  // ====================================================================
  // Tests for buildHadoopS3Config
  // ====================================================================

  @Test void testBuildHadoopS3ConfigAllFields() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildHadoopS3Config", Map.class);
    method.setAccessible(true);

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AKID");
    s3Config.put("secretAccessKey", "SECRET");
    s3Config.put("endpoint", "https://endpoint.example.com");
    s3Config.put("region", "us-east-1");

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) method.invoke(writer, s3Config);

    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", result.get("fs.s3a.impl"));
    assertEquals("AKID", result.get("fs.s3a.access.key"));
    assertEquals("SECRET", result.get("fs.s3a.secret.key"));
    assertEquals("https://endpoint.example.com", result.get("fs.s3a.endpoint"));
    assertEquals("true", result.get("fs.s3a.path.style.access"));
    assertEquals("us-east-1", result.get("fs.s3a.endpoint.region"));
  }

  @Test void testBuildHadoopS3ConfigMinimalFields() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildHadoopS3Config", Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result =
        (Map<String, String>) method.invoke(writer, new HashMap<String, String>());
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", result.get("fs.s3a.impl"));
    assertNull(result.get("fs.s3a.access.key"));
    assertNull(result.get("fs.s3a.endpoint"));
  }

  // ====================================================================
  // Tests for buildHadoopConfiguration
  // ====================================================================

  @Test void testBuildHadoopConfigurationWithAndWithoutMap() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method buildHadoopConfig =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildHadoopConfiguration");
    buildHadoopConfig.setAccessible(true);

    Field catalogConfigField = IcebergMaterializationWriter.class.getDeclaredField("catalogConfig");
    catalogConfigField.setAccessible(true);

    Map<String, Object> catalogCfg = new HashMap<String, Object>();
    Map<String, String> hadoopMap = new HashMap<String, String>();
    hadoopMap.put("fs.s3a.access.key", "test-key");
    catalogCfg.put("hadoopConfig", hadoopMap);
    catalogConfigField.set(writer, catalogCfg);

    org.apache.hadoop.conf.Configuration conf =
        (org.apache.hadoop.conf.Configuration) buildHadoopConfig.invoke(writer);
    assertEquals("test-key", conf.get("fs.s3a.access.key"));

    catalogCfg.clear();
    conf = (org.apache.hadoop.conf.Configuration) buildHadoopConfig.invoke(writer);
    assertNull(conf.get("fs.s3a.access.key"));
  }

  // ====================================================================
  // Tests for escapeString
  // ====================================================================

  @Test void testEscapeString() throws Exception {
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("escapeString", String.class);
    method.setAccessible(true);

    assertEquals("hello", method.invoke(null, "hello"));
    assertEquals("it''s", method.invoke(null, "it's"));
    assertEquals("a''b''c", method.invoke(null, "a'b'c"));
  }

  // ====================================================================
  // Tests for getRemoteParentPath
  // ====================================================================

  @Test void testGetRemoteParentPath() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("getRemoteParentPath", String.class);
    method.setAccessible(true);

    assertEquals("/parent", method.invoke(writer, "/parent/child"));
    assertNull(method.invoke(writer, "/"));
    assertNull(method.invoke(writer, "noSlash"));
    assertNull(method.invoke(writer, "s3://buck"));
    assertEquals("s3a://buck", method.invoke(writer, "s3a://buck/"));
    assertEquals("s3://bucket/parent", method.invoke(writer, "s3://bucket/parent/child"));
    assertEquals("s3a://bucket/parent", method.invoke(writer, "s3a://bucket/parent/child"));
  }

  // ====================================================================
  // Tests for constructor and initialization
  // ====================================================================

  @Test void testConstructorWithNullTracker() {
    IcebergMaterializationWriter w =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNotNull(w);
    assertEquals(MaterializeConfig.Format.ICEBERG, w.getFormat());
    assertEquals(0, w.getTotalRowsWritten());
    assertEquals(0, w.getTotalFilesWritten());
  }

  @Test void testInitializeWithNullConfig() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(null));
  }

  @Test void testInitializeWithDisabledConfig() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IOException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeWithWrongFormat() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeWithNoTargetTableId() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  // ====================================================================
  // Tests for writeBatch edge cases
  // ====================================================================

  @Test void testWriteBatchBeforeInitialize() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(new HashMap<String, Object>());
    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(data.iterator(), null));
  }

  @Test void testWriteBatchWithNullData() throws Exception {
    writer = createInitializedWriter("null_data_table");
    long result = writer.writeBatch(null, null);
    assertEquals(0, result);
  }

  @Test void testWriteBatchWithEmptyIterator() throws Exception {
    writer = createInitializedWriter("empty_iter_table");
    List<Map<String, Object>> emptyList = Collections.emptyList();
    long result = writer.writeBatch(emptyList.iterator(), null);
    assertEquals(0, result);
  }

  // ====================================================================
  // Tests for commit and close edge cases
  // ====================================================================

  @Test void testCommitBeforeInitialize() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertThrows(IllegalStateException.class, () -> writer.commit());
  }

  @Test void testGetTableLocationBeforeInitialize() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNull(writer.getTableLocation());
  }

  @Test void testGetTableLocationAfterInitialize() throws Exception {
    writer = createInitializedWriter("location_table");
    String location = writer.getTableLocation();
    assertNotNull(location);
  }

  @Test void testGetEtlPropertyBeforeInitialize() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNull(writer.getEtlProperty("etl.config-hash"));
  }

  @Test void testStoreEtlPropertiesBeforeInitialize() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    writer.storeEtlProperties("hash", "sig", 100);
  }

  @Test void testStoreEtlPropertiesAfterInitialize() throws Exception {
    writer = createInitializedWriter("etl_props_table");
    writer.storeEtlProperties("hash123", "sig456", 1000);
    assertEquals("hash123", writer.getEtlProperty("etl.config-hash"));
    assertEquals("sig456", writer.getEtlProperty("etl.signature"));
    assertEquals("1000", writer.getEtlProperty("etl.row-count"));
  }

  @Test void testCloseWithoutInitialize() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    writer.close();
  }

  @Test void testCloseAfterInitialize() throws Exception {
    writer = createInitializedWriter("close_table");
    writer.close();
  }

  // ====================================================================
  // Tests for cleanupStagingDirectory
  // ====================================================================

  @Test void testCleanupStagingDirectoryLocal() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("cleanup_local_table", MaterializeOptionsConfig.StagingMode.LOCAL);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    Path stagingDir = tempDir.resolve("staging_local");
    java.nio.file.Files.createDirectories(stagingDir);
    java.nio.file.Files.write(stagingDir.resolve("test.parquet"), new byte[]{1, 2, 3});

    method.invoke(writer, stagingDir.toString());
    assertFalse(java.nio.file.Files.exists(stagingDir));
  }

  @Test void testCleanupStagingDirectoryRemoteS3Skipped() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Field stagingModeField = IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    stagingModeField.setAccessible(true);
    stagingModeField.set(writer, MaterializeOptionsConfig.StagingMode.REMOTE);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    method.invoke(writer, "s3://bucket/staging/test");
    method.invoke(writer, "s3a://bucket/staging/test");
  }

  // ====================================================================
  // Tests for commit with empty pending files
  // ====================================================================

  @Test void testCommitWithNoPendingFiles() throws Exception {
    writer = createInitializedWriter("no_pending_table");
    writer.commit();
    assertEquals(0, writer.getTotalRowsWritten());
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private IcebergMaterializationWriter createInitializedWriter(String tableName) throws Exception {
    return createInitializedWriterWithStagingMode(tableName, null);
  }

  @SuppressWarnings("unchecked")
  private IcebergMaterializationWriter createInitializedWriterWithStagingMode(
      String tableName, MaterializeOptionsConfig.StagingMode stagingMode) throws Exception {
    IcebergMaterializationWriter w =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("data_col").type("VARCHAR").build());
    columns.add(ColumnConfig.builder().name("value").type("INTEGER").build());

    MaterializeConfig.Builder builder = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId(tableName)
        .name(tableName)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build());

    if (stagingMode != null) {
      builder.options(MaterializeOptionsConfig.builder()
          .stagingMode(stagingMode)
          .build());
    }

    MaterializeConfig config = builder.build();
    w.initialize(config);
    return w;
  }
}
