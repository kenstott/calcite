/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link SparkJdbcSchemaFactory}.
 * Focuses on private helper methods using reflection: resolveParquetPath,
 * registerSqlViewsInSpark, configureS3Credentials, configureIcebergCatalog,
 * and registerFilesAsViews edge cases.
 */
@Tag("unit")
class SparkJdbcSchemaFactoryDeepCoverageTest2 {

  // ====================================================================
  // Tests for resolveParquetPath (private static method)
  // ====================================================================

  @Test
  void testResolveParquetPathWithViewScanPattern() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "s3://bucket/table/**/*.parquet";
    record.parquetCacheFile = "/cache/file.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("s3://bucket/table/**/*.parquet", result);
  }

  @Test
  void testResolveParquetPathWithParquetCacheFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = "/cache/file.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/cache/file.parquet", result);
  }

  @Test
  void testResolveParquetPathWithSourceFileParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = "/data/source.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/data/source.parquet", result);
  }

  @Test
  void testResolveParquetPathWithSourceFileNonParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = "/data/source.csv";

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  @Test
  void testResolveParquetPathWithConvertedFileParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = "/data/converted.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/data/converted.parquet", result);
  }

  @Test
  void testResolveParquetPathWithConvertedFileJsonBrackets() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = "{\"key\":\"value\"}";

    String result = invokeResolveParquetPath(record);
    assertEquals("{\"key\":\"value\"}", result);
  }

  @Test
  void testResolveParquetPathWithConvertedFileNonParquetNonBracket() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = "/data/output.json";

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  @Test
  void testResolveParquetPathAllNull() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = null;

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  // ====================================================================
  // Tests for registerSqlViewsInSpark (private static method)
  // ====================================================================

  @Test
  void testRegisterSqlViewsInSparkWithNullOperand() throws Exception {
    Connection conn = mock(Connection.class);
    // Should not throw; null operand means no views to register
    invokeRegisterSqlViewsInSpark(conn, "myschema", null);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterSqlViewsInSparkWithNoTables() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();
    // No "tables" key at all
    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterSqlViewsInSparkWithEmptyTables() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", new ArrayList<>());
    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterSqlViewsInSparkSkipsNonViewType() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("type", "table");
    tableDef.put("name", "my_table");
    tableDef.put("sql", "SELECT 1");
    tables.add(tableDef);

    operand.put("tables", tables);
    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterSqlViewsInSparkSkipsMissingViewName() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("type", "view");
    tableDef.put("name", null);
    tableDef.put("sql", "SELECT 1");
    tables.add(tableDef);

    operand.put("tables", tables);
    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterSqlViewsInSparkSkipsMissingSql() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("type", "view");
    tableDef.put("name", "my_view");
    tableDef.put("sql", null);
    tables.add(tableDef);

    operand.put("tables", tables);
    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterSqlViewsInSparkExecutesView() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    Map<String, Object> operand = new HashMap<>();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("type", "view");
    tableDef.put("name", "my_view");
    tableDef.put("sql", "SELECT 1 AS id");
    tables.add(tableDef);
    operand.put("tables", tables);

    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
    verify(conn, atLeastOnce()).createStatement();
    verify(stmt).execute(argThat(sql -> sql.contains("CREATE OR REPLACE VIEW")
        && sql.contains("SELECT 1 AS id")));
  }

  @Test
  void testRegisterSqlViewsInSparkHandlesSqlException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("View creation failed"));

    Map<String, Object> operand = new HashMap<>();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("type", "VIEW");
    tableDef.put("name", "failing_view");
    tableDef.put("sql", "SELECT BAD SYNTAX");
    tables.add(tableDef);
    operand.put("tables", tables);

    // Should not throw; errors are logged and swallowed
    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
  }

  // ====================================================================
  // Tests for configureS3Credentials (private static method)
  // ====================================================================

  @Test
  void testConfigureS3CredentialsWithNullFileSchema() throws Exception {
    Connection conn = mock(Connection.class);
    invokeConfigureS3Credentials(conn, null);
    verify(conn, never()).createStatement();
  }

  @Test
  void testConfigureS3CredentialsWithNullStorageConfig() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getStorageConfig()).thenReturn(null);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test
  void testConfigureS3CredentialsWithMissingKeys() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", null);
    storageConfig.put("secretAccessKey", null);
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test
  void testConfigureS3CredentialsWithKeysNoEndpoint() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", "AKIAEXAMPLE");
    storageConfig.put("secretAccessKey", "secretkey123");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    invokeConfigureS3Credentials(conn, fileSchema);

    verify(stmt).execute(contains("fs.s3a.impl"));
    verify(stmt).execute(contains("fs.s3a.access.key"));
    verify(stmt).execute(contains("fs.s3a.secret.key"));
    verify(stmt, never()).execute(contains("fs.s3a.endpoint"));
  }

  @Test
  void testConfigureS3CredentialsWithEndpoint() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", "AKIAEXAMPLE");
    storageConfig.put("secretAccessKey", "secretkey123");
    storageConfig.put("endpoint", "http://minio:9000");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    invokeConfigureS3Credentials(conn, fileSchema);

    verify(stmt).execute(contains("fs.s3a.endpoint"));
    verify(stmt).execute(contains("path.style.access"));
  }

  @Test
  void testConfigureS3CredentialsHandlesSqlException() throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.createStatement()).thenThrow(new SQLException("Connection lost"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", "AKIAEXAMPLE");
    storageConfig.put("secretAccessKey", "secretkey123");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    // Should not throw; errors are logged
    invokeConfigureS3Credentials(conn, fileSchema);
  }

  // ====================================================================
  // Tests for configureIcebergCatalog (private static method)
  // ====================================================================

  @Test
  void testConfigureIcebergCatalogAppliesSettings() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    // configureIcebergCatalog uses SparkConfig.toIcebergCatalogSettings()
    // Mock the behavior via the real SparkConfig
    org.apache.calcite.adapter.file.execution.spark.SparkConfig config =
        new org.apache.calcite.adapter.file.execution.spark.SparkConfig();

    invokeConfigureIcebergCatalog(conn, config, "/data/warehouse");
    // Should attempt to execute settings
    verify(conn, atLeastOnce()).createStatement();
  }

  @Test
  void testConfigureIcebergCatalogHandlesSqlException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("Setting not supported"));

    org.apache.calcite.adapter.file.execution.spark.SparkConfig config =
        new org.apache.calcite.adapter.file.execution.spark.SparkConfig();

    // Should not throw; errors are logged per setting
    invokeConfigureIcebergCatalog(conn, config, "/data/warehouse");
  }

  // ====================================================================
  // Tests for registerFilesAsViews edge cases (private static method)
  // ====================================================================

  @Test
  void testRegisterFilesAsViewsWithNullFileSchema() throws Exception {
    Connection conn = mock(Connection.class);
    try {
      invokeRegisterFilesAsViews(conn, "/data", false, "schema", null);
      fail("Expected exception for null fileSchema");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof SQLException,
          "Expected SQLException for null FileSchema");
      assertTrue(e.getCause().getMessage().contains("FileSchema"));
    }
  }

  @Test
  void testRegisterFilesAsViewsWithEmptyRecords() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getAllTableRecords()).thenReturn(Collections.emptyMap());

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterFilesAsViewsSkipsNullTableName() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = null;
    records.put("key1", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterFilesAsViewsSkipsEmptyTableName() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "";
    records.put("key1", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test
  void testRegisterFilesAsViewsHandlesFileNotFoundError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("FILE_NOT_FOUND: path /missing.parquet"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.viewScanPattern = "/data/test.parquet";
    records.put("test_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should not throw; FILE_NOT_FOUND is handled gracefully
    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
  }

  @Test
  void testRegisterFilesAsViewsHandles404Error() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Error 404 Not Found"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table_404";
    record.viewScanPattern = "/data/missing.parquet";
    records.put("table_404", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should not throw; 404 error is an expected error category
    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
  }

  @Test
  void testRegisterFilesAsViewsHandlesNoSuchKeyError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("NoSuchKey: The specified key does not exist."));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table_nosuchkey";
    record.viewScanPattern = "s3://bucket/missing.parquet";
    records.put("table_nosuchkey", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
  }

  @Test
  void testRegisterFilesAsViewsHandlesPathDoesNotExistError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Path does not exist: hdfs:///missing"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table_pathnotexist";
    record.viewScanPattern = "/data/missing.parquet";
    records.put("table_pathnotexist", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
  }

  @Test
  void testRegisterFilesAsViewsHandlesDoesNotExistError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Table or view does not exist: schema.test"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table_doesnotexist";
    record.viewScanPattern = "/data/test.parquet";
    records.put("table_doesnotexist", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
  }

  @Test
  void testRegisterFilesAsViewsHandlesUnexpectedSqlError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Unexpected internal error"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table_unexpected";
    record.viewScanPattern = "/data/test.parquet";
    records.put("table_unexpected", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should not throw; unexpected errors are logged but don't stop processing
    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
  }

  @Test
  void testRegisterFilesAsViewsWithIcebergTableSourceFileNoParquetSuffix() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_table";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "/data/iceberg/my_table";
    records.put("iceberg_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
    // Should register as Iceberg table
    verify(conn, atLeastOnce()).createStatement();
  }

  @Test
  void testRegisterFilesAsViewsWithIcebergTableSourceFileParquetSuffix() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_table2";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "/data/file.parquet";
    records.put("iceberg_table2", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
    verify(conn, atLeastOnce()).createStatement();
  }

  @Test
  void testRegisterFilesAsViewsWithIcebergTableNullSourceFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_table3";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = null;
    records.put("iceberg_table3", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
    verify(conn, atLeastOnce()).createStatement();
  }

  @Test
  void testRegisterFilesAsViewsWithParquetTableNoPath() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    Map<String, ConversionMetadata.ConversionRecord> records = new LinkedHashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "no_path_table";
    record.conversionType = "DIRECT";
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = null;
    records.put("no_path_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should skip view creation when no parquet path found
    invokeRegisterFilesAsViews(conn, "/data", false, "schema", fileSchema);
  }

  @Test
  void testRegisterSqlViewsMultipleViewsOneNonView() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    Map<String, Object> operand = new HashMap<>();
    List<Map<String, Object>> tables = new ArrayList<>();

    // view 1
    Map<String, Object> view1 = new HashMap<>();
    view1.put("type", "view");
    view1.put("name", "v1");
    view1.put("sql", "SELECT 1");
    tables.add(view1);

    // not a view
    Map<String, Object> table1 = new HashMap<>();
    table1.put("type", "table");
    table1.put("name", "t1");
    table1.put("sql", "SELECT 2");
    tables.add(table1);

    // view 2
    Map<String, Object> view2 = new HashMap<>();
    view2.put("type", "view");
    view2.put("name", "v2");
    view2.put("sql", "SELECT 3");
    tables.add(view2);

    operand.put("tables", tables);
    invokeRegisterSqlViewsInSpark(conn, "myschema", operand);
    // Only the 2 views should be executed
    verify(stmt, times(2)).execute(anyString());
  }

  // ====================================================================
  // Helper methods using reflection
  // ====================================================================

  private String invokeResolveParquetPath(ConversionMetadata.ConversionRecord record)
      throws Exception {
    Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "resolveParquetPath", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (String) method.invoke(null, record);
  }

  private void invokeRegisterSqlViewsInSpark(Connection conn, String sparkSchema,
      Map<String, Object> operand) throws Exception {
    Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "registerSqlViewsInSpark", Connection.class, String.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, conn, sparkSchema, operand);
  }

  private void invokeConfigureS3Credentials(Connection conn, FileSchema fileSchema)
      throws Exception {
    Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "configureS3Credentials", Connection.class,
        org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, fileSchema);
  }

  private void invokeConfigureIcebergCatalog(Connection conn,
      org.apache.calcite.adapter.file.execution.spark.SparkConfig config,
      String directoryPath) throws Exception {
    Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "configureIcebergCatalog", Connection.class,
        org.apache.calcite.adapter.file.execution.spark.SparkConfig.class, String.class);
    method.setAccessible(true);
    method.invoke(null, conn, config, directoryPath);
  }

  private void invokeRegisterFilesAsViews(Connection conn, String directoryPath,
      boolean recursive, String sparkSchema, FileSchema fileSchema) throws Exception {
    Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "registerFilesAsViews", Connection.class, String.class,
        boolean.class, String.class, org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, directoryPath, recursive, sparkSchema, fileSchema);
  }
}
