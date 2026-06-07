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
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link SparkJdbcSchemaFactory}.
 *
 * <p>Tests the factory's configuration parsing, SQL view registration,
 * parquet path resolution, S3 credential configuration, and Iceberg catalog
 * setup using mocked JDBC connections. Does NOT require a running Spark server.
 */
@Tag("unit")
class SparkJdbcSchemaFactoryCoverageTest {

  // ---- resolveParquetPath tests (via reflection) ----

  @Test void testResolveParquetPathWithViewScanPattern() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "s3://bucket/data/*.parquet";
    record.parquetCacheFile = "/cache/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("s3://bucket/data/*.parquet", result,
        "viewScanPattern should take priority");
  }

  @Test void testResolveParquetPathWithParquetCacheFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.parquetCacheFile = "/cache/data.parquet";
    record.sourceFile = "/src/data.csv";

    String result = invokeResolveParquetPath(record);
    assertEquals("/cache/data.parquet", result);
  }

  @Test void testResolveParquetPathWithSourceFileParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/src/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/src/data.parquet", result);
  }

  @Test void testResolveParquetPathWithSourceFileNonParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/src/data.csv";

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  @Test void testResolveParquetPathWithConvertedFileParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/converted/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/converted/data.parquet", result);
  }

  @Test void testResolveParquetPathWithConvertedFileGlobPattern() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "{s3://bucket/*.parquet}";

    String result = invokeResolveParquetPath(record);
    assertEquals("{s3://bucket/*.parquet}", result);
  }

  @Test void testResolveParquetPathWithConvertedFileNonParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/converted/data.json";

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  @Test void testResolveParquetPathAllNull() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  // ---- registerSqlViewsInSpark tests (via reflection) ----

  @Test void testRegisterSqlViewsNullOperand() throws Exception {
    Connection conn = mock(Connection.class);
    invokeRegisterSqlViews(conn, "testschema", null);
    // Should not throw
  }

  @Test void testRegisterSqlViewsNoTables() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    invokeRegisterSqlViews(conn, "testschema", operand);
    // Should not throw
  }

  @Test void testRegisterSqlViewsEmptyTables() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", new ArrayList<Map<String, Object>>());
    invokeRegisterSqlViews(conn, "testschema", operand);
    // Should not throw
  }

  @Test void testRegisterSqlViewsWithValidView() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    Map<String, Object> viewDef = new LinkedHashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "my_view");
    viewDef.put("sql", "SELECT 1");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "testschema", operand);
    verify(stmt).execute(org.mockito.ArgumentMatchers.contains("my_view"));
  }

  @Test void testRegisterSqlViewsSkipsNonViewType() throws Exception {
    Connection conn = mock(Connection.class);

    Map<String, Object> tableDef = new LinkedHashMap<String, Object>();
    tableDef.put("type", "table");
    tableDef.put("name", "my_table");
    tableDef.put("sql", "SELECT 1");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(tableDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "testschema", operand);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsSkipsMissingNameOrSql() throws Exception {
    Connection conn = mock(Connection.class);

    Map<String, Object> noName = new LinkedHashMap<String, Object>();
    noName.put("type", "view");
    noName.put("sql", "SELECT 1");

    Map<String, Object> noSql = new LinkedHashMap<String, Object>();
    noSql.put("type", "view");
    noSql.put("name", "my_view");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(noName);
    tables.add(noSql);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "testschema", operand);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsHandlesSQLException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("View creation failed"));

    Map<String, Object> viewDef = new LinkedHashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "bad_view");
    viewDef.put("sql", "INVALID SQL");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    // Should not throw
    invokeRegisterSqlViews(conn, "testschema", operand);
  }

  // ---- configureS3Credentials tests (via reflection) ----

  @Test void testConfigureS3CredentialsNullFileSchema() throws Exception {
    Connection conn = mock(Connection.class);
    invokeConfigureS3Credentials(conn, null);
    // Should not throw
  }

  @Test void testConfigureS3CredentialsNullStorageConfig() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getStorageConfig()).thenReturn(null);

    invokeConfigureS3Credentials(conn, fileSchema);
    // Should not throw
  }

  @Test void testConfigureS3CredentialsNoKeys() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test void testConfigureS3CredentialsWithKeys() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accessKeyId", "AKID");
    storageConfig.put("secretAccessKey", "SECRET");
    storageConfig.put("endpoint", "http://minio:9000");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    invokeConfigureS3Credentials(conn, fileSchema);
    // Should have SET statements for S3 credentials
    verify(stmt).execute(org.mockito.ArgumentMatchers.contains("fs.s3a.access.key"));
    verify(stmt).execute(org.mockito.ArgumentMatchers.contains("fs.s3a.secret.key"));
    verify(stmt).execute(org.mockito.ArgumentMatchers.contains("fs.s3a.endpoint"));
  }

  @Test void testConfigureS3CredentialsWithoutEndpoint() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accessKeyId", "AKID");
    storageConfig.put("secretAccessKey", "SECRET");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(stmt).execute(org.mockito.ArgumentMatchers.contains("fs.s3a.access.key"));
    // Should not set endpoint
    verify(stmt, never()).execute(org.mockito.ArgumentMatchers.contains("fs.s3a.endpoint"));
  }

  @Test void testConfigureS3CredentialsHandlesSQLException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("SET failed"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accessKeyId", "AKID");
    storageConfig.put("secretAccessKey", "SECRET");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    // Should not throw
    invokeConfigureS3Credentials(conn, fileSchema);
  }

  // ---- configureIcebergCatalog tests (via reflection) ----

  @Test void testConfigureIcebergCatalog() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    invokeConfigureIcebergCatalog(conn, "/data/warehouse");
    // Should execute multiple SET statements for Iceberg catalog
    verify(stmt, org.mockito.Mockito.atLeastOnce()).execute(anyString());
  }

  @Test void testConfigureIcebergCatalogHandlesException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("SET not supported"));

    // Should not throw - errors are logged
    invokeConfigureIcebergCatalog(conn, "/data/warehouse");
  }

  // ---- registerFilesAsViews error paths (via reflection) ----

  @Test void testRegisterFilesAsViewsNullFileSchema() throws Exception {
    Connection conn = mock(Connection.class);

    try {
      invokeRegisterFilesAsViews(conn, "/data", false, "myschema", null);
      fail("Should throw SQLException for null fileSchema");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof SQLException);
      assertTrue(e.getCause().getMessage().contains("FileSchema"));
    }
  }

  @Test void testRegisterFilesAsViewsEmptyRecords() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getAllTableRecords())
        .thenReturn(Collections.<String, ConversionMetadata.ConversionRecord>emptyMap());

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    // Should complete without error
  }

  @Test void testRegisterFilesAsViewsSkipsNullTableName() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    // tableName is null by default

    Map<String, ConversionMetadata.ConversionRecord> records =
        new HashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("key1", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    // Should skip the record with null tableName
  }

  // ---- createSparkDialect tests (via reflection) ----

  @Test void testCreateSparkDialect() throws Exception {
    java.lang.reflect.Method method =
        SparkJdbcSchemaFactory.class.getDeclaredMethod("createSparkDialect");
    method.setAccessible(true);
    Object dialect = method.invoke(null);

    assertNotNull(dialect);
    assertTrue(dialect instanceof org.apache.calcite.sql.SqlDialect);

    org.apache.calcite.sql.SqlDialect sqlDialect = (org.apache.calcite.sql.SqlDialect) dialect;
    assertTrue(sqlDialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.COUNT));
    assertTrue(sqlDialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.SUM));
    assertTrue(sqlDialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.MIN));
    assertTrue(sqlDialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.MAX));
  }

  // ---- create() entry point test ----

  @Test void testCreateMethodFailsWithoutDriver() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    try {
      SparkJdbcSchemaFactory.create(
          parentSchema, "testSchema", "/data", false, null, operand);
      fail("Should throw RuntimeException when Hive driver is not available");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to create Spark JDBC schema")
              || e.getMessage().contains("HiveDriver"),
          "Should fail due to missing Hive driver: " + e.getMessage());
    }
  }

  // ---- Helper methods ----

  private String invokeResolveParquetPath(ConversionMetadata.ConversionRecord record)
      throws Exception {
    java.lang.reflect.Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "resolveParquetPath", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (String) method.invoke(null, record);
  }

  private void invokeRegisterSqlViews(Connection conn, String schema, Map<String, Object> operand)
      throws Exception {
    java.lang.reflect.Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "registerSqlViewsInSpark", Connection.class, String.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, conn, schema, operand);
  }

  private void invokeConfigureS3Credentials(Connection conn, FileSchema fileSchema)
      throws Exception {
    java.lang.reflect.Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "configureS3Credentials", Connection.class,
        org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, fileSchema);
  }

  private void invokeConfigureIcebergCatalog(Connection conn, String directoryPath)
      throws Exception {
    java.lang.reflect.Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "configureIcebergCatalog", Connection.class,
        org.apache.calcite.adapter.file.execution.spark.SparkConfig.class,
        String.class);
    method.setAccessible(true);

    org.apache.calcite.adapter.file.execution.spark.SparkConfig config =
        new org.apache.calcite.adapter.file.execution.spark.SparkConfig();
    method.invoke(null, conn, config, directoryPath);
  }

  private void invokeRegisterFilesAsViews(Connection conn, String directoryPath,
      boolean recursive, String sparkSchema, FileSchema fileSchema) throws Exception {
    java.lang.reflect.Method method = SparkJdbcSchemaFactory.class.getDeclaredMethod(
        "registerFilesAsViews", Connection.class, String.class, boolean.class,
        String.class, org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, directoryPath, recursive, sparkSchema, fileSchema);
  }
}
