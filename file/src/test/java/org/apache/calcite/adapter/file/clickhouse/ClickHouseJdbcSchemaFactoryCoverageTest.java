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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
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
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link ClickHouseJdbcSchemaFactory}.
 *
 * <p>Tests the factory's configuration parsing, SQL view registration,
 * parquet path resolution, and error handling using mocked JDBC connections.
 * Does NOT require a running ClickHouse server.
 */
@Tag("unit")
class ClickHouseJdbcSchemaFactoryCoverageTest {

  // ---- resolveParquetPath tests (via reflection) ----

  @Test void testResolveParquetPathWithViewScanPattern() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "s3://bucket/data/*.parquet";
    record.parquetCacheFile = "/cache/data.parquet";
    record.sourceFile = "/src/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("s3://bucket/data/*.parquet", result,
        "viewScanPattern should take priority");
  }

  @Test void testResolveParquetPathWithParquetCacheFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.parquetCacheFile = "/cache/data.parquet";
    record.sourceFile = "/src/data.csv";

    String result = invokeResolveParquetPath(record);
    assertEquals("/cache/data.parquet", result,
        "parquetCacheFile should be used when viewScanPattern is null");
  }

  @Test void testResolveParquetPathWithSourceFileParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/src/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/src/data.parquet", result,
        "sourceFile ending in .parquet should be used");
  }

  @Test void testResolveParquetPathWithSourceFileNonParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/src/data.csv";

    String result = invokeResolveParquetPath(record);
    assertNull(result,
        "sourceFile not ending in .parquet and no convertedFile should return null");
  }

  @Test void testResolveParquetPathWithConvertedFileParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/converted/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/converted/data.parquet", result,
        "convertedFile ending in .parquet should be used");
  }

  @Test void testResolveParquetPathWithConvertedFileGlobPattern() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "{s3://bucket/path/*.parquet}";

    String result = invokeResolveParquetPath(record);
    assertEquals("{s3://bucket/path/*.parquet}", result,
        "convertedFile wrapped in braces should be treated as glob pattern");
  }

  @Test void testResolveParquetPathWithConvertedFileNonParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/converted/data.json";

    String result = invokeResolveParquetPath(record);
    assertNull(result,
        "Non-parquet convertedFile without braces should return null");
  }

  @Test void testResolveParquetPathAllNull() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();

    String result = invokeResolveParquetPath(record);
    assertNull(result, "All null fields should return null");
  }

  // ---- registerSqlViewsInClickHouse tests (via reflection) ----

  @Test void testRegisterSqlViewsNullOperand() throws Exception {
    Connection conn = mock(Connection.class);
    invokeRegisterSqlViews(conn, "testSchema", null);
    // Should not throw
  }

  @Test void testRegisterSqlViewsNoTables() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    invokeRegisterSqlViews(conn, "testSchema", operand);
    // Should not throw
  }

  @Test void testRegisterSqlViewsEmptyTables() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", new ArrayList<Map<String, Object>>());

    invokeRegisterSqlViews(conn, "testSchema", operand);
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

    invokeRegisterSqlViews(conn, "testSchema", operand);
    // Verify the SQL was executed
    org.mockito.Mockito.verify(stmt).execute(
        "CREATE OR REPLACE VIEW \"testSchema\".\"my_view\" AS SELECT 1");
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

    invokeRegisterSqlViews(conn, "testSchema", operand);
    // Should not create any statements since type is not "view"
    org.mockito.Mockito.verify(conn, org.mockito.Mockito.never()).createStatement();
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

    invokeRegisterSqlViews(conn, "testSchema", operand);
    // Neither should create a statement
    org.mockito.Mockito.verify(conn, org.mockito.Mockito.never()).createStatement();
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

    // Should not throw - errors are logged as warnings
    invokeRegisterSqlViews(conn, "testSchema", operand);
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
    // Should not throw, but no SQL should be executed
    org.mockito.Mockito.verify(conn, org.mockito.Mockito.never()).createStatement();
  }

  @Test void testConfigureS3CredentialsWithKeys() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accessKeyId", "AKID");
    storageConfig.put("secretAccessKey", "SECRET");
    storageConfig.put("region", "us-east-1");
    storageConfig.put("endpoint", "http://minio:9000");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    invokeConfigureS3Credentials(conn, fileSchema);
    org.mockito.Mockito.verify(stmt).execute(org.mockito.ArgumentMatchers.contains("access_key_id"));
  }

  @Test void testConfigureS3CredentialsHandlesSQLException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("Named collection not supported"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accessKeyId", "AKID");
    storageConfig.put("secretAccessKey", "SECRET");
    when(fileSchema.getStorageConfig()).thenReturn(storageConfig);

    // Should not throw - falls back to inline credentials
    invokeConfigureS3Credentials(conn, fileSchema);
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
    // Should complete without error when no records exist
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
    // Should skip the record with null tableName and not crash
  }

  // ---- createClickHouseDialect tests (via reflection) ----

  @Test void testCreateClickHouseDialect() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("createClickHouseDialect");
    method.setAccessible(true);
    Object dialect = method.invoke(null);

    assertNotNull(dialect, "Dialect should not be null");
    assertTrue(dialect instanceof org.apache.calcite.sql.SqlDialect,
        "Should return a SqlDialect");

    org.apache.calcite.sql.SqlDialect sqlDialect = (org.apache.calcite.sql.SqlDialect) dialect;

    // Test supportsAggregateFunction (overridden to return true for all)
    assertTrue(sqlDialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.COUNT));
    assertTrue(sqlDialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.SUM));
    assertTrue(sqlDialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.AVG));
  }

  // ---- create() entry point test ----

  @Test void testCreateMethodFailsWithoutDriver() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "testSchema", "/data", false, null, operand);
      fail("Should throw RuntimeException when ClickHouse driver is not available");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to create ClickHouse JDBC schema")
              || e.getMessage().contains("ClickHouseDriver"),
          "Should fail due to missing ClickHouse driver: " + e.getMessage());
    }
  }

  // ---- findFreePort test (via reflection) ----

  @Test void testFindFreePort() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("findFreePort");
    method.setAccessible(true);
    int port = (Integer) method.invoke(null);

    assertTrue(port > 0, "Port should be positive");
    assertTrue(port <= 65535, "Port should be within valid range");
  }

  // ---- Helper methods for reflective invocation ----

  private String invokeResolveParquetPath(ConversionMetadata.ConversionRecord record)
      throws Exception {
    java.lang.reflect.Method method = ClickHouseJdbcSchemaFactory.class.getDeclaredMethod(
        "resolveParquetPath", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (String) method.invoke(null, record);
  }

  private void invokeRegisterSqlViews(Connection conn, String schema, Map<String, Object> operand)
      throws Exception {
    java.lang.reflect.Method method = ClickHouseJdbcSchemaFactory.class.getDeclaredMethod(
        "registerSqlViewsInClickHouse", Connection.class, String.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, conn, schema, operand);
  }

  private void invokeConfigureS3Credentials(Connection conn, FileSchema fileSchema)
      throws Exception {
    java.lang.reflect.Method method = ClickHouseJdbcSchemaFactory.class.getDeclaredMethod(
        "configureS3Credentials", Connection.class,
        org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, fileSchema);
  }

  private void invokeRegisterFilesAsViews(Connection conn, String directoryPath,
      boolean recursive, String clickhouseSchema, FileSchema fileSchema) throws Exception {
    java.lang.reflect.Method method = ClickHouseJdbcSchemaFactory.class.getDeclaredMethod(
        "registerFilesAsViews", Connection.class, String.class, boolean.class,
        String.class, org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, directoryPath, recursive, clickhouseSchema, fileSchema);
  }
}
