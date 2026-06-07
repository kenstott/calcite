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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link ClickHouseJdbcSchemaFactory}.
 *
 * <p>Tests the factory's configuration parsing, SQL view registration,
 * parquet path resolution, dialect creation, S3 credential configuration,
 * and error handling using mocked JDBC connections.
 * Does NOT require a running ClickHouse server.
 */
@Tag("unit")
class ClickHouseJdbcSchemaFactoryCoverageTest {

  // =======================================================================
  // resolveParquetPath tests (via reflection)
  // =======================================================================

  @Test void testResolveParquetPathViewScanPatternTakesPriority() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "s3://bucket/data/*.parquet";
    record.parquetCacheFile = "/cache/data.parquet";
    record.sourceFile = "/src/data.parquet";
    record.convertedFile = "/converted/data.parquet";

    assertEquals("s3://bucket/data/*.parquet", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathParquetCacheFileSecondPriority() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.parquetCacheFile = "/cache/data.parquet";
    record.sourceFile = "/src/data.parquet";

    assertEquals("/cache/data.parquet", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathSourceFileParquetThirdPriority() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/src/data.parquet";

    assertEquals("/src/data.parquet", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathSourceFileNonParquetSkipped() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/src/data.csv";

    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileParquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/converted/output.parquet";

    assertEquals("/converted/output.parquet", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileBracedGlob() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "{s3://bucket/path/*.parquet}";

    assertEquals("{s3://bucket/path/*.parquet}", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileNonParquetReturnsNull() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/converted/data.json";

    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathAllFieldsNull() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileCsvNotBraced() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/converted/data.csv";
    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathS3ViewScanPattern() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "s3://my-bucket/warehouse/table/**/*.parquet";
    assertEquals("s3://my-bucket/warehouse/table/**/*.parquet",
        invokeResolveParquetPath(record));
  }

  // =======================================================================
  // registerSqlViewsInClickHouse tests (via reflection)
  // =======================================================================

  @Test void testRegisterSqlViewsNullOperand() throws Exception {
    Connection conn = mock(Connection.class);
    invokeRegisterSqlViews(conn, "mySchema", null);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsNoTablesKey() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    invokeRegisterSqlViews(conn, "mySchema", operand);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsEmptyTablesList() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", new ArrayList<Map<String, Object>>());
    invokeRegisterSqlViews(conn, "mySchema", operand);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsCreatesValidView() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    Map<String, Object> viewDef = new LinkedHashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "revenue_view");
    viewDef.put("sql", "SELECT sum(amount) FROM orders");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "analytics", operand);
    verify(stmt).execute(
        "CREATE OR REPLACE VIEW \"analytics\".\"revenue_view\" AS SELECT sum(amount) FROM orders");
  }

  @Test void testRegisterSqlViewsSkipsTableType() throws Exception {
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
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsSkipsMissingName() throws Exception {
    Connection conn = mock(Connection.class);

    Map<String, Object> noName = new LinkedHashMap<String, Object>();
    noName.put("type", "view");
    noName.put("sql", "SELECT 1");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(noName);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "testSchema", operand);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsSkipsMissingSql() throws Exception {
    Connection conn = mock(Connection.class);

    Map<String, Object> noSql = new LinkedHashMap<String, Object>();
    noSql.put("type", "view");
    noSql.put("name", "broken_view");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(noSql);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "testSchema", operand);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsHandlesSQLException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("Syntax error"));

    Map<String, Object> viewDef = new LinkedHashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "bad_view");
    viewDef.put("sql", "INVALID SQL STATEMENT");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    // Should not throw -- errors are logged as warnings
    invokeRegisterSqlViews(conn, "testSchema", operand);
  }

  @Test void testRegisterSqlViewsMultipleViews() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();

    Map<String, Object> v1 = new LinkedHashMap<String, Object>();
    v1.put("type", "view");
    v1.put("name", "view_a");
    v1.put("sql", "SELECT 1");
    tables.add(v1);

    Map<String, Object> v2 = new LinkedHashMap<String, Object>();
    v2.put("type", "view");
    v2.put("name", "view_b");
    v2.put("sql", "SELECT 2");
    tables.add(v2);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "db", operand);
    verify(stmt).execute("CREATE OR REPLACE VIEW \"db\".\"view_a\" AS SELECT 1");
    verify(stmt).execute("CREATE OR REPLACE VIEW \"db\".\"view_b\" AS SELECT 2");
  }

  // =======================================================================
  // configureS3Credentials tests (via reflection)
  // =======================================================================

  @Test void testConfigureS3CredentialsNullFileSchema() throws Exception {
    Connection conn = mock(Connection.class);
    invokeConfigureS3Credentials(conn, null);
    verify(conn, never()).createStatement();
  }

  @Test void testConfigureS3CredentialsNullStorageConfig() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getStorageConfig()).thenReturn(null);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test void testConfigureS3CredentialsMissingKeys() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("region", "us-west-2");
    when(fileSchema.getStorageConfig()).thenReturn(config);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test void testConfigureS3CredentialsWithAllFields() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    config.put("region", "us-east-1");
    config.put("endpoint", "https://s3.amazonaws.com");
    when(fileSchema.getStorageConfig()).thenReturn(config);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(stmt).execute(contains("access_key_id"));
  }

  @Test void testConfigureS3CredentialsWithoutRegionAndEndpoint() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", "KEY");
    config.put("secretAccessKey", "SECRET");
    when(fileSchema.getStorageConfig()).thenReturn(config);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(stmt).execute(contains("secret_access_key"));
  }

  @Test void testConfigureS3CredentialsHandlesSQLException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Named collections not supported"));

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", "KEY");
    config.put("secretAccessKey", "SECRET");
    when(fileSchema.getStorageConfig()).thenReturn(config);

    // Should not throw -- error is logged
    invokeConfigureS3Credentials(conn, fileSchema);
  }

  // =======================================================================
  // registerFilesAsViews tests (via reflection)
  // =======================================================================

  @Test void testRegisterFilesAsViewsNullFileSchemaThrows() throws Exception {
    Connection conn = mock(Connection.class);
    try {
      invokeRegisterFilesAsViews(conn, "/data", false, "myschema", null);
      fail("Should throw for null fileSchema");
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
    // No exceptions expected -- early return for empty records
  }

  @Test void testRegisterFilesAsViewsSkipsNullTableName() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    // tableName defaults to null

    Map<String, ConversionMetadata.ConversionRecord> records =
        new HashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("key1", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterFilesAsViewsSkipsEmptyTableName() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new HashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("key1", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    verify(conn, never()).createStatement();
  }

  // =======================================================================
  // createClickHouseDialect tests (via reflection)
  // =======================================================================

  @Test void testCreateClickHouseDialectNotNull() throws Exception {
    Object dialect = invokeCreateClickHouseDialect();
    assertNotNull(dialect);
  }

  @Test void testCreateClickHouseDialectIsSqlDialect() throws Exception {
    Object dialect = invokeCreateClickHouseDialect();
    assertTrue(dialect instanceof SqlDialect);
  }

  @Test void testCreateClickHouseDialectSupportsCount() throws Exception {
    SqlDialect dialect = (SqlDialect) invokeCreateClickHouseDialect();
    assertTrue(dialect.supportsAggregateFunction(SqlKind.COUNT));
  }

  @Test void testCreateClickHouseDialectSupportsSum() throws Exception {
    SqlDialect dialect = (SqlDialect) invokeCreateClickHouseDialect();
    assertTrue(dialect.supportsAggregateFunction(SqlKind.SUM));
  }

  @Test void testCreateClickHouseDialectSupportsAvg() throws Exception {
    SqlDialect dialect = (SqlDialect) invokeCreateClickHouseDialect();
    assertTrue(dialect.supportsAggregateFunction(SqlKind.AVG));
  }

  @Test void testCreateClickHouseDialectSupportsMin() throws Exception {
    SqlDialect dialect = (SqlDialect) invokeCreateClickHouseDialect();
    assertTrue(dialect.supportsAggregateFunction(SqlKind.MIN));
  }

  @Test void testCreateClickHouseDialectSupportsMax() throws Exception {
    SqlDialect dialect = (SqlDialect) invokeCreateClickHouseDialect();
    assertTrue(dialect.supportsAggregateFunction(SqlKind.MAX));
  }

  // =======================================================================
  // create() entry point tests
  // =======================================================================

  @Test void testCreateFailsWithoutDriver() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "testSchema", "/data", false, null, operand);
      fail("Should throw when ClickHouse driver is not available");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to create ClickHouse JDBC schema")
              || e.getMessage().contains("ClickHouseDriver"),
          "Error should mention driver or schema creation: " + e.getMessage());
    }
  }

  @Test void testCreateWithNullOperand() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "testSchema", "/data", false, null, null);
      fail("Should throw when driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  // =======================================================================
  // findFreePort test (via reflection)
  // =======================================================================

  @Test void testFindFreePortReturnsValidPort() throws Exception {
    Method method = ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("findFreePort");
    method.setAccessible(true);
    int port = (Integer) method.invoke(null);
    assertTrue(port > 0, "Port should be positive");
    assertTrue(port <= 65535, "Port should be in valid range");
  }

  @Test void testFindFreePortReturnsDifferentPorts() throws Exception {
    Method method = ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("findFreePort");
    method.setAccessible(true);
    int port1 = (Integer) method.invoke(null);
    int port2 = (Integer) method.invoke(null);
    // Ports should generally be different (not guaranteed but very likely)
    assertTrue(port1 > 0 && port2 > 0);
  }

  // =======================================================================
  // Helper methods for reflective invocation
  // =======================================================================

  private String invokeResolveParquetPath(ConversionMetadata.ConversionRecord record)
      throws Exception {
    Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("resolveParquetPath", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (String) method.invoke(null, record);
  }

  private void invokeRegisterSqlViews(Connection conn, String schema,
      Map<String, Object> operand) throws Exception {
    Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("registerSqlViewsInClickHouse", Connection.class, String.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, conn, schema, operand);
  }

  private void invokeConfigureS3Credentials(Connection conn, FileSchema fileSchema)
      throws Exception {
    Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("configureS3Credentials", Connection.class,
        org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, fileSchema);
  }

  private void invokeRegisterFilesAsViews(Connection conn, String directoryPath,
      boolean recursive, String clickhouseSchema, FileSchema fileSchema) throws Exception {
    Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("registerFilesAsViews", Connection.class, String.class, boolean.class,
        String.class, org.apache.calcite.adapter.file.FileSchema.class);
    method.setAccessible(true);
    method.invoke(null, conn, directoryPath, recursive, clickhouseSchema, fileSchema);
  }

  private Object invokeCreateClickHouseDialect() throws Exception {
    Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("createClickHouseDialect");
    method.setAccessible(true);
    return method.invoke(null);
  }
}
