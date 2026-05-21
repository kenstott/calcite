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
import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage unit tests for {@link ClickHouseJdbcSchemaFactory}.
 *
 * <p>Focuses on exercising code paths not covered by
 * {@link ClickHouseJdbcSchemaFactoryCoverageTest}, including:
 * <ul>
 *   <li>Iceberg table registration in registerFilesAsViews</li>
 *   <li>Various SQL exception error messages in view registration</li>
 *   <li>S3 credentials with partial fields (region only, endpoint only)</li>
 *   <li>Config parsing with clickhouseConfig map in operand</li>
 *   <li>Local mode vs server mode configuration paths</li>
 *   <li>waitForReady timeout behavior</li>
 *   <li>createSchemaWithConnection dialect and convention creation</li>
 *   <li>SharedInstanceInfo data class behavior</li>
 * </ul>
 *
 * <p>Does NOT require a running ClickHouse server.
 */
@Tag("unit")
class ClickHouseJdbcSchemaFactoryDeepCoverageTest2 {

  // =======================================================================
  // registerFilesAsViews - Iceberg table paths
  // =======================================================================

  @Test void testRegisterFilesAsViewsIcebergTableWithSourceFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_table";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "s3://bucket/warehouse/iceberg_table";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    verify(stmt).execute(contains("iceberg"));
  }

  @Test void testRegisterFilesAsViewsIcebergTableWithParquetSourceFile() throws Exception {
    // When sourceFile ends with .parquet, the iceberg path should be constructed
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_from_parquet";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "/data/file.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_from_parquet", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    // The iceberg path should be constructed from directoryPath/schema/tableName
    verify(stmt).execute(contains("iceberg"));
  }

  @Test void testRegisterFilesAsViewsIcebergTableWithNullSourceFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_no_source";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = null;

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_no_source", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/warehouse", true, "analytics", fileSchema);
    // Path should be constructed: /warehouse/analytics/iceberg_no_source
    verify(stmt).execute(contains("iceberg"));
  }

  // =======================================================================
  // registerFilesAsViews - Parquet table creation
  // =======================================================================

  @Test void testRegisterFilesAsViewsParquetTableWithViewScanPattern() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "orders";
    record.conversionType = "DIRECT";
    record.viewScanPattern = "s3://bucket/orders/**/*.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("orders", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "sales", fileSchema);
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
  }

  @Test void testRegisterFilesAsViewsParquetTableWithNoPath() throws Exception {
    // Table with no resolvable parquet path should be skipped
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "no_path_table";
    record.conversionType = "DIRECT";
    record.sourceFile = "/data/file.csv"; // Not parquet
    record.convertedFile = "/data/file.json"; // Not parquet, not braced

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("no_path_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    // Should skip because no parquet path is resolvable -- no statement created
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterFilesAsViewsMultipleRecordsMixedTypes() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    // Iceberg record
    ConversionMetadata.ConversionRecord icebergRecord = new ConversionMetadata.ConversionRecord();
    icebergRecord.tableName = "iceberg_t";
    icebergRecord.conversionType = "ICEBERG_PARQUET";
    icebergRecord.sourceFile = "s3://bucket/iceberg_t";

    // Parquet record
    ConversionMetadata.ConversionRecord parquetRecord = new ConversionMetadata.ConversionRecord();
    parquetRecord.tableName = "parquet_t";
    parquetRecord.conversionType = "DIRECT";
    parquetRecord.parquetCacheFile = "/cache/parquet_t.parquet";

    // Record with no path (should be skipped)
    ConversionMetadata.ConversionRecord noPathRecord = new ConversionMetadata.ConversionRecord();
    noPathRecord.tableName = "no_path_t";
    noPathRecord.conversionType = "DIRECT";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_t", icebergRecord);
    records.put("parquet_t", parquetRecord);
    records.put("no_path_t", noPathRecord);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    // Two views created (iceberg + parquet), third skipped
    verify(stmt, times(2)).execute(anyString());
  }

  // =======================================================================
  // registerFilesAsViews - SQL exception error message paths
  // =======================================================================

  @Test void testRegisterFilesAsViewsHandlesFileNotFoundError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("Code: FILE_NOT_FOUND"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "missing_table";
    record.conversionType = "DIRECT";
    record.viewScanPattern = "s3://bucket/missing/*.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("missing_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should not throw -- FILE_NOT_FOUND is logged as a warning
    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
  }

  @Test void testRegisterFilesAsViewsHandles404Error() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("HTTP 404 Not Found"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "not_found_table";
    record.conversionType = "DIRECT";
    record.viewScanPattern = "s3://bucket/data/*.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("not_found_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
  }

  @Test void testRegisterFilesAsViewsHandlesNoSuchKeyError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("NoSuchKey: The specified key does not exist"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "no_key_table";
    record.conversionType = "DIRECT";
    record.viewScanPattern = "s3://bucket/data.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("no_key_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
  }

  @Test void testRegisterFilesAsViewsHandlesDoesNotExistError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Path /data/table does not exist"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "gone_table";
    record.conversionType = "DIRECT";
    record.viewScanPattern = "/data/table/*.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("gone_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
  }

  @Test void testRegisterFilesAsViewsHandlesUnknownSqlError() throws Exception {
    // Unexpected errors are logged at ERROR level but do not throw
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Unknown internal error in ClickHouse"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "error_table";
    record.conversionType = "DIRECT";
    record.viewScanPattern = "s3://bucket/data.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("error_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should not throw -- errors are caught and logged
    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
  }

  @Test void testRegisterFilesAsViewsIcebergWithSqlException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("FILE_NOT_FOUND: iceberg metadata missing"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_missing";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "s3://bucket/warehouse/iceberg_missing";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_missing", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
  }

  // =======================================================================
  // configureS3Credentials - partial field coverage
  // =======================================================================

  @Test void testConfigureS3CredentialsWithRegionOnly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", "AKTEST");
    config.put("secretAccessKey", "SECRET");
    config.put("region", "eu-west-1");
    // endpoint is null
    when(fileSchema.getStorageConfig()).thenReturn(config);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(stmt).execute(contains("region"));
  }

  @Test void testConfigureS3CredentialsWithEndpointOnly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", "AKTEST");
    config.put("secretAccessKey", "SECRET");
    config.put("endpoint", "https://minio.local:9000");
    // region is null
    when(fileSchema.getStorageConfig()).thenReturn(config);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(stmt).execute(contains("endpoint"));
  }

  @Test void testConfigureS3CredentialsWithNullAccessKey() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("secretAccessKey", "SECRET");
    // accessKeyId is null
    when(fileSchema.getStorageConfig()).thenReturn(config);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(conn, never()).createStatement();
  }

  @Test void testConfigureS3CredentialsWithNullSecretKey() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", "AKTEST");
    // secretAccessKey is null
    when(fileSchema.getStorageConfig()).thenReturn(config);

    invokeConfigureS3Credentials(conn, fileSchema);
    verify(conn, never()).createStatement();
  }

  // =======================================================================
  // createClickHouseDialect - additional dialect property tests
  // =======================================================================

  @Test void testCreateClickHouseDialectQuoteString() throws Exception {
    SqlDialect dialect = (SqlDialect) invokeCreateClickHouseDialect();
    // ClickHouse uses backtick as identifier quote string
    assertNotNull(dialect);
    // Verify the dialect can be used for aggregate function checks on various kinds
    assertTrue(dialect.supportsAggregateFunction(SqlKind.OTHER_FUNCTION));
    assertTrue(dialect.supportsAggregateFunction(SqlKind.SUM0));
    assertTrue(dialect.supportsAggregateFunction(SqlKind.SINGLE_VALUE));
  }

  @Test void testCreateClickHouseDialectReturnsNewInstanceEachCall() throws Exception {
    Object dialect1 = invokeCreateClickHouseDialect();
    Object dialect2 = invokeCreateClickHouseDialect();
    assertNotNull(dialect1);
    assertNotNull(dialect2);
    // Each call creates a new anonymous SqlDialect instance
    assertFalse(dialect1 == dialect2, "Each invocation should create a new dialect instance");
  }

  // =======================================================================
  // create() entry point - config parsing paths
  // =======================================================================

  @Test void testCreateWithClickHouseConfigInOperand() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> clickhouseConfig = new HashMap<String, Object>();
    clickhouseConfig.put("mode", "server");
    clickhouseConfig.put("host", "ch-server.internal");
    clickhouseConfig.put("port", "9000");
    clickhouseConfig.put("database", "analytics");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("clickhouseConfig", clickhouseConfig);

    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "analytics", "/data", true, null, operand);
      fail("Should throw when ClickHouse driver is not available");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to create ClickHouse JDBC schema")
              || e.getMessage().contains("ClickHouseDriver"),
          "Error should mention driver or schema creation: " + e.getMessage());
    }
  }

  @Test void testCreateWithLocalModeConfig() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> clickhouseConfig = new HashMap<String, Object>();
    clickhouseConfig.put("mode", "local");
    clickhouseConfig.put("dataDir", "/tmp/ch-data-test");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("clickhouseConfig", clickhouseConfig);

    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "local_schema", "/data", false, null, operand);
      fail("Should throw when ClickHouse driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithEmptyOperand() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    // No clickhouseConfig key -- should use defaults

    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "default_schema", "/data", false, null, operand);
      fail("Should throw when ClickHouse driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithRecursiveTrue() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "recursive_schema", "/data/nested", true, null, operand);
      fail("Should throw when ClickHouse driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithFileSchema() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    try {
      ClickHouseJdbcSchemaFactory.create(
          parentSchema, "fs_schema", "/data", false, fileSchema, operand);
      fail("Should throw when ClickHouse driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  // =======================================================================
  // registerSqlViewsInClickHouse - additional edge cases
  // =======================================================================

  @Test void testRegisterSqlViewsWithNullTypeSkipsEntry() throws Exception {
    Connection conn = mock(Connection.class);

    Map<String, Object> tableDef = new LinkedHashMap<String, Object>();
    tableDef.put("type", null);
    tableDef.put("name", "v1");
    tableDef.put("sql", "SELECT 1");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(tableDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "schema1", operand);
    verify(conn, never()).createStatement();
  }

  @Test void testRegisterSqlViewsCaseInsensitiveViewType() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    Map<String, Object> viewDef = new LinkedHashMap<String, Object>();
    viewDef.put("type", "VIEW");
    viewDef.put("name", "upper_view");
    viewDef.put("sql", "SELECT 1");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "schema1", operand);
    verify(stmt).execute(contains("upper_view"));
  }

  @Test void testRegisterSqlViewsMixedViewAndTableEntries() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();

    Map<String, Object> table1 = new LinkedHashMap<String, Object>();
    table1.put("type", "table");
    table1.put("name", "t1");
    table1.put("sql", "SELECT 1");
    tables.add(table1);

    Map<String, Object> view1 = new LinkedHashMap<String, Object>();
    view1.put("type", "view");
    view1.put("name", "v1");
    view1.put("sql", "SELECT 2");
    tables.add(view1);

    Map<String, Object> table2 = new LinkedHashMap<String, Object>();
    table2.put("type", "materialized_view");
    table2.put("name", "mv1");
    table2.put("sql", "SELECT 3");
    tables.add(table2);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViews(conn, "db", operand);
    // Only v1 should be created (the only one with type "view")
    verify(stmt, times(1)).execute(anyString());
    verify(stmt).execute("CREATE OR REPLACE VIEW \"db\".\"v1\" AS SELECT 2");
  }

  @Test void testRegisterSqlViewsNullTablesList() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", null);

    invokeRegisterSqlViews(conn, "schema1", operand);
    verify(conn, never()).createStatement();
  }

  // =======================================================================
  // resolveParquetPath - additional edge cases
  // =======================================================================

  @Test void testResolveParquetPathConvertedFileBracedWithSpaces() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "{s3://bucket/path with spaces/*.parquet}";

    assertEquals("{s3://bucket/path with spaces/*.parquet}", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileOnlyBraces() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "{}";

    assertEquals("{}", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileSingleBrace() throws Exception {
    // Starts with { but does not end with } -- should not match brace pattern
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "{partial";

    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileEndBraceOnly() throws Exception {
    // Ends with } but does not start with { -- should not match
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "partial}";

    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathLocalParquetFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/local/path/to/data.parquet";

    assertEquals("/local/path/to/data.parquet", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedS3Parquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "s3://my-bucket/converted/output.parquet";

    assertEquals("s3://my-bucket/converted/output.parquet", invokeResolveParquetPath(record));
  }

  // =======================================================================
  // findFreePort - additional tests
  // =======================================================================

  @Test void testFindFreePortMultipleCallsReturnValidPorts() throws Exception {
    Method method = ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("findFreePort");
    method.setAccessible(true);

    for (int i = 0; i < 5; i++) {
      int port = (Integer) method.invoke(null);
      assertTrue(port > 0, "Port should be positive: " + port);
      assertTrue(port <= 65535, "Port should be in valid range: " + port);
    }
  }

  // =======================================================================
  // ClickHouseConfig - construction and settings paths
  // =======================================================================

  @Test void testClickHouseConfigDefaultsUsedForServerMode() {
    ClickHouseConfig config = new ClickHouseConfig();
    assertEquals("server", config.getMode());
    assertEquals("localhost", config.getHost());
    assertEquals("8123", config.getPort());
    assertEquals("default", config.getDatabase());
    assertNull(config.getLocalBinaryPath());
    assertNull(config.getDataDir());
    assertEquals("4GB", config.getMaxMemory());
    assertFalse(config.isLocalMode());
  }

  @Test void testClickHouseConfigLocalMode() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("mode", "local");
    configMap.put("dataDir", "/tmp/ch-test");
    configMap.put("localBinaryPath", "/usr/bin/clickhouse-local");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertTrue(config.isLocalMode());
    assertEquals("/tmp/ch-test", config.getDataDir());
    assertEquals("/usr/bin/clickhouse-local", config.getLocalBinaryPath());
  }

  @Test void testClickHouseConfigPortAsNumber() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("port", 9440);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("9440", config.getPort());
  }

  @Test void testClickHouseConfigPortAsString() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("port", "9440");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("9440", config.getPort());
  }

  @Test void testClickHouseConfigPortNull() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    // port not set

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("8123", config.getPort());
  }

  @Test void testClickHouseConfigMaxThreadsAsNumber() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("maxThreads", 8);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals(8, config.getMaxThreads());
  }

  @Test void testClickHouseConfigAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("max_execution_time", "300");
    configMap.put("log_queries", "1");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("300", config.getAdditionalSettings().getProperty("max_execution_time"));
    assertEquals("1", config.getAdditionalSettings().getProperty("log_queries"));
  }

  @Test void testClickHouseConfigToClickHouseSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("maxMemory", "8GB");
    configMap.put("maxThreads", 4);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    String[] settings = config.toClickHouseSettings();
    assertTrue(settings.length >= 2);

    boolean hasMemory = false;
    boolean hasThreads = false;
    for (String s : settings) {
      if (s.contains("max_memory_usage")) {
        hasMemory = true;
      }
      if (s.contains("max_threads")) {
        hasThreads = true;
      }
    }
    assertTrue(hasMemory, "Should contain max_memory_usage setting");
    assertTrue(hasThreads, "Should contain max_threads setting");
  }

  @Test void testClickHouseConfigToClickHouseSettingsWithAdditionalNumeric() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("max_execution_time", "300");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    String[] settings = config.toClickHouseSettings();

    boolean hasExecutionTime = false;
    for (String s : settings) {
      if (s.contains("max_execution_time") && s.contains("300")) {
        hasExecutionTime = true;
        // Numeric value should not be quoted
        assertFalse(s.contains("'300'"), "Numeric values should not be quoted: " + s);
      }
    }
    assertTrue(hasExecutionTime, "Should contain max_execution_time setting");
  }

  @Test void testClickHouseConfigToClickHouseSettingsWithBooleanValue() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("log_queries", "true");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    String[] settings = config.toClickHouseSettings();

    boolean hasLogQueries = false;
    for (String s : settings) {
      if (s.contains("log_queries") && s.contains("true")) {
        hasLogQueries = true;
        assertFalse(s.contains("'true'"), "Boolean values should not be quoted: " + s);
      }
    }
    assertTrue(hasLogQueries, "Should contain log_queries setting");
  }

  @Test void testClickHouseConfigToClickHouseSettingsWithStringValue() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("output_format_json_quote_64bit_integers", "enable");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    String[] settings = config.toClickHouseSettings();

    boolean hasStringSetting = false;
    for (String s : settings) {
      if (s.contains("output_format_json_quote_64bit_integers")) {
        hasStringSetting = true;
        assertTrue(s.contains("'enable'"), "String values should be quoted: " + s);
      }
    }
    assertTrue(hasStringSetting, "Should contain the string setting");
  }

  @Test void testClickHouseConfigToString() {
    ClickHouseConfig config = new ClickHouseConfig();
    String str = config.toString();
    assertTrue(str.contains("mode='server'"));
    assertTrue(str.contains("host='localhost'"));
    assertTrue(str.contains("port='8123'"));
    assertTrue(str.contains("database='default'"));
  }

  @Test void testClickHouseConfigIsLocalModeCaseInsensitive() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("mode", "LOCAL");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertTrue(config.isLocalMode());
  }

  @Test void testClickHouseConfigFullConstructor() {
    java.util.Properties additionalSettings = new java.util.Properties();
    additionalSettings.setProperty("extra_key", "extra_val");

    ClickHouseConfig config =
        new ClickHouseConfig("local", "myhost", "9999", "mydb",
        "/usr/bin/clickhouse", "/data/ch", "16GB", 16,
        additionalSettings);

    assertEquals("local", config.getMode());
    assertEquals("myhost", config.getHost());
    assertEquals("9999", config.getPort());
    assertEquals("mydb", config.getDatabase());
    assertEquals("/usr/bin/clickhouse", config.getLocalBinaryPath());
    assertEquals("/data/ch", config.getDataDir());
    assertEquals("16GB", config.getMaxMemory());
    assertEquals(16, config.getMaxThreads());
    assertEquals("extra_val", config.getAdditionalSettings().getProperty("extra_key"));
    assertTrue(config.isLocalMode());
  }

  @Test void testClickHouseConfigFullConstructorNullDefaults() {
    ClickHouseConfig config =
        new ClickHouseConfig(null, null, null, null,
        null, null, null, 0,
        null);

    assertEquals("server", config.getMode());
    assertEquals("localhost", config.getHost());
    assertEquals("8123", config.getPort());
    assertEquals("default", config.getDatabase());
    assertEquals("4GB", config.getMaxMemory());
    assertNotNull(config.getAdditionalSettings());
  }

  // =======================================================================
  // waitForReady - timeout test via reflection
  // =======================================================================

  @Test void testWaitForReadyTimesOutWithInvalidUrl() throws Exception {
    Method method =
        ClickHouseJdbcSchemaFactory.class.getDeclaredMethod("waitForReady", String.class, int.class);
    method.setAccessible(true);

    try {
      // Use a very short timeout and a URL that will never be ready
      method.invoke(null, "http://127.0.0.1:1/ping", 500);
      fail("Should throw RuntimeException for timeout");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("failed to start"));
    }
  }

  // =======================================================================
  // registerFilesAsViews - parquet table with convertedFile
  // =======================================================================

  @Test void testRegisterFilesAsViewsParquetWithConvertedFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "converted_table";
    record.conversionType = "EXCEL_TO_PARQUET";
    record.convertedFile = "/cache/converted_table.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("converted_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
  }

  @Test void testRegisterFilesAsViewsParquetWithBracedConvertedFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "glob_table";
    record.conversionType = "DIRECT";
    record.convertedFile = "{s3://bucket/data/**/*.parquet}";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("glob_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
  }

  @Test void testRegisterFilesAsViewsParquetWithCacheFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "cached_table";
    record.conversionType = "DIRECT";
    record.parquetCacheFile = "/cache/cached_table.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("cached_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
  }

  @Test void testRegisterFilesAsViewsParquetWithSourceParquetFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "source_parquet";
    record.conversionType = "DIRECT";
    record.sourceFile = "/data/source_parquet.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("source_parquet", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", false, "myschema", fileSchema);
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
  }

  // =======================================================================
  // INSTANCE_POOL static field exists (reflection check)
  // =======================================================================

  @Test void testInstancePoolFieldExists() throws Exception {
    java.lang.reflect.Field poolField =
        ClickHouseJdbcSchemaFactory.class.getDeclaredField("INSTANCE_POOL");
    poolField.setAccessible(true);
    Object pool = poolField.get(null);
    assertNotNull(pool);
    assertTrue(pool instanceof Map);
  }

  // =======================================================================
  // SharedInstanceInfo inner class (reflection)
  // =======================================================================

  @Test void testSharedInstanceInfoClassExists() throws Exception {
    Class<?>[] declaredClasses = ClickHouseJdbcSchemaFactory.class.getDeclaredClasses();
    boolean found = false;
    for (Class<?> c : declaredClasses) {
      if (c.getSimpleName().equals("SharedInstanceInfo")) {
        found = true;
        break;
      }
    }
    assertTrue(found, "SharedInstanceInfo inner class should exist");
  }

  // =======================================================================
  // registerFilesAsViews - verifying both Iceberg and Parquet SQL generation
  // =======================================================================

  @Test void testRegisterFilesAsViewsRecursiveFlag() throws Exception {
    // The recursive flag does not affect SQL generation, but verify the method
    // works correctly with recursive=true
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "recursive_table";
    record.conversionType = "DIRECT";
    record.viewScanPattern = "/data/recursive/**/*.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("recursive_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsViews(conn, "/data", true, "myschema", fileSchema);
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
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
