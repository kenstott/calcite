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
package org.apache.calcite.adapter.file.trino;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.execution.trino.TrinoConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
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
import java.util.Properties;

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
 * Deep coverage unit tests for {@link TrinoJdbcSchemaFactory}.
 *
 * <p>Focuses on exercising code paths not covered by
 * {@link TrinoJdbcSchemaFactoryCoverageTest}, including:
 * <ul>
 *   <li>Iceberg table registration in registerFilesAsTables</li>
 *   <li>Various SQL exception error message branches</li>
 *   <li>Config parsing with trinoConfig map in operand</li>
 *   <li>User/password connection paths</li>
 *   <li>Schema name lowercasing logic</li>
 *   <li>createSchemaWithConnection dialect and convention creation</li>
 *   <li>TrinoConfig catalog file generation and property methods</li>
 *   <li>SharedInstanceInfo data class behavior</li>
 * </ul>
 *
 * <p>Does NOT require a running Trino server.
 */
@Tag("unit")
class TrinoJdbcSchemaFactoryDeepCoverageTest {

  // =======================================================================
  // registerFilesAsTables - Iceberg table paths
  // =======================================================================

  @Test void testRegisterFilesAsTablesIcebergWithSourceFile() throws Exception {
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

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    verify(stmt).execute(contains("register_table"));
  }

  @Test void testRegisterFilesAsTablesIcebergWithParquetSourceFile() throws Exception {
    // When sourceFile ends with .parquet, path should be constructed from directory
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_from_pq";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "/data/file.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_from_pq", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "schema1", fileSchema, new TrinoConfig());
    // Path constructed: /data/schema1/iceberg_from_pq
    verify(stmt).execute(contains("register_table"));
  }

  @Test void testRegisterFilesAsTablesIcebergWithNullSourceFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_null_src";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = null;

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_null_src", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/warehouse", true, "analytics", fileSchema, new TrinoConfig());
    // Path constructed: /warehouse/analytics/iceberg_null_src
    verify(stmt).execute(contains("register_table"));
  }

  // =======================================================================
  // registerFilesAsTables - Parquet table creation
  // =======================================================================

  @Test void testRegisterFilesAsTablesParquetWithViewScanPattern() throws Exception {
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

    invokeRegisterFilesAsTables(conn, "/data", false, "sales", fileSchema, new TrinoConfig());
    verify(stmt).execute(contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test void testRegisterFilesAsTablesParquetWithCacheFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "cached_table";
    record.conversionType = "DIRECT";
    record.parquetCacheFile = "/cache/data.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("cached_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    verify(stmt).execute(contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test void testRegisterFilesAsTablesParquetWithSourceParquetFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "source_pq";
    record.conversionType = "DIRECT";
    record.sourceFile = "/data/source.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("source_pq", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    verify(stmt).execute(contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test void testRegisterFilesAsTablesParquetWithConvertedFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "converted_tbl";
    record.conversionType = "EXCEL_TO_PARQUET";
    record.convertedFile = "/cache/output.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("converted_tbl", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    verify(stmt).execute(contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test void testRegisterFilesAsTablesParquetWithBracedConvertedFile() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "glob_tbl";
    record.conversionType = "DIRECT";
    record.convertedFile = "{s3://bucket/data/**/*.parquet}";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("glob_tbl", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    verify(stmt).execute(contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test void testRegisterFilesAsTablesNoParquetPath() throws Exception {
    Connection conn = mock(Connection.class);
    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "no_path_table";
    record.conversionType = "DIRECT";
    record.sourceFile = "/data/file.csv";
    record.convertedFile = "/data/file.json";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("no_path_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    // Should be skipped because no parquet path is resolvable
    verify(conn, never()).createStatement();
  }

  // =======================================================================
  // registerFilesAsTables - SQL exception error message branches
  // =======================================================================

  @Test void testRegisterFilesAsTablesHandlesAlreadyExistsMessage() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("Table already exists in catalog"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "existing_table";
    record.viewScanPattern = "/data/existing.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("existing_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesHandlesTableAlreadyExists() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("ALREADY_EXISTS: table test.t"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "already_t";
    record.viewScanPattern = "/data/already.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("already_t", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesHandles404Error() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("HTTP 404 Not Found"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "missing_404";
    record.viewScanPattern = "s3://bucket/missing.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("missing_404", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesHandlesNoSuchKeyError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("NoSuchKey: key not found in S3 bucket"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "no_key";
    record.viewScanPattern = "s3://bucket/no_key.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("no_key", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesHandlesDoesNotExistError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Path /data/table does not exist"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "gone_table";
    record.viewScanPattern = "/data/table/*.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("gone_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesHandlesPathDoesNotExist() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Path does not exist: s3://bucket/missing/"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "path_gone";
    record.viewScanPattern = "s3://bucket/missing/*.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("path_gone", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesHandlesUnknownSqlError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("Unknown internal Trino error"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "error_table";
    record.viewScanPattern = "s3://bucket/data.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("error_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesIcebergSqlException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("ALREADY_EXISTS: table already registered"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_dup";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "s3://bucket/warehouse/iceberg_dup";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_dup", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  // =======================================================================
  // registerFilesAsTables - multiple records with mixed types
  // =======================================================================

  @Test void testRegisterFilesAsTablesMixedTypes() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord icebergRecord = new ConversionMetadata.ConversionRecord();
    icebergRecord.tableName = "iceberg_t";
    icebergRecord.conversionType = "ICEBERG_PARQUET";
    icebergRecord.sourceFile = "s3://bucket/iceberg_t";

    ConversionMetadata.ConversionRecord parquetRecord = new ConversionMetadata.ConversionRecord();
    parquetRecord.tableName = "parquet_t";
    parquetRecord.conversionType = "DIRECT";
    parquetRecord.parquetCacheFile = "/cache/parquet_t.parquet";

    ConversionMetadata.ConversionRecord noPathRecord = new ConversionMetadata.ConversionRecord();
    noPathRecord.tableName = "no_path_t";
    noPathRecord.conversionType = "DIRECT";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_t", icebergRecord);
    records.put("parquet_t", parquetRecord);
    records.put("no_path_t", noPathRecord);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    // Two tables created (iceberg + parquet), third skipped
    verify(stmt, times(2)).execute(anyString());
  }

  // =======================================================================
  // registerSqlViewsInTrino - additional edge cases
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

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
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

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
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

    Map<String, Object> mv1 = new LinkedHashMap<String, Object>();
    mv1.put("type", "materialized_view");
    mv1.put("name", "mv1");
    mv1.put("sql", "SELECT 3");
    tables.add(mv1);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // Only v1 should be created
    verify(stmt, times(1)).execute(anyString());
  }

  @Test void testRegisterSqlViewsVerifiesQualifiedName() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    Map<String, Object> viewDef = new LinkedHashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "revenue");
    viewDef.put("sql", "SELECT sum(amount) FROM orders");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInTrino(conn, "analytics", new TrinoConfig(), operand);
    // Trino uses qualifyName from TrinoDialect which quotes identifiers
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
    verify(stmt).execute(contains("revenue"));
  }

  // =======================================================================
  // createTrinoDialect - additional dialect property tests
  // =======================================================================

  @Test void testCreateTrinoDialectReturnsNewInstanceEachCall() throws Exception {
    SqlDialect dialect1 = invokeCreateTrinoDialect();
    SqlDialect dialect2 = invokeCreateTrinoDialect();
    assertNotNull(dialect1);
    assertNotNull(dialect2);
    assertFalse(dialect1 == dialect2, "Each invocation should create a new dialect instance");
  }

  @Test void testCreateTrinoDialectSupportsVariousAggregates() throws Exception {
    SqlDialect dialect = invokeCreateTrinoDialect();
    assertTrue(dialect.supportsAggregateFunction(SqlKind.OTHER_FUNCTION));
    assertTrue(dialect.supportsAggregateFunction(SqlKind.SUM0));
    assertTrue(dialect.supportsAggregateFunction(SqlKind.SINGLE_VALUE));
  }

  // =======================================================================
  // create() entry point - various config paths
  // =======================================================================

  @Test void testCreateWithTrinoConfigInOperand() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> trinoConfig = new HashMap<String, Object>();
    trinoConfig.put("host", "trino.cluster.local");
    trinoConfig.put("port", "8443");
    trinoConfig.put("catalog", "iceberg");
    trinoConfig.put("schema", "warehouse");
    trinoConfig.put("user", "admin");
    trinoConfig.put("password", "secret123");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trinoConfig", trinoConfig);

    try {
      TrinoJdbcSchemaFactory.create(
          parentSchema, "MySchema", "/data", true, null, operand);
      fail("Should throw when Trino driver is not available");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to create Trino JDBC schema")
              || e.getMessage().contains("TrinoDriver"),
          "Error should mention driver or schema creation: " + e.getMessage());
    }
  }

  @Test void testCreateWithNullOperand() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    try {
      TrinoJdbcSchemaFactory.create(
          parentSchema, "testSchema", "/data", false, null, null);
      fail("Should throw when Trino driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithEmptyOperand() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    try {
      TrinoJdbcSchemaFactory.create(
          parentSchema, "defaultSchema", "/data", false, null, operand);
      fail("Should throw when Trino driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithRecursiveTrue() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    try {
      TrinoJdbcSchemaFactory.create(
          parentSchema, "recursiveSchema", "/data/nested", true, null, operand);
      fail("Should throw when Trino driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithFileSchema() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    FileSchema fileSchema = mock(FileSchema.class);
    Map<String, Object> operand = new HashMap<String, Object>();

    try {
      TrinoJdbcSchemaFactory.create(
          parentSchema, "fsSchema", "/data", false, fileSchema, operand);
      fail("Should throw when Trino driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithUserNoPassword() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> trinoConfig = new HashMap<String, Object>();
    trinoConfig.put("user", "analyst");
    // password is null

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trinoConfig", trinoConfig);

    try {
      TrinoJdbcSchemaFactory.create(
          parentSchema, "userSchema", "/data", false, null, operand);
      fail("Should throw when Trino driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testCreateWithNoUser() {
    // When user is null, should default to system user
    SchemaPlus parentSchema = mock(SchemaPlus.class);
    Map<String, Object> trinoConfig = new HashMap<String, Object>();
    // user and password not set

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trinoConfig", trinoConfig);

    try {
      TrinoJdbcSchemaFactory.create(
          parentSchema, "noUserSchema", "/data", false, null, operand);
      fail("Should throw when Trino driver is not available");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
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

  @Test void testResolveParquetPathConvertedFileSingleStartBrace() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "{partial";

    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedFileSingleEndBrace() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "partial}";

    assertNull(invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathS3Source() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "s3://my-bucket/data/file.parquet";

    assertEquals("s3://my-bucket/data/file.parquet", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathConvertedS3Parquet() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "s3://bucket/output.parquet";

    assertEquals("s3://bucket/output.parquet", invokeResolveParquetPath(record));
  }

  // =======================================================================
  // TrinoConfig - additional coverage for catalog properties
  // =======================================================================

  @Test void testTrinoConfigHiveCatalogPropertiesWithS3NoEndpoint() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("s3AccessKey", "AKTEST");
    configMap.put("s3SecretKey", "SECRET");
    // s3Endpoint not set

    TrinoConfig config = new TrinoConfig(configMap);
    Properties props = config.getHiveCatalogProperties();
    assertEquals("AKTEST", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("SECRET", props.getProperty("hive.s3.aws-secret-key"));
    assertNull(props.getProperty("hive.s3.endpoint"));
    assertNull(props.getProperty("hive.s3.path-style-access"));
  }

  @Test void testTrinoConfigIcebergCatalogPropertiesWithS3Endpoint() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("s3AccessKey", "AKTEST");
    configMap.put("s3SecretKey", "SECRET");
    configMap.put("s3Endpoint", "http://minio:9000");

    TrinoConfig config = new TrinoConfig(configMap);
    Properties props = config.getIcebergCatalogProperties();
    assertEquals("AKTEST", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("SECRET", props.getProperty("hive.s3.aws-secret-key"));
    assertEquals("http://minio:9000", props.getProperty("hive.s3.endpoint"));
    assertEquals("true", props.getProperty("hive.s3.path-style-access"));
  }

  @Test void testTrinoConfigIcebergCatalogPropertiesNoS3() {
    TrinoConfig config = new TrinoConfig();
    Properties props = config.getIcebergCatalogProperties();
    assertNull(props.getProperty("hive.s3.aws-access-key"));
    assertNull(props.getProperty("hive.s3.aws-secret-key"));
  }

  @Test void testTrinoConfigHiveCatalogPropertiesNoS3() {
    TrinoConfig config = new TrinoConfig();
    Properties props = config.getHiveCatalogProperties();
    assertNull(props.getProperty("hive.s3.aws-access-key"));
    assertNull(props.getProperty("hive.s3.aws-secret-key"));
  }

  @Test void testTrinoConfigGenerateCatalogFiles() throws IOException {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("s3AccessKey", "AKTEST");
    configMap.put("s3SecretKey", "SECRET");
    configMap.put("s3Endpoint", "http://minio:9000");

    TrinoConfig config = new TrinoConfig(configMap);

    File tempDir = File.createTempFile("trino-test", "");
    tempDir.delete();
    tempDir.mkdirs();

    try {
      config.generateCatalogFiles(tempDir.getAbsolutePath());

      File hiveFile = new File(tempDir, "hive.properties");
      File icebergFile = new File(tempDir, "iceberg.properties");

      assertTrue(hiveFile.exists(), "Hive catalog file should exist");
      assertTrue(icebergFile.exists(), "Iceberg catalog file should exist");
      assertTrue(hiveFile.length() > 0, "Hive file should not be empty");
      assertTrue(icebergFile.length() > 0, "Iceberg file should not be empty");
    } finally {
      // Cleanup
      File[] files = tempDir.listFiles();
      if (files != null) {
        for (File f : files) {
          f.delete();
        }
      }
      tempDir.delete();
    }
  }

  @Test void testTrinoConfigGenerateCatalogFilesCustomCatalogNames() throws IOException {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("catalog", "my_hive");
    configMap.put("icebergCatalog", "my_iceberg");

    TrinoConfig config = new TrinoConfig(configMap);

    File tempDir = File.createTempFile("trino-test-custom", "");
    tempDir.delete();
    tempDir.mkdirs();

    try {
      config.generateCatalogFiles(tempDir.getAbsolutePath());

      File hiveFile = new File(tempDir, "my_hive.properties");
      File icebergFile = new File(tempDir, "my_iceberg.properties");

      assertTrue(hiveFile.exists(), "Custom hive catalog file should exist");
      assertTrue(icebergFile.exists(), "Custom iceberg catalog file should exist");
    } finally {
      File[] files = tempDir.listFiles();
      if (files != null) {
        for (File f : files) {
          f.delete();
        }
      }
      tempDir.delete();
    }
  }

  @Test void testTrinoConfigGenerateCatalogFilesCreatesDirectory() throws IOException {
    File tempDir = new File(System.getProperty("java.io.tmpdir"),
        "trino-test-mkdir-" + System.currentTimeMillis());

    try {
      assertFalse(tempDir.exists());
      TrinoConfig config = new TrinoConfig();
      config.generateCatalogFiles(tempDir.getAbsolutePath());
      assertTrue(tempDir.exists(), "Directory should have been created");
    } finally {
      File[] files = tempDir.listFiles();
      if (files != null) {
        for (File f : files) {
          f.delete();
        }
      }
      tempDir.delete();
    }
  }

  // =======================================================================
  // TrinoConfig - full constructor and edge cases
  // =======================================================================

  @Test void testTrinoConfigFullConstructor() {
    Properties additionalSettings = new Properties();
    additionalSettings.setProperty("extra_key", "extra_val");

    TrinoConfig config = new TrinoConfig(
        "trino.host", "9443", "delta", "warehouse",
        "admin", "pass123", "ice_catalog", "/mnt/warehouse",
        "AKID", "SECRET_KEY", "http://s3:9000",
        additionalSettings);

    assertEquals("trino.host", config.getHost());
    assertEquals("9443", config.getPort());
    assertEquals("delta", config.getCatalog());
    assertEquals("warehouse", config.getSchema());
    assertEquals("admin", config.getUser());
    assertEquals("pass123", config.getPassword());
    assertEquals("ice_catalog", config.getIcebergCatalog());
    assertEquals("/mnt/warehouse", config.getWarehouseDir());
    assertEquals("AKID", config.getS3AccessKey());
    assertEquals("SECRET_KEY", config.getS3SecretKey());
    assertEquals("http://s3:9000", config.getS3Endpoint());
    assertEquals("extra_val", config.getAdditionalSettings().getProperty("extra_key"));
  }

  @Test void testTrinoConfigFullConstructorNullDefaults() {
    TrinoConfig config = new TrinoConfig(
        null, null, null, null,
        null, null, null, null,
        null, null, null, null);

    assertEquals("localhost", config.getHost());
    assertEquals("8080", config.getPort());
    assertEquals("hive", config.getCatalog());
    assertEquals("default", config.getSchema());
    assertEquals("iceberg", config.getIcebergCatalog());
    assertEquals("/data/warehouse", config.getWarehouseDir());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertNull(config.getS3AccessKey());
    assertNull(config.getS3SecretKey());
    assertNull(config.getS3Endpoint());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
  }

  @Test void testTrinoConfigFromMapWithAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("query_max_memory", "4GB");
    configMap.put("join_distribution_type", "AUTOMATIC");

    TrinoConfig config = new TrinoConfig(configMap);
    String[] settings = config.toSessionSettings();
    assertEquals(2, settings.length);

    boolean hasMemory = false;
    boolean hasJoin = false;
    for (String s : settings) {
      if (s.contains("query_max_memory")) {
        hasMemory = true;
      }
      if (s.contains("join_distribution_type")) {
        hasJoin = true;
      }
    }
    assertTrue(hasMemory);
    assertTrue(hasJoin);
  }

  @Test void testTrinoConfigToStringNullUser() {
    TrinoConfig config = new TrinoConfig();
    String str = config.toString();
    assertTrue(str.contains("user='null'"));
  }

  @Test void testTrinoConfigPortAsNonStringNonNumber() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("port", new Object()); // Not a String or Number

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("8080", config.getPort()); // Should use default
  }

  // =======================================================================
  // INSTANCE_POOL static field exists
  // =======================================================================

  @Test void testInstancePoolFieldExists() throws Exception {
    java.lang.reflect.Field poolField =
        TrinoJdbcSchemaFactory.class.getDeclaredField("INSTANCE_POOL");
    poolField.setAccessible(true);
    Object pool = poolField.get(null);
    assertNotNull(pool);
    assertTrue(pool instanceof Map);
  }

  // =======================================================================
  // SharedInstanceInfo inner class exists
  // =======================================================================

  @Test void testSharedInstanceInfoClassExists() throws Exception {
    Class<?>[] declaredClasses = TrinoJdbcSchemaFactory.class.getDeclaredClasses();
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
  // registerFilesAsTables - recursive flag
  // =======================================================================

  @Test void testRegisterFilesAsTablesRecursiveFlag() throws Exception {
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

    invokeRegisterFilesAsTables(conn, "/data", true, "myschema", fileSchema, new TrinoConfig());
    verify(stmt).execute(contains("CREATE TABLE IF NOT EXISTS"));
  }

  // =======================================================================
  // registerFilesAsTables - iceberg with FILE_NOT_FOUND
  // =======================================================================

  @Test void testRegisterFilesAsTablesIcebergFileNotFound() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(
        new SQLException("FILE_NOT_FOUND: metadata.json missing"));

    FileSchema fileSchema = mock(FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "iceberg_missing";
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = "s3://bucket/warehouse/iceberg_missing";

    Map<String, ConversionMetadata.ConversionRecord> records =
        new LinkedHashMap<String, ConversionMetadata.ConversionRecord>();
    records.put("iceberg_missing", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  // =======================================================================
  // Reflection helper methods
  // =======================================================================

  private String invokeResolveParquetPath(ConversionMetadata.ConversionRecord record)
      throws Exception {
    Method method = TrinoJdbcSchemaFactory.class.getDeclaredMethod(
        "resolveParquetPath", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (String) method.invoke(null, record);
  }

  private SqlDialect invokeCreateTrinoDialect() throws Exception {
    Method method = TrinoJdbcSchemaFactory.class.getDeclaredMethod("createTrinoDialect");
    method.setAccessible(true);
    return (SqlDialect) method.invoke(null);
  }

  private void invokeRegisterSqlViewsInTrino(Connection conn, String trinoSchema,
      TrinoConfig config, Map<String, Object> operand) throws Exception {
    Method method = TrinoJdbcSchemaFactory.class.getDeclaredMethod(
        "registerSqlViewsInTrino",
        Connection.class, String.class, TrinoConfig.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, conn, trinoSchema, config, operand);
  }

  private void invokeRegisterFilesAsTables(Connection conn, String directoryPath,
      boolean recursive, String trinoSchema,
      FileSchema fileSchema, TrinoConfig config) throws Exception {
    Method method = TrinoJdbcSchemaFactory.class.getDeclaredMethod(
        "registerFilesAsTables",
        Connection.class, String.class, boolean.class, String.class,
        org.apache.calcite.adapter.file.FileSchema.class, TrinoConfig.class);
    method.setAccessible(true);
    method.invoke(null, conn, directoryPath, recursive, trinoSchema, fileSchema, config);
  }
}
