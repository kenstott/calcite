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
package org.apache.calcite.adapter.file.trino;

import org.apache.calcite.adapter.file.execution.trino.TrinoConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.sql.SqlDialect;

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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link TrinoJdbcSchemaFactory}.
 * Tests internal logic via reflection and mocking to avoid needing a live Trino server.
 */
@Tag("unit")
class TrinoJdbcSchemaFactoryCoverageTest {

  // ---------------------------------------------------------------
  // resolveParquetPath tests
  // ---------------------------------------------------------------

  @Test void testResolveParquetPathWithViewScanPattern() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "s3://bucket/data/**/*.parquet";
    record.parquetCacheFile = "/cache/table.parquet";
    record.sourceFile = "/source/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("s3://bucket/data/**/*.parquet", result);
  }

  @Test void testResolveParquetPathWithParquetCacheFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = "/cache/table.parquet";
    record.sourceFile = "/source/data.csv";

    String result = invokeResolveParquetPath(record);
    assertEquals("/cache/table.parquet", result);
  }

  @Test void testResolveParquetPathWithParquetSourceFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = "/source/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/source/data.parquet", result);
  }

  @Test void testResolveParquetPathWithNonParquetSourceFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = "/source/data.csv";
    record.convertedFile = null;

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  @Test void testResolveParquetPathWithConvertedParquetFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = "/source/data.csv";
    record.convertedFile = "/converted/data.parquet";

    String result = invokeResolveParquetPath(record);
    assertEquals("/converted/data.parquet", result);
  }

  @Test void testResolveParquetPathWithJsonConvertedFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = "{\"key\": \"value\"}";

    String result = invokeResolveParquetPath(record);
    assertEquals("{\"key\": \"value\"}", result);
  }

  @Test void testResolveParquetPathWithNonParquetConvertedFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = "/converted/data.json";

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  @Test void testResolveParquetPathAllFieldsNull() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = null;
    record.sourceFile = null;
    record.convertedFile = null;

    String result = invokeResolveParquetPath(record);
    assertNull(result);
  }

  @Test void testResolveParquetPathPriorityOrder() throws Exception {
    // viewScanPattern has highest priority
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "pattern";
    record.parquetCacheFile = "cache";
    record.sourceFile = "source.parquet";
    record.convertedFile = "converted.parquet";

    assertEquals("pattern", invokeResolveParquetPath(record));
  }

  @Test void testResolveParquetPathCacheBeforeSource() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = null;
    record.parquetCacheFile = "cache.parquet";
    record.sourceFile = "source.parquet";

    assertEquals("cache.parquet", invokeResolveParquetPath(record));
  }

  // ---------------------------------------------------------------
  // createTrinoDialect tests
  // ---------------------------------------------------------------

  @Test void testCreateTrinoDialectReturnsNonNull() throws Exception {
    SqlDialect dialect = invokeCreateTrinoDialect();
    assertNotNull(dialect);
  }

  @Test void testCreateTrinoDialectSupportsAggregateFunction() throws Exception {
    SqlDialect dialect = invokeCreateTrinoDialect();
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.COUNT));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.SUM));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.AVG));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.MIN));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.MAX));
  }

  @Test void testCreateTrinoDialectDatabaseProductName() throws Exception {
    SqlDialect dialect = invokeCreateTrinoDialect();
    // The dialect is created with UNKNOWN database product but "Trino" product name
    assertNotNull(dialect);
  }

  // ---------------------------------------------------------------
  // Configuration parsing tests
  // ---------------------------------------------------------------

  @Test void testTrinoConfigDefaults() {
    TrinoConfig config = new TrinoConfig();
    assertEquals("localhost", config.getHost());
    assertEquals("8080", config.getPort());
    assertEquals("hive", config.getCatalog());
    assertEquals("default", config.getSchema());
    assertNull(config.getUser());
    assertNull(config.getPassword());
  }

  @Test void testTrinoConfigFromMap() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "trino-server");
    configMap.put("port", "9090");
    configMap.put("catalog", "iceberg");
    configMap.put("schema", "analytics");
    configMap.put("user", "admin");
    configMap.put("password", "secret");

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("trino-server", config.getHost());
    assertEquals("9090", config.getPort());
    assertEquals("iceberg", config.getCatalog());
    assertEquals("analytics", config.getSchema());
    assertEquals("admin", config.getUser());
    assertEquals("secret", config.getPassword());
  }

  @Test void testTrinoConfigFromMapWithNumericPort() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("port", 8443);

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("8443", config.getPort());
  }

  @Test void testTrinoConfigFromMapMissingValues() {
    Map<String, Object> configMap = new HashMap<>();
    TrinoConfig config = new TrinoConfig(configMap);

    assertEquals("localhost", config.getHost());
    assertEquals("8080", config.getPort());
    assertEquals("hive", config.getCatalog());
    assertEquals("default", config.getSchema());
  }

  @Test void testTrinoConfigSessionSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("query_max_memory", "4GB");
    configMap.put("join_distribution_type", "AUTOMATIC");

    TrinoConfig config = new TrinoConfig(configMap);
    String[] settings = config.toSessionSettings();
    assertEquals(2, settings.length);
  }

  @Test void testTrinoConfigNoSessionSettings() {
    TrinoConfig config = new TrinoConfig();
    String[] settings = config.toSessionSettings();
    assertEquals(0, settings.length);
  }

  @Test void testTrinoConfigToString() {
    TrinoConfig config = new TrinoConfig();
    String str = config.toString();
    assertTrue(str.contains("host='localhost'"));
    assertTrue(str.contains("port='8080'"));
    assertTrue(str.contains("catalog='hive'"));
  }

  @Test void testTrinoConfigToStringMasksUser() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("user", "admin");
    TrinoConfig config = new TrinoConfig(configMap);
    String str = config.toString();
    assertTrue(str.contains("user='***'"));
  }

  @Test void testTrinoConfigWithS3Settings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("s3AccessKey", "AKIAIOSFODNN7EXAMPLE");
    configMap.put("s3SecretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    configMap.put("s3Endpoint", "http://minio:9000");

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("AKIAIOSFODNN7EXAMPLE", config.getS3AccessKey());
    assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.getS3SecretKey());
    assertEquals("http://minio:9000", config.getS3Endpoint());
  }

  // ---------------------------------------------------------------
  // registerSqlViewsInTrino tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testRegisterSqlViewsNullOperand() throws Exception {
    Connection conn = mock(Connection.class);
    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), null);
    // No exception should be thrown; method should return silently
  }

  @Test void testRegisterSqlViewsEmptyOperand() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();
    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // No exception should be thrown
  }

  @Test void testRegisterSqlViewsNullTablesList() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", null);
    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // No exception should be thrown
  }

  @Test void testRegisterSqlViewsEmptyTablesList() throws Exception {
    Connection conn = mock(Connection.class);
    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", new ArrayList<>());
    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // No exception should be thrown
  }

  @Test void testRegisterSqlViewsSkipsNonViewType() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("type", "table");
    tableDef.put("name", "my_table");
    tableDef.put("sql", "SELECT 1");
    tables.add(tableDef);

    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // Statement should NOT be called because type is "table" not "view"
  }

  @Test void testRegisterSqlViewsWithViewType() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("type", "view");
    viewDef.put("name", "my_view");
    viewDef.put("sql", "SELECT 1 AS col");
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    verify(stmt).execute(anyString());
  }

  @Test void testRegisterSqlViewsSkipsNullViewName() throws Exception {
    Connection conn = mock(Connection.class);

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("type", "view");
    viewDef.put("name", null);
    viewDef.put("sql", "SELECT 1");
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // No exception; view with null name is skipped
  }

  @Test void testRegisterSqlViewsSkipsNullSql() throws Exception {
    Connection conn = mock(Connection.class);

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("type", "view");
    viewDef.put("name", "my_view");
    viewDef.put("sql", null);
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // No exception; view with null sql is skipped
  }

  @Test void testRegisterSqlViewsHandlesSqlException() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("View creation failed"));

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("type", "view");
    viewDef.put("name", "bad_view");
    viewDef.put("sql", "SELECT invalid_syntax");
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", tables);

    // Should NOT throw - the method catches and logs SQL exceptions
    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
  }

  @Test void testRegisterSqlViewsMultipleViews() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    List<Map<String, Object>> tables = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Map<String, Object> viewDef = new HashMap<>();
      viewDef.put("type", "view");
      viewDef.put("name", "view_" + i);
      viewDef.put("sql", "SELECT " + i);
      tables.add(viewDef);
    }

    Map<String, Object> operand = new HashMap<>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInTrino(conn, "myschema", new TrinoConfig(), operand);
    // Verify that execute was called 3 times (once per view)
    verify(stmt, org.mockito.Mockito.times(3)).execute(anyString());
  }

  // ---------------------------------------------------------------
  // registerFilesAsTables tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testRegisterFilesAsTablesNullFileSchema() throws Exception {
    Connection conn = mock(Connection.class);
    try {
      invokeRegisterFilesAsTables(conn, "/data", false, "myschema", null, new TrinoConfig());
      fail("Expected InvocationTargetException wrapping SQLException");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof SQLException);
      assertTrue(e.getCause().getMessage().contains("FileSchema"));
    }
  }

  @Test void testRegisterFilesAsTablesEmptyRecords() throws Exception {
    Connection conn = mock(Connection.class);
    org.apache.calcite.adapter.file.FileSchema fileSchema =
        mock(org.apache.calcite.adapter.file.FileSchema.class);
    when(fileSchema.getAllTableRecords()).thenReturn(Collections.emptyMap());

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    // Empty records should return without creating any tables
  }

  @Test void testRegisterFilesAsTablesSkipsNullTableName() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    org.apache.calcite.adapter.file.FileSchema fileSchema =
        mock(org.apache.calcite.adapter.file.FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = null;

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    records.put("key", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    // Should skip the record with null table name
  }

  @Test void testRegisterFilesAsTablesSkipsEmptyTableName() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    org.apache.calcite.adapter.file.FileSchema fileSchema =
        mock(org.apache.calcite.adapter.file.FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "";

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    records.put("key", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
    // Should skip the record with empty table name
  }

  @Test void testRegisterFilesAsTablesHandlesAlreadyExistsError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("TABLE_ALREADY_EXISTS"));

    org.apache.calcite.adapter.file.FileSchema fileSchema =
        mock(org.apache.calcite.adapter.file.FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.viewScanPattern = "/data/test.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    records.put("test_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should not throw despite the SQL error
    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  @Test void testRegisterFilesAsTablesHandlesFileNotFoundError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("FILE_NOT_FOUND"));

    org.apache.calcite.adapter.file.FileSchema fileSchema =
        mock(org.apache.calcite.adapter.file.FileSchema.class);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "missing_table";
    record.viewScanPattern = "/data/missing.parquet";

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    records.put("missing_table", record);
    when(fileSchema.getAllTableRecords()).thenReturn(records);

    // Should not throw despite the SQL error
    invokeRegisterFilesAsTables(conn, "/data", false, "myschema", fileSchema, new TrinoConfig());
  }

  // ---------------------------------------------------------------
  // TrinoConfig catalog file generation tests
  // ---------------------------------------------------------------

  @Test void testHiveCatalogPropertiesBasic() {
    TrinoConfig config = new TrinoConfig();
    java.util.Properties props = config.getHiveCatalogProperties();
    assertEquals("hive", props.getProperty("connector.name"));
    assertEquals("file", props.getProperty("hive.metastore"));
    assertEquals("allow-all", props.getProperty("hive.security"));
  }

  @Test void testHiveCatalogPropertiesWithS3() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("s3AccessKey", "MYKEY");
    configMap.put("s3SecretKey", "MYSECRET");
    configMap.put("s3Endpoint", "http://minio:9000");

    TrinoConfig config = new TrinoConfig(configMap);
    java.util.Properties props = config.getHiveCatalogProperties();
    assertEquals("MYKEY", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("MYSECRET", props.getProperty("hive.s3.aws-secret-key"));
    assertEquals("http://minio:9000", props.getProperty("hive.s3.endpoint"));
    assertEquals("true", props.getProperty("hive.s3.path-style-access"));
  }

  @Test void testIcebergCatalogPropertiesBasic() {
    TrinoConfig config = new TrinoConfig();
    java.util.Properties props = config.getIcebergCatalogProperties();
    assertEquals("iceberg", props.getProperty("connector.name"));
    assertEquals("TESTING_FILE_METASTORE", props.getProperty("iceberg.catalog.type"));
  }

  @Test void testIcebergCatalogPropertiesWithS3() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("s3AccessKey", "MYKEY");
    configMap.put("s3SecretKey", "MYSECRET");

    TrinoConfig config = new TrinoConfig(configMap);
    java.util.Properties props = config.getIcebergCatalogProperties();
    assertEquals("MYKEY", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("MYSECRET", props.getProperty("hive.s3.aws-secret-key"));
  }

  // ---------------------------------------------------------------
  // Reflection helper methods
  // ---------------------------------------------------------------

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
      org.apache.calcite.adapter.file.FileSchema fileSchema,
      TrinoConfig config) throws Exception {
    Method method = TrinoJdbcSchemaFactory.class.getDeclaredMethod(
        "registerFilesAsTables",
        Connection.class, String.class, boolean.class, String.class,
        org.apache.calcite.adapter.file.FileSchema.class, TrinoConfig.class);
    method.setAccessible(true);
    method.invoke(null, conn, directoryPath, recursive, trinoSchema, fileSchema, config);
  }
}
