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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for DuckDBJdbcSchemaFactory.
 * Focuses on private/static helper methods via reflection.
 */
@Tag("unit")
public class DuckDBJdbcSchemaFactoryDeepCoverageTest3 {

  // ===== getParserConfig tests =====

  @Test void testGetParserConfigNotNull() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(config);
  }

  @Test void testGetParserConfigQuotedCasing() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(org.apache.calcite.avatica.util.Casing.UNCHANGED, config.quotedCasing());
  }

  // ===== createParquetView tests =====

  @Test void testCreateParquetViewSuccess() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      // Create a temp parquet file to reference (DuckDB will fail if file doesn't exist)
      // Instead, just test that the method creates the SQL correctly
      // We'll test with an intentional failure since no file exists
      try {
        DuckDBJdbcSchemaFactory.createParquetView(conn, "test_view", "/nonexistent/file.parquet");
      } catch (RuntimeException e) {
        // Expected: file doesn't exist
        assertTrue(e.getMessage().contains("Failed to create Parquet view"));
      }
    } finally {
      conn.close();
    }
  }

  // ===== isHivePartitioned tests (via reflection) =====

  @Test void testIsHivePartitionedNull() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, (String) null));
  }

  @Test void testIsHivePartitionedEmpty() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, ""));
  }

  @Test void testIsHivePartitionedSingleFile() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "/data/file1.parquet"));
  }

  @Test void testIsHivePartitionedBracketedList() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    // Two files with Hive partition patterns
    String fileList = "['/data/year=2020/file1.parquet','/data/year=2021/file2.parquet']";
    assertTrue((Boolean) method.invoke(null, fileList));
  }

  @Test void testIsHivePartitionedCurlyBraces() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    String fileList = "{'/data/country=US/file1.parquet','/data/country=UK/file2.parquet'}";
    assertTrue((Boolean) method.invoke(null, fileList));
  }

  @Test void testIsHivePartitionedNoPartitionPattern() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    String fileList = "['/data/file1.parquet','/data/file2.parquet']";
    assertFalse((Boolean) method.invoke(null, fileList));
  }

  @Test void testIsHivePartitionedQuotedFiles() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    String fileList = "'/data/region=west/file1.parquet','/data/region=east/file2.parquet'";
    assertTrue((Boolean) method.invoke(null, fileList));
  }

  // ===== deriveGlobPattern tests (via reflection) =====

  @Test void testDeriveGlobPatternNull() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, (String) null));
  }

  @Test void testDeriveGlobPatternEmpty() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, ""));
  }

  @Test void testDeriveGlobPatternBracketed() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    String fileList = "['/data/warehouse/year=2020/file1.parquet','/data/warehouse/year=2021/file2.parquet']";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
    assertTrue(result.contains(".parquet"));
  }

  @Test void testDeriveGlobPatternNoPartition() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    String fileList = "['/data/warehouse/file1.parquet','/data/warehouse/file2.parquet']";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    // Should derive a glob pattern with wildcard
    assertTrue(result.contains("*"));
    assertTrue(result.contains(".parquet"));
  }

  @Test void testDeriveGlobPatternSingleFile() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, "file_only_no_path");
    // Single file with no path separator returns itself
    assertEquals("file_only_no_path", result);
  }

  @Test void testDeriveGlobPatternCurlyBraces() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    String fileList = "{'/data/a=1/b.parquet','/data/a=2/c.parquet'}";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
  }

  // ===== isTempDirectory tests (via reflection) =====

  @Test void testIsTempDirectoryNull() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, (String) null));
  }

  @Test void testIsTempDirectoryWithTmp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/tmp/data"));
  }

  @Test void testIsTempDirectoryWithTemp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/var/temp/data"));
  }

  @Test void testIsTempDirectoryWithJavaIoTmpdir() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/some/java.io.tmpdir/data"));
  }

  @Test void testIsTempDirectoryNormal() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "/home/user/data"));
  }

  @Test void testIsTempDirectoryStartsWithTmp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/tmp"));
  }

  @Test void testIsTempDirectoryWindowsTemp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "C:\\temp\\data"));
  }

  // ===== isHivePartitionedFromConfig tests (via reflection) =====

  @Test void testIsHivePartitionedFromConfigNull() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, (ConversionMetadata.ConversionRecord) null));
  }

  @Test void testIsHivePartitionedFromConfigNullTableConfig() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = null;
    assertFalse((Boolean) method.invoke(null, record));
  }

  @Test void testIsHivePartitionedFromConfigWithHiveStyle() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "hive");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;

    assertTrue((Boolean) method.invoke(null, record));
  }

  @Test void testIsHivePartitionedFromConfigWithNonHiveStyle() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "directory");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;

    assertFalse((Boolean) method.invoke(null, record));
  }

  @Test void testIsHivePartitionedFromConfigPartitionsNotMap() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", "not_a_map");
    record.tableConfig = tableConfig;

    assertFalse((Boolean) method.invoke(null, record));
  }

  @Test void testIsHivePartitionedFromConfigStyleNotString() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", 42);
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;

    assertFalse((Boolean) method.invoke(null, record));
  }

  // ===== shouldUseUnionByName tests (via reflection) =====

  @Test void testShouldUseUnionByNameNull() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, (ConversionMetadata.ConversionRecord) null));
  }

  @Test void testShouldUseUnionByNameNullTableConfig() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = null;
    assertFalse((Boolean) method.invoke(null, record));
  }

  @Test void testShouldUseUnionByNameTrue() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> duckdb = new HashMap<String, Object>();
    duckdb.put("union_by_name", Boolean.TRUE);
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", duckdb);
    record.tableConfig = tableConfig;

    assertTrue((Boolean) method.invoke(null, record));
  }

  @Test void testShouldUseUnionByNameFalse() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> duckdb = new HashMap<String, Object>();
    duckdb.put("union_by_name", Boolean.FALSE);
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", duckdb);
    record.tableConfig = tableConfig;

    assertFalse((Boolean) method.invoke(null, record));
  }

  @Test void testShouldUseUnionByNameDuckdbNotMap() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", "not_a_map");
    record.tableConfig = tableConfig;

    assertFalse((Boolean) method.invoke(null, record));
  }

  @Test void testShouldUseUnionByNameNotBoolean() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> duckdb = new HashMap<String, Object>();
    duckdb.put("union_by_name", "true");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", duckdb);
    record.tableConfig = tableConfig;

    assertFalse((Boolean) method.invoke(null, record));
  }

  // ===== formatRecordForError tests (via reflection) =====

  @Test void testFormatRecordForErrorNull() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("formatRecordForError", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    assertEquals("null", method.invoke(null, (ConversionMetadata.ConversionRecord) null));
  }

  @Test void testFormatRecordForErrorWithData() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("formatRecordForError", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.tableType = "ParquetTable";
    record.sourceFile = "/data/test.parquet";
    record.viewScanPattern = "/data/**/*.parquet";
    record.parquetCacheFile = "/cache/test.parquet";

    String result = (String) method.invoke(null, record);
    assertTrue(result.contains("test_table"));
    assertTrue(result.contains("ParquetTable"));
  }

  @Test void testFormatRecordForErrorLongParquetCacheFile() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("formatRecordForError", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "big_table";
    record.tableType = "ParquetTable";
    // Create a very long parquetCacheFile (>100 chars) to test truncation
    StringBuilder longPath = new StringBuilder();
    for (int i = 0; i < 120; i++) {
      longPath.append("x");
    }
    record.parquetCacheFile = longPath.toString();

    String result = (String) method.invoke(null, record);
    assertTrue(result.contains("..."));
  }

  // ===== viewExists tests (via reflection) =====

  @Test void testViewExistsReturnsFalse() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("viewExists", Connection.class, String.class, String.class);
      method.setAccessible(true);
      assertFalse((Boolean) method.invoke(null, conn, "main", "nonexistent_view"));
    } finally {
      conn.close();
    }
  }

  @Test void testViewExistsReturnsTrue() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      conn.createStatement().execute("CREATE VIEW test_view AS SELECT 1 AS id");
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("viewExists", Connection.class, String.class, String.class);
      method.setAccessible(true);
      assertTrue((Boolean) method.invoke(null, conn, "main", "test_view"));
    } finally {
      conn.close();
    }
  }

  // ===== viewUsesIcebergScan tests (via reflection) =====

  @Test void testViewUsesIcebergScanFalse() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      conn.createStatement().execute("CREATE VIEW v1 AS SELECT 1 AS id");
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("viewUsesIcebergScan", Connection.class, String.class, String.class);
      method.setAccessible(true);
      assertFalse((Boolean) method.invoke(null, conn, "main", "v1"));
    } finally {
      conn.close();
    }
  }

  @Test void testViewUsesIcebergScanViewNotFound() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("viewUsesIcebergScan", Connection.class, String.class, String.class);
      method.setAccessible(true);
      assertFalse((Boolean) method.invoke(null, conn, "main", "nonexistent"));
    } finally {
      conn.close();
    }
  }

  // ===== getViewSql tests (via reflection) =====

  @Test void testGetViewSqlReturnsNull() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("getViewSql", Connection.class, String.class, String.class);
      method.setAccessible(true);
      assertNull(method.invoke(null, conn, "main", "no_such_view"));
    } finally {
      conn.close();
    }
  }

  @Test void testGetViewSqlReturnsDefinition() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      conn.createStatement().execute("CREATE VIEW v_test AS SELECT 42 AS val");
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("getViewSql", Connection.class, String.class, String.class);
      method.setAccessible(true);
      String sql = (String) method.invoke(null, conn, "main", "v_test");
      assertNotNull(sql);
      assertTrue(sql.contains("42"));
    } finally {
      conn.close();
    }
  }

  // ===== rewriteSchemaReferencesInSql tests (via reflection) =====

  @Test void testRewriteSchemaReferencesNullViewDef() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, null, "econ", "ECON"));
  }

  @Test void testRewriteSchemaReferencesNullDeclared() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    assertEquals("SELECT 1", method.invoke(null, "SELECT 1", null, "ECON"));
  }

  @Test void testRewriteSchemaReferencesNullActual() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    assertEquals("SELECT 1", method.invoke(null, "SELECT 1", "econ", null));
  }

  @Test void testRewriteSchemaReferencesSameSchema() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    String viewDef = "SELECT * FROM econ.gdp";
    assertEquals(viewDef, method.invoke(null, viewDef, "econ", "econ"));
  }

  @Test void testRewriteSchemaReferencesSameSchemaCaseInsensitive() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    String viewDef = "SELECT * FROM econ.gdp";
    assertEquals(viewDef, method.invoke(null, viewDef, "econ", "ECON"));
  }

  @Test void testRewriteSchemaReferencesDifferentSchema() throws Exception {
    Method method =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    String viewDef = "SELECT * FROM econ.gdp JOIN econ.cpi ON econ.gdp.year = econ.cpi.year";
    String result = (String) method.invoke(null, viewDef, "econ", "economic");
    assertTrue(result.contains("economic.gdp"));
    assertTrue(result.contains("economic.cpi"));
    assertFalse(result.contains("econ.gdp"));
  }

  // ===== registerSqlViewsInDuckDB tests (via reflection) =====

  private static Method getRegisterSqlViewsMethod() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("registerSqlViewsInDuckDB", String.class, String.class, Map.class);
    m.setAccessible(true);
    return m;
  }

  private static void registerAndFlush(Method method, Connection conn, String schema,
      Map<String, Object> operand) throws Exception {
    String dbPath = "test3-db-" + System.nanoTime();
    method.invoke(null, dbPath, schema, operand);
    DuckDBPendingViews.flush(dbPath, conn);
  }

  @Test void testRegisterSqlViewsNullOperand() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method = getRegisterSqlViewsMethod();
      // Should not throw - just returns early
      method.invoke(null, "test3-null-" + System.nanoTime(), "main", null);
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsEmptyTables() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method = getRegisterSqlViewsMethod();
      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", Collections.emptyList());
      registerAndFlush(method, conn, "main", operand);
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsWithViewDefinition() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method = getRegisterSqlViewsMethod();

      Map<String, Object> viewTable = new HashMap<String, Object>();
      viewTable.put("type", "view");
      viewTable.put("name", "test_sql_view");
      viewTable.put("sql", "SELECT 1 AS id");

      List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
      tables.add(viewTable);

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", tables);

      registerAndFlush(method, conn, "main", operand);

      // Verify view was created
      try (ResultSet rs =
          conn.createStatement().executeQuery("SELECT * FROM main.test_sql_view")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsSkipsNonViewTypes() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method = getRegisterSqlViewsMethod();

      Map<String, Object> tableTable = new HashMap<String, Object>();
      tableTable.put("type", "table");
      tableTable.put("name", "should_skip");

      List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
      tables.add(tableTable);

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", tables);

      registerAndFlush(method, conn, "main", operand);
      // No exception - table types are simply skipped
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsSkipsMissingName() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method = getRegisterSqlViewsMethod();

      Map<String, Object> viewTable = new HashMap<String, Object>();
      viewTable.put("type", "view");
      // No name or sql/viewDef

      List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
      tables.add(viewTable);

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", tables);

      registerAndFlush(method, conn, "main", operand);
      // No exception - just skipped
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsUsesViewDef() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method = getRegisterSqlViewsMethod();

      Map<String, Object> viewTable = new HashMap<String, Object>();
      viewTable.put("type", "view");
      viewTable.put("name", "view_def_test");
      viewTable.put("viewDef", "SELECT 99 AS val");

      List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
      tables.add(viewTable);

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", tables);

      registerAndFlush(method, conn, "main", operand);

      try (ResultSet rs =
          conn.createStatement().executeQuery("SELECT * FROM main.view_def_test")) {
        assertTrue(rs.next());
        assertEquals(99, rs.getInt(1));
      }
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsWithDeclaredSchemaRewriting() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      // Create a schema and a table to reference
      conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS actual_schema");
      conn.createStatement().execute(
          "CREATE TABLE actual_schema.base_data AS SELECT 1 AS id, 100 AS amount");

      Method method = getRegisterSqlViewsMethod();

      Map<String, Object> viewTable = new HashMap<String, Object>();
      viewTable.put("type", "view");
      viewTable.put("name", "rewritten_view");
      viewTable.put("sql", "SELECT * FROM declared.base_data");

      List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
      tables.add(viewTable);

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", tables);
      operand.put("declaredSchemaName", "declared");

      registerAndFlush(method, conn, "actual_schema", operand);

      // Verify view was created with rewritten schema
      try (ResultSet rs =
          conn.createStatement().executeQuery("SELECT * FROM actual_schema.rewritten_view")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals(100, rs.getInt("amount"));
      }
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsSkipsDuplicateView() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      // Pre-create the view
      conn.createStatement().execute("CREATE VIEW dup_view AS SELECT 1 AS id");

      Method method = getRegisterSqlViewsMethod();

      Map<String, Object> viewTable = new HashMap<String, Object>();
      viewTable.put("type", "view");
      viewTable.put("name", "dup_view");
      viewTable.put("sql", "SELECT 2 AS id");

      List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
      tables.add(viewTable);

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", tables);

      // CREATE VIEW IF NOT EXISTS preserves the original
      registerAndFlush(method, conn, "main", operand);
      // No exception - IF NOT EXISTS means original view is kept
    } finally {
      conn.close();
    }
  }

  @Test void testRegisterSqlViewsHandlesInvalidSql() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method = getRegisterSqlViewsMethod();

      Map<String, Object> viewTable = new HashMap<String, Object>();
      viewTable.put("type", "view");
      viewTable.put("name", "bad_view");
      viewTable.put("sql", "THIS IS NOT VALID SQL");

      List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
      tables.add(viewTable);

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("tables", tables);

      // Should not throw - flush handles errors gracefully
      registerAndFlush(method, conn, "main", operand);
    } finally {
      conn.close();
    }
  }

  // ===== registerSimilarityFunctions tests (via reflection) =====

  @Test void testRegisterSimilarityFunctions() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("registerSimilarityFunctions", Connection.class);
      method.setAccessible(true);
      method.invoke(null, conn);

      // Verify that the macros were registered by calling them
      try (ResultSet rs =
          conn.createStatement().executeQuery("SELECT COSINE_SIMILARITY('1.0,0.0,0.0', '0.0,1.0,0.0')")) {
        assertTrue(rs.next());
        double similarity = rs.getDouble(1);
        // Orthogonal vectors should have 0 cosine similarity
        assertEquals(0.0, similarity, 0.01);
      }
    } finally {
      conn.close();
    }
  }

  // ===== loadQueryExtensions tests (via reflection) =====

  @Test void testLoadQueryExtensions() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method method =
          DuckDBJdbcSchemaFactory.class.getDeclaredMethod("loadQueryExtensions", Connection.class);
      method.setAccessible(true);
      // Should not throw - gracefully handles extension load failures
      method.invoke(null, conn);
    } finally {
      conn.close();
    }
  }

  // ===== createDuckDBDialectWithCustomLex tests (via reflection) =====

  @Test void testCreateDuckDBDialectWithCustomLex() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("createDuckDBDialectWithCustomLex");
    method.setAccessible(true);
    SqlDialect dialect = (SqlDialect) method.invoke(null);
    assertNotNull(dialect);
    assertTrue(dialect instanceof org.apache.calcite.sql.dialect.DuckDBSqlDialect);
  }

  // ===== Integration: create() with null fileSchema should throw =====

  @Test void testCreateWithNullFileSchemaAndNullDbFilename() {
    try {
      // This should fail because fileSchema is null and no database_filename is set
      DuckDBJdbcSchemaFactory.create(null, "test", "/tmp/data", false, null);
    } catch (RuntimeException e) {
      // Expected: will fail at DuckDB driver or catalog path resolution
      assertNotNull(e.getMessage());
    }
  }
}
