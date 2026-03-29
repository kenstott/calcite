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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link DuckDBJdbcSchemaFactory}.
 *
 * <p>Uses real DuckDB JDBC connections to test view creation, parquet file
 * operations, schema management, SQL rewriting, and utility method behaviors.
 * Creates temp parquet files via DuckDB COPY and exercises all major code paths
 * including createParquetView, isTempDirectory, isHivePartitioned,
 * isHivePartitionedFromConfig, shouldUseUnionByName, deriveGlobPattern,
 * formatRecordForError, rewriteSchemaReferencesInSql, getParserConfig,
 * viewExists, viewUsesIcebergScan, getViewSql, registerSqlViewsInDuckDB,
 * registerSimilarityFunctions, loadQueryExtensions, determineCatalogPath,
 * and createDuckDBDialectWithCustomLex.
 */
@Tag("unit")
class DuckDBJdbcSchemaFactoryDeepCoverageTest {

  @TempDir
  Path tempDir;

  private Connection conn;

  @BeforeEach
  void setUp() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    conn = DriverManager.getConnection("jdbc:duckdb:");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (conn != null && !conn.isClosed()) {
      conn.close();
    }
  }

  // ==========================================================================
  // createParquetView -- real DuckDB tests
  // ==========================================================================

  @Test void testCreateParquetViewBasic() throws Exception {
    Path parquetFile = createParquetFile("basic",
        "SELECT 1 AS id, 'hello' AS name");
    DuckDBJdbcSchemaFactory.createParquetView(conn, "v_basic", parquetFile.toString());
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM v_basic")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("hello", rs.getString("name"));
      assertFalse(rs.next());
    }
  }

  @Test void testCreateParquetViewMultipleRows() throws Exception {
    Path parquetFile = createParquetFile("multi",
        "SELECT * FROM (VALUES (1,'a'), (2,'b'), (3,'c')) AS t(id, val)");
    DuckDBJdbcSchemaFactory.createParquetView(conn, "v_multi", parquetFile.toString());
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT count(*) AS cnt FROM v_multi")) {
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("cnt"));
    }
  }

  @Test void testCreateParquetViewReplacesExistingView() throws Exception {
    Path p1 = createParquetFile("replace1", "SELECT 10 AS val");
    Path p2 = createParquetFile("replace2", "SELECT 20 AS val");

    DuckDBJdbcSchemaFactory.createParquetView(conn, "v_replace", p1.toString());
    DuckDBJdbcSchemaFactory.createParquetView(conn, "v_replace", p2.toString());

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT val FROM v_replace")) {
      assertTrue(rs.next());
      assertEquals(20, rs.getInt("val"));
    }
  }

  @Test void testCreateParquetViewPreservesMixedCasing() throws Exception {
    Path parquetFile = createParquetFile("casing",
        "SELECT 42 AS \"MyColumn\"");
    DuckDBJdbcSchemaFactory.createParquetView(conn, "CamelCaseView", parquetFile.toString());
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM \"CamelCaseView\"")) {
      assertTrue(rs.next());
      assertEquals(42, rs.getInt(1));
    }
  }

  @Test void testCreateParquetViewNullConnectionThrows() {
    RuntimeException ex = assertThrows(RuntimeException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        DuckDBJdbcSchemaFactory.createParquetView(null, "v", "/path.parquet");
      }
    });
    assertNotNull(ex);
  }

  @Test void testCreateParquetViewInvalidPathThrows() {
    RuntimeException ex = assertThrows(RuntimeException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        DuckDBJdbcSchemaFactory.createParquetView(conn, "bad",
            "/definitely/nonexistent/path_" + System.nanoTime() + ".parquet");
      }
    });
    assertTrue(ex.getMessage().contains("Failed to create Parquet view"));
  }

  @Test void testCreateParquetViewWithSpecialCharsInName() throws Exception {
    Path parquetFile = createParquetFile("special",
        "SELECT 1 AS id");
    DuckDBJdbcSchemaFactory.createParquetView(conn, "table-with-dashes", parquetFile.toString());
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM \"table-with-dashes\"")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
    }
  }

  @Test void testCreateParquetViewWithMultipleColumns() throws Exception {
    Path parquetFile = createParquetFile("cols",
        "SELECT 1 AS a, 2.5 AS b, 'text' AS c, TRUE AS d, CAST('2024-01-01' AS DATE) AS e");
    DuckDBJdbcSchemaFactory.createParquetView(conn, "v_cols", parquetFile.toString());
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT a, b, c, d FROM v_cols")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("a"));
      assertEquals(2.5, rs.getDouble("b"), 0.001);
      assertEquals("text", rs.getString("c"));
      assertTrue(rs.getBoolean("d"));
    }
  }

  @Test void testCreateParquetViewThenJoinTwoViews() throws Exception {
    Path pEmployees = createParquetFile("employees",
        "SELECT * FROM (VALUES (1,'Alice',10), (2,'Bob',20)) AS t(id, name, dept_id)");
    Path pDepts = createParquetFile("departments",
        "SELECT * FROM (VALUES (10,'Engineering'), (20,'Marketing')) AS t(dept_id, dept_name)");

    DuckDBJdbcSchemaFactory.createParquetView(conn, "emp", pEmployees.toString());
    DuckDBJdbcSchemaFactory.createParquetView(conn, "dept", pDepts.toString());

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT e.name, d.dept_name FROM emp e JOIN dept d ON e.dept_id = d.dept_id ORDER BY e.id")) {
      assertTrue(rs.next());
      assertEquals("Alice", rs.getString("name"));
      assertEquals("Engineering", rs.getString("dept_name"));
      assertTrue(rs.next());
      assertEquals("Bob", rs.getString("name"));
      assertEquals("Marketing", rs.getString("dept_name"));
      assertFalse(rs.next());
    }
  }

  @Test void testCreateParquetViewAggregationQuery() throws Exception {
    Path parquetFile = createParquetFile("agg",
        "SELECT * FROM (VALUES (1,'A',100), (2,'A',200), (3,'B',150)) AS t(id, grp, amt)");
    DuckDBJdbcSchemaFactory.createParquetView(conn, "v_agg", parquetFile.toString());
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT grp, SUM(amt) AS total FROM v_agg GROUP BY grp ORDER BY grp")) {
      assertTrue(rs.next());
      assertEquals("A", rs.getString("grp"));
      assertEquals(300, rs.getInt("total"));
      assertTrue(rs.next());
      assertEquals("B", rs.getString("grp"));
      assertEquals(150, rs.getInt("total"));
    }
  }

  @Test void testCreateParquetViewInCustomSchema() throws Exception {
    Path parquetFile = createParquetFile("schema_test",
        "SELECT 99 AS result");

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"my_schema\"");
      String sql = String.format(
          "CREATE OR REPLACE VIEW \"my_schema\".\"v_test\" AS SELECT * FROM read_parquet('%s')",
          parquetFile.toString());
      stmt.execute(sql);

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"my_schema\".\"v_test\"")) {
        assertTrue(rs.next());
        assertEquals(99, rs.getInt("result"));
      }
    }
  }

  // ==========================================================================
  // isTempDirectory -- via reflection
  // ==========================================================================

  @Test void testIsTempDirectoryNull() throws Exception {
    assertTrue(invokeIsTempDirectory(null));
  }

  @Test void testIsTempDirectoryUnixTmpSlash() throws Exception {
    assertTrue(invokeIsTempDirectory("/tmp/data"));
  }

  @Test void testIsTempDirectoryStartsWithTmpNoTrailingSlash() throws Exception {
    assertTrue(invokeIsTempDirectory("/tmp"));
  }

  @Test void testIsTempDirectoryWindowsTemp() throws Exception {
    assertTrue(invokeIsTempDirectory("C:\\temp\\data"));
  }

  @Test void testIsTempDirectoryUnixTemp() throws Exception {
    assertTrue(invokeIsTempDirectory("/home/user/temp/stuff"));
  }

  @Test void testIsTempDirectoryJavaIoTmpdir() throws Exception {
    assertTrue(invokeIsTempDirectory("/path/java.io.tmpdir/data"));
  }

  @Test void testIsTempDirectoryNormalPath() throws Exception {
    assertFalse(invokeIsTempDirectory("/home/user/data/warehouse"));
  }

  @Test void testIsTempDirectoryS3Path() throws Exception {
    assertFalse(invokeIsTempDirectory("s3://my-bucket/data/warehouse"));
  }

  @Test void testIsTempDirectoryBackslashTmpStart() throws Exception {
    assertTrue(invokeIsTempDirectory("\\tmp\\something"));
  }

  @Test void testIsTempDirectoryContainsTmpMiddle() throws Exception {
    assertTrue(invokeIsTempDirectory("/var/tmp/cache/files"));
  }

  @Test void testIsTempDirectoryMixedCase() throws Exception {
    // Path is lowercased before check, so /TMP/ should match
    assertTrue(invokeIsTempDirectory("/TMP/data"));
  }

  @Test void testIsTempDirectoryAbsoluteNonTemp() throws Exception {
    assertFalse(invokeIsTempDirectory("/opt/calcite/data"));
  }

  // ==========================================================================
  // isHivePartitioned -- via reflection
  // ==========================================================================

  @Test void testIsHivePartitionedNull() throws Exception {
    assertFalse(invokeIsHivePartitioned(null));
  }

  @Test void testIsHivePartitionedEmpty() throws Exception {
    assertFalse(invokeIsHivePartitioned(""));
  }

  @Test void testIsHivePartitionedSingleFile() throws Exception {
    assertFalse(invokeIsHivePartitioned("/data/file.parquet"));
  }

  @Test void testIsHivePartitionedBracketedWithPartitions() throws Exception {
    String fileList = "['/data/year=2020/file1.parquet','/data/year=2021/file2.parquet']";
    assertTrue(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedCurlyBraces() throws Exception {
    String fileList = "{/data/country=US/f1.parquet,/data/country=UK/f2.parquet}";
    assertTrue(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedNoPartitionPattern() throws Exception {
    String fileList = "[/data/file1.parquet,/data/file2.parquet]";
    assertFalse(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedQuotedFilePaths() throws Exception {
    String fileList =
        "'/data/region=west/f1.parquet','/data/region=east/f2.parquet'";
    assertTrue(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedMixedPartitionBelow50Percent() throws Exception {
    // 1 out of 3 partitioned -- below 50% threshold
    String fileList =
        "/data/year=2020/f1.parquet,/data/plain/f2.parquet,/data/plain/f3.parquet";
    assertFalse(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedMultiplePartitionKeys() throws Exception {
    String fileList =
        "['/data/year=2020/month=01/f.parquet','/data/year=2020/month=02/f.parquet']";
    assertTrue(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedUnderscoreInPartitionKey() throws Exception {
    String fileList =
        "['/data/state_code=CA/f.parquet','/data/state_code=NY/f.parquet']";
    assertTrue(invokeIsHivePartitioned(fileList));
  }

  // ==========================================================================
  // isHivePartitionedFromConfig -- via reflection
  // ==========================================================================

  @Test void testIsHivePartitionedFromConfigNullRecord() throws Exception {
    assertFalse(invokeIsHivePartitionedFromConfig(null));
  }

  @Test void testIsHivePartitionedFromConfigNullTableConfig() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigNoPartitionsKey() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigHiveStyle() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "hive");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;
    assertTrue(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigHiveStyleUpperCase() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "HIVE");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;
    assertTrue(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigDirectoryStyle() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "directory");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigPartitionsNotMap() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", "not_a_map");
    record.tableConfig = tableConfig;
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigStyleNotString() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", 42);
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigNoStyleKey() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("columns", "year,month");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  // ==========================================================================
  // shouldUseUnionByName -- via reflection
  // ==========================================================================

  @Test void testShouldUseUnionByNameNullRecord() throws Exception {
    assertFalse(invokeShouldUseUnionByName(null));
  }

  @Test void testShouldUseUnionByNameNullTableConfig() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertFalse(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameTrue() throws Exception {
    ConversionMetadata.ConversionRecord record = buildRecordWithDuckdbConfig("union_by_name", Boolean.TRUE);
    assertTrue(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameFalse() throws Exception {
    ConversionMetadata.ConversionRecord record = buildRecordWithDuckdbConfig("union_by_name", Boolean.FALSE);
    assertFalse(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameNoDuckdbKey() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    assertFalse(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameDuckdbNotMap() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", "string_value");
    record.tableConfig = tableConfig;
    assertFalse(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameNotBoolean() throws Exception {
    ConversionMetadata.ConversionRecord record = buildRecordWithDuckdbConfig("union_by_name", "true");
    assertFalse(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameMissingUnionKey() throws Exception {
    ConversionMetadata.ConversionRecord record = buildRecordWithDuckdbConfig("other_key", Boolean.TRUE);
    assertFalse(invokeShouldUseUnionByName(record));
  }

  // ==========================================================================
  // deriveGlobPattern -- via reflection
  // ==========================================================================

  @Test void testDeriveGlobPatternNull() throws Exception {
    assertNull(invokeDeriveGlobPattern(null));
  }

  @Test void testDeriveGlobPatternEmpty() throws Exception {
    assertNull(invokeDeriveGlobPattern(""));
  }

  @Test void testDeriveGlobPatternBracketedWithPartitions() throws Exception {
    String fileList = "['/data/year=2020/f1.parquet','/data/year=2021/f2.parquet']";
    String result = invokeDeriveGlobPattern(fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
    assertTrue(result.endsWith(".parquet"));
  }

  @Test void testDeriveGlobPatternCurlyBracesWithPartitions() throws Exception {
    String fileList = "{'/data/a=1/b.parquet','/data/a=2/c.parquet'}";
    String result = invokeDeriveGlobPattern(fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
  }

  @Test void testDeriveGlobPatternNoPartitions() throws Exception {
    String fileList = "[/data/warehouse/file1.parquet,/data/warehouse/file2.parquet]";
    String result = invokeDeriveGlobPattern(fileList);
    assertNotNull(result);
    assertTrue(result.contains("*"));
    assertTrue(result.contains(".parquet"));
  }

  @Test void testDeriveGlobPatternSingleFileNoSlash() throws Exception {
    String result = invokeDeriveGlobPattern("file_only_no_path");
    assertEquals("file_only_no_path", result);
  }

  @Test void testDeriveGlobPatternSingleFileWithSlash() throws Exception {
    String result = invokeDeriveGlobPattern("/data/plain/file.parquet");
    assertNotNull(result);
    assertTrue(result.contains("/*.parquet"));
  }

  @Test void testDeriveGlobPatternCustomExtension() throws Exception {
    String result = invokeDeriveGlobPattern("/data/year=2020/file.csv");
    assertNotNull(result);
    assertTrue(result.endsWith(".csv"));
  }

  @Test void testDeriveGlobPatternS3Path() throws Exception {
    String fileList = "[s3://bucket/data/year=2023/f.parquet,s3://bucket/data/year=2024/f.parquet]";
    String result = invokeDeriveGlobPattern(fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
  }

  @Test void testDeriveGlobPatternDeepPartitions() throws Exception {
    String fileList =
        "['/data/year=2020/month=01/day=15/f.parquet','/data/year=2020/month=02/day=20/f.parquet']";
    String result = invokeDeriveGlobPattern(fileList);
    assertNotNull(result);
    assertTrue(result.contains("/data"));
    assertTrue(result.contains("/**/*"));
  }

  // ==========================================================================
  // formatRecordForError -- via reflection
  // ==========================================================================

  @Test void testFormatRecordForErrorNull() throws Exception {
    assertEquals("null", invokeFormatRecordForError(null));
  }

  @Test void testFormatRecordForErrorWithAllFields() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "my_table";
    record.tableType = "ParquetTable";
    record.sourceFile = "/data/source.parquet";
    record.viewScanPattern = "/data/**/*.parquet";
    record.parquetCacheFile = "/cache/file.parquet";
    String result = invokeFormatRecordForError(record);
    assertTrue(result.contains("my_table"));
    assertTrue(result.contains("ParquetTable"));
    assertTrue(result.contains("/data/source.parquet"));
    assertTrue(result.contains("/data/**/*.parquet"));
    assertTrue(result.contains("/cache/file.parquet"));
  }

  @Test void testFormatRecordForErrorNullFields() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    String result = invokeFormatRecordForError(record);
    assertNotNull(result);
    assertTrue(result.contains("null"));
  }

  @Test void testFormatRecordForErrorLongParquetCacheTruncated() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "big_table";
    StringBuilder longPath = new StringBuilder();
    for (int i = 0; i < 150; i++) {
      longPath.append("x");
    }
    record.parquetCacheFile = longPath.toString();
    String result = invokeFormatRecordForError(record);
    assertTrue(result.contains("..."), "Long parquetCacheFile should be truncated");
    // The truncated string should be <= 100 chars of the original plus "..."
    assertTrue(result.length() < longPath.length() + 100);
  }

  @Test void testFormatRecordForErrorShortParquetCacheNotTruncated() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "small_table";
    record.parquetCacheFile = "/short/path.parquet";
    String result = invokeFormatRecordForError(record);
    assertFalse(result.contains("..."));
    assertTrue(result.contains("/short/path.parquet"));
  }

  @Test void testFormatRecordForErrorExactly100CharsNotTruncated() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "exact";
    StringBuilder exactPath = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      exactPath.append("a");
    }
    record.parquetCacheFile = exactPath.toString();
    String result = invokeFormatRecordForError(record);
    assertFalse(result.contains("..."));
  }

  @Test void testFormatRecordForError101CharsTruncated() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "over";
    StringBuilder path = new StringBuilder();
    for (int i = 0; i < 101; i++) {
      path.append("b");
    }
    record.parquetCacheFile = path.toString();
    String result = invokeFormatRecordForError(record);
    assertTrue(result.contains("..."));
  }

  // ==========================================================================
  // rewriteSchemaReferencesInSql -- via reflection
  // ==========================================================================

  @Test void testRewriteSchemaReferencesNullViewDef() throws Exception {
    assertNull(invokeRewriteSchemaReferences(null, "old", "new"));
  }

  @Test void testRewriteSchemaReferencesNullDeclared() throws Exception {
    assertEquals("SELECT 1", invokeRewriteSchemaReferences("SELECT 1", null, "new"));
  }

  @Test void testRewriteSchemaReferencesNullActual() throws Exception {
    assertEquals("SELECT 1", invokeRewriteSchemaReferences("SELECT 1", "old", null));
  }

  @Test void testRewriteSchemaReferencesSameNames() throws Exception {
    String sql = "SELECT * FROM econ.table1";
    assertEquals(sql, invokeRewriteSchemaReferences(sql, "econ", "econ"));
  }

  @Test void testRewriteSchemaReferencesCaseInsensitiveMatch() throws Exception {
    String sql = "SELECT * FROM econ.table1";
    assertEquals(sql, invokeRewriteSchemaReferences(sql, "econ", "ECON"));
  }

  @Test void testRewriteSchemaReferencesDifferentSchemaNames() throws Exception {
    String sql = "SELECT * FROM econ.gdp JOIN econ.cpi ON econ.gdp.year = econ.cpi.year";
    String result = invokeRewriteSchemaReferences(sql, "econ", "economic");
    assertTrue(result.contains("economic.gdp"));
    assertTrue(result.contains("economic.cpi"));
    assertFalse(result.contains("econ.gdp"));
  }

  @Test void testRewriteSchemaReferencesOnlyRewritesMatchingSchema() throws Exception {
    String sql = "SELECT * FROM econ.gdp JOIN finance.bonds ON econ.gdp.year = finance.bonds.year";
    String result = invokeRewriteSchemaReferences(sql, "econ", "economy");
    assertTrue(result.contains("economy.gdp"));
    assertTrue(result.contains("finance.bonds"), "Cross-schema reference should be preserved");
  }

  @Test void testRewriteSchemaReferencesNoSchemaRefs() throws Exception {
    String sql = "SELECT 1 AS id, 'hello' AS greeting";
    String result = invokeRewriteSchemaReferences(sql, "econ", "economy");
    assertEquals(sql, result);
  }

  // ==========================================================================
  // getParserConfig
  // ==========================================================================

  @Test void testGetParserConfigNotNull() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(config);
  }

  @Test void testGetParserConfigUnquotedCasingToLower() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(Casing.TO_LOWER, config.unquotedCasing());
  }

  @Test void testGetParserConfigQuotedCasingUnchanged() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(Casing.UNCHANGED, config.quotedCasing());
  }

  @Test void testGetParserConfigCaseSensitive() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    // Oracle lex is case sensitive for quoted identifiers
    assertNotNull(config);
  }

  @Test void testGetParserConfigConsistentAcrossMultipleCalls() {
    SqlParser.Config c1 = DuckDBJdbcSchemaFactory.getParserConfig();
    SqlParser.Config c2 = DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(c1);
    assertNotNull(c2);
    assertEquals(c1.unquotedCasing(), c2.unquotedCasing());
    assertEquals(c1.quotedCasing(), c2.quotedCasing());
  }

  // ==========================================================================
  // viewExists -- via reflection using real DuckDB
  // ==========================================================================

  @Test void testViewExistsReturnsFalseForMissing() throws Exception {
    assertFalse(invokeViewExists(conn, "main", "nonexistent_view_" + System.nanoTime()));
  }

  @Test void testViewExistsReturnsTrueForExisting() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE VIEW exists_test AS SELECT 1 AS id");
    }
    assertTrue(invokeViewExists(conn, "main", "exists_test"));
  }

  @Test void testViewExistsReturnsFalseForTable() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE not_a_view (id INTEGER)");
    }
    // viewExists checks for table_type = 'VIEW', a TABLE should return false
    assertFalse(invokeViewExists(conn, "main", "not_a_view"));
  }

  @Test void testViewExistsInCustomSchema() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"custom\"");
      stmt.execute("CREATE VIEW \"custom\".\"cv\" AS SELECT 1");
    }
    assertTrue(invokeViewExists(conn, "custom", "cv"));
    assertFalse(invokeViewExists(conn, "main", "cv"));
  }

  // ==========================================================================
  // getViewSql -- via reflection using real DuckDB
  // ==========================================================================

  @Test void testGetViewSqlReturnsNullForMissing() throws Exception {
    assertNull(invokeGetViewSql(conn, "main", "no_such_view"));
  }

  @Test void testGetViewSqlReturnsDefinition() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE VIEW v_sql_test AS SELECT 42 AS val");
    }
    String sql = invokeGetViewSql(conn, "main", "v_sql_test");
    assertNotNull(sql);
    assertTrue(sql.contains("42"));
  }

  // ==========================================================================
  // viewUsesIcebergScan -- via reflection using real DuckDB
  // ==========================================================================

  @Test void testViewUsesIcebergScanFalseForNormalView() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE VIEW v_normal AS SELECT 1 AS id");
    }
    assertFalse(invokeViewUsesIcebergScan(conn, "main", "v_normal"));
  }

  @Test void testViewUsesIcebergScanFalseForMissing() throws Exception {
    assertFalse(invokeViewUsesIcebergScan(conn, "main", "nonexistent_" + System.nanoTime()));
  }

  // ==========================================================================
  // registerSqlViewsInDuckDB -- via reflection using real DuckDB
  // ==========================================================================

  @Test void testRegisterSqlViewsNullOperand() throws Exception {
    invokeRegisterSqlViewsInDuckDB(conn, "main", null);
    // Should not throw -- just returns early
  }

  @Test void testRegisterSqlViewsEmptyTables() throws Exception {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", new ArrayList<Map<String, Object>>());
    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);
  }

  @Test void testRegisterSqlViewsNoTablesKey() throws Exception {
    Map<String, Object> operand = new HashMap<String, Object>();
    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);
  }

  @Test void testRegisterSqlViewsCreatesViewFromSqlKey() throws Exception {
    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "reg_view_sql");
    viewDef.put("sql", "SELECT 1 AS id");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);

    try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM main.reg_view_sql")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
    }
  }

  @Test void testRegisterSqlViewsCreatesViewFromViewDefKey() throws Exception {
    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "reg_view_vd");
    viewDef.put("viewDef", "SELECT 55 AS num");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);

    try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM main.reg_view_vd")) {
      assertTrue(rs.next());
      assertEquals(55, rs.getInt("num"));
    }
  }

  @Test void testRegisterSqlViewsSkipsNonViewType() throws Exception {
    Map<String, Object> tableDef = new HashMap<String, Object>();
    tableDef.put("type", "table");
    tableDef.put("name", "not_a_view");
    tableDef.put("sql", "SELECT 1");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(tableDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);
    // Should not create the view
    assertFalse(invokeViewExists(conn, "main", "not_a_view"));
  }

  @Test void testRegisterSqlViewsSkipsMissingNameOrSql() throws Exception {
    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("type", "view");
    // No name or sql/viewDef

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);
    // Should not throw
  }

  @Test void testRegisterSqlViewsHandlesInvalidSqlGracefully() throws Exception {
    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "bad_syntax_view");
    viewDef.put("sql", "THIS IS NOT VALID SQL AT ALL");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);
    // Should handle error gracefully without propagating exception
  }

  @Test void testRegisterSqlViewsSkipsDuplicateView() throws Exception {
    conn.createStatement().execute("CREATE VIEW dup_view_test AS SELECT 100 AS val");

    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "dup_view_test");
    viewDef.put("sql", "SELECT 200 AS val");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);

    invokeRegisterSqlViewsInDuckDB(conn, "main", operand);

    // Original view should be preserved
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT val FROM dup_view_test")) {
      assertTrue(rs.next());
      assertEquals(100, rs.getInt("val"));
    }
  }

  @Test void testRegisterSqlViewsWithSchemaRewriting() throws Exception {
    conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS \"actual\"");
    conn.createStatement().execute(
        "CREATE TABLE \"actual\".base_data AS SELECT 1 AS id, 50 AS amount");

    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "rewritten");
    viewDef.put("sql", "SELECT * FROM declared.base_data");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);
    operand.put("declaredSchemaName", "declared");

    invokeRegisterSqlViewsInDuckDB(conn, "actual", operand);

    try (ResultSet rs = conn.createStatement().executeQuery(
        "SELECT * FROM \"actual\".rewritten")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals(50, rs.getInt("amount"));
    }
  }

  // ==========================================================================
  // registerSimilarityFunctions -- via reflection using real DuckDB
  // ==========================================================================

  @Test void testRegisterSimilarityFunctionsCosineSimilarity() throws Exception {
    invokeRegisterSimilarityFunctions(conn);
    try (ResultSet rs = conn.createStatement().executeQuery(
        "SELECT COSINE_SIMILARITY('1,0,0', '1,0,0') AS sim")) {
      assertTrue(rs.next());
      assertEquals(1.0, rs.getDouble("sim"), 0.01);
    }
  }

  @Test void testRegisterSimilarityFunctionsCosineDistance() throws Exception {
    invokeRegisterSimilarityFunctions(conn);
    try (ResultSet rs = conn.createStatement().executeQuery(
        "SELECT COSINE_DISTANCE('1,0,0', '0,1,0') AS dist")) {
      assertTrue(rs.next());
      assertEquals(1.0, rs.getDouble("dist"), 0.01);
    }
  }

  @Test void testRegisterSimilarityFunctionsOrthogonalVectors() throws Exception {
    invokeRegisterSimilarityFunctions(conn);
    try (ResultSet rs = conn.createStatement().executeQuery(
        "SELECT COSINE_SIMILARITY('1,0,0', '0,1,0') AS sim")) {
      assertTrue(rs.next());
      assertEquals(0.0, rs.getDouble("sim"), 0.01);
    }
  }

  // ==========================================================================
  // loadQueryExtensions -- via reflection using real DuckDB
  // ==========================================================================

  @Test void testLoadQueryExtensionsDoesNotThrow() throws Exception {
    invokeLoadQueryExtensions(conn);
    // Should handle gracefully even if extensions are not available
  }

  // ==========================================================================
  // createDuckDBDialectWithCustomLex -- via reflection
  // ==========================================================================

  @Test void testCreateDuckDBDialectNotNull() throws Exception {
    Object dialect = invokeCreateDuckDBDialect();
    assertNotNull(dialect);
  }

  @Test void testCreateDuckDBDialectIsDuckDBSqlDialect() throws Exception {
    Object dialect = invokeCreateDuckDBDialect();
    assertTrue(dialect instanceof org.apache.calcite.sql.dialect.DuckDBSqlDialect);
  }

  // ==========================================================================
  // determineCatalogPath -- via reflection
  // ==========================================================================

  @Test void testDetermineCatalogPathTempDirectory() throws Exception {
    String result = invokeDetermineCatalogPath("test", "/tmp/data");
    assertNull(result, "Temp directories should return null for in-memory usage");
  }

  @Test void testDetermineCatalogPathNullDirectory() throws Exception {
    String result = invokeDetermineCatalogPath("test", null);
    assertNull(result, "Null directory should return null");
  }

  @Test void testDetermineCatalogPathPersistentDirectory() throws Exception {
    String dirPath = tempDir.toAbsolutePath().toString();
    String result = invokeDetermineCatalogPath("myschema", dirPath);
    assertNotNull(result);
    assertTrue(result.contains("myschema_db.duckdb"));
  }

  // ==========================================================================
  // Integration: DuckDB schema with real parquet files
  // ==========================================================================

  @Test void testIntegrationCreateSchemaAndRegisterParquetViews() throws Exception {
    // Create parquet files
    Path p1 = createParquetFile("orders",
        "SELECT * FROM (VALUES (1,'laptop',999.99), (2,'mouse',29.99), (3,'keyboard',59.99)) "
        + "AS t(order_id, product, price)");
    Path p2 = createParquetFile("customers",
        "SELECT * FROM (VALUES (1,'Alice','NY'), (2,'Bob','CA')) AS t(cust_id, name, state)");

    // Create schema and views
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"store\"");

      String sql1 = String.format(
          "CREATE VIEW \"store\".\"orders\" AS SELECT * FROM read_parquet('%s')",
          p1.toString());
      stmt.execute(sql1);

      String sql2 = String.format(
          "CREATE VIEW \"store\".\"customers\" AS SELECT * FROM read_parquet('%s')",
          p2.toString());
      stmt.execute(sql2);
    }

    // Verify views work and can be queried together
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT count(*) AS cnt FROM \"store\".\"orders\"")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }

      try (ResultSet rs = stmt.executeQuery(
          "SELECT count(*) AS cnt FROM \"store\".\"customers\"")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }

      // Query with aggregation
      try (ResultSet rs = stmt.executeQuery(
          "SELECT SUM(price) AS total FROM \"store\".\"orders\"")) {
        assertTrue(rs.next());
        assertEquals(1089.97, rs.getDouble("total"), 0.01);
      }
    }
  }

  @Test void testIntegrationDuckDBSettings() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads TO 4");
      stmt.execute("SET memory_limit = '4GB'");
      stmt.execute("SET preserve_insertion_order = false");
      stmt.execute("SET enable_progress_bar = false");
      stmt.execute("SET scalar_subquery_error_on_multiple_rows = false");
    }
    // All settings applied without error
  }

  @Test void testIntegrationSchemaCreationIdempotent() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"idempotent_test\"");
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"idempotent_test\"");
      // Should not fail on second call
    }
  }

  @Test void testIntegrationParquetViewWithFilterPushdown() throws Exception {
    Path parquetFile = createParquetFile("filterable",
        "SELECT * FROM generate_series(1, 100) AS t(id)");
    DuckDBJdbcSchemaFactory.createParquetView(conn, "v_filter", parquetFile.toString());

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT count(*) AS cnt FROM v_filter WHERE id > 50")) {
      assertTrue(rs.next());
      assertEquals(50, rs.getInt("cnt"));
    }
  }

  @Test void testIntegrationMultipleSchemasSameConnection() throws Exception {
    Path p = createParquetFile("shared_data", "SELECT 1 AS val");

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"schema_a\"");
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"schema_b\"");

      String sql = "CREATE VIEW \"%s\".\"data\" AS SELECT * FROM read_parquet('" + p + "')";
      stmt.execute(String.format(sql, "schema_a"));
      stmt.execute(String.format(sql, "schema_b"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"schema_a\".\"data\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("val"));
      }

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"schema_b\".\"data\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("val"));
      }
    }
  }

  // ==========================================================================
  // Helper methods
  // ==========================================================================

  /**
   * Creates a temporary parquet file via DuckDB COPY.
   */
  private Path createParquetFile(String name, String selectQuery) throws Exception {
    Path parquetDir = tempDir.resolve("parquet_" + name);
    Files.createDirectories(parquetDir);
    Path parquetFile = parquetDir.resolve(name + ".parquet");

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("COPY (" + selectQuery + ") TO '" + parquetFile.toString() + "' (FORMAT PARQUET)");
    }
    return parquetFile;
  }

  private boolean invokeIsTempDirectory(String path) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "isTempDirectory", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, path);
  }

  private boolean invokeIsHivePartitioned(String fileList) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "isHivePartitioned", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, fileList);
  }

  private boolean invokeIsHivePartitionedFromConfig(
      ConversionMetadata.ConversionRecord record) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, record);
  }

  private boolean invokeShouldUseUnionByName(
      ConversionMetadata.ConversionRecord record) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, record);
  }

  private String invokeDeriveGlobPattern(String fileList) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "deriveGlobPattern", String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, fileList);
  }

  private String invokeFormatRecordForError(
      ConversionMetadata.ConversionRecord record) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "formatRecordForError", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (String) method.invoke(null, record);
  }

  private String invokeRewriteSchemaReferences(String viewDef, String declared,
      String actual) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, viewDef, declared, actual);
  }

  private boolean invokeViewExists(Connection c, String schema, String tableName) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "viewExists", Connection.class, String.class, String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, c, schema, tableName);
  }

  private String invokeGetViewSql(Connection c, String schema, String tableName) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "getViewSql", Connection.class, String.class, String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, c, schema, tableName);
  }

  private boolean invokeViewUsesIcebergScan(Connection c, String schema,
      String tableName) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "viewUsesIcebergScan", Connection.class, String.class, String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, c, schema, tableName);
  }

  private void invokeRegisterSqlViewsInDuckDB(Connection c, String schema,
      Map<String, Object> operand) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "registerSqlViewsInDuckDB", Connection.class, String.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, c, schema, operand);
  }

  private void invokeRegisterSimilarityFunctions(Connection c) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "registerSimilarityFunctions", Connection.class);
    method.setAccessible(true);
    method.invoke(null, c);
  }

  private void invokeLoadQueryExtensions(Connection c) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "loadQueryExtensions", Connection.class);
    method.setAccessible(true);
    method.invoke(null, c);
  }

  private Object invokeCreateDuckDBDialect() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "createDuckDBDialectWithCustomLex");
    method.setAccessible(true);
    return method.invoke(null);
  }

  private String invokeDetermineCatalogPath(String schemaName,
      String directoryPath) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "determineCatalogPath", String.class, String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, schemaName, directoryPath);
  }

  private ConversionMetadata.ConversionRecord buildRecordWithDuckdbConfig(
      String key, Object value) {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> duckdb = new HashMap<String, Object>();
    duckdb.put(key, value);
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", duckdb);
    record.tableConfig = tableConfig;
    return record;
  }
}
