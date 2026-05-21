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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link DuckDBJdbcSchemaFactory} targeting the remaining
 * 230 missed lines. Focuses on static utility methods accessible via reflection:
 * isTempDirectory edge cases, isHivePartitioned permutations, deriveGlobPattern
 * variants, determineCatalogPath branches, getParserConfig, createDuckDBDialectWithCustomLex,
 * createParquetView error handling, registerSimilarityFunctions, loadQueryExtensions.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class DuckDBJdbcSchemaFactoryDeepCoverageTest5 {

  @TempDir
  Path tempDir;

  // ========== isTempDirectory ==========

  @Test void testIsTempDirectoryVariousPatterns() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    m.setAccessible(true);

    // null => true
    assertTrue((Boolean) m.invoke(null, (String) null));

    // /tmp prefix
    assertTrue((Boolean) m.invoke(null, "/tmp"));
    assertTrue((Boolean) m.invoke(null, "/tmp/calcite-data"));

    // /tmp/ in path
    assertTrue((Boolean) m.invoke(null, "/var/tmp/data"));
    assertTrue((Boolean) m.invoke(null, "/home/user/tmp/data"));

    // /temp/ in path (Unix style)
    assertTrue((Boolean) m.invoke(null, "/home/user/temp/data"));

    // \\temp\\ in path (Windows style)
    assertTrue((Boolean) m.invoke(null, "C:\\Users\\user\\temp\\data"));

    // java.io.tmpdir substring
    assertTrue((Boolean) m.invoke(null, "/some/java.io.tmpdir/based/path"));

    // \\tmp prefix (Windows UNC path)
    assertTrue((Boolean) m.invoke(null, "\\tmp\\data"));

    // NOT temp
    assertFalse((Boolean) m.invoke(null, "/home/user/data"));
    assertFalse((Boolean) m.invoke(null, "/var/calcite/schemas"));
    assertFalse((Boolean) m.invoke(null, "s3://my-bucket/data"));
  }

  // ========== isHivePartitioned ==========

  @Test void testIsHivePartitionedNullAndEmpty() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    assertFalse((Boolean) m.invoke(null, (String) null));
    assertFalse((Boolean) m.invoke(null, ""));
  }

  @Test void testIsHivePartitionedSingleFile() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    // Needs >= 2 files
    assertFalse((Boolean) m.invoke(null, "/data/year=2020/file.parquet"));
  }

  @Test void testIsHivePartitionedWithSquareBrackets() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    String fileList = "[/data/year=2020/f1.parquet, /data/year=2021/f2.parquet]";
    assertTrue((Boolean) m.invoke(null, fileList));
  }

  @Test void testIsHivePartitionedWithCurlyBrackets() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    String fileList = "{/data/year=2020/f1.parquet, /data/year=2021/f2.parquet}";
    assertTrue((Boolean) m.invoke(null, fileList));
  }

  @Test void testIsHivePartitionedNotPartitioned() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    // No key=value patterns
    assertFalse((Boolean) m.invoke(null, "/data/f1.parquet, /data/f2.parquet"));
  }

  @Test void testIsHivePartitionedQuotedFiles() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    // Files with single quotes
    String fileList = "'/data/year=2020/a.parquet', '/data/year=2021/b.parquet'";
    assertTrue((Boolean) m.invoke(null, fileList));
  }

  @Test void testIsHivePartitionedMixed() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    // 2 out of 3 partitioned => >50%
    assertTrue(
        (Boolean) m.invoke(null,
        "/data/year=2020/a.parquet, /data/year=2021/b.parquet, /data/flat.parquet"));
    // 1 out of 3 partitioned => <=50%
    assertFalse(
        (Boolean) m.invoke(null,
        "/data/x.parquet, /data/y.parquet, /data/year=2020/z.parquet"));
  }

  @Test void testIsHivePartitionedWindowsPaths() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    String fileList = "C:\\data\\year=2020\\a.parquet, C:\\data\\year=2021\\b.parquet";
    assertTrue((Boolean) m.invoke(null, fileList));
  }

  @Test void testIsHivePartitionedUnderscoreColumn() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    m.setAccessible(true);
    String fileList = "/data/geo_type=STATE/a.parquet, /data/geo_type=COUNTY/b.parquet";
    assertTrue((Boolean) m.invoke(null, fileList));
  }

  // ========== deriveGlobPattern ==========

  @Test void testDeriveGlobPatternNullAndEmpty() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    m.setAccessible(true);
    assertNull(m.invoke(null, (String) null));
    assertNull(m.invoke(null, ""));
  }

  @Test void testDeriveGlobPatternWithHivePartitions() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    m.setAccessible(true);
    String fileList = "s3://bucket/base/year=2020/geo=US/f.parquet, s3://bucket/base/year=2021/geo=UK/f.parquet";
    String result = (String) m.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"), "Should contain recursive glob: " + result);
    assertTrue(result.endsWith(".parquet"), "Should end with .parquet: " + result);
    assertTrue(result.startsWith("s3://bucket/base"), "Should have base path: " + result);
  }

  @Test void testDeriveGlobPatternWithBrackets() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    m.setAccessible(true);
    String fileList = "['/data/year=2020/a.parquet', '/data/year=2021/b.parquet']";
    String result = (String) m.invoke(null, fileList);
    assertNotNull(result);
  }

  @Test void testDeriveGlobPatternNoPartitions() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    m.setAccessible(true);
    // Files without partition dirs
    String result = (String) m.invoke(null, "/data/subdir/a.parquet, /data/subdir/b.parquet");
    assertNotNull(result);
    assertTrue(result.contains("*"), "Should contain wildcard: " + result);
  }

  @Test void testDeriveGlobPatternCsvExtension() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    m.setAccessible(true);
    String result = (String) m.invoke(null, "/data/year=2020/a.csv, /data/year=2021/b.csv");
    assertNotNull(result);
    assertTrue(result.endsWith(".csv"), "Should preserve csv extension: " + result);
  }

  @Test void testDeriveGlobPatternSingleFileNoSlash() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    m.setAccessible(true);
    String result = (String) m.invoke(null, "bare_file.parquet");
    assertEquals("bare_file.parquet", result);
  }

  @Test void testDeriveGlobPatternQuotedFirstFile() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    m.setAccessible(true);
    String fileList = "'/home/data/year=2020/data.parquet', '/home/data/year=2021/data.parquet'";
    String result = (String) m.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.contains("/home/data"), "Should derive base path without quotes: " + result);
  }

  // ========== determineCatalogPath ==========

  @Test void testDetermineCatalogPathNullDir() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("determineCatalogPath", String.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(null, "schema1", (String) null));
  }

  @Test void testDetermineCatalogPathTempDir() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("determineCatalogPath", String.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(null, "test_schema", "/tmp/test-data"));
  }

  @Test void testDetermineCatalogPathPersistentDir() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("determineCatalogPath", String.class, String.class);
    m.setAccessible(true);
    String path = tempDir.toFile().getAbsolutePath();
    String result = (String) m.invoke(null, "myschema", path);
    assertNotNull(result);
    assertTrue(result.contains(".duckdb"), "Should contain .duckdb dir: " + result);
    assertTrue(result.contains("myschema_db.duckdb"), "Should contain schema name: " + result);
  }

  // ========== getParserConfig ==========

  @Test void testGetParserConfig() {
    org.apache.calcite.sql.parser.SqlParser.Config config =
        DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(config);
    // Verify parser config is non-null - specific getter names vary by version
    assertNotNull(config.toString());
  }

  // ========== createDuckDBDialectWithCustomLex ==========

  @Test void testDialectSupportsAggregateFunctions() throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("createDuckDBDialectWithCustomLex");
    m.setAccessible(true);
    org.apache.calcite.sql.SqlDialect dialect =
        (org.apache.calcite.sql.SqlDialect) m.invoke(null);
    assertNotNull(dialect);
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.COUNT));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.SUM));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.AVG));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.MIN));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.MAX));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.STDDEV_POP));
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.VAR_SAMP));
    // Any kind returns true
    assertTrue(dialect.supportsAggregateFunction(org.apache.calcite.sql.SqlKind.LITERAL));
  }

  // ========== createParquetView error ==========

  @Test void testCreateParquetViewThrowsOnSqlError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("Simulated error"));

    assertThrows(RuntimeException.class, () ->
        DuckDBJdbcSchemaFactory.createParquetView(conn, "test_view", "/path/to/file.parquet"));
  }

  @Test void testCreateParquetViewSuccess() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    // Should not throw
    DuckDBJdbcSchemaFactory.createParquetView(conn, "my_table", "/data/file.parquet");
    verify(stmt).execute(contains("CREATE OR REPLACE VIEW"));
  }

  // ========== registerSimilarityFunctions ==========

  @Test void testRegisterSimilarityFunctionsSuccess() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("registerSimilarityFunctions", Connection.class);
    m.setAccessible(true);

    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    // Should not throw
    m.invoke(null, conn);
    // Should create COSINE_SIMILARITY, COSINE_DISTANCE, ARRAY_COSINE_SIMILARITY
    verify(stmt, atLeast(3)).execute(anyString());
  }

  @Test void testRegisterSimilarityFunctionsFailure() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("registerSimilarityFunctions", Connection.class);
    m.setAccessible(true);

    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenThrow(new SQLException("Function already exists"));

    // Should not throw - graceful degradation
    m.invoke(null, conn);
  }

  // ========== loadQueryExtensions ==========

  @Test void testLoadQueryExtensionsSuccess() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("loadQueryExtensions", Connection.class);
    m.setAccessible(true);

    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.execute(anyString())).thenReturn(true);

    // Should not throw
    m.invoke(null, conn);
    // Should attempt to install and load spatial, vss, fts => 6 calls minimum
    verify(stmt, atLeast(6)).execute(anyString());
  }

  @Test void testLoadQueryExtensionsPartialFailure() throws Exception {
    Method m =
        DuckDBJdbcSchemaFactory.class.getDeclaredMethod("loadQueryExtensions", Connection.class);
    m.setAccessible(true);

    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    // First extension succeeds, second fails, third succeeds
    when(stmt.execute(anyString()))
        .thenReturn(true)  // INSTALL spatial
        .thenReturn(true)  // LOAD spatial
        .thenThrow(new SQLException("vss not available"))  // INSTALL vss
        .thenReturn(true)  // INSTALL fts
        .thenReturn(true); // LOAD fts

    // Should not throw - graceful degradation
    m.invoke(null, conn);
  }

  // ========== SharedDatabaseInfo (inner class coverage) ==========

  @Test void testSharedDatabaseInfoFields() throws Exception {
    // Access the inner class via reflection
    Class<?> sdiClass = null;
    for (Class<?> inner : DuckDBJdbcSchemaFactory.class.getDeclaredClasses()) {
      if (inner.getSimpleName().equals("SharedDatabaseInfo")) {
        sdiClass = inner;
        break;
      }
    }
    assertNotNull(sdiClass, "SharedDatabaseInfo inner class should exist");

    java.lang.reflect.Constructor<?> ctor = sdiClass.getDeclaredConstructors()[0];
    ctor.setAccessible(true);

    javax.sql.DataSource ds = mock(javax.sql.DataSource.class);
    Connection conn = mock(Connection.class);
    String url = "jdbc:duckdb:/path/to/db";
    String catalogPath = "/path/to/db";

    Object instance = ctor.newInstance(ds, conn, url, catalogPath);
    assertNotNull(instance);

    // Access fields
    java.lang.reflect.Field dsField = sdiClass.getDeclaredField("dataSource");
    dsField.setAccessible(true);
    assertSame(ds, dsField.get(instance));

    java.lang.reflect.Field connField = sdiClass.getDeclaredField("setupConnection");
    connField.setAccessible(true);
    assertSame(conn, connField.get(instance));

    java.lang.reflect.Field urlField = sdiClass.getDeclaredField("jdbcUrl");
    urlField.setAccessible(true);
    assertEquals(url, urlField.get(instance));

    java.lang.reflect.Field cpField = sdiClass.getDeclaredField("catalogPath");
    cpField.setAccessible(true);
    assertEquals(catalogPath, cpField.get(instance));
  }
}
