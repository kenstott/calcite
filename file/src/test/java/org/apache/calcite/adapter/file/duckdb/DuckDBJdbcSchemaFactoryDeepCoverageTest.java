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

import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link DuckDBJdbcSchemaFactory}.
 * Focuses on static utility methods and SQL generation paths.
 */
@Tag("unit")
class DuckDBJdbcSchemaFactoryDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ========== getParserConfig() ==========

  @Test void testGetParserConfig() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(config);
  }

  // ========== createParquetView() ==========

  @Test void testCreateParquetViewWithDuckDB() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    // Create a simple parquet file for viewing
    Path parquetDir = tempDir.resolve("parquet_data");
    Files.createDirectories(parquetDir);

    // Create data via DuckDB first
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE test_data (id INTEGER, name VARCHAR)");
      stmt.execute("INSERT INTO test_data VALUES (1, 'Alice'), (2, 'Bob')");

      String parquetPath = parquetDir.resolve("test.parquet").toString();
      stmt.execute("COPY test_data TO '" + parquetPath + "' (FORMAT PARQUET)");

      // Now create a view using the factory method
      DuckDBJdbcSchemaFactory.createParquetView(conn, "test_view", parquetPath);

      // Verify the view works
      java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM test_view ORDER BY id");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("Alice", rs.getString("name"));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));
      assertEquals("Bob", rs.getString("name"));
      assertFalse(rs.next());
      rs.close();
    }

    conn.close();
  }

  @Test void testCreateParquetViewPreservesCasing() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    Path parquetDir = tempDir.resolve("casing_data");
    Files.createDirectories(parquetDir);

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE casing_test (id INTEGER)");
      stmt.execute("INSERT INTO casing_test VALUES (1)");

      String parquetPath = parquetDir.resolve("casing.parquet").toString();
      stmt.execute("COPY casing_test TO '" + parquetPath + "' (FORMAT PARQUET)");

      // Mixed case view name should be preserved via quoting
      DuckDBJdbcSchemaFactory.createParquetView(conn, "MixedCaseView", parquetPath);

      java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM \"MixedCaseView\"");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      rs.close();
    }

    conn.close();
  }

  @Test void testCreateParquetViewInvalidPath() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    // Creating a view with a non-existent parquet path should throw
    assertThrows(RuntimeException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() throws Exception {
        DuckDBJdbcSchemaFactory.createParquetView(conn,
            "bad_view", "/nonexistent/path/file.parquet");
      }
    });

    conn.close();
  }

  // ========== createParquetView with OR REPLACE ==========

  @Test void testCreateParquetViewReplacesExisting() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    Path parquetDir = tempDir.resolve("replace_data");
    Files.createDirectories(parquetDir);

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE data1 (id INTEGER, name VARCHAR)");
      stmt.execute("INSERT INTO data1 VALUES (1, 'first')");
      String path1 = parquetDir.resolve("data1.parquet").toString();
      stmt.execute("COPY data1 TO '" + path1 + "' (FORMAT PARQUET)");

      stmt.execute("CREATE TABLE data2 (id INTEGER, name VARCHAR)");
      stmt.execute("INSERT INTO data2 VALUES (2, 'second')");
      String path2 = parquetDir.resolve("data2.parquet").toString();
      stmt.execute("COPY data2 TO '" + path2 + "' (FORMAT PARQUET)");

      // Create view pointing to first file
      DuckDBJdbcSchemaFactory.createParquetView(conn, "replaceable_view", path1);

      // Replace with second file
      DuckDBJdbcSchemaFactory.createParquetView(conn, "replaceable_view", path2);

      // Should now return data from second file
      java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM replaceable_view");
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));
      assertEquals("second", rs.getString("name"));
      rs.close();
    }

    conn.close();
  }

  // ========== Multiple views in same connection ==========

  @Test void testMultipleViewsInSameConnection() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    Path parquetDir = tempDir.resolve("multi_view_data");
    Files.createDirectories(parquetDir);

    try (Statement stmt = conn.createStatement()) {
      // Create first table/parquet
      stmt.execute("CREATE TABLE employees (id INTEGER, name VARCHAR)");
      stmt.execute("INSERT INTO employees VALUES (1, 'Alice')");
      String empPath = parquetDir.resolve("employees.parquet").toString();
      stmt.execute("COPY employees TO '" + empPath + "' (FORMAT PARQUET)");

      // Create second table/parquet
      stmt.execute("CREATE TABLE departments (dept_id INTEGER, dept_name VARCHAR)");
      stmt.execute("INSERT INTO departments VALUES (10, 'Engineering')");
      String deptPath = parquetDir.resolve("departments.parquet").toString();
      stmt.execute("COPY departments TO '" + deptPath + "' (FORMAT PARQUET)");

      // Register both as views
      DuckDBJdbcSchemaFactory.createParquetView(conn, "emp_view", empPath);
      DuckDBJdbcSchemaFactory.createParquetView(conn, "dept_view", deptPath);

      // Both views should be queryable
      java.sql.ResultSet rs1 = stmt.executeQuery("SELECT * FROM emp_view");
      assertTrue(rs1.next());
      rs1.close();

      java.sql.ResultSet rs2 = stmt.executeQuery("SELECT * FROM dept_view");
      assertTrue(rs2.next());
      rs2.close();
    }

    conn.close();
  }

  // ========== Schema creation in DuckDB ==========

  @Test void testSchemaCreationInDuckDB() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    try (Statement stmt = conn.createStatement()) {
      // Test schema creation with IF NOT EXISTS (idempotent)
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"test_schema\"");
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"test_schema\""); // Should not fail

      // Create a view in the schema
      stmt.execute("CREATE TABLE test_data (id INTEGER)");
      stmt.execute("INSERT INTO test_data VALUES (1)");
      String parquetPath = tempDir.resolve("schema_test.parquet").toString();
      stmt.execute("COPY test_data TO '" + parquetPath + "' (FORMAT PARQUET)");

      String sql = "CREATE OR REPLACE VIEW \"test_schema\".\"my_view\" AS "
          + "SELECT * FROM read_parquet('" + parquetPath + "')";
      stmt.execute(sql);

      java.sql.ResultSet rs = stmt.executeQuery(
          "SELECT * FROM \"test_schema\".\"my_view\"");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      rs.close();
    }

    conn.close();
  }

  // ========== DuckDB settings configuration ==========

  @Test void testDuckDBSettingsConfiguration() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    try (Statement stmt = conn.createStatement()) {
      // Test the same settings that the factory applies
      stmt.execute("SET threads TO 4");
      stmt.execute("SET memory_limit = '4GB'");
      stmt.execute("SET preserve_insertion_order = false");
      stmt.execute("SET enable_progress_bar = false");
      stmt.execute("SET scalar_subquery_error_on_multiple_rows = false");
    }

    conn.close();
  }

  // ========== Extension loading ==========

  @Test void testExtensionLoading() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    try (Statement stmt = conn.createStatement()) {
      // These extensions should either load or fail gracefully
      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
      } catch (SQLException e) {
        // OK - may already be built-in
      }

      try {
        stmt.execute("INSTALL json");
        stmt.execute("LOAD json");
      } catch (SQLException e) {
        // OK - may already be built-in
      }
    }

    conn.close();
  }

  // ========== Similarity function macro registration ==========

  @Test void testSimilarityMacroRegistration() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    try (Statement stmt = conn.createStatement()) {
      // Register the same macros the factory would register
      String cosineSimilarityMacro =
          "CREATE OR REPLACE MACRO COSINE_SIMILARITY(vector1, vector2) AS "
          + "list_cosine_similarity("
          + "  list_transform(string_split(vector1, ','), x -> CAST(x AS DOUBLE)), "
          + "  list_transform(string_split(vector2, ','), x -> CAST(x AS DOUBLE))"
          + ")";
      stmt.execute(cosineSimilarityMacro);

      // Test the macro
      java.sql.ResultSet rs = stmt.executeQuery(
          "SELECT COSINE_SIMILARITY('1,0,0', '1,0,0') AS sim");
      assertTrue(rs.next());
      double sim = rs.getDouble("sim");
      assertEquals(1.0, sim, 0.01);
      rs.close();
    }

    conn.close();
  }

  // ========== ParserConfig returns proper settings ==========

  @Test void testParserConfigValues() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    // Should have proper casing settings configured
    assertNotNull(config.unquotedCasing());
    assertNotNull(config.quotedCasing());
  }
}
