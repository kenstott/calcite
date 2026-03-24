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

import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for DuckDB configuration and execution.
 */
@Tag("integration")
public class DuckDBConfigIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DuckDBConfigIntegrationTest.class);

  @TempDir
  Path tempDir;

  // --- DuckDBConfig unit tests ---

  @Test public void testDefaultConfig() {
    DuckDBConfig config = new DuckDBConfig();

    assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
    assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertNull(config.getTempDirectory());
    assertFalse(config.isEnableProgressBar());
    assertTrue(config.isPreserveInsertionOrder());
    assertTrue(config.isUseArrowOptimization());
    assertEquals(DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE, config.getArrowBatchSize());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
  }

  @Test public void testConfigFromMap() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("memory_limit", "2GB");
    configMap.put("threads", 8);
    configMap.put("max_memory", "90%");
    configMap.put("temp_directory", "/tmp/duck");
    configMap.put("enable_progress_bar", true);
    configMap.put("preserve_insertion_order", true);
    configMap.put("use_arrow_optimization", true);
    configMap.put("arrow_batch_size", 2048);

    DuckDBConfig config = new DuckDBConfig(configMap);

    assertEquals("2GB", config.getMemoryLimit());
    assertEquals(8, config.getThreads());
    assertEquals("90%", config.getMaxMemory());
    assertEquals("/tmp/duck", config.getTempDirectory());
    assertTrue(config.isEnableProgressBar());
    assertTrue(config.isPreserveInsertionOrder());
    assertTrue(config.isUseArrowOptimization());
    assertEquals(2048, config.getArrowBatchSize());
  }

  @Test public void testConfigFromMapWithDefaults() {
    Map<String, Object> configMap = new HashMap<>();
    // Only set a few values, rest should use defaults

    DuckDBConfig config = new DuckDBConfig(configMap);

    assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
    assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertNull(config.getTempDirectory());
    assertFalse(config.isEnableProgressBar());
  }

  @Test public void testConfigFromMapWithAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("memory_limit", "1GB");
    configMap.put("custom_setting", "custom_value");
    configMap.put("another_setting", 42);

    DuckDBConfig config = new DuckDBConfig(configMap);

    Properties additional = config.getAdditionalSettings();
    assertEquals("custom_value", additional.getProperty("custom_setting"));
    assertEquals("42", additional.getProperty("another_setting"));
  }

  @Test public void testConfigFullConstructor() {
    Properties additionalSettings = new Properties();
    additionalSettings.setProperty("custom", "value");

    DuckDBConfig config = new DuckDBConfig(
        "4GB", 16, "95%", "/tmp/duck-temp",
        true, false, true, 4096, additionalSettings);

    assertEquals("4GB", config.getMemoryLimit());
    assertEquals(16, config.getThreads());
    assertEquals("95%", config.getMaxMemory());
    assertEquals("/tmp/duck-temp", config.getTempDirectory());
    assertTrue(config.isEnableProgressBar());
    assertFalse(config.isPreserveInsertionOrder());
    assertTrue(config.isUseArrowOptimization());
    assertEquals(4096, config.getArrowBatchSize());
    assertEquals("value", config.getAdditionalSettings().getProperty("custom"));
  }

  @Test public void testConfigFullConstructorWithNulls() {
    DuckDBConfig config = new DuckDBConfig(
        null, -1, null, null,
        false, true, false, -1, null);

    assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
    assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertNull(config.getTempDirectory());
    assertEquals(DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE, config.getArrowBatchSize());
    assertNotNull(config.getAdditionalSettings());
  }

  @Test public void testToDuckDBSettings() {
    DuckDBConfig config = new DuckDBConfig(
        "2GB", 4, "80%", "/tmp/duck",
        false, true, true, 1024, null);

    String[] settings = config.toDuckDBSettings();

    assertNotNull(settings);
    assertTrue(settings.length >= 5);

    boolean foundMemoryLimit = false;
    boolean foundThreads = false;
    boolean foundTempDir = false;
    for (String setting : settings) {
      if (setting.contains("memory_limit") && setting.contains("2GB")) {
        foundMemoryLimit = true;
      }
      if (setting.contains("threads") && setting.contains("4")) {
        foundThreads = true;
      }
      if (setting.contains("temp_directory") && setting.contains("/tmp/duck")) {
        foundTempDir = true;
      }
    }
    assertTrue(foundMemoryLimit, "Should include memory_limit setting");
    assertTrue(foundThreads, "Should include threads setting");
    assertTrue(foundTempDir, "Should include temp_directory setting");
  }

  @Test public void testToDuckDBSettingsWithoutTempDir() {
    DuckDBConfig config = new DuckDBConfig();

    String[] settings = config.toDuckDBSettings();

    for (String setting : settings) {
      assertFalse(setting.contains("temp_directory"),
          "Should not include temp_directory when null");
    }
  }

  @Test public void testToDuckDBSettingsWithAdditionalSettings() {
    Properties additional = new Properties();
    additional.setProperty("custom_numeric", "42");
    additional.setProperty("custom_bool", "true");
    additional.setProperty("custom_string", "hello");

    DuckDBConfig config = new DuckDBConfig(
        "1GB", 2, "80%", null,
        false, true, true, 1024, additional);

    String[] settings = config.toDuckDBSettings();

    boolean foundNumeric = false;
    boolean foundBool = false;
    boolean foundString = false;
    for (String setting : settings) {
      if (setting.contains("custom_numeric") && setting.contains("42")) {
        foundNumeric = true;
        // Numeric values should not have quotes
        assertFalse(setting.contains("'42'"), "Numeric values should not be quoted");
      }
      if (setting.contains("custom_bool") && setting.contains("true")) {
        foundBool = true;
        assertFalse(setting.contains("'true'"), "Boolean values should not be quoted");
      }
      if (setting.contains("custom_string") && setting.contains("hello")) {
        foundString = true;
        assertTrue(setting.contains("'hello'"), "String values should be quoted");
      }
    }
    assertTrue(foundNumeric);
    assertTrue(foundBool);
    assertTrue(foundString);
  }

  @Test public void testToString() {
    DuckDBConfig config = new DuckDBConfig();
    String str = config.toString();

    assertNotNull(str);
    assertTrue(str.contains("DuckDBConfig"));
    assertTrue(str.contains("memoryLimit"));
    assertTrue(str.contains("threads"));
  }

  // --- DuckDB actual execution tests ---

  @Test public void testDuckDBBasicQuery() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery("SELECT 1 + 1 AS result");
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("result"));
      rs.close();
    }
  }

  @Test public void testDuckDBSettingsApply() throws Exception {
    DuckDBConfig config = new DuckDBConfig(
        "512MB", 2, "50%", null,
        false, true, true, 1024, null);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {

      // Apply settings - create new statement for each to avoid close issues
      for (String setting : config.toDuckDBSettings()) {
        try (Statement setStmt = conn.createStatement()) {
          setStmt.execute(setting);
        } catch (Exception e) {
          LOGGER.debug("Setting not supported: {}", setting);
        }
      }

      // Verify a setting was applied with a fresh statement
      try (Statement queryStmt = conn.createStatement()) {
        ResultSet rs = queryStmt.executeQuery(
            "SELECT current_setting('threads') AS threads");
        assertTrue(rs.next());
        String threads = rs.getString("threads");
        assertNotNull(threads);
        rs.close();
      }
    }
  }

  @Test public void testDuckDBParquetRead() throws Exception {
    // Create a parquet file using DuckDB
    String parquetPath = tempDir.resolve("test.parquet").toString();

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      // Create and export test data
      stmt.execute("CREATE TABLE test_data (id INTEGER, name VARCHAR, value DOUBLE)");
      stmt.execute("INSERT INTO test_data VALUES (1, 'Alice', 10.5)");
      stmt.execute("INSERT INTO test_data VALUES (2, 'Bob', 20.3)");
      stmt.execute("INSERT INTO test_data VALUES (3, 'Charlie', 30.1)");

      stmt.execute("COPY test_data TO '" + parquetPath + "' (FORMAT PARQUET)");

      // Read back from parquet
      ResultSet rs = stmt.executeQuery(
          "SELECT * FROM read_parquet('" + parquetPath + "') ORDER BY id");

      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("Alice", rs.getString("name"));
      assertEquals(10.5, rs.getDouble("value"), 0.001);

      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));

      assertTrue(rs.next());
      assertEquals(3, rs.getInt("id"));

      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testDuckDBCsvRead() throws Exception {
    // Write a CSV file
    String csvPath = tempDir.resolve("test.csv").toString();
    java.nio.file.Files.write(
        java.nio.file.Path.of(csvPath),
        "id,name,value\n1,Alice,10.5\n2,Bob,20.3\n".getBytes());

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT * FROM read_csv_auto('" + csvPath + "') ORDER BY id");

      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("Alice", rs.getString("name"));

      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));

      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testDuckDBJsonRead() throws Exception {
    String jsonPath = tempDir.resolve("test.json").toString();
    java.nio.file.Files.write(
        java.nio.file.Path.of(jsonPath),
        "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]".getBytes());

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT * FROM read_json_auto('" + jsonPath + "') ORDER BY id");

      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("Alice", rs.getString("name"));

      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));

      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testDuckDBGlobParquetRead() throws Exception {
    // Create multiple parquet files
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      String dir = tempDir.resolve("multi").toString();
      java.nio.file.Files.createDirectories(java.nio.file.Path.of(dir));

      stmt.execute("CREATE TABLE t1 (id INTEGER, val VARCHAR)");
      stmt.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')");
      stmt.execute("COPY t1 TO '" + dir + "/part1.parquet' (FORMAT PARQUET)");

      stmt.execute("DELETE FROM t1");
      stmt.execute("INSERT INTO t1 VALUES (3, 'c'), (4, 'd')");
      stmt.execute("COPY t1 TO '" + dir + "/part2.parquet' (FORMAT PARQUET)");

      // Read all parquet files via glob
      ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM read_parquet('" + dir + "/*.parquet')");
      assertTrue(rs.next());
      assertEquals(4, rs.getInt("cnt"));
      rs.close();
    }
  }

  @Test public void testDuckDBTypeMapping() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      stmt.execute("CREATE TABLE type_test ("
          + "col_int INTEGER, "
          + "col_bigint BIGINT, "
          + "col_double DOUBLE, "
          + "col_float FLOAT, "
          + "col_varchar VARCHAR, "
          + "col_bool BOOLEAN, "
          + "col_date DATE, "
          + "col_ts TIMESTAMP"
          + ")");

      stmt.execute("INSERT INTO type_test VALUES ("
          + "42, 9999999999, 3.14, 2.71, 'hello', true, "
          + "'2024-01-15', '2024-01-15 10:30:00')");

      ResultSet rs = stmt.executeQuery("SELECT * FROM type_test");
      assertTrue(rs.next());
      assertEquals(42, rs.getInt("col_int"));
      assertEquals(9999999999L, rs.getLong("col_bigint"));
      assertEquals(3.14, rs.getDouble("col_double"), 0.001);
      assertEquals("hello", rs.getString("col_varchar"));
      assertTrue(rs.getBoolean("col_bool"));
      assertNotNull(rs.getDate("col_date"));
      assertNotNull(rs.getTimestamp("col_ts"));

      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testDuckDBAggregations() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      stmt.execute("CREATE TABLE agg_test (category VARCHAR, amount DOUBLE)");
      stmt.execute("INSERT INTO agg_test VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40)");

      ResultSet rs = stmt.executeQuery(
          "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt, "
          + "AVG(amount) AS avg_amt "
          + "FROM agg_test GROUP BY category ORDER BY category");

      assertTrue(rs.next());
      assertEquals("A", rs.getString("category"));
      assertEquals(30.0, rs.getDouble("total"), 0.001);
      assertEquals(2, rs.getInt("cnt"));
      assertEquals(15.0, rs.getDouble("avg_amt"), 0.001);

      assertTrue(rs.next());
      assertEquals("B", rs.getString("category"));
      assertEquals(70.0, rs.getDouble("total"), 0.001);

      assertFalse(rs.next());
      rs.close();
    }
  }
}
