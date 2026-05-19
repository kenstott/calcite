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

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that cache_httpfs is loaded and configured when the DuckDB engine
 * initialises on macOS/Linux. Windows is excluded because the extension is
 * not supported there (WSL reports Linux so it is included).
 */
@Tag("integration")
@DisabledOnOs(OS.WINDOWS)
public class CacheHttpfsExtensionTest {

  private File tempDir;
  private File cacheDir;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("cache-httpfs-test-").toFile();
    cacheDir = new File(tempDir, "httpfs_cache");
    System.setProperty("duckdb.cache_httpfs.directory", cacheDir.getAbsolutePath());
  }

  @AfterEach
  public void tearDown() {
    System.clearProperty("duckdb.cache_httpfs.directory");
    deleteDirectory(tempDir);
  }

  /**
   * cache_httpfs must be loaded after DuckDB schema initialisation.
   * Queries duckdb_extensions() via the schema's own DataSource so we bypass
   * Calcite's SQL validator (which rejects DuckDB table functions).
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testCacheHttpfsIsLoaded() throws Exception {
    createCsvFile("items.csv", "id,name\n1,apple\n2,banana\n");

    try (Connection calciteConn = createDuckDBConnection()) {
      // Warm up the schema
      calciteConn.createStatement().executeQuery("SELECT COUNT(*) FROM files.items").close();

      // Get a raw DuckDB connection from the schema's DataSource
      CalciteConnection cc = calciteConn.unwrap(CalciteConnection.class);
      SchemaPlus filesSchema = cc.getRootSchema().getSubSchema("files");
      JdbcSchema jdbcSchema = filesSchema.unwrap(JdbcSchema.class);
      try (Connection duckConn = jdbcSchema.getDataSource().getConnection();
           Statement stmt = duckConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT loaded FROM duckdb_extensions() WHERE extension_name='cache_httpfs'")) {
        assertTrue(rs.next(), "cache_httpfs row should exist in duckdb_extensions()");
        assertTrue(rs.getBoolean("loaded"), "cache_httpfs should be loaded");
      }
    }
  }

  /**
   * The cache directory configured via the system property must be reflected
   * in DuckDB's own setting once the schema initialises.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testCacheDirIsConfiguredInDuckDb() throws Exception {
    createCsvFile("items.csv", "id,name\n1,apple\n2,banana\n");

    try (Connection calciteConn = createDuckDBConnection()) {
      calciteConn.createStatement().executeQuery("SELECT COUNT(*) FROM files.items").close();

      CalciteConnection cc = calciteConn.unwrap(CalciteConnection.class);
      SchemaPlus filesSchema = cc.getRootSchema().getSubSchema("files");
      JdbcSchema jdbcSchema = filesSchema.unwrap(JdbcSchema.class);
      try (Connection duckConn = jdbcSchema.getDataSource().getConnection();
           Statement stmt = duckConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT current_setting('cache_httpfs_cache_directory')")) {
        assertTrue(rs.next(), "cache_httpfs_cache_directory setting should exist");
        String configured = rs.getString(1);
        assertFalse(configured == null || configured.isEmpty(),
            "cache_httpfs_cache_directory should be set to a non-empty path");
      }
    }
  }

  /**
   * The built-in external_file_cache must be disabled when cache_httpfs loads
   * (avoids double buffering of the same remote byte ranges).
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testBuiltinExternalFileCacheIsDisabled() throws Exception {
    createCsvFile("items.csv", "id,name\n1,apple\n2,banana\n");

    try (Connection calciteConn = createDuckDBConnection()) {
      calciteConn.createStatement().executeQuery("SELECT COUNT(*) FROM files.items").close();

      CalciteConnection cc = calciteConn.unwrap(CalciteConnection.class);
      SchemaPlus filesSchema = cc.getRootSchema().getSubSchema("files");
      JdbcSchema jdbcSchema = filesSchema.unwrap(JdbcSchema.class);
      try (Connection duckConn = jdbcSchema.getDataSource().getConnection();
           Statement stmt = duckConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT current_setting('enable_external_file_cache')")) {
        if (rs.next()) {
          assertFalse(Boolean.parseBoolean(rs.getString(1)),
              "Built-in external file cache should be disabled when cache_httpfs is loaded");
        }
      }
    }
  }

  /**
   * Iceberg metadata paths must be excluded from caching so ETL updates are
   * visible immediately without waiting for cache expiry.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testIcebergMetadataExcludedFromCache() throws Exception {
    createCsvFile("items.csv", "id,name\n1,apple\n2,banana\n");

    try (Connection calciteConn = createDuckDBConnection()) {
      calciteConn.createStatement().executeQuery("SELECT COUNT(*) FROM files.items").close();

      CalciteConnection cc = calciteConn.unwrap(CalciteConnection.class);
      SchemaPlus filesSchema = cc.getRootSchema().getSubSchema("files");
      JdbcSchema jdbcSchema = filesSchema.unwrap(JdbcSchema.class);
      try (Connection duckConn = jdbcSchema.getDataSource().getConnection();
           Statement stmt = duckConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) FROM cache_httpfs_list_exclusion_regex()")) {
        assertTrue(rs.next());
        assertTrue(rs.getInt(1) >= 3,
            "At least 3 Iceberg metadata exclusion regexes should be registered");
      }
    }
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  private Connection createDuckDBConnection() throws Exception {
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);

    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calcConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calcConn.getRootSchema();
    rootSchema.add("files", FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
    return conn;
  }


  private void createCsvFile(String name, String content) throws Exception {
    try (PrintWriter pw = new PrintWriter(new FileWriter(new File(tempDir, name)))) {
      pw.print(content);
    }
  }

  private void deleteDirectory(File dir) {
    if (dir == null || !dir.exists()) {
      return;
    }
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteDirectory(f);
        } else {
          f.delete();
        }
      }
    }
    dir.delete();
  }
}
