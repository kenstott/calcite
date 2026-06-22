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

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBExecutionEngine;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #229: the DuckDB engine over a directory of plain CSV files.
 *
 * <p>The Trino file connector sets {@code duckdbNativeFormats=true} so DuckDB reads CSV/TSV/JSON
 * directly via {@code read_csv_auto()}/{@code read_json_auto()} instead of converting them to
 * Parquet through the Hadoop ParquetWriter (which cannot run on the JDK 25 that Trino requires).
 * Previously a CSV-only mount registered {@code read_parquet('…/customers.parquet')} views over
 * files that never existed, so every query failed with "No files found that match the pattern".
 */
@Tag("integration")
public class DuckDBNativeCsvReadTest {

  private File tempDir;
  private Connection calciteConn;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("duckdb-native-csv-").toFile();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }
    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /** A directory of CSV files is queryable through the DuckDB engine with no Parquet on disk. */
  @Test public void testQueryCsvDirectoryViaDuckDBNativeRead() throws Exception {
    assumeTrue(DuckDBExecutionEngine.isAvailable(), "DuckDB driver not available on classpath");

    File dataDir = new File(tempDir, "northwind");
    dataDir.mkdirs();
    writeCsv(new File(dataDir, "customers.csv"),
        "id,name\n1,Alice\n2,Bob\n3,Carol\n");
    writeCsv(new File(dataDir, "orders.csv"),
        "id,customer_id,total\n10,1,99.5\n11,2,12.0\n");

    setupDuckDBConnection(dataDir, /* baseDirectory */ null);

    // The query that failed in issue #229.
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT count(*) AS cnt FROM \"files\".\"customers\"")) {
      assertTrue(rs.next(), "Should have a result row");
      assertEquals(3, rs.getInt("cnt"), "customers.csv has 3 data rows");
    }

    // Column values come back correctly (read_csv_auto inferred id as integer).
    List<String> names = new ArrayList<>();
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT \"name\" FROM \"files\".\"customers\" ORDER BY \"id\"")) {
      while (rs.next()) {
        names.add(rs.getString("name"));
      }
    }
    assertEquals(java.util.Arrays.asList("Alice", "Bob", "Carol"), names);

    // A second CSV in the same mount is independently queryable.
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT count(*) AS cnt FROM \"files\".\"orders\"")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("cnt"), "orders.csv has 2 data rows");
    }
  }

  /** The native path must not invoke the Hadoop converter: no Parquet cache file is created. */
  @Test public void testNativeReadCreatesNoParquetCache() throws Exception {
    assumeTrue(DuckDBExecutionEngine.isAvailable(), "DuckDB driver not available on classpath");

    File dataDir = new File(tempDir, "northwind");
    dataDir.mkdirs();
    writeCsv(new File(dataDir, "customers.csv"), "id,name\n1,Alice\n2,Bob\n");

    File cacheDir = new File(tempDir, "cache");
    cacheDir.mkdirs();
    setupDuckDBConnection(dataDir, cacheDir);

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT count(*) AS cnt FROM \"files\".\"customers\"")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("cnt"));
    }

    assertFalse(containsParquetFile(cacheDir),
        "DuckDB native CSV read must not produce a Parquet cache file under " + cacheDir);
  }

  /** YAML can't be read by DuckDB directly; it is materialized to JSON and read via read_json_auto. */
  @Test public void testYamlReadViaNativeMaterialization() throws Exception {
    assumeTrue(DuckDBExecutionEngine.isAvailable(), "DuckDB driver not available on classpath");

    File dataDir = new File(tempDir, "yaml");
    dataDir.mkdirs();
    writeCsv(new File(dataDir, "people.yaml"),
        "- id: 1\n  name: Alice\n- id: 2\n  name: Bob\n- id: 3\n  name: Carol\n");

    setupDuckDBConnection(dataDir, null);

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT count(*) AS cnt FROM \"files\".\"people\"")) {
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("cnt"), "people.yaml has 3 records");
    }
    List<String> names = new ArrayList<>();
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT \"name\" FROM \"files\".\"people\" ORDER BY \"id\"")) {
      while (rs.next()) {
        names.add(rs.getString("name"));
      }
    }
    assertEquals(java.util.Arrays.asList("Alice", "Bob", "Carol"), names);
  }

  private void setupDuckDBConnection(File dataDir, File baseDirectory) throws Exception {
    setupDuckDBConnection(dataDir, baseDirectory, null);
  }

  private void setupDuckDBConnection(File dataDir, File baseDirectory,
      Map<String, Object> extraOperand) throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", dataDir.toString());
    operand.put("executionEngine", "DUCKDB");
    operand.put("recursive", true);
    // The flag the Trino file connector sets: read CSV/TSV/JSON natively, no Parquet conversion.
    operand.put("duckdbNativeFormats", true);
    if (extraOperand != null) {
      operand.putAll(extraOperand);
    }
    if (baseDirectory != null) {
      operand.put("baseDirectory", baseDirectory.toString());
      operand.put("ephemeralCache", false);
    } else {
      operand.put("ephemeralCache", true);
    }

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }

  private static void writeCsv(File file, String contents) throws Exception {
    Files.write(file.toPath(), contents.getBytes(StandardCharsets.UTF_8));
  }

  private static boolean containsParquetFile(File dir) {
    File[] entries = dir.listFiles();
    if (entries == null) {
      return false;
    }
    for (File entry : entries) {
      if (entry.isDirectory()) {
        if (containsParquetFile(entry)) {
          return true;
        }
      } else if (entry.getName().endsWith(".parquet")) {
        return true;
      }
    }
    return false;
  }

  private static void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
}
