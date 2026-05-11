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

import org.apache.calcite.adapter.file.execution.trino.TrinoConfig;
import org.apache.calcite.adapter.file.jdbc.TrinoDialect;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Docker-based integration tests for Trino execution engine.
 *
 * <p>These tests start a Trino container with Hive and Iceberg catalogs configured,
 * then connect via JDBC to verify schema creation, external table registration,
 * and query execution. Tests are skipped if Docker is not available.
 *
 * <p>The test is fully self-contained:
 * <ol>
 *   <li>{@link TrinoConfig#generateCatalogFiles} writes catalog property files</li>
 *   <li>Docker mounts both data and catalog directories into the container</li>
 *   <li>Trino picks up the catalogs on startup</li>
 * </ol>
 *
 * <p>Uses {@code docker} CLI via ProcessBuilder -- zero additional dependencies.
 */
@Tag("integration")
@EnabledIf("isDockerAvailable")
@ResourceLock("docker-integration")
class TrinoDockerIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TrinoDockerIntegrationTest.class);

  /** Trino Docker image version (matches JDBC driver version). */
  private static final String TRINO_IMAGE = "trinodb/trino:439";

  /** Timeout for waiting for Trino to become ready. */
  private static final int STARTUP_TIMEOUT_MS = 120_000;

  /** Poll interval when waiting for Trino. */
  private static final int POLL_INTERVAL_MS = 3000;

  private static String containerName;
  private static int hostPort;
  private static String jdbcUrl;
  private static File dataDir;

  @TempDir
  static File tempDir;

  /**
   * Checks if Docker is available on this machine.
   */
  static boolean isDockerAvailable() {
    try {
      Process p = new ProcessBuilder("docker", "info")
          .redirectErrorStream(true)
          .start();
      boolean finished = p.waitFor(10, TimeUnit.SECONDS);
      if (finished && p.exitValue() == 0) {
        LOGGER.info("Docker is available");
        return true;
      }
    } catch (Exception e) {
      LOGGER.debug("Docker not available: {}", e.getMessage());
    }
    return false;
  }

  @BeforeAll
  static void startTrinoContainer() throws Exception {
    try {
      containerName = "trino-test-" + UUID.randomUUID().toString().substring(0, 8);
      hostPort = findFreePort();

      // Register a JVM shutdown hook so the container is removed even if the
      // test is interrupted (e.g. by gtimeout or IDE stop). @AfterAll handles
      // normal termination; the hook covers SIGTERM / unexpected JVM exits.
      final String nameToRemove = containerName;
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override public void run() {
          try {
            new ProcessBuilder("docker", "rm", "-f", nameToRemove)
                .redirectErrorStream(true).start().waitFor(10, TimeUnit.SECONDS);
          } catch (Exception ignored) {
          }
        }
      }));

      LOGGER.info("Starting Trino container '{}' on port {}", containerName, hostPort);

      // Create data directory for test files
      dataDir = new File(tempDir, "data");
      if (!dataDir.exists() && !dataDir.mkdirs()) {
        throw new RuntimeException("Failed to create data directory");
      }

      // Generate catalog files using TrinoConfig.
      // Use tempDir-based warehouse dir so paths match between host and container.
      File catalogDir = new File(tempDir, "catalogs");
      File warehouseDirFile = new File(tempDir, "warehouse");
      if (!warehouseDirFile.exists() && !warehouseDirFile.mkdirs()) {
        throw new RuntimeException("Failed to create warehouse directory");
      }
      String warehouseDir = warehouseDirFile.getAbsolutePath();
      Map<String, Object> configMap = new HashMap<>();
      configMap.put("warehouseDir", warehouseDir);
      TrinoConfig config = new TrinoConfig(configMap);
      config.generateCatalogFiles(catalogDir.getAbsolutePath());

      LOGGER.info("Generated catalog files in: {}", catalogDir.getAbsolutePath());

      // Start the Trino container with:
      // 1. tempDir mounted at its own host absolute path (so paths match host<->container)
      // 2. Catalog files mounted to Trino's catalog directory
      ProcessBuilder pb = new ProcessBuilder(
          "docker", "run", "-d",
          "--name", containerName,
          "-p", hostPort + ":8080",
          "-v", tempDir.getAbsolutePath() + ":" + tempDir.getAbsolutePath(),
          "-v", catalogDir.getAbsolutePath() + ":/etc/trino/catalog",
          TRINO_IMAGE
      );
      pb.redirectErrorStream(true);
      Process p = pb.start();

      // Read container ID from stdout
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          LOGGER.debug("docker run output: {}", line);
        }
      } finally {
        reader.close();
      }

      boolean started = p.waitFor(30, TimeUnit.SECONDS);
      if (!started || p.exitValue() != 0) {
        String logs = getContainerLogs();
        throw new RuntimeException("Failed to start Trino container. Exit code: "
            + (started ? p.exitValue() : "timeout") + ". Logs: " + logs);
      }

      // Build JDBC URL (use 127.0.0.1 to avoid IPv6 resolution issues with Docker)
      jdbcUrl = "jdbc:trino://127.0.0.1:" + hostPort + "/hive/default";
      LOGGER.info("Trino container started. JDBC URL: {}", jdbcUrl);

      // Wait for Trino to become ready
      waitForTrino();
    } catch (Exception e) {
      LOGGER.warn("Trino container startup failed: {}", e.getMessage());
      assumeTrue(false,
          "Trino Docker container not available: " + e.getMessage());
    }
  }

  @AfterAll
  static void stopTrinoContainer() {
    if (containerName != null) {
      try {
        LOGGER.info("Stopping and removing Trino container: {}", containerName);
        Process p = new ProcessBuilder("docker", "rm", "-f", containerName)
            .redirectErrorStream(true)
            .start();
        p.waitFor(15, TimeUnit.SECONDS);
        LOGGER.info("Trino container removed");
      } catch (Exception e) {
        LOGGER.warn("Failed to remove Trino container: {}", e.getMessage());
      }
    }
  }

  @Test void testConnectToTrino() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", "test");
    Connection conn = DriverManager.getConnection(jdbcUrl, props);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT 1");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          LOGGER.info("Successfully connected to Trino and executed SELECT 1");
        } finally {
          rs.close();
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testCreateSchemaAndExternalTable() throws Exception {
    String schemaName = "test_schema_"
        + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    Properties props = new Properties();
    props.setProperty("user", "test");
    Connection conn = DriverManager.getConnection(jdbcUrl, props);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create schema
        stmt.execute("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        // Create a simple table with data to test
        stmt.execute("CREATE TABLE hive." + schemaName
            + ".test_table AS SELECT 42 AS answer, 'hello' AS greeting");

        // Query the table
        ResultSet rs = stmt.executeQuery(
            "SELECT answer, greeting FROM hive." + schemaName + ".test_table");
        try {
          assertTrue(rs.next());
          assertEquals(42, rs.getInt("answer"));
          assertEquals("hello", rs.getString("greeting"));
        } finally {
          rs.close();
        }

        // Cleanup
        stmt.execute("DROP TABLE IF EXISTS hive." + schemaName + ".test_table");
        stmt.execute("DROP SCHEMA IF EXISTS hive." + schemaName);
        LOGGER.info("Successfully created schema, table, queried, and cleaned up");
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testQueryParquetFile() throws Exception {
    String schemaName = "test_parquet_"
        + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    Properties props = new Properties();
    props.setProperty("user", "test");
    Connection conn = DriverManager.getConnection(jdbcUrl, props);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create schema (uses default warehouse location)
        stmt.execute("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        // Create a table (Trino writes Parquet by default for Hive connector)
        stmt.execute("CREATE TABLE hive." + schemaName + ".parquet_test "
            + "WITH (format = 'PARQUET') AS "
            + "SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'");

        // Read back
        ResultSet rs = stmt.executeQuery(
            "SELECT * FROM hive." + schemaName + ".parquet_test ORDER BY id");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("id"));
          assertEquals("alice", rs.getString("name"));
          assertTrue(rs.next());
          assertEquals(2, rs.getInt("id"));
          assertEquals("bob", rs.getString("name"));
        } finally {
          rs.close();
        }

        // Cleanup
        stmt.execute("DROP TABLE IF EXISTS hive." + schemaName + ".parquet_test");
        stmt.execute("DROP SCHEMA IF EXISTS hive." + schemaName);
        LOGGER.info("Successfully wrote and read Parquet data via Trino");
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testIcebergTableRegistration() throws Exception {
    String schemaName = "test_iceberg_"
        + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    String icebergLocation = new File(tempDir, "iceberg_" + schemaName).getAbsolutePath();
    Properties props = new Properties();
    props.setProperty("user", "test");
    Connection conn = DriverManager.getConnection(jdbcUrl, props);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create Iceberg schema
        stmt.execute("CREATE SCHEMA IF NOT EXISTS iceberg." + schemaName
            + " WITH (location = '" + icebergLocation + "')");

        // Create an Iceberg table
        stmt.execute("CREATE TABLE iceberg." + schemaName + ".test_iceberg "
            + "(id INTEGER, name VARCHAR) WITH (format = 'PARQUET')");

        // Insert data
        stmt.execute("INSERT INTO iceberg." + schemaName
            + ".test_iceberg VALUES (1, 'test')");

        // Query back
        ResultSet rs = stmt.executeQuery(
            "SELECT * FROM iceberg." + schemaName + ".test_iceberg");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("id"));
          assertEquals("test", rs.getString("name"));
        } finally {
          rs.close();
        }

        LOGGER.info("Successfully created and queried Iceberg table via Trino");
      } finally {
        try {
          stmt.execute("DROP TABLE IF EXISTS iceberg." + schemaName + ".test_iceberg");
          stmt.execute("DROP SCHEMA IF EXISTS iceberg." + schemaName);
        } catch (Exception e) {
          LOGGER.debug("Cleanup error: {}", e.getMessage());
        }
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testTrinoConfig() throws Exception {
    String warehouseDir = new File(tempDir, "warehouse").getAbsolutePath();
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", hostPort);
    configMap.put("catalog", "hive");
    configMap.put("schema", "default");
    configMap.put("icebergCatalog", "iceberg");
    configMap.put("warehouseDir", warehouseDir);

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("localhost", config.getHost());
    assertEquals(String.valueOf(hostPort), config.getPort());
    assertEquals("hive", config.getCatalog());
    assertEquals("default", config.getSchema());
    assertEquals("iceberg", config.getIcebergCatalog());

    // Verify JDBC URL construction
    Map<String, String> urlConfig = new HashMap<>();
    urlConfig.put("host", config.getHost());
    urlConfig.put("port", config.getPort());
    urlConfig.put("catalog", config.getCatalog());
    urlConfig.put("schema", config.getSchema());
    String url = TrinoDialect.INSTANCE.buildJdbcUrl(urlConfig);
    assertEquals("jdbc:trino://localhost:" + hostPort + "/hive/default", url);

    // Verify catalog file generation
    File catalogDir = new File(tempDir, "test_catalogs");
    config.generateCatalogFiles(catalogDir.getAbsolutePath());

    File hiveFile = new File(catalogDir, "hive.properties");
    assertTrue(hiveFile.exists());
    Properties hiveProps = new Properties();
    FileInputStream in = new FileInputStream(hiveFile);
    try {
      hiveProps.load(in);
    } finally {
      in.close();
    }
    assertEquals("hive", hiveProps.getProperty("connector.name"));
    assertEquals(warehouseDir, hiveProps.getProperty("hive.metastore.catalog.dir"));
  }

  @Test void testAdapterIntegrationWithCalcite() throws Exception {
    // Each adapter test uses a unique schema to avoid parallel test conflicts
    String schema = "adapter_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createTrinoSchema(schema);
    createTrinoTable(schema, "employees",
        "SELECT * FROM (VALUES (1, 'Alice', 75000.0e0), (2, 'Bob', 85000.0e0), "
        + "(3, 'Charlie', 95000.0e0)) AS t(id, name, salary)");

    Connection conn = openCalciteAdapterConnection(schema);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT * FROM employees ORDER BY id");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("id"));
          assertEquals("Alice", rs.getString("name"));
          assertEquals(75000.0, rs.getDouble("salary"), 0.01);

          assertTrue(rs.next());
          assertEquals(2, rs.getInt("id"));
          assertEquals("Bob", rs.getString("name"));
          assertEquals(85000.0, rs.getDouble("salary"), 0.01);

          assertTrue(rs.next());
          assertEquals(3, rs.getInt("id"));
          assertEquals("Charlie", rs.getString("name"));
          assertEquals(95000.0, rs.getDouble("salary"), 0.01);

          LOGGER.info("Calcite adapter integration test passed with 3 rows");
        } finally {
          rs.close();
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testAdapterWithMultipleTables() throws Exception {
    String schema = "multi_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createTrinoSchema(schema);
    createTrinoTable(schema, "employees",
        "SELECT * FROM (VALUES (1, 'Alice', 75000.0e0), (2, 'Bob', 85000.0e0), "
        + "(3, 'Charlie', 95000.0e0)) AS t(id, name, salary)");
    createTrinoTable(schema, "departments",
        "SELECT * FROM (VALUES (1, 'Engineering', 500000.0e0), "
        + "(2, 'Marketing', 300000.0e0)) AS t(id, name, budget)");

    Connection conn = openCalciteAdapterConnection(schema);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Query employees
        ResultSet rs1 = stmt.executeQuery("SELECT * FROM employees ORDER BY id");
        try {
          assertTrue(rs1.next());
          assertEquals(1, rs1.getInt("id"));
          assertEquals("Alice", rs1.getString("name"));
          assertTrue(rs1.next());
          assertTrue(rs1.next());
          LOGGER.info("Employees table returned 3 rows");
        } finally {
          rs1.close();
        }

        // Query departments
        ResultSet rs2 = stmt.executeQuery("SELECT * FROM departments ORDER BY id");
        try {
          assertTrue(rs2.next());
          assertEquals(1, rs2.getInt("id"));
          assertEquals("Engineering", rs2.getString("name"));
          assertEquals(500000.0, rs2.getDouble("budget"), 0.01);

          assertTrue(rs2.next());
          assertEquals(2, rs2.getInt("id"));
          assertEquals("Marketing", rs2.getString("name"));
          assertEquals(300000.0, rs2.getDouble("budget"), 0.01);

          LOGGER.info("Departments table returned 2 rows");
        } finally {
          rs2.close();
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testAdapterQueryWithFilter() throws Exception {
    String schema = "filter_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createTrinoSchema(schema);
    createTrinoTable(schema, "employees",
        "SELECT * FROM (VALUES (1, 'Alice', 75000.0e0), (2, 'Bob', 85000.0e0), "
        + "(3, 'Charlie', 95000.0e0)) AS t(id, name, salary)");

    Connection conn = openCalciteAdapterConnection(schema);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery(
            "SELECT name, salary FROM employees WHERE salary > 80000 ORDER BY salary");
        try {
          assertTrue(rs.next(), "Expected first row (Bob)");
          assertEquals("Bob", rs.getString("name"));
          assertEquals(85000.0, rs.getDouble("salary"), 0.01);

          assertTrue(rs.next(), "Expected second row (Charlie)");
          assertEquals("Charlie", rs.getString("name"));
          assertEquals(95000.0, rs.getDouble("salary"), 0.01);

          assertFalse(rs.next(), "Expected only 2 rows");
          LOGGER.info("Filter query returned correct 2 rows");
        } finally {
          rs.close();
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Tests the full FileSchemaFactory → TrinoJdbcSchemaFactory code path.
   *
   * <p>Pre-creates tables in Trino, writes a dummy parquet file for FileSchema
   * discovery, then opens a CalciteConnection via model JSON that routes through the
   * production FileSchemaFactory. This exercises TrinoJdbcSchemaFactory.create(),
   * TrinoJdbcSchema, TrinoConvention, and TrinoDialect.
   */
  @Test void testFileSchemaFactoryIntegration() throws Exception {
    // Step 1: Pre-create a schema and table in Trino
    String schemaName = "fstest";
    createTrinoSchema(schemaName);
    createTrinoTable(schemaName, "employees",
        "SELECT * FROM (VALUES (1, 'Alice', 75000.0e0), (2, 'Bob', 85000.0e0), "
        + "(3, 'Charlie', 95000.0e0)) AS t(id, name, salary)");

    // Step 2: Write a parquet file for FileSchema to discover
    File fsDataDir = new File(tempDir, "fs_trino_data");
    fsDataDir.mkdirs();
    writeParquetFile(fsDataDir, "dummy",
        "SELECT 1 AS id, 'dummy' AS name, 0.0 AS salary");

    // Step 3: Write Trino catalog files (needed for the schema to reference)
    File fsCatalogDir = new File(tempDir, "fs_catalogs");
    File fsWarehouseDir = new File(tempDir, "fs_warehouse");
    fsWarehouseDir.mkdirs();
    Map<String, Object> fsCatalogConfig = new HashMap<>();
    fsCatalogConfig.put("warehouseDir", fsWarehouseDir.getAbsolutePath());
    new TrinoConfig(fsCatalogConfig).generateCatalogFiles(fsCatalogDir.getAbsolutePath());

    // Step 4: Write model JSON that routes through FileSchemaFactory
    File modelFile = new File(tempDir, "trino_model.json");
    java.io.FileWriter modelWriter = new java.io.FileWriter(modelFile);
    try {
      modelWriter.write("{\n"
          + "  \"version\": \"1.0\",\n"
          + "  \"defaultSchema\": \"" + schemaName + "\",\n"
          + "  \"schemas\": [{\n"
          + "    \"name\": \"" + schemaName + "\",\n"
          + "    \"type\": \"custom\",\n"
          + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
          + "    \"operand\": {\n"
          + "      \"directory\": \"" + fsDataDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n"
          + "      \"executionEngine\": \"trino\",\n"
          + "      \"ephemeralCache\": true,\n"
          + "      \"trinoConfig\": {\n"
          + "        \"host\": \"127.0.0.1\",\n"
          + "        \"port\": " + hostPort + ",\n"
          + "        \"catalog\": \"hive\",\n"
          + "        \"schema\": \"default\",\n"
          + "        \"user\": \"test\"\n"
          + "      }\n"
          + "    }\n"
          + "  }]\n"
          + "}\n");
    } finally {
      modelWriter.close();
    }

    // Step 5: Open CalciteConnection via model (exercises FileSchemaFactory → TrinoJdbcSchemaFactory)
    java.sql.Driver calciteDriver = new org.apache.calcite.jdbc.Driver();
    Connection conn = calciteDriver.connect(
        "jdbc:calcite:model=" + modelFile.getAbsolutePath() + ";caseSensitive=false",
        new java.util.Properties());
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT * FROM employees ORDER BY id");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("id"));
          assertEquals("Alice", rs.getString("name"));
          assertEquals(75000.0, rs.getDouble("salary"), 0.01);

          assertTrue(rs.next());
          assertEquals(2, rs.getInt("id"));
          assertEquals("Bob", rs.getString("name"));

          assertTrue(rs.next());
          assertEquals(3, rs.getInt("id"));
          assertEquals("Charlie", rs.getString("name"));

          LOGGER.info("FileSchemaFactory → TrinoJdbcSchemaFactory integration test passed");
        } finally {
          rs.close();
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testAdapterAggregation() throws Exception {
    String schema = "agg_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createTrinoSchema(schema);
    createTrinoTable(schema, "employees",
        "SELECT * FROM (VALUES (1, 'Alice', 75000.0e0), (2, 'Bob', 85000.0e0), "
        + "(3, 'Charlie', 95000.0e0)) AS t(id, name, salary)");

    Connection conn = openCalciteAdapterConnection(schema);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) AS cnt, SUM(salary) AS total FROM employees");
        try {
          assertTrue(rs.next(), "Expected aggregation result row");
          assertEquals(3, rs.getInt("cnt"));
          assertEquals(255000.0, rs.getDouble("total"), 0.01);
          LOGGER.info("Aggregation query returned correct results: cnt=3, total=255000.0");
        } finally {
          rs.close();
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Creates a schema in Trino's hive catalog.
   */
  private static void createTrinoSchema(String schemaName) throws Exception {
    Properties props = new Properties();
    props.setProperty("user", "test");
    Connection conn = DriverManager.getConnection(jdbcUrl, props);
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.execute("CREATE SCHEMA hive." + schemaName);
        LOGGER.info("Created hive.{} schema in Trino", schemaName);
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Creates a table in a Trino schema via CTAS.
   */
  private static void createTrinoTable(String schemaName, String tableName,
      String selectSql) throws Exception {
    Properties props = new Properties();
    props.setProperty("user", "test");
    Connection conn = DriverManager.getConnection(jdbcUrl, props);
    try {
      Statement stmt = conn.createStatement();
      try {
        String sql = "CREATE TABLE hive." + schemaName + "." + tableName
            + " WITH (format = 'PARQUET') AS " + selectSql;
        LOGGER.info("Creating Trino table: {}", sql);
        stmt.execute(sql);
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Opens a Calcite connection with a JdbcSchema backed by a direct Trino JDBC
   * driver instance. This bypasses both DriverManager (JVM-global singleton) and
   * DBCP2's BasicDataSource, ensuring no cross-contamination when multiple JDBC
   * drivers (Hive, Trino, ClickHouse) coexist in the same JVM.
   */
  private static Connection openCalciteAdapterConnection(String schemaName) throws Exception {
    java.util.Properties info = new java.util.Properties();
    info.setProperty("caseSensitive", "false");
    java.sql.Driver calciteDriver = new org.apache.calcite.jdbc.Driver();
    Connection conn = calciteDriver.connect("jdbc:calcite:", info);
    CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);

    // Create isolated DataSource using a dedicated Trino driver instance
    final java.sql.Driver trinoDriver = new io.trino.jdbc.TrinoDriver();
    final String url = "jdbc:trino://127.0.0.1:" + hostPort + "/hive/" + schemaName;
    DataSource ds = new DataSource() {
      @Override public Connection getConnection() throws SQLException {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", "test");
        return trinoDriver.connect(url, props);
      }
      @Override public Connection getConnection(String u, String p) throws SQLException {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", u);
        return trinoDriver.connect(url, props);
      }
      @Override public PrintWriter getLogWriter() { return null; }
      @Override public void setLogWriter(PrintWriter out) { }
      @Override public void setLoginTimeout(int seconds) { }
      @Override public int getLoginTimeout() { return 0; }
      @Override public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("Trino");
      }
      @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) { return iface.cast(this); }
        throw new SQLException("Cannot unwrap to " + iface);
      }
      @Override public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
      }
    };

    org.apache.calcite.schema.SchemaPlus rootSchema = calciteConn.getRootSchema();
    JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, "TR_TEST", ds, null, schemaName);
    rootSchema.add("TR_TEST", jdbcSchema);
    calciteConn.setSchema("TR_TEST");
    return conn;
  }

  /**
   * Writes a Parquet file to the given directory using DuckDB as a local Parquet writer.
   */
  private static void writeParquetFile(File dir, String tableName, String selectSql)
      throws Exception {
    java.sql.Driver duckDriver = (java.sql.Driver) Class.forName("org.duckdb.DuckDBDriver")
        .getDeclaredConstructor().newInstance();
    Connection duckConn = duckDriver.connect("jdbc:duckdb:", new java.util.Properties());
    try {
      Statement stmt = duckConn.createStatement();
      try {
        String path = new File(dir, tableName + ".parquet").getAbsolutePath();
        stmt.execute("COPY (" + selectSql + ") TO '" + path + "' (FORMAT PARQUET)");
        LOGGER.info("Wrote parquet file: {}", path);
      } finally {
        stmt.close();
      }
    } finally {
      duckConn.close();
    }
  }

  /**
   * Waits for Trino to become ready by polling JDBC connections.
   */
  private static void waitForTrino() throws Exception {
    long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_MS;
    Exception lastException = null;

    LOGGER.info("Waiting for Trino to become ready (timeout: {}ms)...", STARTUP_TIMEOUT_MS);

    while (System.currentTimeMillis() < deadline) {
      try {
        Class.forName("io.trino.jdbc.TrinoDriver");
        Properties props = new Properties();
        props.setProperty("user", "test");
        Connection conn = DriverManager.getConnection(jdbcUrl, props);
        try {
          Statement stmt = conn.createStatement();
          try {
            // Use a query that requires worker nodes to be ready,
            // not just the coordinator
            ResultSet rs = stmt.executeQuery(
                "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'");
            try {
              if (rs.next() && rs.getInt(1) > 0) {
                LOGGER.info("Trino is ready with {} active node(s)!", rs.getInt(1));
                return;
              }
            } finally {
              rs.close();
            }
          } finally {
            stmt.close();
          }
        } finally {
          conn.close();
        }
      } catch (Exception e) {
        lastException = e;
        LOGGER.debug("Trino not ready yet: {}", e.getMessage());
      }

      Thread.sleep(POLL_INTERVAL_MS);
    }

    throw new RuntimeException(
        "Trino failed to start within " + STARTUP_TIMEOUT_MS + "ms. "
        + "Last error: " + (lastException != null ? lastException.getMessage() : "unknown"),
        lastException);
  }

  /**
   * Captures the last 50 lines of container logs for debugging.
   */
  private static String getContainerLogs() {
    try {
      Process p = new ProcessBuilder("docker", "logs", "--tail", "50", containerName)
          .redirectErrorStream(true)
          .start();
      StringBuilder sb = new StringBuilder();
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append("\n");
        }
      } finally {
        reader.close();
      }
      p.waitFor(10, TimeUnit.SECONDS);
      return sb.toString();
    } catch (Exception e) {
      return "Failed to get logs: " + e.getMessage();
    }
  }

  /**
   * Finds a free ephemeral port.
   */
  private static int findFreePort() throws Exception {
    ServerSocket socket = new ServerSocket(0);
    try {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    } finally {
      socket.close();
    }
  }
}
