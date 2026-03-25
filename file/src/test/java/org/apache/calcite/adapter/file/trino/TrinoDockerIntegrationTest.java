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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * <p>Uses {@code docker} CLI via ProcessBuilder — zero additional dependencies.
 */
@Tag("integration")
@EnabledIf("isDockerAvailable")
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
    containerName = "trino-test-" + UUID.randomUUID().toString().substring(0, 8);
    hostPort = findFreePort();

    LOGGER.info("Starting Trino container '{}' on port {}", containerName, hostPort);

    // Generate catalog files using TrinoConfig
    File dataDir = new File(tempDir, "data");
    if (!dataDir.exists() && !dataDir.mkdirs()) {
      throw new RuntimeException("Failed to create data directory");
    }

    File catalogDir = new File(tempDir, "catalogs");
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("warehouseDir", "/data");
    TrinoConfig config = new TrinoConfig(configMap);
    config.generateCatalogFiles(catalogDir.getAbsolutePath());

    LOGGER.info("Generated catalog files in: {}", catalogDir.getAbsolutePath());

    // Start the Trino container with data and catalog dirs mounted
    ProcessBuilder pb = new ProcessBuilder(
        "docker", "run", "-d",
        "--name", containerName,
        "-p", hostPort + ":8080",
        "-v", dataDir.getAbsolutePath() + ":/data",
        "-v", catalogDir.getAbsolutePath() + ":/etc/trino/catalog",
        TRINO_IMAGE
    );
    pb.redirectErrorStream(true);
    Process p = pb.start();

    // Read container ID from stdout
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        LOGGER.debug("docker run output: {}", line);
      }
    }

    boolean started = p.waitFor(30, TimeUnit.SECONDS);
    if (!started || p.exitValue() != 0) {
      String logs = getContainerLogs();
      throw new RuntimeException("Failed to start Trino container. Exit code: "
          + (started ? p.exitValue() : "timeout") + ". Logs: " + logs);
    }

    // Build JDBC URL
    jdbcUrl = "jdbc:trino://localhost:" + hostPort + "/hive/default";
    LOGGER.info("Trino container started. JDBC URL: {}", jdbcUrl);

    // Wait for Trino to become ready
    waitForTrino();
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
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      LOGGER.info("Successfully connected to Trino and executed SELECT 1");
    }
  }

  @Test void testCreateSchemaAndExternalTable() throws Exception {
    String schemaName = "test_schema_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    Properties props = new Properties();
    props.setProperty("user", "test");
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {
      // Create schema
      stmt.execute("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

      // Create a simple table with data to test
      stmt.execute("CREATE TABLE hive." + schemaName + ".test_table AS SELECT 42 AS answer, 'hello' AS greeting");

      // Query the table
      try (ResultSet rs = stmt.executeQuery("SELECT answer, greeting FROM hive." + schemaName + ".test_table")) {
        assertTrue(rs.next());
        assertEquals(42, rs.getInt("answer"));
        assertEquals("hello", rs.getString("greeting"));
      }

      // Cleanup
      stmt.execute("DROP TABLE IF EXISTS hive." + schemaName + ".test_table");
      stmt.execute("DROP SCHEMA IF EXISTS hive." + schemaName);
      LOGGER.info("Successfully created schema, table, queried, and cleaned up");
    }
  }

  @Test void testQueryParquetFile() throws Exception {
    String schemaName = "test_parquet_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    Properties props = new Properties();
    props.setProperty("user", "test");
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // Create schema
      stmt.execute("CREATE SCHEMA IF NOT EXISTS hive." + schemaName
          + " WITH (location = '/data/" + schemaName + "')");

      // Create a table (Trino writes Parquet by default for Hive connector)
      stmt.execute("CREATE TABLE hive." + schemaName + ".parquet_test "
          + "WITH (format = 'PARQUET') AS "
          + "SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'");

      // Read back
      try (ResultSet rs = stmt.executeQuery(
          "SELECT * FROM hive." + schemaName + ".parquet_test ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("alice", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("bob", rs.getString("name"));
      }

      // Cleanup
      stmt.execute("DROP TABLE IF EXISTS hive." + schemaName + ".parquet_test");
      stmt.execute("DROP SCHEMA IF EXISTS hive." + schemaName);
      LOGGER.info("Successfully wrote and read Parquet data via Trino");
    }
  }

  @Test void testIcebergTableRegistration() throws Exception {
    String schemaName = "test_iceberg_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    Properties props = new Properties();
    props.setProperty("user", "test");
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {
      try {
        // Create Iceberg schema
        stmt.execute("CREATE SCHEMA IF NOT EXISTS iceberg." + schemaName
            + " WITH (location = '/data/iceberg_" + schemaName + "')");

        // Create an Iceberg table
        stmt.execute("CREATE TABLE iceberg." + schemaName + ".test_iceberg "
            + "(id INTEGER, name VARCHAR) WITH (format = 'PARQUET')");

        // Insert data
        stmt.execute("INSERT INTO iceberg." + schemaName + ".test_iceberg VALUES (1, 'test')");

        // Query back
        try (ResultSet rs = stmt.executeQuery(
            "SELECT * FROM iceberg." + schemaName + ".test_iceberg")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("id"));
          assertEquals("test", rs.getString("name"));
        }

        LOGGER.info("Successfully created and queried Iceberg table via Trino");
      } finally {
        try {
          stmt.execute("DROP TABLE IF EXISTS iceberg." + schemaName + ".test_iceberg");
          stmt.execute("DROP SCHEMA IF EXISTS iceberg." + schemaName);
        } catch (Exception e) {
          LOGGER.debug("Cleanup error: {}", e.getMessage());
        }
      }
    }
  }

  @Test void testTrinoConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", hostPort);
    configMap.put("catalog", "hive");
    configMap.put("schema", "default");
    configMap.put("icebergCatalog", "iceberg");
    configMap.put("warehouseDir", "/data");

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
    try (FileInputStream in = new FileInputStream(hiveFile)) {
      hiveProps.load(in);
    }
    assertEquals("hive", hiveProps.getProperty("connector.name"));
    assertEquals("/data", hiveProps.getProperty("hive.metastore.catalog.dir"));
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
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
          if (rs.next()) {
            LOGGER.info("Trino is ready!");
            return;
          }
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
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append("\n");
        }
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
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }
}
