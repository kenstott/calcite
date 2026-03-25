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
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.file.execution.spark.SparkConfig;
import org.apache.calcite.adapter.file.jdbc.SparkSqlDialect;

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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Docker-based integration tests for Spark SQL via Thrift Server.
 *
 * <p>These tests start an Apache Spark container with the Thrift Server enabled,
 * then connect via JDBC to verify schema creation, view registration, and query
 * execution. Tests are skipped if Docker is not available.
 *
 * <p>Uses {@code docker} CLI via ProcessBuilder — zero additional dependencies.
 */
@Tag("integration")
@EnabledIf("isDockerAvailable")
class SparkDockerIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkDockerIntegrationTest.class);

  /** Spark Docker image to use. */
  private static final String SPARK_IMAGE = "apache/spark:3.5.1";

  /** Iceberg Spark runtime version matching the Spark version. */
  private static final String ICEBERG_VERSION = "1.4.0";

  /** Maven Central URL template for the Iceberg Spark runtime jar. */
  private static final String ICEBERG_JAR_URL =
      "https://repo1.maven.org/maven2/org/apache/iceberg/"
      + "iceberg-spark-runtime-3.5_2.12/" + ICEBERG_VERSION
      + "/iceberg-spark-runtime-3.5_2.12-" + ICEBERG_VERSION + ".jar";

  /** Timeout for waiting for the Thrift Server to become ready. */
  private static final int STARTUP_TIMEOUT_MS = 120_000;

  /** Poll interval when waiting for Thrift Server. */
  private static final int POLL_INTERVAL_MS = 2000;

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
  static void startSparkContainer() throws Exception {
    containerName = "spark-thrift-test-" + UUID.randomUUID().toString().substring(0, 8);
    hostPort = findFreePort();

    LOGGER.info("Starting Spark Thrift Server container '{}' on port {}", containerName, hostPort);

    // Download Iceberg runtime jar to the mounted data directory.
    // The base Spark image does not include Iceberg — we download from Maven Central
    // and mount via --jars so that CREATE TABLE ... USING iceberg works.
    Path icebergJar = new File(tempDir, "iceberg-spark-runtime.jar").toPath();
    if (!Files.exists(icebergJar)) {
      LOGGER.info("Downloading Iceberg Spark runtime from Maven Central...");
      try (InputStream in = URI.create(ICEBERG_JAR_URL).toURL().openStream()) {
        Files.copy(in, icebergJar, StandardCopyOption.REPLACE_EXISTING);
      }
      LOGGER.info("Downloaded Iceberg runtime jar ({} bytes)", Files.size(icebergJar));
    }

    // Start the Spark Thrift Server container.
    // Key findings from Docker testing:
    //  - start-thriftserver.sh runs as a daemon and the container exits immediately.
    //  - Instead, use spark-submit with HiveThriftServer2 class directly (foreground).
    //  - The Iceberg jar is mounted via the /data volume and loaded via --jars.
    //  - Derby metastore is redirected to /tmp to avoid permission issues.
    ProcessBuilder pb = new ProcessBuilder(
        "docker", "run", "-d",
        "--name", containerName,
        "-p", hostPort + ":10000",
        "-v", tempDir.getAbsolutePath() + ":/data",
        SPARK_IMAGE,
        "/opt/spark/bin/spark-submit",
        "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
        "--master", "local[2]",
        "--name", "Thrift JDBC/ODBC Server",
        "--jars", "/data/iceberg-spark-runtime.jar",
        "--conf", "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby",
        "--hiveconf", "hive.server2.thrift.port=10000",
        "--conf", "spark.sql.catalog.aperio_iceberg=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.aperio_iceberg.type=hadoop",
        "--conf", "spark.sql.catalog.aperio_iceberg.warehouse=/data/warehouse",
        "local:///opt/spark/jars/spark-hive-thriftserver_2.12-3.5.1.jar"
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
      // Capture container logs for debugging
      String logs = getContainerLogs();
      throw new RuntimeException("Failed to start Spark container. Exit code: "
          + (started ? p.exitValue() : "timeout") + ". Logs: " + logs);
    }

    // Build JDBC URL
    jdbcUrl = "jdbc:hive2://localhost:" + hostPort + "/default";
    LOGGER.info("Spark container started. JDBC URL: {}", jdbcUrl);

    // Wait for Thrift Server to become ready
    waitForThriftServer();
  }

  @AfterAll
  static void stopSparkContainer() {
    if (containerName != null) {
      try {
        LOGGER.info("Stopping and removing Spark container: {}", containerName);
        Process p = new ProcessBuilder("docker", "rm", "-f", containerName)
            .redirectErrorStream(true)
            .start();
        p.waitFor(15, TimeUnit.SECONDS);
        LOGGER.info("Spark container removed");
      } catch (Exception e) {
        LOGGER.warn("Failed to remove Spark container: {}", e.getMessage());
      }
    }
  }

  @Test void testConnectToSparkThriftServer() throws Exception {
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      LOGGER.info("Successfully connected to Spark Thrift Server and executed SELECT 1");
    }
  }

  @Test void testCreateDatabaseAndView() throws Exception {
    String dbName = "test_db_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement()) {
      // Create database
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);

      // Create a simple view
      stmt.execute("CREATE OR REPLACE VIEW " + dbName + ".test_view AS SELECT 42 AS answer");

      // Query the view
      try (ResultSet rs = stmt.executeQuery("SELECT answer FROM " + dbName + ".test_view")) {
        assertTrue(rs.next());
        assertEquals(42, rs.getInt("answer"));
      }

      // Cleanup
      stmt.execute("DROP VIEW IF EXISTS " + dbName + ".test_view");
      stmt.execute("DROP DATABASE IF EXISTS " + dbName);
      LOGGER.info("Successfully created database, view, queried, and cleaned up");
    }
  }

  @Test void testQueryParquetFile() throws Exception {
    // Write a simple parquet file using Spark itself
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement()) {

      // Create a temp table and write as parquet
      stmt.execute("CREATE OR REPLACE TEMPORARY VIEW tmp_data AS "
          + "SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'");

      // Write to parquet in the mounted directory
      String parquetPath = "/data/test_data.parquet";
      stmt.execute("CREATE TABLE IF NOT EXISTS parquet_output "
          + "USING parquet LOCATION '" + parquetPath + "' "
          + "AS SELECT * FROM tmp_data");

      // Read back via parquet backtick syntax
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM parquet.`" + parquetPath + "` ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("alice", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("bob", rs.getString("name"));
      }

      LOGGER.info("Successfully wrote and read parquet file via Spark");
    }
  }

  @Test void testIcebergCatalogRegistration() throws Exception {
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement()) {

      // Verify the Iceberg catalog was set up
      // Create a simple Iceberg table
      String tableName = "test_iceberg_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
      try {
        stmt.execute("CREATE TABLE IF NOT EXISTS aperio_iceberg.default." + tableName
            + " (id INT, name STRING) USING iceberg");

        // Insert data
        stmt.execute("INSERT INTO aperio_iceberg.default." + tableName
            + " VALUES (1, 'test')");

        // Query back
        try (ResultSet rs = stmt.executeQuery(
            "SELECT * FROM aperio_iceberg.default." + tableName)) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("id"));
          assertEquals("test", rs.getString("name"));
        }

        LOGGER.info("Successfully created and queried Iceberg table via catalog");
      } finally {
        try {
          stmt.execute("DROP TABLE IF EXISTS aperio_iceberg.default." + tableName);
        } catch (Exception e) {
          LOGGER.debug("Cleanup error: {}", e.getMessage());
        }
      }
    }
  }

  @Test void testSparkConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", hostPort);
    configMap.put("database", "default");
    configMap.put("icebergCatalogType", "hadoop");
    configMap.put("icebergWarehouse", "/data/warehouse");

    SparkConfig config = new SparkConfig(configMap);
    assertEquals("localhost", config.getHost());
    assertEquals(String.valueOf(hostPort), config.getPort());
    assertEquals("default", config.getDatabase());

    // Verify Iceberg catalog settings generation
    String[] icebergSettings = config.toIcebergCatalogSettings("/data/warehouse");
    assertNotNull(icebergSettings);
    assertTrue(icebergSettings.length >= 2);

    // Verify JDBC URL
    Map<String, String> urlConfig = new HashMap<>();
    urlConfig.put("host", config.getHost());
    urlConfig.put("port", config.getPort());
    urlConfig.put("database", config.getDatabase());
    String url = SparkSqlDialect.INSTANCE.buildJdbcUrl(urlConfig);
    assertEquals("jdbc:hive2://localhost:" + hostPort + "/default", url);
  }

  /**
   * Waits for the Spark Thrift Server to become ready by polling JDBC connections.
   */
  private static void waitForThriftServer() throws Exception {
    long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_MS;
    Exception lastException = null;

    LOGGER.info("Waiting for Spark Thrift Server to become ready (timeout: {}ms)...", STARTUP_TIMEOUT_MS);

    while (System.currentTimeMillis() < deadline) {
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
          if (rs.next()) {
            LOGGER.info("Spark Thrift Server is ready!");
            return;
          }
        }
      } catch (Exception e) {
        lastException = e;
        LOGGER.debug("Thrift Server not ready yet: {}", e.getMessage());
      }

      Thread.sleep(POLL_INTERVAL_MS);
    }

    throw new RuntimeException(
        "Spark Thrift Server failed to start within " + STARTUP_TIMEOUT_MS + "ms. "
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
