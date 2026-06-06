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
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.file.execution.spark.SparkConfig;
import org.apache.calcite.adapter.file.jdbc.SparkSqlDialect;

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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Docker-based integration tests for Spark SQL via Thrift Server.
 *
 * <p>These tests start an Apache Spark container with the Thrift Server enabled,
 * then connect via JDBC to verify schema creation, view registration, and query
 * execution. Tests are skipped if Docker is not available.
 *
 * <p>Uses {@code docker} CLI via ProcessBuilder -- zero additional dependencies.
 */
@Tag("integration")
@EnabledIf("isDockerAvailable")
@ResourceLock("docker-integration")
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
  static void startSparkContainer() throws Exception {
    try {
      containerName = "spark-thrift-test-"
          + UUID.randomUUID().toString().substring(0, 8);
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

      LOGGER.info("Starting Spark Thrift Server container '{}' on port {}",
          containerName, hostPort);

      // Create data directory for test files
      dataDir = new File(tempDir, "data");
      if (!dataDir.exists() && !dataDir.mkdirs()) {
        throw new RuntimeException("Failed to create data directory");
      }

      // Download Iceberg runtime jar to the mounted data directory.
      // The base Spark image does not include Iceberg -- we download from Maven Central
      // and mount via --jars so that CREATE TABLE ... USING iceberg works.
      Path icebergJar = new File(tempDir, "iceberg-spark-runtime.jar").toPath();
      if (!Files.exists(icebergJar)) {
        LOGGER.info("Downloading Iceberg Spark runtime from Maven Central...");
        InputStream in = URI.create(ICEBERG_JAR_URL).toURL().openStream();
        try {
          Files.copy(in, icebergJar, StandardCopyOption.REPLACE_EXISTING);
        } finally {
          in.close();
        }
        LOGGER.info("Downloaded Iceberg runtime jar ({} bytes)", Files.size(icebergJar));
      }

      String warehouseDir = new File(tempDir, "warehouse").getAbsolutePath();

      // Start the Spark Thrift Server container.
      // Key: mount tempDir at its own absolute path so host paths match container paths.
      // This lets the adapter write parquet files that Spark can read at the same path.
      ProcessBuilder pb = new ProcessBuilder(
          "docker", "run", "-d",
          "--name", containerName,
          "-p", hostPort + ":10000",
          "-v", tempDir.getAbsolutePath() + ":" + tempDir.getAbsolutePath(),
          SPARK_IMAGE,
          "/opt/spark/bin/spark-submit",
          "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
          "--master", "local[2]",
          "--name", "Thrift JDBC/ODBC Server",
          "--jars", tempDir.getAbsolutePath() + "/iceberg-spark-runtime.jar",
          "--conf", "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby",
          "--hiveconf", "hive.server2.thrift.port=10000",
          "--conf", "spark.sql.catalog.aperio_iceberg=org.apache.iceberg.spark.SparkCatalog",
          "--conf", "spark.sql.catalog.aperio_iceberg.type=hadoop",
          "--conf", "spark.sql.catalog.aperio_iceberg.warehouse=" + warehouseDir,
          "local:///opt/spark/jars/spark-hive-thriftserver_2.12-3.5.1.jar"
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
    } catch (Exception e) {
      LOGGER.warn("Spark container startup failed: {}", e.getMessage());
      assumeTrue(false,
          "Spark Docker container not available: " + e.getMessage());
    }
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
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT 1");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          LOGGER.info("Successfully connected to Spark Thrift Server and executed SELECT 1");
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

  @Test void testCreateDatabaseAndView() throws Exception {
    String dbName = "test_db_"
        + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create database
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);

        // Create a simple view
        stmt.execute("CREATE OR REPLACE VIEW " + dbName
            + ".test_view AS SELECT 42 AS answer");

        // Query the view
        ResultSet rs = stmt.executeQuery(
            "SELECT answer FROM " + dbName + ".test_view");
        try {
          assertTrue(rs.next());
          assertEquals(42, rs.getInt("answer"));
        } finally {
          rs.close();
        }

        // Cleanup
        stmt.execute("DROP VIEW IF EXISTS " + dbName + ".test_view");
        stmt.execute("DROP DATABASE IF EXISTS " + dbName);
        LOGGER.info("Successfully created database, view, queried, and cleaned up");
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testQueryParquetFile() throws Exception {
    // Write a simple parquet file using Spark itself
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create a temp table and write as parquet
        stmt.execute("CREATE OR REPLACE TEMPORARY VIEW tmp_data AS "
            + "SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'");

        // Write to parquet in the mounted directory (using tempDir-based path)
        String parquetPath = dataDir.getAbsolutePath() + "/test_data.parquet";
        stmt.execute("CREATE TABLE IF NOT EXISTS parquet_output "
            + "USING parquet LOCATION '" + parquetPath + "' "
            + "AS SELECT * FROM tmp_data");

        // Read back via parquet backtick syntax
        ResultSet rs = stmt.executeQuery(
            "SELECT * FROM parquet.`" + parquetPath + "` ORDER BY id");
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

        LOGGER.info("Successfully wrote and read parquet file via Spark");
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testIcebergCatalogRegistration() throws Exception {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Verify the Iceberg catalog was set up
        String tableName = "test_iceberg_"
            + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        try {
          stmt.execute("CREATE TABLE IF NOT EXISTS aperio_iceberg.default." + tableName
              + " (id INT, name STRING) USING iceberg");

          // Insert data
          stmt.execute("INSERT INTO aperio_iceberg.default." + tableName
              + " VALUES (1, 'test')");

          // Query back
          ResultSet rs = stmt.executeQuery(
              "SELECT * FROM aperio_iceberg.default." + tableName);
          try {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("test", rs.getString("name"));
          } finally {
            rs.close();
          }

          LOGGER.info("Successfully created and queried Iceberg table via catalog");
        } finally {
          try {
            stmt.execute("DROP TABLE IF EXISTS aperio_iceberg.default." + tableName);
          } catch (Exception e) {
            LOGGER.debug("Cleanup error: {}", e.getMessage());
          }
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testSparkConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", hostPort);
    configMap.put("database", "default");
    configMap.put("icebergCatalogType", "hadoop");
    String warehouseDir = new File(tempDir, "warehouse").getAbsolutePath();
    configMap.put("icebergWarehouse", warehouseDir);

    SparkConfig config = new SparkConfig(configMap);
    assertEquals("localhost", config.getHost());
    assertEquals(String.valueOf(hostPort), config.getPort());
    assertEquals("default", config.getDatabase());

    // Verify Iceberg catalog settings generation
    String[] icebergSettings = config.toIcebergCatalogSettings(warehouseDir);
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

  @Test void testAdapterIntegrationWithCalcite() throws Exception {
    // Each adapter test uses a unique database to avoid parallel test conflicts
    String db = "adapter_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createSparkDatabase(db);
    createSparkTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    Connection conn = openCalciteAdapterConnection(db);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT * FROM employees ORDER BY id");
        try {
          int rowCount = 0;
          while (rs.next()) {
            rowCount++;
          }
          assertEquals(3, rowCount, "Expected 3 employee rows");
          LOGGER.info("testAdapterIntegrationWithCalcite: queried {} rows via Calcite adapter",
              rowCount);
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
    String db = "multi_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createSparkDatabase(db);
    createSparkTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");
    createSparkTable(db, "departments",
        "SELECT 10 AS id, 'Engineering' AS name, 500000.0 AS budget "
        + "UNION ALL SELECT 20, 'Marketing', 300000.0");

    Connection conn = openCalciteAdapterConnection(db);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Query employees
        ResultSet rs1 = stmt.executeQuery("SELECT * FROM employees ORDER BY id");
        try {
          int empCount = 0;
          while (rs1.next()) {
            empCount++;
          }
          assertEquals(3, empCount, "Expected 3 employee rows");
        } finally {
          rs1.close();
        }

        // Query departments
        ResultSet rs2 = stmt.executeQuery("SELECT * FROM departments ORDER BY id");
        try {
          int deptCount = 0;
          while (rs2.next()) {
            deptCount++;
          }
          assertEquals(2, deptCount, "Expected 2 department rows");
        } finally {
          rs2.close();
        }

        LOGGER.info("testAdapterWithMultipleTables: queried employees and departments");
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testAdapterQueryWithFilter() throws Exception {
    String db = "filter_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createSparkDatabase(db);
    createSparkTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    Connection conn = openCalciteAdapterConnection(db);
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
          LOGGER.info("testAdapterQueryWithFilter: filter query returned Bob and Charlie");
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
   * Tests the full FileSchemaFactory → SparkJdbcSchemaFactory code path.
   *
   * <p>Pre-creates tables in Spark, writes a dummy parquet file for FileSchema
   * discovery, then opens a CalciteConnection via model JSON that routes through the
   * production FileSchemaFactory. This exercises SparkJdbcSchemaFactory.create(),
   * SparkJdbcSchema, SparkConvention, and SparkSqlDialect.
   */
  @Test void testFileSchemaFactoryIntegration() throws Exception {
    // Step 1: Pre-create a database and table in Spark
    String dbName = "fstest";
    createSparkDatabase(dbName);
    createSparkTable(dbName, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    // Step 2: Write a parquet file for FileSchema to discover
    File fsDataDir = new File(tempDir, "fs_spark_data");
    fsDataDir.mkdirs();
    writeParquetFile(fsDataDir, "dummy",
        "SELECT 1 AS id, 'dummy' AS name, 0.0 AS salary");

    // Step 3: Write model JSON that routes through FileSchemaFactory
    File modelFile = new File(tempDir, "spark_model.json");
    java.io.FileWriter modelWriter = new java.io.FileWriter(modelFile);
    try {
      modelWriter.write("{\n"
          + "  \"version\": \"1.0\",\n"
          + "  \"defaultSchema\": \"" + dbName + "\",\n"
          + "  \"schemas\": [{\n"
          + "    \"name\": \"" + dbName + "\",\n"
          + "    \"type\": \"custom\",\n"
          + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
          + "    \"operand\": {\n"
          + "      \"directory\": \"" + fsDataDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n"
          + "      \"executionEngine\": \"spark\",\n"
          + "      \"ephemeralCache\": true,\n"
          + "      \"sparkConfig\": {\n"
          + "        \"host\": \"localhost\",\n"
          + "        \"port\": " + hostPort + ",\n"
          + "        \"database\": \"default\"\n"
          + "      }\n"
          + "    }\n"
          + "  }]\n"
          + "}\n");
    } finally {
      modelWriter.close();
    }

    // Step 4: Open CalciteConnection via model (exercises FileSchemaFactory → SparkJdbcSchemaFactory)
    java.sql.Driver calciteDriver = new org.apache.calcite.jdbc.Driver();
    Connection conn = calciteDriver.connect(
        "jdbc:calcite:model=" + modelFile.getAbsolutePath() + ";caseSensitive=false",
        new java.util.Properties());
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT * FROM employees ORDER BY id");
        try {
          int rowCount = 0;
          while (rs.next()) {
            rowCount++;
          }
          assertEquals(3, rowCount, "Expected 3 employee rows via FileSchemaFactory path");
          LOGGER.info("FileSchemaFactory → SparkJdbcSchemaFactory integration test passed");
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
    String db = "agg_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createSparkDatabase(db);
    createSparkTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    Connection conn = openCalciteAdapterConnection(db);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) AS cnt, SUM(salary) AS total FROM employees");
        try {
          assertTrue(rs.next(), "Expected one aggregation row");
          assertEquals(3, rs.getInt("cnt"));
          assertEquals(255000.0, rs.getDouble("total"), 0.01);
          LOGGER.info("testAdapterAggregation: cnt=3, total=255000.0");
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
   * Creates a database in Spark via the Thrift Server.
   */
  private static void createSparkDatabase(String dbName) throws Exception {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
        LOGGER.info("Created Spark database: {}", dbName);
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Creates a table in a Spark database via CTAS.
   */
  private static void createSparkTable(String dbName, String tableName,
      String selectSql) throws Exception {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        String sql = "CREATE TABLE " + dbName + "." + tableName
            + " USING parquet AS " + selectSql;
        LOGGER.info("Creating Spark table: {}", sql);
        stmt.execute(sql);
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Opens a Calcite connection with a JdbcSchema backed by a direct Hive JDBC
   * driver instance. This bypasses both DriverManager (JVM-global singleton) and
   * DBCP2's BasicDataSource, ensuring no cross-contamination when multiple JDBC
   * drivers (Hive, Trino, ClickHouse) coexist in the same JVM.
   */
  private static Connection openCalciteAdapterConnection(String dbName) throws Exception {
    java.util.Properties info = new java.util.Properties();
    info.setProperty("caseSensitive", "false");
    java.sql.Driver calciteDriver = new org.apache.calcite.jdbc.Driver();
    Connection conn = calciteDriver.connect("jdbc:calcite:", info);
    org.apache.calcite.jdbc.CalciteConnection calciteConn =
        conn.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);

    // Create isolated DataSource using a dedicated Hive driver instance
    // (loaded via reflection since Hive JDBC is a runtime-only dependency)
    final java.sql.Driver hiveDriver = (java.sql.Driver) Class.forName(
        "org.apache.hive.jdbc.HiveDriver").getDeclaredConstructor().newInstance();
    final String url = "jdbc:hive2://localhost:" + hostPort + "/" + dbName;
    javax.sql.DataSource ds = new javax.sql.DataSource() {
      @Override public Connection getConnection() throws java.sql.SQLException {
        return hiveDriver.connect(url, new java.util.Properties());
      }
      @Override public Connection getConnection(String u, String p) throws java.sql.SQLException {
        return hiveDriver.connect(url, new java.util.Properties());
      }
      @Override public java.io.PrintWriter getLogWriter() { return null; }
      @Override public void setLogWriter(java.io.PrintWriter out) { }
      @Override public void setLoginTimeout(int seconds) { }
      @Override public int getLoginTimeout() { return 0; }
      @Override public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("Spark");
      }
      @Override public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
        if (iface.isInstance(this)) { return iface.cast(this); }
        throw new java.sql.SQLException("Cannot unwrap to " + iface);
      }
      @Override public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
      }
    };

    // Create JdbcSchema with HiveSqlDialectFactory for proper Spark SQL generation
    org.apache.calcite.sql.SqlDialectFactory dialectFactory =
        new org.apache.calcite.adapter.file.jdbc.HiveSqlDialectFactory();
    org.apache.calcite.schema.SchemaPlus rootSchema = calciteConn.getRootSchema();
    org.apache.calcite.adapter.jdbc.JdbcSchema jdbcSchema =
        org.apache.calcite.adapter.jdbc.JdbcSchema.create(
            rootSchema, "SP_TEST", ds, dialectFactory, null, dbName);
    rootSchema.add("SP_TEST", jdbcSchema);
    calciteConn.setSchema("SP_TEST");
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
   * Waits for the Spark Thrift Server to become ready by polling JDBC connections.
   */
  private static void waitForThriftServer() throws Exception {
    long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_MS;
    Exception lastException = null;

    LOGGER.info("Waiting for Spark Thrift Server to become ready (timeout: {}ms)...",
        STARTUP_TIMEOUT_MS);

    while (System.currentTimeMillis() < deadline) {
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(jdbcUrl);
        try {
          Statement stmt = conn.createStatement();
          try {
            ResultSet rs = stmt.executeQuery("SELECT 1");
            try {
              if (rs.next()) {
                LOGGER.info("Spark Thrift Server is ready!");
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
