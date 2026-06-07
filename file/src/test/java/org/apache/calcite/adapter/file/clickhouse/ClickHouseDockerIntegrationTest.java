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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.jdbc.ClickHouseDialect;
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
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Docker-based integration tests for ClickHouse execution engine.
 *
 * <p>These tests start a ClickHouse container, then connect via JDBC to verify
 * connectivity, table creation, Parquet file querying, dialect SQL validation,
 * and full Calcite adapter integration. Tests are skipped if Docker is not available.
 *
 * <p>Uses {@code docker} CLI via ProcessBuilder -- zero additional dependencies.
 */
@Tag("integration")
@EnabledIf("isDockerAvailable")
@ResourceLock("docker-integration")
class ClickHouseDockerIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClickHouseDockerIntegrationTest.class);

  /** ClickHouse Docker image version. */
  private static final String CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.3";

  /** Path inside the container for raw JDBC tests (relative to user_files_path). */
  private static String containerDataPath;

  /** Timeout for waiting for ClickHouse to become ready. */
  private static final int STARTUP_TIMEOUT_MS = 60_000;

  /** Poll interval when waiting for ClickHouse. */
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
  static void startClickHouseContainer() throws Exception {
    try {
      containerName = "clickhouse-test-" + UUID.randomUUID().toString().substring(0, 8);
      hostPort = findFreePort();

      // Register a JVM shutdown hook so the container is removed even if the
      // test is interrupted (e.g. by gtimeout or IDE stop). @AfterAll handles
      // normal termination; the hook covers SIGTERM / unexpected JVM exits.
      final String nameToRemove = containerName;
      Runtime.getRuntime().addShutdownHook(
          new Thread(new Runnable() {
        @Override public void run() {
          try {
            new ProcessBuilder("docker", "rm", "-f", nameToRemove)
                .redirectErrorStream(true).start().waitFor(10, TimeUnit.SECONDS);
          } catch (Exception ignored) {
          }
        }
      }));

      LOGGER.info("Starting ClickHouse container '{}' on port {}", containerName, hostPort);

      dataDir = new File(tempDir, "data");
      if (!dataDir.exists() && !dataDir.mkdirs()) {
        throw new RuntimeException("Failed to create data directory");
      }
      containerDataPath = dataDir.getAbsolutePath();

      // Create ClickHouse config override to set user_files_path to the mounted
      // tempDir. This allows ClickHouse's file() function to read parquet files
      // written by the host JVM (adapter tests), since both use the same absolute path.
      File configDir = new File(tempDir, "config.d");
      configDir.mkdirs();
      PrintWriter configWriter =
          new PrintWriter(new FileWriter(new File(configDir, "user_files.xml")));
      try {
        configWriter.println("<clickhouse>");
        configWriter.println("  <listen_host>0.0.0.0</listen_host>");
        configWriter.println("  <user_files_path>"
            + tempDir.getAbsolutePath() + "</user_files_path>");
        configWriter.println("</clickhouse>");
      } finally {
        configWriter.close();
      }

      // Start the ClickHouse container with:
      // 1. tempDir mounted at its own host absolute path (so paths match host<->container)
      // 2. Config override setting user_files_path to tempDir
      // This lets both raw JDBC tests and adapter tests use the same paths.
      ProcessBuilder pb =
          new ProcessBuilder("docker", "run", "-d",
          "--name", containerName,
          "-p", hostPort + ":8123",
          "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
          "-e", "CLICKHOUSE_PASSWORD=",
          "-v", tempDir.getAbsolutePath() + ":" + tempDir.getAbsolutePath(),
          "-v", configDir.getAbsolutePath() + ":/etc/clickhouse-server/config.d",
          "--ulimit", "nofile=262144:262144",
          CLICKHOUSE_IMAGE);
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
        throw new RuntimeException("Failed to start ClickHouse container. Exit code: "
            + (started ? p.exitValue() : "timeout") + ". Logs: " + logs);
      }

      // Build JDBC URL
      jdbcUrl = "jdbc:clickhouse://localhost:" + hostPort + "/default";
      LOGGER.info("ClickHouse container started. JDBC URL: {}", jdbcUrl);

      // Wait for ClickHouse to become ready
      waitForClickHouse();
    } catch (Exception e) {
      LOGGER.warn("ClickHouse container startup failed: {}", e.getMessage());
      assumeTrue(false,
          "ClickHouse Docker container not available: " + e.getMessage());
    }
  }

  @AfterAll
  static void stopClickHouseContainer() {
    if (containerName != null) {
      try {
        LOGGER.info("Stopping and removing ClickHouse container: {}", containerName);
        Process p = new ProcessBuilder("docker", "rm", "-f", containerName)
            .redirectErrorStream(true)
            .start();
        p.waitFor(15, TimeUnit.SECONDS);
        LOGGER.info("ClickHouse container removed");
      } catch (Exception e) {
        LOGGER.warn("Failed to remove ClickHouse container: {}", e.getMessage());
      }
    }
  }

  @Test void testConnectToClickHouse() throws Exception {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT 1");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          LOGGER.info("Successfully connected to ClickHouse and executed SELECT 1");
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

  @Test void testCreateDatabaseAndTable() throws Exception {
    String dbName = "test_db_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create database
        stmt.execute("CREATE DATABASE " + dbName);

        // Create table
        stmt.execute("CREATE TABLE " + dbName + ".test_table "
            + "(id Int32, name String, salary Float64) "
            + "ENGINE = MergeTree() ORDER BY id");

        // Insert data
        stmt.execute("INSERT INTO " + dbName + ".test_table VALUES "
            + "(1, 'Alice', 75000.0), (2, 'Bob', 85000.0)");

        // Query back
        ResultSet rs =
            stmt.executeQuery("SELECT id, name, salary FROM " + dbName + ".test_table ORDER BY id");
        try {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("id"));
          assertEquals("Alice", rs.getString("name"));
          assertEquals(75000.0, rs.getDouble("salary"), 0.01);

          assertTrue(rs.next());
          assertEquals(2, rs.getInt("id"));
          assertEquals("Bob", rs.getString("name"));
          assertEquals(85000.0, rs.getDouble("salary"), 0.01);
        } finally {
          rs.close();
        }

        LOGGER.info("Successfully created database, table, inserted and queried data");

        // Cleanup
        stmt.execute("DROP TABLE IF EXISTS " + dbName + ".test_table");
        stmt.execute("DROP DATABASE IF EXISTS " + dbName);
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testQueryParquetFile() throws Exception {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create a temporary table and insert data
        stmt.execute("CREATE TABLE default.temp_data "
            + "(id Int32, name String) ENGINE = MergeTree() ORDER BY id");
        stmt.execute("INSERT INTO default.temp_data VALUES (1, 'alice'), (2, 'bob')");

        // Export to Parquet file via ClickHouse file() function
        String parquetFile = containerDataPath + "/query_test.parquet";
        stmt.execute("INSERT INTO FUNCTION file('" + parquetFile + "', 'Parquet') "
            + "SELECT * FROM default.temp_data");

        // Create a view reading from the Parquet file
        stmt.execute("CREATE OR REPLACE VIEW default.test_parquet "
            + "AS SELECT * FROM file('" + parquetFile + "', 'Parquet')");

        // Query the view
        ResultSet rs =
            stmt.executeQuery("SELECT * FROM default.test_parquet ORDER BY id");
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

        LOGGER.info("Successfully wrote and read Parquet data via ClickHouse");

        // Cleanup
        stmt.execute("DROP VIEW IF EXISTS default.test_parquet");
        stmt.execute("DROP TABLE IF EXISTS default.temp_data");
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testClickHouseDialectSqlInServer() throws Exception {
    // First ensure the Parquet file exists for the view to reference
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Create temp data and export to Parquet
        stmt.execute("CREATE TABLE IF NOT EXISTS default.dialect_temp "
            + "(id Int32, name String) ENGINE = MergeTree() ORDER BY id");
        stmt.execute("INSERT INTO default.dialect_temp VALUES (1, 'test')");
        String dialectParquet = containerDataPath + "/dialect_test.parquet";
        stmt.execute("INSERT INTO FUNCTION file('" + dialectParquet + "', 'Parquet') "
            + "SELECT * FROM default.dialect_temp");

        // Generate SQL using ClickHouseDialect and execute against live server
        String createViewSql =
            ClickHouseDialect.INSTANCE.createParquetViewSql("default", "dialect_test", dialectParquet, false);
        LOGGER.info("Dialect-generated CREATE VIEW SQL: {}", createViewSql);
        stmt.execute(createViewSql);

        // Verify the view works
        ResultSet rs = stmt.executeQuery("SELECT * FROM default.dialect_test");
        try {
          assertTrue(rs.next());
          LOGGER.info("Dialect-generated view works correctly");
        } finally {
          rs.close();
        }

        // Generate and execute DROP SQL
        String dropViewSql = ClickHouseDialect.INSTANCE.dropViewSql("default", "dialect_test");
        LOGGER.info("Dialect-generated DROP VIEW SQL: {}", dropViewSql);
        stmt.execute(dropViewSql);

        // Cleanup
        stmt.execute("DROP TABLE IF EXISTS default.dialect_temp");
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  @Test void testAdapterIntegrationWithCalcite() throws Exception {
    String db = "adapter_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createClickHouseDatabase(db);
    createClickHouseTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    Connection conn = openCalciteAdapterConnection(db);
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
    String db = "multi_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createClickHouseDatabase(db);
    createClickHouseTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");
    createClickHouseTable(db, "departments",
        "SELECT 1 AS id, 'Engineering' AS name, 500000.0 AS budget "
        + "UNION ALL SELECT 2, 'Marketing', 300000.0");

    Connection conn = openCalciteAdapterConnection(db);
    try {
      Statement stmt = conn.createStatement();
      try {
        // Query employees table
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

        // Query departments table
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
    String db = "filter_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    createClickHouseDatabase(db);
    createClickHouseTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    Connection conn = openCalciteAdapterConnection(db);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs =
            stmt.executeQuery("SELECT name, salary FROM employees WHERE salary > 80000 ORDER BY salary");
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
   * Tests the full FileSchemaFactory → ClickHouseJdbcSchemaFactory code path.
   *
   * <p>Pre-creates tables in ClickHouse, writes a dummy parquet file for FileSchema
   * discovery, then opens a CalciteConnection via model JSON that routes through the
   * production FileSchemaFactory. This exercises ClickHouseJdbcSchemaFactory.create(),
   * ClickHouseJdbcSchema, ClickHouseConvention, and ClickHouseDialect.
   */
  @Test void testFileSchemaFactoryIntegration() throws Exception {
    // Step 1: Pre-create a database and table in ClickHouse
    String dbName = "fstest";
    createClickHouseDatabase(dbName);
    createClickHouseTable(dbName, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    // Step 2: Write a parquet file for FileSchema to discover.
    // The engine-specific factory will create a view for it AND discover
    // the pre-created table via JDBC metadata.
    File fsDataDir = new File(tempDir, "fs_ch_data");
    fsDataDir.mkdirs();
    writeParquetFile(fsDataDir, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    // Step 3: Write model JSON that routes through FileSchemaFactory
    File modelFile = new File(tempDir, "ch_model.json");
    FileWriter modelWriter = new FileWriter(modelFile);
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
          + "      \"executionEngine\": \"clickhouse\",\n"
          + "      \"ephemeralCache\": true,\n"
          + "      \"clickhouseConfig\": {\n"
          + "        \"host\": \"localhost\",\n"
          + "        \"port\": " + hostPort + "\n"
          + "      }\n"
          + "    }\n"
          + "  }]\n"
          + "}\n");
    } finally {
      modelWriter.close();
    }

    // Step 4: Open CalciteConnection via model (exercises FileSchemaFactory → ClickHouseJdbcSchemaFactory)
    java.sql.Driver calciteDriver = new org.apache.calcite.jdbc.Driver();
    Connection conn =
        calciteDriver.connect("jdbc:calcite:model=" + modelFile.getAbsolutePath() + ";caseSensitive=false",
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

          LOGGER.info("FileSchemaFactory → ClickHouseJdbcSchemaFactory integration test passed");
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
    createClickHouseDatabase(db);
    createClickHouseTable(db, "employees",
        "SELECT 1 AS id, 'Alice' AS name, 75000.0 AS salary "
        + "UNION ALL SELECT 2, 'Bob', 85000.0 "
        + "UNION ALL SELECT 3, 'Charlie', 95000.0");

    Connection conn = openCalciteAdapterConnection(db);
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs =
            stmt.executeQuery("SELECT COUNT(*) AS cnt, SUM(salary) AS total FROM employees");
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
   * Creates a database in ClickHouse.
   */
  private static void createClickHouseDatabase(String dbName) throws Exception {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.execute("CREATE DATABASE IF NOT EXISTS \"" + dbName + "\"");
        LOGGER.info("Created ClickHouse database: {}", dbName);
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Creates a table in a ClickHouse database via CTAS.
   */
  private static void createClickHouseTable(String dbName, String tableName,
      String selectSql) throws Exception {
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try {
      Statement stmt = conn.createStatement();
      try {
        String sql = "CREATE TABLE \"" + dbName + "\".\"" + tableName + "\" "
            + "ENGINE = MergeTree() ORDER BY tuple() AS " + selectSql;
        LOGGER.info("Creating ClickHouse table: {}", sql);
        stmt.execute(sql);
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * Opens a Calcite connection with a JdbcSchema backed by a direct ClickHouse
   * JDBC driver instance. This bypasses both DriverManager (JVM-global singleton)
   * and FileSchemaFactory, ensuring no cross-contamination when multiple JDBC
   * drivers (Hive, Trino, ClickHouse) coexist in the same JVM.
   */
  private static Connection openCalciteAdapterConnection(String dbName) throws Exception {
    java.util.Properties info = new java.util.Properties();
    info.setProperty("caseSensitive", "false");
    java.sql.Driver calciteDriver = new org.apache.calcite.jdbc.Driver();
    Connection conn = calciteDriver.connect("jdbc:calcite:", info);
    CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);

    // Create isolated DataSource using a dedicated ClickHouse driver instance
    final java.sql.Driver chDriver =
        (java.sql.Driver) Class.forName("com.clickhouse.jdbc.ClickHouseDriver").getDeclaredConstructor().newInstance();
    final String url = "jdbc:clickhouse://localhost:" + hostPort + "/" + dbName;
    DataSource ds = new DataSource() {
      @Override public Connection getConnection() throws SQLException {
        return chDriver.connect(url, new java.util.Properties());
      }
      @Override public Connection getConnection(String u, String p) throws SQLException {
        return chDriver.connect(url, new java.util.Properties());
      }
      @Override public PrintWriter getLogWriter() { return null; }
      @Override public void setLogWriter(PrintWriter out) { }
      @Override public void setLoginTimeout(int seconds) { }
      @Override public int getLoginTimeout() { return 0; }
      @Override public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("ClickHouse");
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
    JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, "CH_TEST", ds, dbName, null);
    rootSchema.add("CH_TEST", jdbcSchema);
    calciteConn.setSchema("CH_TEST");
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
   * Waits for ClickHouse to become ready by polling the HTTP /ping endpoint.
   */
  private static void waitForClickHouse() throws Exception {
    long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_MS;
    Exception lastException = null;

    LOGGER.info("Waiting for ClickHouse to become ready (timeout: {}ms)...", STARTUP_TIMEOUT_MS);

    while (System.currentTimeMillis() < deadline) {
      try {
        HttpURLConnection connection = (HttpURLConnection)
            URI.create("http://localhost:" + hostPort + "/ping").toURL().openConnection();
        try {
          connection.setRequestMethod("GET");
          connection.setConnectTimeout(2000);
          connection.setReadTimeout(2000);
          int responseCode = connection.getResponseCode();
          if (responseCode == 200) {
            LOGGER.info("ClickHouse is ready!");
            return;
          }
        } finally {
          connection.disconnect();
        }
      } catch (Exception e) {
        lastException = e;
        LOGGER.debug("ClickHouse not ready yet: {}", e.getMessage());
      }

      Thread.sleep(POLL_INTERVAL_MS);
    }

    throw new RuntimeException(
        "ClickHouse failed to start within " + STARTUP_TIMEOUT_MS + "ms. "
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
