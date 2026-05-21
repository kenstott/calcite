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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseConfig;
import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseExecutionEngine;
import org.apache.calcite.adapter.file.jdbc.ClickHouseDialect;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * Factory for creating JDBC schema backed by ClickHouse.
 *
 * <p>Supports two deployment modes:
 * <ul>
 *   <li><b>Server mode</b>: Connect to an existing ClickHouse server via JDBC</li>
 *   <li><b>Local/embedded mode</b>: Start {@code clickhouse-local} as a subprocess
 *       with HTTP interface on an ephemeral port, connect via standard JDBC</li>
 * </ul>
 *
 * <p>Both modes use the same {@link ClickHouseJdbcSchema} and JDBC connection logic.
 * The only difference is how the connection URL is determined.
 */
public class ClickHouseJdbcSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseJdbcSchemaFactory.class);

  /** Default timeout in milliseconds to wait for clickhouse-local to start. */
  private static final int LOCAL_STARTUP_TIMEOUT_MS = 30_000;

  /** Poll interval in milliseconds when waiting for clickhouse-local. */
  private static final int LOCAL_POLL_INTERVAL_MS = 200;

  /**
   * Pool of shared ClickHouse instances keyed by connection identifier.
   * For server mode: keyed by JDBC URL. For local mode: keyed by data directory path.
   */
  private static final Map<String, SharedInstanceInfo> INSTANCE_POOL = new ConcurrentHashMap<>();

  /**
   * Information about a shared ClickHouse instance.
   */
  private static class SharedInstanceInfo {
    final DataSource dataSource;
    final Connection setupConnection;
    final String jdbcUrl;
    final Process localProcess; // Non-null only for local mode

    SharedInstanceInfo(DataSource dataSource, Connection setupConnection,
                       String jdbcUrl, Process localProcess) {
      this.dataSource = dataSource;
      this.setupConnection = setupConnection;
      this.jdbcUrl = jdbcUrl;
      this.localProcess = localProcess;
    }
  }

  /**
   * Creates a ClickHouse JDBC schema with full configuration.
   *
   * @param parentSchema parent Calcite schema
   * @param schemaName name of the schema to create
   * @param directoryPath path to the data directory
   * @param recursive whether to search recursively for files
   * @param fileSchema the FileSchema with conversion metadata
   * @param operand schema operand map containing clickhouseConfig and other settings
   * @return a JdbcSchema backed by ClickHouse
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName,
                                  String directoryPath, boolean recursive,
                                  org.apache.calcite.adapter.file.FileSchema fileSchema,
                                  Map<String, Object> operand) {
    LOGGER.info("Creating ClickHouse JDBC schema for: {} with name: {} (recursive={}, hasFileSchema={})",
                directoryPath, schemaName, recursive, fileSchema != null);

    return createInternal(parentSchema, schemaName, directoryPath, recursive, fileSchema, operand);
  }

  /**
   * Internal implementation of create() with full operand support.
   */
  private static JdbcSchema createInternal(SchemaPlus parentSchema, String schemaName,
                                           String directoryPath, boolean recursive,
                                           org.apache.calcite.adapter.file.FileSchema fileSchema,
                                           Map<String, Object> operand) {

    // Parse ClickHouse configuration
    @SuppressWarnings("unchecked")
    Map<String, Object> clickhouseConfigMap = operand != null
        ? (Map<String, Object>) operand.get("clickhouseConfig") : null;
    ClickHouseConfig config = clickhouseConfigMap != null
        ? new ClickHouseConfig(clickhouseConfigMap) : new ClickHouseConfig();

    LOGGER.info("ClickHouse config: {}", config);

    try {
      // Create a dedicated driver instance instead of using DriverManager.
      // DriverManager is a JVM-global singleton; when multiple JDBC drivers
      // (Hive, Trino, ClickHouse) coexist, DriverManager.getConnection() may
      // dispatch to the wrong driver. Direct driver.connect() is deterministic.
      final java.sql.Driver clickhouseDriver =
          (java.sql.Driver) Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
              .getDeclaredConstructor().newInstance();

      String jdbcUrl;
      Process localProcess = null;
      String poolKey;

      if (config.isLocalMode()) {
        // Embedded mode: start clickhouse-local subprocess
        String dataDir = config.getDataDir();
        if (dataDir == null) {
          // Default data directory
          File workingDir = new File(System.getProperty("user.dir"));
          dataDir =
              new File(new File(workingDir, ".aperio"), ".clickhouse" + File.separator + schemaName).getAbsolutePath();
        }
        poolKey = dataDir;

        // Check if we already have a running instance for this data directory
        SharedInstanceInfo sharedInfo = INSTANCE_POOL.get(poolKey);
        if (sharedInfo != null && sharedInfo.localProcess != null && sharedInfo.localProcess.isAlive()) {
          LOGGER.info("Reusing existing clickhouse-local instance for: {}", dataDir);
          return createSchemaWithConnection(sharedInfo.dataSource, sharedInfo.setupConnection,
              parentSchema, schemaName, directoryPath, recursive, fileSchema,
              operand, config, sharedInfo.localProcess);
        }

        // Find clickhouse-local binary
        String binaryPath = ClickHouseExecutionEngine.findLocalBinaryPath(config.getLocalBinaryPath());
        if (binaryPath == null) {
          throw new RuntimeException(
              "clickhouse-local binary not found. Set CLICKHOUSE_LOCAL_PATH environment variable "
              + "or add clickhouse-local to PATH, or configure localBinaryPath in clickhouseConfig");
        }

        // Find an ephemeral port
        int port = findFreePort();

        // Ensure data directory exists
        File dataDirFile = new File(dataDir);
        if (!dataDirFile.exists()) {
          dataDirFile.mkdirs();
          LOGGER.info("Created ClickHouse data directory: {}", dataDir);
        }

        // Start clickhouse-local with HTTP interface
        LOGGER.info("Starting clickhouse-local on port {} with data dir: {}", port, dataDir);
        ProcessBuilder pb =
            new ProcessBuilder(binaryPath,
            "--http_port", String.valueOf(port),
            "--path", dataDir,
            "--log-level", "warning");
        pb.redirectErrorStream(true);
        localProcess = pb.start();

        // Register shutdown hook for cleanup
        final Process processRef = localProcess;
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
          if (processRef.isAlive()) {
            LOGGER.info("Shutdown hook: destroying clickhouse-local process");
            processRef.destroy();
          }
        }));

        // Wait for HTTP interface to become available
        waitForReady("http://localhost:" + port + "/ping", LOCAL_STARTUP_TIMEOUT_MS);

        jdbcUrl = "jdbc:clickhouse://localhost:" + port + "/" + config.getDatabase();
        LOGGER.info("clickhouse-local started successfully at {}", jdbcUrl);

      } else {
        // Server mode: connect to existing ClickHouse server
        jdbcUrl =
            String.format("jdbc:clickhouse://%s:%s/%s", config.getHost(), config.getPort(), config.getDatabase());
        poolKey = jdbcUrl;

        // Check if we already have a connection for this server
        SharedInstanceInfo sharedInfo = INSTANCE_POOL.get(poolKey);
        if (sharedInfo != null) {
          LOGGER.info("Reusing existing ClickHouse connection for: {}", jdbcUrl);
          return createSchemaWithConnection(sharedInfo.dataSource, sharedInfo.setupConnection,
              parentSchema, schemaName, directoryPath, recursive, fileSchema,
              operand, config, null);
        }
      }

      // Create initial connection for setup using direct driver instance
      Connection setupConn = clickhouseDriver.connect(jdbcUrl, new java.util.Properties());

      // Apply ClickHouse settings
      String[] settings = config.toClickHouseSettings();
      for (String setting : settings) {
        try {
          setupConn.createStatement().execute(setting);
          LOGGER.debug("Applied ClickHouse setting: {}", setting);
        } catch (SQLException e) {
          LOGGER.warn("Failed to apply setting '{}': {}", setting, e.getMessage());
        }
      }

      // Configure S3 credentials if available
      configureS3Credentials(setupConn, fileSchema);

      // Create database (ClickHouse uses databases instead of schemas)
      String createDbSql = "CREATE DATABASE IF NOT EXISTS \"" + schemaName + "\"";
      LOGGER.info("Creating ClickHouse database: \"{}\"", schemaName);
      setupConn.createStatement().execute(createDbSql);
      LOGGER.info("Created ClickHouse database: \"{}\"", schemaName);

      // Register files as views
      registerFilesAsViews(setupConn, directoryPath, recursive, schemaName, fileSchema);

      // Register SQL views from operand
      registerSqlViewsInClickHouse(setupConn, schemaName, operand);

      // Create DataSource for new connections - use schemaName as the database
      // so that JDBC metadata queries find the views created in the schema's database
      final String finalJdbcUrl =
          String.format("jdbc:clickhouse://%s:%s/%s", config.getHost(), config.getPort(), schemaName);
      DataSource dataSource = new DataSource() {
        @Override public Connection getConnection() throws SQLException {
          return clickhouseDriver.connect(finalJdbcUrl, new java.util.Properties());
        }

        @Override public Connection getConnection(String username, String password) throws SQLException {
          return getConnection();
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() {
          return java.util.logging.Logger.getLogger("ClickHouse");
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
          if (iface.isInstance(this)) {
            return iface.cast(this);
          }
          throw new SQLException("Cannot unwrap to " + iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface) {
          return iface.isInstance(this);
        }
      };

      // Store in pool for reuse
      SharedInstanceInfo sharedInfo = new SharedInstanceInfo(dataSource, setupConn, jdbcUrl, localProcess);
      INSTANCE_POOL.put(poolKey, sharedInfo);

      return createSchemaWithConnection(dataSource, setupConn, parentSchema, schemaName,
          directoryPath, recursive, fileSchema, operand, config, localProcess);

    } catch (Exception e) {
      throw new RuntimeException("Failed to create ClickHouse JDBC schema", e);
    }
  }

  /**
   * Creates a ClickHouseJdbcSchema with the given connection setup.
   */
  private static JdbcSchema createSchemaWithConnection(DataSource dataSource, Connection setupConn,
      SchemaPlus parentSchema, String schemaName, String directoryPath, boolean recursive,
      org.apache.calcite.adapter.file.FileSchema fileSchema, Map<String, Object> operand,
      ClickHouseConfig config, Process localProcess) {

    SqlDialect dialect = createClickHouseDialect();
    Expression expression = Schemas.subSchemaExpression(parentSchema, schemaName, JdbcSchema.class);
    ClickHouseConvention convention = ClickHouseConvention.of(dialect, expression, schemaName);

    // ClickHouse maps databases to catalogs (no schema concept within databases)
    return new ClickHouseJdbcSchema(dataSource, dialect, convention, schemaName, null,
        directoryPath, recursive, setupConn, fileSchema, localProcess);
  }

  /**
   * Creates a Calcite SqlDialect configured for ClickHouse.
   */
  private static SqlDialect createClickHouseDialect() {
    SqlDialect.Context context = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.CLICKHOUSE)
        .withIdentifierQuoteString("`")
        .withNullCollation(NullCollation.LAST)
        .withUnquotedCasing(Casing.TO_LOWER)
        .withQuotedCasing(Casing.UNCHANGED)
        .withCaseSensitive(false);

    return new SqlDialect(context) {
      @Override public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlKind kind) {
        return true;
      }
    };
  }

  /**
   * Configures S3 credentials for ClickHouse.
   * ClickHouse uses inline credentials in the s3() function, but we can also
   * set up named collections for reuse.
   */
  private static void configureS3Credentials(Connection conn,
      org.apache.calcite.adapter.file.FileSchema fileSchema) {
    if (fileSchema == null || fileSchema.getStorageConfig() == null) {
      LOGGER.debug("No storage config available for S3 credential setup");
      return;
    }

    Map<String, Object> storageConfig = fileSchema.getStorageConfig();
    String accessKey = (String) storageConfig.get("accessKeyId");
    String secretKey = (String) storageConfig.get("secretAccessKey");
    String region = (String) storageConfig.get("region");
    String endpoint = (String) storageConfig.get("endpoint");

    if (accessKey == null || secretKey == null) {
      LOGGER.debug("No S3 credentials found in storage config");
      return;
    }

    try {
      // Try to create a named collection for S3 credentials (ClickHouse 23.8+)
      StringBuilder sql = new StringBuilder();
      sql.append("CREATE NAMED COLLECTION IF NOT EXISTS s3_creds AS ");
      sql.append("access_key_id = '").append(accessKey).append("', ");
      sql.append("secret_access_key = '").append(secretKey).append("'");
      if (region != null) {
        sql.append(", region = '").append(region).append("'");
      }
      if (endpoint != null) {
        sql.append(", endpoint = '").append(endpoint).append("'");
      }

      conn.createStatement().execute(sql.toString());
      LOGGER.info("Created ClickHouse S3 named collection");
    } catch (SQLException e) {
      // Named collections may not be supported in this ClickHouse version
      // S3 credentials will be passed inline in s3() calls
      LOGGER.debug("Could not create named collection (will use inline credentials): {}", e.getMessage());
    }
  }

  /**
   * Registers tables from the FileSchema's conversion registry as ClickHouse views.
   */
  private static void registerFilesAsViews(Connection conn, String directoryPath,
      boolean recursive, String clickhouseSchema,
      org.apache.calcite.adapter.file.FileSchema fileSchema)
      throws SQLException {
    LOGGER.info("=== Starting ClickHouse table registration for schema '{}' ===", clickhouseSchema);

    if (fileSchema == null) {
      LOGGER.error("No FileSchema available - cannot register views");
      throw new SQLException("ClickHouse engine requires FileSchema for table discovery");
    }

    java.util.Map<String, ConversionMetadata.ConversionRecord> records = fileSchema.getAllTableRecords();
    LOGGER.info("Found {} entries in FileSchema's conversion registry", records.size());

    if (records.isEmpty()) {
      LOGGER.warn("No table records found in FileSchema - no views to register");
      return;
    }

    int viewCount = 0;
    for (java.util.Map.Entry<String, ConversionMetadata.ConversionRecord> entry : records.entrySet()) {
      ConversionMetadata.ConversionRecord record = entry.getValue();
      String tableName = record.getTableName();

      if (tableName == null || tableName.isEmpty()) {
        LOGGER.error("Table record missing tableName, key: '{}'", entry.getKey());
        continue;
      }

      try {
        boolean isIcebergTable = "ICEBERG_PARQUET".equals(record.getConversionType());

        if (isIcebergTable) {
          // Use ClickHouse's native iceberg() function
          String icebergPath;
          if (record.sourceFile != null && !record.sourceFile.endsWith(".parquet")) {
            icebergPath = record.sourceFile;
          } else {
            icebergPath = directoryPath + "/" + clickhouseSchema + "/" + tableName;
          }

          String viewSql =
              ClickHouseDialect.INSTANCE.createIcebergViewSql(clickhouseSchema, tableName, icebergPath);
          LOGGER.info("Creating ClickHouse Iceberg view: {}", viewSql);
          conn.createStatement().execute(viewSql);
          viewCount++;
          LOGGER.info("Created Iceberg view: {}.{}", clickhouseSchema, tableName);

        } else {
          // Determine parquet path from metadata
          String parquetPath = resolveParquetPath(record);

          if (parquetPath != null) {
            String viewSql =
                ClickHouseDialect.INSTANCE.createParquetViewSql(clickhouseSchema, tableName, parquetPath, false);
            LOGGER.info("Creating ClickHouse Parquet view for '{}': {}", tableName, viewSql);
            conn.createStatement().execute(viewSql);
            viewCount++;
            LOGGER.info("Created Parquet view: {}.{}", clickhouseSchema, tableName);
          } else {
            LOGGER.warn("No parquet path found for table '{}', skipping view creation", tableName);
          }
        }
      } catch (SQLException e) {
        // Expected errors (file not found, etc.) are logged as warnings
        String msg = e.getMessage();
        if (msg != null && (msg.contains("FILE_NOT_FOUND") || msg.contains("404")
            || msg.contains("NoSuchKey") || msg.contains("does not exist"))) {
          LOGGER.warn("Expected error creating view for '{}': {}", tableName, msg);
        } else {
          LOGGER.error("Failed to create view for table '{}': {}", tableName, msg, e);
        }
      }
    }

    LOGGER.info("=== ClickHouse table registration complete: {} views created ===", viewCount);
  }

  /**
   * Resolves the parquet path from a conversion record.
   */
  private static String resolveParquetPath(ConversionMetadata.ConversionRecord record) {
    if (record.viewScanPattern != null) {
      return record.viewScanPattern;
    }
    if (record.parquetCacheFile != null) {
      return record.parquetCacheFile;
    }
    if (record.sourceFile != null && record.sourceFile.endsWith(".parquet")) {
      return record.sourceFile;
    }
    if (record.convertedFile != null) {
      if (record.convertedFile.endsWith(".parquet")) {
        return record.convertedFile;
      }
      if (record.convertedFile.startsWith("{") && record.convertedFile.endsWith("}")) {
        return record.convertedFile;
      }
    }
    return null;
  }

  /**
   * Registers SQL views defined in the operand configuration.
   */
  private static void registerSqlViewsInClickHouse(Connection conn, String clickhouseSchema,
      Map<String, Object> operand) {
    if (operand == null) {
      return;
    }

    @SuppressWarnings("unchecked")
    java.util.List<Map<String, Object>> tables =
        (java.util.List<Map<String, Object>>) operand.get("tables");
    if (tables == null || tables.isEmpty()) {
      return;
    }

    for (Map<String, Object> tableDef : tables) {
      String type = (String) tableDef.get("type");
      if (!"view".equalsIgnoreCase(type)) {
        continue;
      }

      String viewName = (String) tableDef.get("name");
      String viewDef = (String) tableDef.get("sql");
      if (viewName == null || viewDef == null) {
        continue;
      }

      try {
        String viewSql =
            String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS %s", clickhouseSchema, viewName, viewDef);
        LOGGER.info("Creating SQL view in ClickHouse: {}.{}", clickhouseSchema, viewName);
        conn.createStatement().execute(viewSql);
      } catch (SQLException e) {
        LOGGER.warn("Failed to create SQL view '{}' in ClickHouse: {}",
            viewName, e.getMessage());
      }
    }
  }

  /**
   * Finds a free ephemeral port for clickhouse-local.
   */
  private static int findFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to find free port for clickhouse-local", e);
    }
  }

  /**
   * Waits for an HTTP endpoint to become available.
   */
  private static void waitForReady(String pingUrl, int timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    Exception lastException = null;

    while (System.currentTimeMillis() < deadline) {
      try {
        HttpURLConnection connection = (HttpURLConnection) new URI(pingUrl).toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(1000);
        connection.setReadTimeout(1000);
        int responseCode = connection.getResponseCode();
        if (responseCode == 200) {
          LOGGER.info("clickhouse-local is ready (ping returned 200)");
          return;
        }
      } catch (Exception e) {
        lastException = e;
      }

      try {
        Thread.sleep(LOCAL_POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for clickhouse-local to start", e);
      }
    }

    throw new RuntimeException(
        "clickhouse-local failed to start within " + timeoutMs + "ms. Last error: "
        + (lastException != null ? lastException.getMessage() : "unknown"),
        lastException);
  }
}
