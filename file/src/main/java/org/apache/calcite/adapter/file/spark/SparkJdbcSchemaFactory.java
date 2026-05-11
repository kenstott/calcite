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

import java.io.PrintWriter;
import java.sql.Connection;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * Factory for creating JDBC schema backed by Spark SQL via Thrift Server.
 *
 * <p>Spark is always server-mode (no embedded option). Requires a running
 * Spark Thrift Server (HiveServer2 protocol) for connections.
 *
 * <p>Handles Iceberg catalog setup by configuring a Hadoop-type Iceberg catalog
 * via session SET commands and registering each ICEBERG_PARQUET table using
 * {@code CREATE TABLE IF NOT EXISTS ... USING iceberg LOCATION '...'}.
 */
public class SparkJdbcSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkJdbcSchemaFactory.class);

  /**
   * Pool of shared Spark connections keyed by JDBC URL.
   */
  private static final Map<String, SharedInstanceInfo> INSTANCE_POOL = new ConcurrentHashMap<>();

  /**
   * Information about a shared Spark connection.
   */
  private static class SharedInstanceInfo {
    final DataSource dataSource;
    final Connection setupConnection;
    final String jdbcUrl;

    SharedInstanceInfo(DataSource dataSource, Connection setupConnection, String jdbcUrl) {
      this.dataSource = dataSource;
      this.setupConnection = setupConnection;
      this.jdbcUrl = jdbcUrl;
    }
  }

  /**
   * Creates a Spark JDBC schema with full configuration.
   *
   * @param parentSchema parent Calcite schema
   * @param schemaName name of the schema to create
   * @param directoryPath path to the data directory
   * @param recursive whether to search recursively for files
   * @param fileSchema the FileSchema with conversion metadata
   * @param operand schema operand map containing sparkConfig and other settings
   * @return a JdbcSchema backed by Spark SQL
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName,
                                  String directoryPath, boolean recursive,
                                  org.apache.calcite.adapter.file.FileSchema fileSchema,
                                  Map<String, Object> operand) {
    LOGGER.info("Creating Spark JDBC schema for: {} with name: {} (recursive={}, hasFileSchema={})",
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

    // Parse Spark configuration
    @SuppressWarnings("unchecked")
    Map<String, Object> sparkConfigMap = operand != null
        ? (Map<String, Object>) operand.get("sparkConfig") : null;
    SparkConfig config = sparkConfigMap != null
        ? new SparkConfig(sparkConfigMap) : new SparkConfig();

    LOGGER.info("Spark config: {}", config);

    try {
      // Create a dedicated driver instance instead of using DriverManager.
      // DriverManager is a JVM-global singleton; when multiple JDBC drivers
      // (Hive, Trino, ClickHouse) coexist, DriverManager.getConnection() may
      // dispatch to the wrong driver. Direct driver.connect() is deterministic.
      final java.sql.Driver sparkDriver =
          (java.sql.Driver) Class.forName("org.apache.hive.jdbc.HiveDriver")
              .getDeclaredConstructor().newInstance();

      // Build JDBC URL via SparkSqlDialect
      Map<String, String> urlConfig = new HashMap<>();
      urlConfig.put("host", config.getHost());
      urlConfig.put("port", config.getPort());
      urlConfig.put("database", config.getDatabase());
      String jdbcUrl = SparkSqlDialect.INSTANCE.buildJdbcUrl(urlConfig);
      String poolKey = jdbcUrl;

      // Check if we already have a connection for this server
      SharedInstanceInfo sharedInfo = INSTANCE_POOL.get(poolKey);
      if (sharedInfo != null) {
        LOGGER.info("Reusing existing Spark connection for: {}", jdbcUrl);
        return createSchemaWithConnection(sharedInfo.dataSource, sharedInfo.setupConnection,
            parentSchema, schemaName, directoryPath, recursive, fileSchema);
      }

      // Create initial connection for setup using direct driver instance
      Connection setupConn;
      if (config.getUser() != null) {
        java.util.Properties connProps = new java.util.Properties();
        connProps.setProperty("user", config.getUser());
        if (config.getPassword() != null) {
          connProps.setProperty("password", config.getPassword());
        }
        setupConn = sparkDriver.connect(jdbcUrl, connProps);
      } else {
        setupConn = sparkDriver.connect(jdbcUrl, new java.util.Properties());
      }

      // Apply Spark session settings
      String[] settings = config.toSparkSettings();
      for (String setting : settings) {
        try {
          setupConn.createStatement().execute(setting);
          LOGGER.debug("Applied Spark setting: {}", setting);
        } catch (SQLException e) {
          LOGGER.warn("Failed to apply setting '{}': {}", setting, e.getMessage());
        }
      }

      // Configure Iceberg catalog
      configureIcebergCatalog(setupConn, config, directoryPath);

      // Configure S3 credentials if available
      configureS3Credentials(setupConn, fileSchema);

      // Create database
      String createDbSql = "CREATE DATABASE IF NOT EXISTS " + schemaName;
      LOGGER.info("Creating Spark database: {}", schemaName);
      try {
        setupConn.createStatement().execute(createDbSql);
        LOGGER.info("Created Spark database: {}", schemaName);
      } catch (SQLException e) {
        LOGGER.warn("Failed to create database '{}': {}", schemaName, e.getMessage());
      }

      // Register files as views (Spark lowercases unquoted names,
      // and qualifyName() quotes identifiers, so we must pass the lowercased name)
      String sparkSchemaLower = schemaName.toLowerCase(java.util.Locale.ROOT);
      registerFilesAsViews(setupConn, directoryPath, recursive, sparkSchemaLower, fileSchema);

      // Register SQL views from operand
      registerSqlViewsInSpark(setupConn, sparkSchemaLower, operand);

      // Create DataSource URL targeting the actual database (Spark lowercases names)
      final String sparkDbName = schemaName.toLowerCase(java.util.Locale.ROOT);
      final String finalJdbcUrl = String.format("jdbc:hive2://%s:%s/%s",
          config.getHost(), config.getPort(), sparkDbName);
      final String user = config.getUser();
      final String password = config.getPassword();
      DataSource dataSource = new DataSource() {
        @Override public Connection getConnection() throws SQLException {
          java.util.Properties props = new java.util.Properties();
          if (user != null) {
            props.setProperty("user", user);
            if (password != null) {
              props.setProperty("password", password);
            }
          }
          return sparkDriver.connect(finalJdbcUrl, props);
        }

        @Override public Connection getConnection(String username, String pwd) throws SQLException {
          java.util.Properties props = new java.util.Properties();
          if (username != null) {
            props.setProperty("user", username);
          }
          if (pwd != null) {
            props.setProperty("password", pwd);
          }
          return sparkDriver.connect(finalJdbcUrl, props);
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() {
          return java.util.logging.Logger.getLogger("Spark");
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
      sharedInfo = new SharedInstanceInfo(dataSource, setupConn, jdbcUrl);
      INSTANCE_POOL.put(poolKey, sharedInfo);

      return createSchemaWithConnection(dataSource, setupConn, parentSchema, schemaName,
          directoryPath, recursive, fileSchema);

    } catch (Exception e) {
      throw new RuntimeException("Failed to create Spark JDBC schema", e);
    }
  }

  /**
   * Creates a SparkJdbcSchema with the given connection setup.
   */
  private static JdbcSchema createSchemaWithConnection(DataSource dataSource, Connection setupConn,
      SchemaPlus parentSchema, String schemaName, String directoryPath, boolean recursive,
      org.apache.calcite.adapter.file.FileSchema fileSchema) {

    String sparkDb = schemaName.toLowerCase(java.util.Locale.ROOT);
    SqlDialect dialect = createSparkDialect();
    Expression expression = Schemas.subSchemaExpression(parentSchema, sparkDb, JdbcSchema.class);
    SparkConvention convention = SparkConvention.of(dialect, expression, sparkDb);

    return new SparkJdbcSchema(dataSource, dialect, convention, sparkDb, null,
        directoryPath, recursive, setupConn, fileSchema);
  }

  /**
   * Creates a Calcite SqlDialect configured for Spark SQL.
   */
  private static SqlDialect createSparkDialect() {
    SqlDialect.Context context = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
        .withIdentifierQuoteString("`")
        .withNullCollation(NullCollation.LOW)
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
   * Configures the Iceberg catalog in the Spark session.
   */
  private static void configureIcebergCatalog(Connection conn, SparkConfig config,
      String directoryPath) {
    String[] icebergSettings = config.toIcebergCatalogSettings(directoryPath);
    for (String setting : icebergSettings) {
      try {
        conn.createStatement().execute(setting);
        LOGGER.debug("Applied Iceberg catalog setting: {}", setting);
      } catch (SQLException e) {
        LOGGER.warn("Failed to apply Iceberg catalog setting '{}': {}", setting, e.getMessage());
      }
    }
    LOGGER.info("Configured Iceberg catalog '{}' with type '{}'",
        SparkConfig.ICEBERG_CATALOG_NAME, config.getIcebergCatalogType());
  }

  /**
   * Configures S3 credentials for Spark via Hadoop S3A properties.
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
    String endpoint = (String) storageConfig.get("endpoint");

    if (accessKey == null || secretKey == null) {
      LOGGER.debug("No S3 credentials found in storage config");
      return;
    }

    try {
      Statement stmt = conn.createStatement();
      stmt.execute("SET spark.hadoop.fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem");
      stmt.execute("SET spark.hadoop.fs.s3a.access.key = " + accessKey);
      stmt.execute("SET spark.hadoop.fs.s3a.secret.key = " + secretKey);
      if (endpoint != null) {
        stmt.execute("SET spark.hadoop.fs.s3a.endpoint = " + endpoint);
        stmt.execute("SET spark.hadoop.fs.s3a.path.style.access = true");
      }
      LOGGER.info("Configured S3 credentials for Spark session");
    } catch (SQLException e) {
      LOGGER.warn("Failed to configure S3 credentials: {}", e.getMessage());
    }
  }

  /**
   * Registers tables from the FileSchema's conversion registry as Spark views.
   */
  private static void registerFilesAsViews(Connection conn, String directoryPath,
      boolean recursive, String sparkSchema,
      org.apache.calcite.adapter.file.FileSchema fileSchema)
      throws SQLException {
    LOGGER.info("=== Starting Spark table registration for schema '{}' ===", sparkSchema);

    if (fileSchema == null) {
      LOGGER.error("No FileSchema available - cannot register views");
      throw new SQLException("Spark engine requires FileSchema for table discovery");
    }

    java.util.Map<String, ConversionMetadata.ConversionRecord> records =
        fileSchema.getAllTableRecords();
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
          // Register Iceberg table in the catalog, then create a view
          String icebergPath;
          if (record.sourceFile != null && !record.sourceFile.endsWith(".parquet")) {
            icebergPath = record.sourceFile;
          } else {
            icebergPath = directoryPath + "/" + sparkSchema + "/" + tableName;
          }

          // Step 1: Register in Iceberg catalog
          String registerSql = SparkSqlDialect.INSTANCE.createIcebergTableSql(
              sparkSchema, tableName, icebergPath);
          LOGGER.info("Registering Iceberg table in Spark catalog: {}", registerSql);
          conn.createStatement().execute(registerSql);

          // Step 2: Create view pointing to the catalog table
          String viewSql = SparkSqlDialect.INSTANCE.createIcebergViewSql(
              sparkSchema, tableName, icebergPath);
          LOGGER.info("Creating Spark Iceberg view: {}", viewSql);
          conn.createStatement().execute(viewSql);
          viewCount++;
          LOGGER.info("Created Iceberg view: {}.{}", sparkSchema, tableName);

        } else {
          // Determine parquet path from metadata
          String parquetPath = resolveParquetPath(record);

          if (parquetPath != null) {
            String viewSql = SparkSqlDialect.INSTANCE.createParquetViewSql(
                sparkSchema, tableName, parquetPath, false);
            LOGGER.info("Creating Spark Parquet view for '{}': {}", tableName, viewSql);
            conn.createStatement().execute(viewSql);
            viewCount++;
            LOGGER.info("Created Parquet view: {}.{}", sparkSchema, tableName);
          } else {
            LOGGER.warn("No parquet path found for table '{}', skipping view creation", tableName);
          }
        }
      } catch (SQLException e) {
        String msg = e.getMessage();
        if (msg != null && (msg.contains("FILE_NOT_FOUND") || msg.contains("404")
            || msg.contains("NoSuchKey") || msg.contains("does not exist")
            || msg.contains("Path does not exist"))) {
          LOGGER.warn("Expected error creating view for '{}': {}", tableName, msg);
        } else {
          LOGGER.error("Failed to create view for table '{}': {}", tableName, msg, e);
        }
      }
    }

    LOGGER.info("=== Spark table registration complete: {} views created ===", viewCount);
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
  private static void registerSqlViewsInSpark(Connection conn, String sparkSchema,
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
        String viewSql = String.format("CREATE OR REPLACE VIEW %s AS %s",
            SparkSqlDialect.INSTANCE.qualifyName(sparkSchema, viewName), viewDef);
        LOGGER.info("Creating SQL view in Spark: {}.{}", sparkSchema, viewName);
        conn.createStatement().execute(viewSql);
      } catch (SQLException e) {
        LOGGER.warn("Failed to create SQL view '{}' in Spark: {}",
            viewName, e.getMessage());
      }
    }
  }
}
