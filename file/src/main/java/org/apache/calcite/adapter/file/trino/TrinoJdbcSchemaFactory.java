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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * Factory for creating JDBC schema backed by Trino.
 *
 * <p>Trino is always server-mode (no embedded option). Requires a running
 * Trino server with Hive and/or Iceberg connectors configured.
 *
 * <p>Unlike Spark (which creates views over backtick Parquet paths), Trino
 * creates external tables directly via {@code CREATE TABLE ... WITH (external_location)}.
 * Iceberg tables are registered via {@code CALL iceberg.system.register_table()}.
 *
 * <p>The execution engine integration is primarily for acid testing and validation.
 * For production use, the recommended pattern is to use {@link TrinoConfig#generateCatalogFiles}
 * to generate Trino catalog configuration, then query via native Trino.
 */
public class TrinoJdbcSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TrinoJdbcSchemaFactory.class);

  /**
   * Pool of shared Trino connections keyed by JDBC URL.
   */
  private static final Map<String, SharedInstanceInfo> INSTANCE_POOL = new ConcurrentHashMap<>();

  /**
   * Information about a shared Trino connection.
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
   * Creates a Trino JDBC schema with full configuration.
   *
   * @param parentSchema parent Calcite schema
   * @param schemaName name of the schema to create
   * @param directoryPath path to the data directory
   * @param recursive whether to search recursively for files
   * @param fileSchema the FileSchema with conversion metadata
   * @param operand schema operand map containing trinoConfig and other settings
   * @return a JdbcSchema backed by Trino
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName,
                                  String directoryPath, boolean recursive,
                                  org.apache.calcite.adapter.file.FileSchema fileSchema,
                                  Map<String, Object> operand) {
    LOGGER.info("Creating Trino JDBC schema for: {} with name: {} (recursive={}, hasFileSchema={})",
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

    // Parse Trino configuration
    @SuppressWarnings("unchecked")
    Map<String, Object> trinoConfigMap = operand != null
        ? (Map<String, Object>) operand.get("trinoConfig") : null;
    TrinoConfig config = trinoConfigMap != null
        ? new TrinoConfig(trinoConfigMap) : new TrinoConfig();

    LOGGER.info("Trino config: {}", config);

    try {
      // Create a dedicated driver instance instead of using DriverManager.
      // DriverManager is a JVM-global singleton; when multiple JDBC drivers
      // (Hive, Trino, ClickHouse) coexist, DriverManager.getConnection() may
      // dispatch to the wrong driver. Direct driver.connect() is deterministic.
      final java.sql.Driver trinoDriver =
          Class.forName("io.trino.jdbc.TrinoDriver")
              .asSubclass(java.sql.Driver.class)
              .getDeclaredConstructor().newInstance();

      // Build JDBC URL via TrinoDialect
      Map<String, String> urlConfig = new HashMap<>();
      urlConfig.put("host", config.getHost());
      urlConfig.put("port", config.getPort());
      urlConfig.put("catalog", config.getCatalog());
      urlConfig.put("schema", config.getSchema());
      String jdbcUrl = TrinoDialect.INSTANCE.buildJdbcUrl(urlConfig);
      String poolKey = jdbcUrl;

      // Check if we already have a connection for this server
      SharedInstanceInfo sharedInfo = INSTANCE_POOL.get(poolKey);
      if (sharedInfo != null) {
        LOGGER.info("Reusing existing Trino connection for: {}", jdbcUrl);
        return createSchemaWithConnection(sharedInfo.dataSource, sharedInfo.setupConnection,
            parentSchema, schemaName, config.getCatalog(),
            directoryPath, recursive, fileSchema);
      }

      // Create initial connection for setup
      Connection setupConn;
      if (config.getUser() != null) {
        Properties connProps = new Properties();
        connProps.setProperty("user", config.getUser());
        if (config.getPassword() != null) {
          connProps.setProperty("password", config.getPassword());
        }
        setupConn = trinoDriver.connect(jdbcUrl, connProps);
      } else {
        // Trino requires a user; default to current system user
        Properties connProps = new Properties();
        connProps.setProperty("user", System.getProperty("user.name", "trino"));
        setupConn = trinoDriver.connect(jdbcUrl, connProps);
      }

      // Apply session settings
      String[] settings = config.toSessionSettings();
      for (String setting : settings) {
        try {
          setupConn.createStatement().execute(setting);
          LOGGER.debug("Applied Trino session setting: {}", setting);
        } catch (SQLException e) {
          LOGGER.warn("Failed to apply setting '{}': {}", setting, e.getMessage());
        }
      }

      // Create schema in Trino
      String createSchemaSql = "CREATE SCHEMA IF NOT EXISTS " + config.getCatalog() + "." + schemaName;
      LOGGER.info("Creating Trino schema: {}", createSchemaSql);
      try {
        setupConn.createStatement().execute(createSchemaSql);
        LOGGER.info("Created Trino schema: {}.{}", config.getCatalog(), schemaName);
      } catch (SQLException e) {
        LOGGER.warn("Failed to create schema '{}.{}': {}. "
            + "Ensure the Trino catalog '{}' is configured. "
            + "Generate catalog files with TrinoConfig.generateCatalogFiles().",
            config.getCatalog(), schemaName, e.getMessage(), config.getCatalog());
      }

      // Register files as external tables (Trino lowercases unquoted schema names,
      // and qualifyName() quotes identifiers, so we must pass the lowercased name)
      String trinoSchemaLower = schemaName.toLowerCase(java.util.Locale.ROOT);
      registerFilesAsTables(setupConn, directoryPath, recursive, trinoSchemaLower, fileSchema, config);

      // Register SQL views from operand
      registerSqlViewsInTrino(setupConn, trinoSchemaLower, config, operand);

      // Create DataSource URL targeting the actual schema (Trino lowercases unquoted names)
      final String trinoSchemaName = schemaName.toLowerCase(java.util.Locale.ROOT);
      final String finalJdbcUrl =
          String.format("jdbc:trino://%s:%s/%s/%s", config.getHost(), config.getPort(), config.getCatalog(), trinoSchemaName);
      final String user = config.getUser();
      final String password = config.getPassword();
      DataSource dataSource = new DataSource() {
        @Override public Connection getConnection() throws SQLException {
          Properties props = new Properties();
          if (user != null) {
            props.setProperty("user", user);
            if (password != null) {
              props.setProperty("password", password);
            }
          } else {
            props.setProperty("user", System.getProperty("user.name", "trino"));
          }
          return trinoDriver.connect(finalJdbcUrl, props);
        }

        @Override public Connection getConnection(String username, String pwd) throws SQLException {
          Properties props = new Properties();
          props.setProperty("user", username);
          if (pwd != null) {
            props.setProperty("password", pwd);
          }
          return trinoDriver.connect(finalJdbcUrl, props);
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() {
          return java.util.logging.Logger.getLogger("Trino");
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
          config.getCatalog(), directoryPath, recursive, fileSchema);

    } catch (Exception e) {
      throw new RuntimeException("Failed to create Trino JDBC schema", e);
    }
  }

  /**
   * Creates a TrinoJdbcSchema with the given connection setup.
   */
  private static JdbcSchema createSchemaWithConnection(DataSource dataSource, Connection setupConn,
      SchemaPlus parentSchema, String schemaName, String trinoCatalog,
      String directoryPath, boolean recursive,
      org.apache.calcite.adapter.file.FileSchema fileSchema) {

    String trinoSchema = schemaName.toLowerCase(java.util.Locale.ROOT);
    SqlDialect dialect = createTrinoDialect();
    Expression expression = Schemas.subSchemaExpression(parentSchema, trinoSchema, JdbcSchema.class);
    TrinoConvention convention = TrinoConvention.of(dialect, expression, trinoSchema);

    return new TrinoJdbcSchema(dataSource, dialect, convention, trinoCatalog, trinoSchema,
        directoryPath, recursive, setupConn, fileSchema);
  }

  /**
   * Creates a Calcite SqlDialect configured for Trino.
   */
  private static SqlDialect createTrinoDialect() {
    SqlDialect.Context context = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.UNKNOWN)
        .withDatabaseProductName("Trino")
        .withIdentifierQuoteString("\"")
        .withNullCollation(NullCollation.LAST)
        .withUnquotedCasing(Casing.TO_LOWER)
        .withQuotedCasing(Casing.UNCHANGED)
        .withCaseSensitive(true);

    return new SqlDialect(context) {
      @Override public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlKind kind) {
        return true;
      }
    };
  }

  /**
   * Registers tables from the FileSchema's conversion registry as Trino external tables.
   */
  @SuppressWarnings("UnusedVariable")
  private static void registerFilesAsTables(Connection conn, String directoryPath,
      boolean recursive, String trinoSchema,
      org.apache.calcite.adapter.file.FileSchema fileSchema,
      TrinoConfig config)
      throws SQLException {
    LOGGER.info("=== Starting Trino table registration for schema '{}' ===", trinoSchema);

    if (fileSchema == null) {
      LOGGER.error("No FileSchema available - cannot register tables");
      throw new SQLException("Trino engine requires FileSchema for table discovery");
    }

    Map<String, ConversionMetadata.ConversionRecord> records =
        fileSchema.getAllTableRecords();
    LOGGER.info("Found {} entries in FileSchema's conversion registry", records.size());

    if (records.isEmpty()) {
      LOGGER.warn("No table records found in FileSchema - no tables to register");
      return;
    }

    int tableCount = 0;
    for (Map.Entry<String, ConversionMetadata.ConversionRecord> entry : records.entrySet()) {
      ConversionMetadata.ConversionRecord record = entry.getValue();
      String tableName = record.getTableName();

      if (tableName == null || tableName.isEmpty()) {
        LOGGER.error("Table record missing tableName, key: '{}'", entry.getKey());
        continue;
      }

      try {
        boolean isIcebergTable = "ICEBERG_PARQUET".equals(record.getConversionType());

        if (isIcebergTable) {
          // Register Iceberg table via CALL procedure
          String icebergPath;
          if (record.sourceFile != null && !record.sourceFile.endsWith(".parquet")) {
            icebergPath = record.sourceFile;
          } else {
            icebergPath = directoryPath + "/" + trinoSchema + "/" + tableName;
          }

          String registerSql =
              TrinoDialect.INSTANCE.createIcebergViewSql(trinoSchema, tableName, icebergPath);
          LOGGER.info("Registering Iceberg table in Trino: {}", registerSql);
          conn.createStatement().execute(registerSql);
          tableCount++;
          LOGGER.info("Registered Iceberg table: {}.{}", trinoSchema, tableName);

        } else {
          // Create external table for Parquet files
          String parquetPath = resolveParquetPath(record);

          if (parquetPath != null) {
            String createTableSql =
                TrinoDialect.INSTANCE.createParquetViewSql(trinoSchema, tableName, parquetPath, false);
            LOGGER.info("Creating Trino external table for '{}': {}", tableName, createTableSql);
            conn.createStatement().execute(createTableSql);
            tableCount++;
            LOGGER.info("Created external table: {}.{}", trinoSchema, tableName);
          } else {
            LOGGER.warn("No parquet path found for table '{}', skipping table creation", tableName);
          }
        }
      } catch (SQLException e) {
        String msg = e.getMessage();
        if (msg != null && (msg.contains("ALREADY_EXISTS") || msg.contains("already exists")
            || msg.contains("TABLE_ALREADY_EXISTS"))) {
          LOGGER.debug("Table '{}' already exists in Trino, skipping", tableName);
        } else if (msg != null && (msg.contains("FILE_NOT_FOUND") || msg.contains("404")
            || msg.contains("NoSuchKey") || msg.contains("does not exist")
            || msg.contains("Path does not exist"))) {
          LOGGER.warn("Expected error creating table for '{}': {}", tableName, msg);
        } else {
          LOGGER.error("Failed to create table for '{}': {}", tableName, msg, e);
        }
      }
    }

    LOGGER.info("=== Trino table registration complete: {} tables created ===", tableCount);
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
  @SuppressWarnings("UnusedVariable")
  private static void registerSqlViewsInTrino(Connection conn, String trinoSchema,
      TrinoConfig config, Map<String, Object> operand) {
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
        String qualifiedName = TrinoDialect.INSTANCE.qualifyName(trinoSchema, viewName);
        String viewSql =
            String.format("CREATE OR REPLACE VIEW %s AS %s", qualifiedName, viewDef);
        LOGGER.info("Creating SQL view in Trino: {}.{}", trinoSchema, viewName);
        conn.createStatement().execute(viewSql);
      } catch (SQLException e) {
        LOGGER.warn("Failed to create SQL view '{}' in Trino: {}",
            viewName, e.getMessage());
      }
    }
  }
}
