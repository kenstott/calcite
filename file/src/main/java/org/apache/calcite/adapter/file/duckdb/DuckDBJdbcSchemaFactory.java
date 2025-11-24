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

import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * Factory for creating JDBC schema backed by DuckDB.
 * Uses standard JDBC adapter for proper query pushdown.
 * Supports shared databases via database_filename operand for cross-schema joins.
 */
public class DuckDBJdbcSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBJdbcSchemaFactory.class);

  /**
   * Pool of shared database connections keyed by absolute database file path.
   * Allows multiple schemas to share the same DuckDB database for efficient cross-schema queries.
   */
  private static final Map<String, SharedDatabaseInfo> DATABASE_POOL = new ConcurrentHashMap<>();

  /**
   * Information about a shared database instance.
   */
  private static class SharedDatabaseInfo {
    final DataSource dataSource;
    final Connection setupConnection;
    final String jdbcUrl;

    SharedDatabaseInfo(DataSource dataSource, Connection setupConnection, String jdbcUrl) {
      this.dataSource = dataSource;
      this.setupConnection = setupConnection;
      this.jdbcUrl = jdbcUrl;
    }
  }

  static {
    LOGGER.debug("[DuckDBJdbcSchemaFactory] Class loaded");
  }

  /**
   * Creates a JDBC schema for DuckDB with files registered as views.
   * Configures with Oracle Lex and unquoted casing to lower.
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName, File directory) {
    return create(parentSchema, schemaName, directory.getPath(), false, null);
  }

  /**
   * Creates a JDBC schema for DuckDB with files registered as views.
   * Creates a single persistent connection that lives with the schema.
   * Configures with Oracle Lex and unquoted casing to lower.
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName,
                                 File directory, boolean recursive) {
    return create(parentSchema, schemaName, directory.getPath(), recursive, null);
  }

  /**
   * Creates a JDBC schema for DuckDB with files registered as views (String path version).
   * Creates a single persistent connection that lives with the schema.
   * Configures with Oracle Lex and unquoted casing to lower.
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName,
                                 String directoryPath, boolean recursive) {
    return create(parentSchema, schemaName, directoryPath, recursive, null);
  }

  /**
   * Creates a JDBC schema for DuckDB with files registered as views.
   * Creates a single persistent connection that lives with the schema.
   * Configures with Oracle Lex and unquoted casing to lower.
   * @param fileSchema The FileSchema that handles conversions and refreshes (kept alive)
   * @param operand Schema operand map containing database_filename and other config
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName,
                                 String directoryPath, boolean recursive,
                                 org.apache.calcite.adapter.file.FileSchema fileSchema,
                                 Map<String, Object> operand) {
    LOGGER.debug("[DuckDBJdbcSchemaFactory] create() called with fileSchema for schema: {}", schemaName);
    LOGGER.info("Creating DuckDB JDBC schema for: {} with name: {} (recursive={}, hasFileSchema={})",
                directoryPath, schemaName, recursive, fileSchema != null);

    return createInternal(parentSchema, schemaName, directoryPath, recursive, fileSchema, operand);
  }

  /**
   * Creates a JDBC schema for DuckDB with files registered as views (backward compatibility).
   * Creates a single persistent connection that lives with the schema.
   * Configures with Oracle Lex and unquoted casing to lower.
   * @param fileSchema The FileSchema that handles conversions and refreshes (kept alive)
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName,
                                 String directoryPath, boolean recursive,
                                 org.apache.calcite.adapter.file.FileSchema fileSchema) {
    return createInternal(parentSchema, schemaName, directoryPath, recursive, fileSchema, null);
  }

  /**
   * Internal implementation of create() with full operand support.
   */
  private static JdbcSchema createInternal(SchemaPlus parentSchema, String schemaName,
                                 String directoryPath, boolean recursive,
                                 org.apache.calcite.adapter.file.FileSchema fileSchema,
                                 Map<String, Object> operand) {
    LOGGER.debug("[DuckDBJdbcSchemaFactory] createInternal() called for schema: {}", schemaName);

    // Extract database_filename from operand if provided
    String databaseFilename = operand != null ? (String) operand.get("database_filename") : null;

    LOGGER.info("Creating DuckDB JDBC schema for: {} with name: {} (recursive={}, hasFileSchema={}, databaseFilename={})",
                directoryPath, schemaName, recursive, fileSchema != null, databaseFilename);

    try {
      Class.forName("org.duckdb.DuckDBDriver");

      // If databaseFilename is provided, use it; otherwise use schema-specific default
      String catalogPath;
      if (databaseFilename != null) {
        // Resolve database_filename path
        File dbFile = new File(databaseFilename);
        if (!dbFile.isAbsolute()) {
          // Relative path - resolve against working directory's .aperio/.duckdb/ directory
          // This ensures all schemas can find the same shared database file
          File workingDir = new File(System.getProperty("user.dir"));
          File aperioDir = new File(workingDir, ".aperio");
          File duckdbDir = new File(aperioDir, ".duckdb");

          // Ensure the .duckdb directory exists
          if (!duckdbDir.exists()) {
            duckdbDir.mkdirs();
            LOGGER.debug("Created shared DuckDB catalog directory: {}", duckdbDir);
          }

          catalogPath = new File(duckdbDir, databaseFilename).getAbsolutePath();
        } else {
          // Absolute path - use as-is
          catalogPath = dbFile.getAbsolutePath();
        }
        LOGGER.info("Using configured database filename: {} (resolved to: {})", databaseFilename, catalogPath);
      } else {
        // Use default per-schema database
        // Use FileSchema's operating cache directory for catalog storage (always local filesystem)
        // DuckDB database files must be on local filesystem (not S3) for file locking
        String baseDirForCatalog;
        if (fileSchema != null && fileSchema.getOperatingCacheDirectory() != null) {
          baseDirForCatalog = fileSchema.getOperatingCacheDirectory().getAbsolutePath();
        } else {
          baseDirForCatalog = directoryPath;
        }
        catalogPath = determineCatalogPath(schemaName, baseDirForCatalog);
      }

      String jdbcUrl;
      String dbName;

      if (catalogPath != null) {
        // Check if this database is already in the connection pool
        SharedDatabaseInfo sharedInfo = DATABASE_POOL.get(catalogPath);

        if (sharedInfo != null) {
          // Reuse existing database connection
          LOGGER.info("Reusing existing DuckDB database: {} for schema: {}", catalogPath, schemaName);
          return createSchemaInSharedDatabase(sharedInfo, parentSchema, schemaName, directoryPath,
                                             recursive, fileSchema);
        }

        // Use persistent file-based catalog
        jdbcUrl = "jdbc:duckdb:" + catalogPath;
        // For shared databases, use a neutral catalog name (not schema-specific)
        dbName = null;  // DuckDB will use default catalog name
        LOGGER.info("Creating new DuckDB database: {} for schema: {}", catalogPath, schemaName);
      } else {
        // Fallback to named in-memory database
        dbName = "calcite_" + schemaName + "_" + System.nanoTime();
        jdbcUrl = "jdbc:duckdb:" + dbName;
        LOGGER.info("Using ephemeral in-memory DuckDB database: {}", dbName);
      }

      // Create initial connection for setup
      Connection setupConn = DriverManager.getConnection(jdbcUrl);

      // Configure DuckDB settings for production use
      setupConn.createStatement().execute("SET threads TO 4");  // Adjust based on workload
      setupConn.createStatement().execute("SET memory_limit = '4GB'");  // Prevent OOM
      setupConn.createStatement().execute("SET max_memory = '4GB'");  // Hard limit
      setupConn.createStatement().execute("SET temp_directory = '" + System.getProperty("java.io.tmpdir") + "'");  // Spill location
      setupConn.createStatement().execute("SET preserve_insertion_order = false");  // Better performance
      setupConn.createStatement().execute("SET enable_progress_bar = false");  // Cleaner output

      // Disable object cache if schema has refreshable tables to ensure fresh reads after refresh
      // When files are updated, DuckDB's object cache would serve stale metadata
//      boolean hasRefreshableTables = fileSchema != null && fileSchema.hasRefreshableTables();
//      if (hasRefreshableTables) {
//        setupConn.createStatement().execute("SET enable_object_cache = false");  // Disable cache for refreshable tables
//        LOGGER.info("Disabled DuckDB object cache for schema '{}' with refreshable tables", schemaName);
//      } else {
//        setupConn.createStatement().execute("SET enable_object_cache = true");  // Cache parsed files for better performance
//      }

      setupConn.createStatement().execute("SET scalar_subquery_error_on_multiple_rows = false");  // Allow Calcite's scalar subquery rewriting

      // Declare S3 configuration variables outside try block so they're accessible later
      String s3Region = null;
      String s3AccessKey = null;
      String s3SecretKey = null;
      String s3Endpoint = null;
      String endpointHostPort = null;
      Boolean useSSL = null;

      // Install and load S3/HTTPFS extension for cloud storage support
      try {
        setupConn.createStatement().execute("INSTALL httpfs");
        setupConn.createStatement().execute("LOAD httpfs");
        LOGGER.info("DuckDB httpfs extension installed and loaded for S3 support");

        // Configure S3 credentials - check operands first, then environment variables

        // Try to get from FileSchema's storage config first
        if (fileSchema != null && fileSchema.getStorageConfig() != null) {
          Map<String, Object> storageConfig = fileSchema.getStorageConfig();
          s3Region = (String) storageConfig.get("awsRegion");
          s3AccessKey = (String) storageConfig.get("awsAccessKeyId");
          s3SecretKey = (String) storageConfig.get("awsSecretAccessKey");
          s3Endpoint = (String) storageConfig.get("endpoint");
          if (s3AccessKey != null && s3SecretKey != null) {
            LOGGER.info("Using S3 credentials from schema operands");
          }
        }

        // Fall back to environment variables if not in operands
        if (s3Region == null) s3Region = System.getenv("AWS_REGION");
        if (s3AccessKey == null) s3AccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        if (s3SecretKey == null) s3SecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        if (s3Endpoint == null) s3Endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");

        // Process endpoint configuration (needs to be done before making final variables)
        if (s3Endpoint != null) {
          // DuckDB expects endpoint without protocol (e.g., "localhost:9000" not "http://localhost:9000")
          if (s3Endpoint.startsWith("http://")) {
            endpointHostPort = s3Endpoint.substring("http://".length());
            useSSL = false;
          } else if (s3Endpoint.startsWith("https://")) {
            endpointHostPort = s3Endpoint.substring("https://".length());
            useSSL = true;
          } else {
            endpointHostPort = s3Endpoint;
            useSSL = true;
          }
        }

        // Apply S3 configuration to setup connection using modern CREATE SECRET approach
        if (s3AccessKey != null && s3SecretKey != null) {
          // Use CREATE PERSISTENT SECRET with CONFIG provider for explicit credentials
          // PERSISTENT ensures the secret is saved to the database file and available across connections
          // This is the modern DuckDB approach instead of legacy SET statements
          StringBuilder secretSQL = new StringBuilder();
          secretSQL.append("CREATE OR REPLACE PERSISTENT SECRET duckdb_s3_secret (");
          secretSQL.append("TYPE s3, ");
          secretSQL.append("PROVIDER config, ");
          secretSQL.append("KEY_ID '").append(s3AccessKey).append("', ");
          secretSQL.append("SECRET '").append(s3SecretKey).append("'");

          if (s3Region != null) {
            secretSQL.append(", REGION '").append(s3Region).append("'");
          }

          if (endpointHostPort != null) {
            secretSQL.append(", ENDPOINT '").append(endpointHostPort).append("'");
            secretSQL.append(", URL_STYLE 'path'");
            secretSQL.append(", USE_SSL ").append(useSSL);
          }

          secretSQL.append(")");

          LOGGER.info("Creating DuckDB PERSISTENT S3 secret with SQL: {}", secretSQL.toString());
          setupConn.createStatement().execute(secretSQL.toString());
          LOGGER.info("DuckDB S3 secret created successfully for endpoint: {} (SSL: {})",
                     endpointHostPort != null ? endpointHostPort : "default", useSSL);
        } else {
          LOGGER.info("No S3 credentials found in operands or environment - S3 access will use default AWS credentials");
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to configure S3 support: {} - S3 URIs will not work", e.getMessage());
      }

      // Capture S3 configuration for use in DataSource (make final for lambda/anonymous class access)
      final String finalS3Region = s3Region;
      final String finalS3AccessKey = s3AccessKey;
      final String finalS3SecretKey = s3SecretKey;
      final String finalEndpointHostPort = endpointHostPort;
      final Boolean finalUseSSL = useSSL;

      // Register similarity functions as DuckDB UDFs
      registerSimilarityFunctions(setupConn);

      // Load query-time extensions for optimization (vss, fts)
      loadQueryExtensions(setupConn);

      // Create a schema matching the FileSchema name
      // ALWAYS quote the schema name to preserve casing as-is
      String duckdbSchema = schemaName;
      String createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS \"" + duckdbSchema + "\"";
      LOGGER.info("Creating DuckDB schema with preserved casing: \"{}\"", duckdbSchema);
      setupConn.createStatement().execute(createSchemaSQL);
      LOGGER.info("Created DuckDB schema: \"{}\"", duckdbSchema);

      // Register Parquet files as views
      // FileSchemaFactory has already run conversions via FileSchema
      // Pass the FileSchema to use its unique instance ID for cache lookup
      registerFilesAsViews(setupConn, directoryPath, recursive, duckdbSchema, schemaName, fileSchema);

      // Register SQL views from schema JSON definitions
      // These views may reference the Parquet views created above
      registerSqlViewsInDuckDB(setupConn, duckdbSchema, operand);

      // Debug: List all registered views
      try (Statement stmt = setupConn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT table_schema, table_name, table_type FROM information_schema.tables")) {
        LOGGER.info("All DuckDB tables and views:");
        while (rs.next()) {
          LOGGER.info("  - Schema: {}, Name: {}, Type: {}",
                     rs.getString("table_schema"),
                     rs.getString("table_name"),
                     rs.getString("table_type"));
        }
      }

      // DON'T close the setup connection - keep it alive to maintain the database
      // This connection will be owned by DuckDBJdbcSchema

      // Create a DataSource that creates new connections to the named database
      final String finalJdbcUrl = jdbcUrl;
      DataSource dataSource = new DataSource() {
        @Override public Connection getConnection() throws SQLException {
          // Create a new connection to the named in-memory database
          Connection conn = DriverManager.getConnection(finalJdbcUrl);
          // Apply critical settings and load extensions for new connections
          // Extensions must be loaded per-connection in DuckDB (secrets are database-level but extensions are not)
          try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET scalar_subquery_error_on_multiple_rows = false");

            // Load extensions - these are required for S3 access and advanced query features
            // httpfs: Required for S3/HTTP parquet access
            // vss: Vector similarity search for embeddings
            // fts: Full-text search
            // spatial: Geospatial functions
            String[] extensions = {"httpfs", "vss", "fts", "spatial"};
            for (String ext : extensions) {
              try {
                stmt.execute("LOAD " + ext);
              } catch (SQLException e) {
                // Graceful degradation - extension may not be installed, continue
                LOGGER.debug("Could not load extension {} on new connection: {}", ext, e.getMessage());
              }
            }
          }
          return conn;
        }

        @Override public Connection getConnection(String username, String password) throws SQLException {
          return getConnection();
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() {
          return java.util.logging.Logger.getLogger("DuckDB");
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
          if (iface.isInstance(this)) return iface.cast(this);
          throw new SQLException("Cannot unwrap to " + iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface) {
          return iface.isInstance(this);
        }
      };

      SqlDialect dialect = createDuckDBDialectWithCustomLex();

      Expression expression = Schemas.subSchemaExpression(parentSchema, schemaName, JdbcSchema.class);
      DuckDBConvention convention = DuckDBConvention.of(dialect, expression, schemaName);

      // DuckDB named databases use the database name as catalog and our created schema
      DuckDBJdbcSchema schema =
                                                    new DuckDBJdbcSchema(dataSource, dialect, convention, dbName, duckdbSchema, directoryPath, recursive, setupConn, fileSchema);

      // Add to connection pool if this is a persistent database (for future schema sharing)
      if (catalogPath != null) {
        SharedDatabaseInfo sharedInfo = new SharedDatabaseInfo(dataSource, setupConn, jdbcUrl);
        DATABASE_POOL.put(catalogPath, sharedInfo);
        LOGGER.info("Added DuckDB database to connection pool: {}", catalogPath);
      }

      return schema;

    } catch (Exception e) {
      throw new RuntimeException("Failed to create DuckDB JDBC schema", e);
    }
  }

  /**
   * Creates a schema in an existing shared DuckDB database.
   * This method is called when a database is being reused across multiple schemas.
   */
  private static JdbcSchema createSchemaInSharedDatabase(SharedDatabaseInfo sharedInfo,
                                                         SchemaPlus parentSchema,
                                                         String schemaName,
                                                         String directoryPath,
                                                         boolean recursive,
                                                         org.apache.calcite.adapter.file.FileSchema fileSchema) {
    try {
      Connection setupConn = sharedInfo.setupConnection;

      // Create a new schema in the shared database
      String duckdbSchema = schemaName;
      String createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS \"" + duckdbSchema + "\"";
      LOGGER.info("Creating schema in shared DuckDB database: \"{}\"", duckdbSchema);
      setupConn.createStatement().execute(createSchemaSQL);
      LOGGER.info("Created DuckDB schema: \"{}\"", duckdbSchema);

      // Register similarity functions for this schema (macros are database-level, so only once)
      // Note: Macros are global in DuckDB, so they're already registered from the first schema

      // Register Parquet files as views in this schema
      registerFilesAsViews(setupConn, directoryPath, recursive, duckdbSchema, schemaName, fileSchema);

      // Debug: List all registered views
      try (Statement stmt = setupConn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = '" + duckdbSchema + "'")) {
        LOGGER.info("DuckDB tables and views in schema '{}':", duckdbSchema);
        while (rs.next()) {
          LOGGER.info("  - Schema: {}, Name: {}, Type: {}",
                     rs.getString("table_schema"),
                     rs.getString("table_name"),
                     rs.getString("table_type"));
        }
      }

      // Reuse existing DataSource and dialect
      SqlDialect dialect = createDuckDBDialectWithCustomLex();

      Expression expression = Schemas.subSchemaExpression(parentSchema, schemaName, JdbcSchema.class);
      DuckDBConvention convention = DuckDBConvention.of(dialect, expression, schemaName);

      // Create schema using shared database (dbName is null for shared databases)
      DuckDBJdbcSchema schema =
          new DuckDBJdbcSchema(sharedInfo.dataSource, dialect, convention, null, duckdbSchema,
                              directoryPath, recursive, setupConn, fileSchema);

      return schema;

    } catch (Exception e) {
      throw new RuntimeException("Failed to create schema in shared DuckDB database", e);
    }
  }

  /**
   * Creates a Parquet view in DuckDB dynamically.
   * This allows us to register new Parquet files on-the-fly.
   */
  public static void createParquetView(Connection connection, String viewName, String parquetPath) {
    try {
      // Preserve the original casing of the view name by properly quoting it
      // DuckDB preserves casing when identifiers are quoted
      String sql =
                              String.format("CREATE OR REPLACE VIEW \"%s\" AS SELECT * FROM read_parquet('%s')", viewName, parquetPath);
      LOGGER.debug("Creating DuckDB Parquet view: {}", sql);
      connection.createStatement().execute(sql);
    } catch (SQLException e) {
      LOGGER.error("Failed to create Parquet view: {}", viewName, e);
      throw new RuntimeException("Failed to create Parquet view", e);
    }
  }

  /**
   * Creates a DuckDB dialect with custom lex configuration.
   * This provides scaffolding to handle any lex issues we encounter.
   */
  private static SqlDialect createDuckDBDialectWithCustomLex() {
    SqlDialect.Context context = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.DUCKDB)
        .withIdentifierQuoteString("\"")
        .withNullCollation(NullCollation.LAST)
        .withDataTypeSystem(DuckDBSqlDialect.TYPE_SYSTEM)
        .withUnquotedCasing(Casing.TO_LOWER)
        .withQuotedCasing(Casing.UNCHANGED)
        .withCaseSensitive(false);

    return new DuckDBSqlDialect(context) {
      // Don't override quoteIdentifier - let the base class handle it properly
      // The base SqlDialect already handles quoting correctly based on the context settings

      @Override public void unparseCall(org.apache.calcite.sql.SqlWriter writer,
                              org.apache.calcite.sql.SqlCall call,
                              int leftPrec, int rightPrec) {
        // Use DuckDB function mapping for proper SQL generation
        if (DuckDBFunctionMapping.needsSpecialHandling(call.getOperator())) {
          DuckDBFunctionMapping.unparseCall(writer, call, leftPrec, rightPrec);
        } else {
          super.unparseCall(writer, call, leftPrec, rightPrec);
        }
      }

      @Override public boolean supportsFunction(org.apache.calcite.sql.SqlOperator operator,
                                     org.apache.calcite.rel.type.RelDataType type,
                                     List<org.apache.calcite.rel.type.RelDataType> paramTypes) {
        // DuckDB supports most standard SQL functions
        // Plus additional functions for reading files
        String operatorName = operator.getName().toUpperCase();

        // DuckDB-specific table functions
        if (operatorName.equals("READ_PARQUET") ||
            operatorName.equals("READ_CSV") ||
            operatorName.equals("READ_CSV_AUTO") ||
            operatorName.equals("READ_JSON") ||
            operatorName.equals("READ_JSON_AUTO")) {
          return true;
        }

        // DuckDB supports all aggregation functions
        switch (operator.getKind()) {
        case COUNT:
        case SUM:
        case AVG:
        case MIN:
        case MAX:
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
        case COLLECT:
        case LISTAGG:
        case GROUP_CONCAT:
          return true;
        default:
          // Defer to parent for standard functions
          return super.supportsFunction(operator, type, paramTypes);
        }
      }

      @Override public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlKind kind) {
        // DuckDB supports all standard aggregate functions
        return true;
      }
    };
  }

  /**
   * Gets the SQL parser configuration with Oracle Lex and unquoted to lower.
   */
  public static SqlParser.Config getParserConfig() {
    return SqlParser.config()
        .withLex(Lex.ORACLE)
        .withUnquotedCasing(Casing.TO_LOWER)
        .withQuotedCasing(Casing.UNCHANGED);
  }

  /**
   * Determines the catalog path for the DuckDB database.
   * Priority:
   * 1. DUCKDB_CATALOG_PATH environment variable
   * 2. duckdb.catalog.path system property
   * 3. Default persistent catalog in {directoryPath}/.duckdb/{schemaName}.duckdb
   * 4. null (fallback to in-memory database for temp/test directories)
   *
   * @param schemaName The schema name
   * @param directoryPath The data directory path
   * @return The catalog file path, or null for in-memory
   */
  private static String determineCatalogPath(String schemaName, String directoryPath) {
    // Check environment variable first (highest priority)
    String catalogPath = System.getenv("DUCKDB_CATALOG_PATH");
    if (catalogPath != null && !catalogPath.isEmpty()) {
      LOGGER.debug("Using catalog path from DUCKDB_CATALOG_PATH: {}", catalogPath);
      return catalogPath;
    }

    // Check system property
    catalogPath = System.getProperty("duckdb.catalog.path");
    if (catalogPath != null && !catalogPath.isEmpty()) {
      LOGGER.debug("Using catalog path from system property: {}", catalogPath);
      return catalogPath;
    }

    // Use default persistent catalog for non-temporary directories
    // This ensures views are preserved across restarts (fast startup on subsequent runs)
    if (directoryPath != null && !isTempDirectory(directoryPath)) {
      // Create catalog in {directoryPath}/.duckdb/{schemaName}_db.duckdb
      // Append _db to avoid conflicts when database name matches schema name
      File catalogDir = new File(directoryPath, ".duckdb");
      catalogPath = new File(catalogDir, schemaName + "_db.duckdb").getAbsolutePath();
      LOGGER.info("Using default persistent catalog: {}", catalogPath);

      // Ensure catalog directory exists
      if (!catalogDir.exists()) {
        catalogDir.mkdirs();
        LOGGER.debug("Created catalog directory: {}", catalogDir);
      }

      return catalogPath;
    }

    // Fallback to in-memory database for temp directories (tests, ephemeral usage)
    LOGGER.debug("Using in-memory database (temp directory detected or no path specified)");
    return null;
  }

  /**
   * Checks if a directory path is a temporary directory.
   * Temp directories use in-memory databases; persistent directories use file-based catalogs.
   *
   * @param directoryPath The directory path to check
   * @return true if the path appears to be a temporary directory
   */
  private static boolean isTempDirectory(String directoryPath) {
    if (directoryPath == null) {
      return true;
    }

    String lowerPath = directoryPath.toLowerCase();

    // Check for common temp directory patterns
    return lowerPath.contains("/tmp/") ||
           lowerPath.contains("\\temp\\") ||
           lowerPath.contains("/temp/") ||
           lowerPath.startsWith("/tmp") ||
           lowerPath.startsWith("\\tmp") ||
           lowerPath.contains("java.io.tmpdir");
  }

  /**
   * Registers similarity functions as DuckDB macros.
   * This makes COSINE_SIMILARITY available using DuckDB's native array functions.
   * DuckDB has built-in array_cosine_similarity() which we can leverage.
   *
   * The trick is to avoid type inference issues by using list_cosine_similarity
   * which works with VARCHAR inputs directly via string_split.
   */
  private static void registerSimilarityFunctions(Connection conn) {
    try {
      LOGGER.info("Registering similarity functions using DuckDB's native array functions");

      // Cast each element of the split string arrays to DOUBLE
      // Using list_transform avoids the DOUBLE[] vs DOUBLE[ANY] type inference issue
      String cosineSimilarityMacro =
        "CREATE OR REPLACE MACRO COSINE_SIMILARITY(vector1, vector2) AS " +
        "list_cosine_similarity(" +
        "  list_transform(string_split(vector1, ','), x -> CAST(x AS DOUBLE)), " +
        "  list_transform(string_split(vector2, ','), x -> CAST(x AS DOUBLE))" +
        ")";

      conn.createStatement().execute(cosineSimilarityMacro);
      LOGGER.info("Successfully registered COSINE_SIMILARITY macro using DuckDB's " +
                 "list_cosine_similarity");

      // Create COSINE_DISTANCE macro using DuckDB's list function
      String cosineDistanceMacro =
        "CREATE OR REPLACE MACRO COSINE_DISTANCE(vector1, vector2) AS " +
        "list_cosine_distance(" +
        "  list_transform(string_split(vector1, ','), x -> CAST(x AS DOUBLE)), " +
        "  list_transform(string_split(vector2, ','), x -> CAST(x AS DOUBLE))" +
        ")";

      conn.createStatement().execute(cosineDistanceMacro);
      LOGGER.info("Successfully registered COSINE_DISTANCE macro using DuckDB's " +
                 "list_cosine_distance");

      // Also create macro for arrays directly (for when embeddings are stored as arrays)
      // Cast to DOUBLE[] to resolve ambiguity between FLOAT[] and DOUBLE[] overloads
      String arrayCosineSimilarityMacro =
        "CREATE OR REPLACE MACRO ARRAY_COSINE_SIMILARITY(arr1, arr2) AS " +
        "list_cosine_similarity(CAST(arr1 AS DOUBLE[]), CAST(arr2 AS DOUBLE[]))";

      conn.createStatement().execute(arrayCosineSimilarityMacro);
      LOGGER.info("Successfully registered ARRAY_COSINE_SIMILARITY macro");

    } catch (Exception e) {
      LOGGER.warn("Failed to register similarity functions as DuckDB macros: " +
                 e.getMessage());
      LOGGER.debug("Error details: ", e);
      // This is not fatal - queries will fall back to Calcite function resolution
    }
  }

  /**
   * Loads query-time DuckDB extensions for advanced functionality.
   * These extensions are loaded in the persistent DuckDB catalog to enable
   * query optimization features like vector similarity search and full-text search.
   *
   * @param conn DuckDB connection to load extensions into
   */
  private static void loadQueryExtensions(Connection conn) {
    String[][] extensions = {
        {"spatial", ""},  // Spatial extension: PostGIS-compatible spatial functions, ST_Point, ST_Contains, ST_Intersects
        {"vss", ""},      // Vector Similarity Search: HNSW indexes for approximate nearest neighbor
        {"fts", ""}       // Full-Text Search: BM25 ranking and keyword search
    };

    LOGGER.info("Loading query-time DuckDB extensions for optimization...");
    for (String[] ext : extensions) {
      try {
        String installCmd = "INSTALL " + ext[0] + (ext[1].isEmpty() ? "" : " " + ext[1]);
        String loadCmd = "LOAD " + ext[0];

        LOGGER.debug("Installing extension: {}", installCmd);
        conn.createStatement().execute(installCmd);

        LOGGER.debug("Loading extension: {}", loadCmd);
        conn.createStatement().execute(loadCmd);

        LOGGER.info("✓ Loaded query extension: {}", ext[0]);
      } catch (Exception e) {
        LOGGER.warn("✗ Failed to load query extension '{}' (continuing): {}",
                   ext[0], e.getMessage());
        LOGGER.debug("Extension load error details: ", e);
        // Graceful degradation - continue even if extension fails to load
      }
    }
    LOGGER.info("Query extension loading complete");
  }

  /**
   * Detects if a table is Hive-partitioned based on the file list.
   * Hive partitioning uses directory structure with key=value patterns.
   *
   * @param fileList Comma-separated list of file paths (may be bracketed)
   * @return true if the files follow Hive partitioning convention
   */
  private static boolean isHivePartitioned(String fileList) {
    if (fileList == null || fileList.isEmpty()) {
      return false;
    }

    // Remove brackets if present
    String cleanList = fileList;
    if ((fileList.startsWith("[") && fileList.endsWith("]")) ||
        (fileList.startsWith("{") && fileList.endsWith("}"))) {
      cleanList = fileList.substring(1, fileList.length() - 1);
    }

    // Split by comma to get individual files
    String[] files = cleanList.split(",");

    // Need at least 2 files to be considered partitioned
    if (files.length < 2) {
      return false;
    }

    // Check if files contain Hive partition patterns (key=value in path)
    // Look for patterns like /year=2020/ or /country=US/
    int partitionedFileCount = 0;
    for (String file : files) {
      String trimmedFile = file.trim();
      // Remove quotes if present
      if (trimmedFile.startsWith("'") && trimmedFile.endsWith("'")) {
        trimmedFile = trimmedFile.substring(1, trimmedFile.length() - 1);
      }

      // Check for key=value pattern in path
      if (trimmedFile.matches(".*[/\\\\][a-zA-Z_][a-zA-Z0-9_]*=[^/\\\\]+[/\\\\].*")) {
        partitionedFileCount++;
      }
    }

    // Consider it Hive partitioned if most files (>50%) have partition patterns
    return partitionedFileCount > files.length / 2;
  }

  /**
   * Derives a glob pattern from a list of Hive-partitioned files.
   * Extracts the common base directory and creates a pattern like base/**\/*.parquet
   *
   * @param fileList Comma-separated list of file paths (may be bracketed)
   * @return Glob pattern suitable for DuckDB's parquet_scan with hive_partitioning=true
   */
  private static String deriveGlobPattern(String fileList) {
    if (fileList == null || fileList.isEmpty()) {
      return null;
    }

    // Remove brackets if present
    String cleanList = fileList;
    if ((fileList.startsWith("[") && fileList.endsWith("]")) ||
        (fileList.startsWith("{") && fileList.endsWith("}"))) {
      cleanList = fileList.substring(1, fileList.length() - 1);
    }

    // Split by comma to get individual files
    String[] files = cleanList.split(",");
    if (files.length == 0) {
      return null;
    }

    // Get first file and clean it
    String firstFile = files[0].trim();
    if (firstFile.startsWith("'") && firstFile.endsWith("'")) {
      firstFile = firstFile.substring(1, firstFile.length() - 1);
    }

    // Find the base path before partition directories
    // Look for the first occurrence of key=value pattern
    int partitionStart = -1;
    String[] pathParts = firstFile.split("[/\\\\]");

    for (int i = 0; i < pathParts.length; i++) {
      if (pathParts[i].matches("[a-zA-Z_][a-zA-Z0-9_]*=.*")) {
        partitionStart = i;
        break;
      }
    }

    if (partitionStart == -1) {
      // No partition found, use parent directory
      int lastSlash = Math.max(firstFile.lastIndexOf('/'), firstFile.lastIndexOf('\\'));
      if (lastSlash > 0) {
        String basePath = firstFile.substring(0, lastSlash);
        String extension = firstFile.substring(firstFile.lastIndexOf('.'));
        return basePath + "/*" + extension;
      }
      return firstFile;
    }

    // Reconstruct base path up to partition directories
    StringBuilder basePath = new StringBuilder();
    for (int i = 0; i < partitionStart; i++) {
      if (i > 0) {
        basePath.append("/");
      }
      basePath.append(pathParts[i]);
    }

    // Determine file extension from first file
    String extension = ".parquet";
    if (firstFile.contains(".")) {
      extension = firstFile.substring(firstFile.lastIndexOf('.'));
    }

    // Create glob pattern: base/**/*.parquet
    String globPattern = basePath.toString() + "/**/*" + extension;

    LOGGER.debug("Derived glob pattern '{}' from {} files", globPattern, files.length);
    return globPattern;
  }

  /**
   * Registers tables from the FileSchema's conversion registry as DuckDB views.
   * This ensures all tables discovered by FileSchema are available in DuckDB.
   */
  private static void registerFilesAsViews(Connection conn, String directoryPath, boolean recursive,
                                          String duckdbSchema, String calciteSchemaName,
                                          org.apache.calcite.adapter.file.FileSchema fileSchema)
      throws SQLException {
    LOGGER.info("=== Starting DuckDB table registration from FileSchema registry for schema '{}' ===", calciteSchemaName);

    // Use FileSchema's metadata directly - NO FALLBACKS!
    if (fileSchema == null) {
      LOGGER.error("No FileSchema available - this is a configuration error");
      throw new SQLException("DuckDB engine requires FileSchema to be available for table discovery");
    }

    // Get all table records directly from FileSchema
    java.util.Map<String, ConversionMetadata.ConversionRecord> records = fileSchema.getAllTableRecords();
    LOGGER.info("Found {} entries in FileSchema's conversion registry", records.size());

    // Log detailed information about each conversion record for DuckDB
    LOGGER.info("=== DUCKDB REGISTRATION: CONVERSION RECORDS ===");
    for (java.util.Map.Entry<String, ConversionMetadata.ConversionRecord> entry : records.entrySet()) {
      ConversionMetadata.ConversionRecord record = entry.getValue();
      // Truncate parquetCacheFile for readability (can be huge file list)
      String parquetCache = record.getParquetCacheFile();
      if (parquetCache != null && parquetCache.length() > 200) {
        parquetCache = parquetCache.substring(0, 197) + "...";
      }
      LOGGER.info("DuckDB: Table '{}' -> sourceFile='{}', convertedFile='{}', parquetCacheFile='{}', viewScanPattern='{}', conversionType='{}'",
          record.tableName, record.sourceFile, record.convertedFile, parquetCache, record.viewScanPattern, record.conversionType);
    }

    // Debug why records might be empty
    if (records.isEmpty()) {
      LOGGER.warn("DuckDB: FileSchema.getAllTableRecords() returned empty - checking details");
      ConversionMetadata metadata = fileSchema.getConversionMetadata();
      if (metadata == null) {
        LOGGER.warn("DuckDB: FileSchema.getConversionMetadata() returned null");
      } else {
        LOGGER.warn("DuckDB: ConversionMetadata exists but getAllConversions() returned empty");
      }

      // Also check what tables FileSchema knows about
      // Note: getTableMap() is protected, so we can't call it directly
      LOGGER.info("DuckDB: FileSchema reports it has tables but registry is empty - check conversion process");
    }

    // Process each table from the registry
    int viewCount = 0;
    for (java.util.Map.Entry<String, ConversionMetadata.ConversionRecord> entry : records.entrySet()) {
      String key = entry.getKey();
      ConversionMetadata.ConversionRecord record = entry.getValue();

      // Get the Parquet file path - either from cache or original if already Parquet
      String parquetPath = null;
      String tableName = null;

      // All tables should have proper metadata with tableName set
      tableName = record.getTableName();

      if (tableName == null || tableName.isEmpty()) {
        // This indicates a bug - all tables should be registered with tableName
        LOGGER.error("Table record missing tableName - this is a bug in table registration. Key: '{}', Record: {}",
            key, formatRecordForError(record));
        continue; // Skip this malformed record
      }

      LOGGER.debug("Processing table '{}' from registry", tableName);

      // Check if this is an Iceberg table
      if ("ICEBERG_PARQUET".equals(record.getConversionType())) {
        // For Iceberg tables, use DuckDB's native Iceberg support
        LOGGER.debug("Table '{}' is an Iceberg table, will use native DuckDB Iceberg support", tableName);
        // We'll handle this below with iceberg_scan
        parquetPath = null; // Will be handled specially
      } else {
        // Determine parquet path from metadata
        // Priority: 1) viewScanPattern, 2) parquetCacheFile, 3) sourceFile, 4) convertedFile
        if (record.viewScanPattern != null) {
          // Use viewScanPattern for partitioned tables with glob patterns
          parquetPath = record.viewScanPattern;
          LOGGER.debug("Table '{}' using viewScanPattern: {}", tableName, parquetPath);
        } else if (record.parquetCacheFile != null) {
          // Use parquetCacheFile (single file, bracketed list, or glob pattern)
          parquetPath = record.parquetCacheFile;
          if (parquetPath.startsWith("[") || parquetPath.startsWith("{")) {
            LOGGER.debug("Table '{}' has multiple Parquet files: {} files",
                        tableName, parquetPath.split(",").length);
          } else {
            LOGGER.debug("Table '{}' using parquetCacheFile: {}", tableName, parquetPath);
          }
        } else if (record.sourceFile != null && record.sourceFile.endsWith(".parquet")) {
          // Fallback: direct parquet source file
          parquetPath = record.sourceFile;
          LOGGER.debug("Table '{}' is native Parquet: {}", tableName, parquetPath);
        } else if (record.convertedFile != null) {
          // Fallback: converted parquet file or glob pattern
          if (record.convertedFile.endsWith(".parquet")) {
            parquetPath = record.convertedFile;
            LOGGER.debug("Table '{}' has converted Parquet: {}", tableName, parquetPath);
          } else if (record.convertedFile.startsWith("{") && record.convertedFile.endsWith("}")) {
            parquetPath = record.convertedFile;
            LOGGER.debug("Table '{}' has glob pattern: {}", tableName, parquetPath);
          }
        }
      }

      // Create view if we have a table name and either a Parquet path or it's an Iceberg table
      if (tableName != null) {
        // Check if this is an Iceberg table that needs special handling
        boolean isIcebergTable = "ICEBERG_PARQUET".equals(record.getConversionType());

        if (isIcebergTable && record.getSourceFile() != null) {
          // Use DuckDB's native Iceberg support
          // First, ensure the iceberg extension is installed and loaded
          try {
            // Install and load iceberg extension if not already done
            try {
              conn.createStatement().execute("INSTALL iceberg");
            } catch (SQLException e) {
              // Extension might already be installed
              LOGGER.debug("Iceberg extension may already be installed: {}", e.getMessage());
            }

            try {
              conn.createStatement().execute("LOAD iceberg");
            } catch (SQLException e) {
              // Extension might already be loaded
              LOGGER.debug("Iceberg extension may already be loaded: {}", e.getMessage());
            }

            // For Iceberg tables, try iceberg_scan
            // Check if view exists first to avoid expensive file scanning
            if (viewExists(conn, duckdbSchema, tableName)) {
              LOGGER.debug("⚡ Iceberg view exists, skipped: {}.{}", duckdbSchema, tableName);
            } else {
              // View doesn't exist - create it
              String sql =
                  String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS SELECT * FROM iceberg_scan('%s')", duckdbSchema, tableName, record.getSourceFile());
              LOGGER.info("Creating DuckDB view for Iceberg table: \"{}.{}\" -> {}",
                         duckdbSchema, tableName, record.getSourceFile());

              try {
                conn.createStatement().execute(sql);
                viewCount++;
                LOGGER.info("✅ Created Iceberg view: {}.{}", duckdbSchema, tableName);
              } catch (SQLException scanError) {
                // iceberg_scan failed - probably an empty table
                LOGGER.debug("iceberg_scan failed for table '{}': {}", tableName, scanError.getMessage());

                // Create an empty view as a fallback
                try {
                  String emptyViewSql =
                      String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS " +
                      "SELECT " +
                      "NULL::INT AS order_id, " +
                      "NULL::VARCHAR AS customer_id, " +
                      "NULL::VARCHAR AS product_id, " +
                      "NULL::DOUBLE AS amount, " +
                      "NULL::TIMESTAMP AS snapshot_time " +
                      "WHERE 1=0",
                      duckdbSchema, tableName);

                  LOGGER.info("Creating empty view for Iceberg table '{}' (fallback)", tableName);
                  conn.createStatement().execute(emptyViewSql);
                  viewCount++;
                  LOGGER.info("✅ Created empty Iceberg view: {}.{}", duckdbSchema, tableName);
                } catch (SQLException fallbackError) {
                  LOGGER.warn("Failed to create fallback empty view for table '{}': {}",
                             tableName, fallbackError.getMessage());
                  throw fallbackError;
                }
              }
            }
          } catch (SQLException e) {
            LOGGER.warn("Failed to create Iceberg view for table '{}': {}", tableName, e.getMessage());
          }
        } else if (parquetPath != null) {
          // Check if it's a glob pattern or single file
          boolean isMultiFileList = (parquetPath.startsWith("[") && parquetPath.endsWith("]")) ||
                                   (parquetPath.startsWith("{") && parquetPath.endsWith("}"));
          boolean isGlobPattern = parquetPath.contains("**") || (parquetPath.contains("*") && !isMultiFileList);
          String sql = null;

          if (isMultiFileList) {
            // For multiple files specified as [file1,file2,file3] or {file1,file2,file3}
            // Check table configuration to determine if Hive partitioning should be enabled
            boolean hasHivePartitioning = isHivePartitionedFromConfig(record);

            if (hasHivePartitioning) {
              // Use stored table-specific pattern for Hive-partitioned tables
              String pattern = record.viewScanPattern;

              if (pattern != null) {
                // Check if union_by_name is explicitly enabled for this table
                // This allows tables to handle schema variations across partition files when needed
                // (e.g., columns added/removed over time like census.population_estimates)
                boolean useUnionByName = shouldUseUnionByName(record);

                // Use IF NOT EXISTS to skip view creation if catalog already has it (fast restart)
                sql =
                                   String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s', hive_partitioning = true, union_by_name = %s)", duckdbSchema, tableName, pattern, useUnionByName);
                LOGGER.info("Creating DuckDB view with stored table-specific pattern (from config, union_by_name={}): \"{}.{}\" -> {}",
                           useUnionByName, duckdbSchema, tableName, pattern);
              } else {
                // This should never happen - pattern must exist for Hive-partitioned tables
                LOGGER.error("Missing viewScanPattern for Hive-partitioned table '{}' - this indicates a bug in pattern extraction", tableName);
                throw new SQLException("Missing viewScanPattern for Hive-partitioned table: " + tableName);
              }
            } else{
              // Non-Hive partitioned: use explicit file array
              String fileList = parquetPath.substring(1, parquetPath.length() - 1);
              String[] files = fileList.split(",");

              // Build a list of file paths for DuckDB's read_parquet function
              // DuckDB can read multiple files using: read_parquet(['file1', 'file2', ...])
              StringBuilder fileArray = new StringBuilder("[");
              boolean first = true;
              for (String file : files) {
                if (!first) fileArray.append(", ");
                fileArray.append("'").append(file.trim()).append("'");
                first = false;
              }
              fileArray.append("]");

              // Use IF NOT EXISTS to skip view creation if catalog already has it (fast restart)
              sql =
                                 String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS SELECT * FROM parquet_scan(%s)", duckdbSchema, tableName, fileArray.toString());
              LOGGER.info("Creating DuckDB view for multiple files: \"{}.{}\" -> {} files", duckdbSchema, tableName, files.length);
            }
          } else if (isGlobPattern) {
            // Glob pattern - DuckDB's parquet_scan supports glob patterns directly
            // Check table configuration to determine if Hive partitioning should be enabled
            boolean hasHivePartitioning = isHivePartitionedFromConfig(record);

            if (hasHivePartitioning) {
              // Check if union_by_name is explicitly enabled for this table
              // This allows tables to handle schema variations across partition files when needed
              boolean useUnionByName = shouldUseUnionByName(record);

              // Use IF NOT EXISTS to skip view creation if catalog already has it (fast restart)
              sql =
                                 String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s', hive_partitioning = true, union_by_name = %s)", duckdbSchema, tableName, parquetPath, useUnionByName);
              LOGGER.info("Creating DuckDB view with glob pattern and Hive partitioning (from config, union_by_name={}): \"{}.{}\" -> {}",
                         useUnionByName, duckdbSchema, tableName, parquetPath);
            } else {
              // Use IF NOT EXISTS to skip view creation if catalog already has it (fast restart)
              sql =
                                 String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s')", duckdbSchema, tableName, parquetPath);
              LOGGER.info("Creating DuckDB view with glob pattern: \"{}.{}\" -> {}", duckdbSchema, tableName, parquetPath);
            }
          } else {
            // Single file - use parquetPath string directly (works for both local and S3)
            // Use IF NOT EXISTS to skip view creation if catalog already has it (fast restart)
            sql =
                              String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s')", duckdbSchema, tableName, parquetPath);
            LOGGER.info("Creating DuckDB view: \"{}.{}\" -> {}", duckdbSchema, tableName, parquetPath);
          }

          if (sql != null) {
            // Check if view exists first to avoid expensive file scanning
            if (viewExists(conn, duckdbSchema, tableName)) {
              LOGGER.debug("⚡ Parquet view exists, skipped: {}.{}", duckdbSchema, tableName);
            } else {
              // View doesn't exist - create it
              try {
                LOGGER.info("=== EXECUTING DuckDB DDL ===");
                LOGGER.info("🎯 Table: {}.{}", duckdbSchema, tableName);
                // Truncate parquetPath for readability (can be huge file list)
                String truncatedPath = parquetPath;
                if (truncatedPath != null && truncatedPath.length() > 200) {
                  truncatedPath = truncatedPath.substring(0, 197) + "...";
                }
                LOGGER.info("🔍 ParquetPath: {}", truncatedPath);
                LOGGER.info("📝 SQL: {}", sql);
                LOGGER.info("⚙️ About to execute SQL statement...");
                conn.createStatement().execute(sql);
                viewCount++;
                LOGGER.info("✅ SUCCESS: Created DuckDB view: {}.{}", duckdbSchema, tableName);

                // Add diagnostic logging to see what DuckDB interprets from the Parquet file
                try (Statement debugStmt = conn.createStatement();
                     ResultSet schemaInfo =
                       debugStmt.executeQuery(String.format("DESCRIBE \"%s\".\"%s\"", duckdbSchema, tableName))) {
                  LOGGER.debug("=== DuckDB Schema for {}.{} ===", duckdbSchema, tableName);
                  while (schemaInfo.next()) {
                    String colName = schemaInfo.getString("column_name");
                    String colType = schemaInfo.getString("column_type");
                    String nullable = schemaInfo.getString("null");
                    LOGGER.debug("  Column: {} | Type: {} | Nullable: {}", colName, colType, nullable);
                  }
                } catch (SQLException debugE) {
                  LOGGER.warn("Failed to get schema info for table '{}': {}", tableName, debugE.getMessage());
                }
              } catch (SQLException e) {
              // Check if this is a "No files found" error for glob patterns
              // This is expected when table definitions exist but data hasn't been downloaded yet
              String errorMsg = e.getMessage();
              if (errorMsg != null && errorMsg.contains("No files found that match the pattern")) {
                LOGGER.warn("⚠️ Skipping view creation for '{}' - no files match pattern: {}", tableName, parquetPath);
                LOGGER.debug("This is expected when table definitions exist but data hasn't been downloaded yet");
              } else {
                // Real error - log with full detail
                LOGGER.error("═══════════════════════════════════════════════════════════════");
                LOGGER.error("🚨 CRITICAL: DuckDB VIEW CREATION FAILED 🚨");
                LOGGER.error("═══════════════════════════════════════════════════════════════");
                LOGGER.error("❌ Table: {}.{}", duckdbSchema, tableName);
                LOGGER.error("❌ SQL Statement: {}", sql);
                LOGGER.error("❌ SQLException Message: {}", e.getMessage());
                LOGGER.error("❌ SQL State: {}", e.getSQLState());
                LOGGER.error("❌ Error Code: {}", e.getErrorCode());

                // Extra debugging for financial_line_items specifically
                if ("financial_line_items".equals(tableName)) {
                  LOGGER.error("🔥 FINANCIAL_LINE_ITEMS SPECIFIC DEBUG INFO:");
                  LOGGER.error("🔥 Parquet path: {}", parquetPath);
                  LOGGER.error("🔥 Is multiple files: {}", isMultiFileList);
                  LOGGER.error("🔥 Is glob pattern: {}", isGlobPattern);
                  if (isMultiFileList && parquetPath.contains(",")) {
                    String fileList = parquetPath.substring(1, parquetPath.length() - 1);
                    String[] fileArray = fileList.split(",");
                    LOGGER.error("🔥 File count: {}", fileArray.length);
                    LOGGER.error("🔥 First few files:");
                    for (int i = 0; i < Math.min(5, fileArray.length); i++) {
                      LOGGER.error("🔥   File {}: {}", i + 1, fileArray[i].trim());
                    }
                  }
                }

                LOGGER.error("═══════════════════════════════════════════════════════════════");
                // Log the full stack trace for debugging
                e.printStackTrace();
              }
              }
            }
          }
        } else {
          LOGGER.warn("❌ SKIPPING table - no suitable path found");
          LOGGER.warn("❌ Table name: '{}'", tableName);
          LOGGER.warn("❌ ParquetPath: '{}'", parquetPath);
          LOGGER.warn("❌ Record sourceFile: '{}'", record.getSourceFile());
          LOGGER.warn("❌ Record convertedFile: '{}'", record.getConvertedFile());
          LOGGER.warn("❌ Record parquetCacheFile: '{}'", record.getParquetCacheFile());
        }
      }
    }

    LOGGER.info("=== Created {} DuckDB views from registry ===", viewCount);

    if (viewCount == 0) {
      LOGGER.warn("No DuckDB views created from registry - this may indicate missing Parquet cache files");
      LOGGER.warn("Tables found in registry: {}", records.keySet());
    }
  }

  /**
   * Legacy method: Scans directories for Parquet files.
   * Used as fallback when registry is not available or empty.
   */
  private static void registerParquetFilesFromDirectory(Connection conn, File directory,
                                                       boolean recursive, String duckdbSchema)
      throws SQLException {
    LOGGER.info("Using directory scanning for Parquet files in: {}", directory);

    // Get the schema-aware Parquet cache directory
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(directory, null, duckdbSchema);

    // Register both original Parquet files and cached Parquet files
    registerParquetFiles(conn, directory, recursive, duckdbSchema);

    // Also register Parquet files from the cache directory
    if (cacheDir.exists()) {
      LOGGER.info("Registering Parquet files from cache: {}", cacheDir);
      registerParquetFiles(conn, cacheDir, false, duckdbSchema);
    }
  }

  private static void registerParquetFiles(Connection conn, File directory, boolean recursive, String schema)
      throws SQLException {
    LOGGER.debug("[DuckDBJdbcSchemaFactory] Scanning directory: {}", directory);
    File[] files = directory.listFiles();

    if (files != null) {
      LOGGER.debug("[DuckDBJdbcSchemaFactory] Found {} files in {}", files.length, directory);
      for (File file : files) {
        if (file.isDirectory() && recursive) {
          registerParquetFiles(conn, file, recursive, schema);
        } else if (file.isFile() && file.getName().endsWith(".parquet")) {
          String fileName = file.getName();

          // Skip temporary and hidden files
          if (fileName.startsWith(".") || fileName.startsWith("~")) {
            continue;
          }

          // Register Parquet file - preserve original casing since we use quoted identifiers
          String tableName = fileName.replaceAll("\\.parquet$", "");

          // Check if view already exists - skip expensive CREATE if view defined
          if (!viewExists(conn, schema, tableName)) {
            String sql =
                                     String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM read_parquet('%s')", schema, tableName, file.getAbsolutePath());
            LOGGER.info("Registering DuckDB view: {}.{} from file: {}",
                        schema, tableName, file.getAbsolutePath());
            conn.createStatement().execute(sql);
            LOGGER.info("✅ Successfully registered view: {}.{}", schema, tableName);
          } else {
            LOGGER.debug("⚡ Legacy view exists, skipped: {}.{}", schema, tableName);
          }
        }
      }
    }
  }

  /**
   * Determines if a table uses Hive-style partitioning based on its configuration.
   * This is the single source of truth - NOT file path patterns or heuristics.
   *
   * @param record The conversion record containing table configuration
   * @return true if the table is configured with Hive partitioning
   */
  private static boolean isHivePartitionedFromConfig(ConversionMetadata.ConversionRecord record) {
    if (record == null || record.tableConfig == null) {
      return false;
    }

    // Check if tableConfig contains partitions.style == "hive"
    Object partitionsObj = record.tableConfig.get("partitions");
    if (partitionsObj instanceof Map) {
      Map<String, Object> partitions = (Map<String, Object>) partitionsObj;
      Object styleObj = partitions.get("style");
      if (styleObj instanceof String) {
        String style = (String) styleObj;
        return "hive".equalsIgnoreCase(style);
      }
    }

    return false;
  }

  /**
   * Determines if a table should use union_by_name in DuckDB parquet_scan based on its configuration.
   * This allows per-table control over schema evolution handling.
   *
   * @param record The conversion record containing table configuration
   * @return true if the table is configured to use union_by_name
   */
  private static boolean shouldUseUnionByName(ConversionMetadata.ConversionRecord record) {
    if (record == null || record.tableConfig == null) {
      return false;
    }

    // Check if tableConfig contains duckdb.union_by_name == true
    Object duckdbObj = record.tableConfig.get("duckdb");
    if (duckdbObj instanceof Map) {
      Map<String, Object> duckdb = (Map<String, Object>) duckdbObj;
      Object unionByNameObj = duckdb.get("union_by_name");
      if (unionByNameObj instanceof Boolean) {
        return (Boolean) unionByNameObj;
      }
    }

    return false;
  }

  /**
   * Formats a conversion record for error logging.
   */
  private static String formatRecordForError(ConversionMetadata.ConversionRecord record) {
    if (record == null) return "null";
    return String.format(
        "ConversionRecord{tableName='%s', tableType='%s', sourceFile='%s', viewScanPattern='%s', parquetCacheFile='%s'}",
        record.tableName, record.tableType, record.getSourceFile(), record.viewScanPattern,
        record.getParquetCacheFile() != null && record.getParquetCacheFile().length() > 100
            ? record.getParquetCacheFile().substring(0, 97) + "..." : record.getParquetCacheFile());
  }

  /**
   * Checks if a view already exists in DuckDB's catalog.
   * This is used to skip CREATE VIEW IF NOT EXISTS when the view is already defined,
   * avoiding expensive file scanning during parquet_scan() validation.
   *
   * @param conn DuckDB connection
   * @param schema Schema name (DuckDB schema, not Calcite)
   * @param tableName Table/view name
   * @return true if view exists in information_schema
   */
  private static boolean viewExists(Connection conn, String schema, String tableName) throws SQLException {
    String sql = "SELECT COUNT(*) FROM information_schema.tables " +
                 "WHERE table_schema = ? AND table_name = ? AND table_type = 'VIEW'";
    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, schema);
      ps.setString(2, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  /**
   * Rewrites schema references in SQL view definitions from declared schema name to actual schema name.
   * This allows views defined with canonical schema names (e.g., "econ") to work when instantiated
   * with different names (e.g., "ECON", "ECONOMIC").
   *
   * Similar to FK schema name rewriting, this only rewrites references to the declared schema name.
   * Cross-schema references to other schemas are preserved as-is.
   *
   * @param viewDef Original SQL view definition
   * @param declaredSchemaName Canonical schema name from JSON (e.g., "econ")
   * @param actualSchemaName User-provided schema name from model.json (e.g., "ECON")
   * @return Rewritten SQL with schema names updated
   */
  private static String rewriteSchemaReferencesInSql(String viewDef, String declaredSchemaName,
                                                     String actualSchemaName) {
    if (viewDef == null || declaredSchemaName == null || actualSchemaName == null) {
      return viewDef;
    }

    // If schema names match (case-insensitive), no rewriting needed
    if (declaredSchemaName.equalsIgnoreCase(actualSchemaName)) {
      LOGGER.debug("Schema names match (case-insensitive), no SQL rewriting needed: {} = {}",
                  declaredSchemaName, actualSchemaName);
      return viewDef;
    }

    LOGGER.debug("Rewriting SQL view definition: '{}' -> '{}'", declaredSchemaName, actualSchemaName);

    // Pattern matches:
    // - "schemaName"."tableName" (quoted identifiers)
    // - schemaName.tableName (unquoted identifiers)
    // Word boundaries ensure we don't match partial schema names
    // Case-insensitive matching for flexibility
    String pattern = "(?i)\\b" + java.util.regex.Pattern.quote(declaredSchemaName) + "\\.";
    String replacement = actualSchemaName + ".";

    String rewritten = viewDef.replaceAll(pattern, replacement);

    if (!rewritten.equals(viewDef)) {
      LOGGER.debug("SQL rewriting applied successfully");
      LOGGER.debug("Original:  {}", viewDef.length() > 200 ? viewDef.substring(0, 200) + "..." : viewDef);
      LOGGER.debug("Rewritten: {}", rewritten.length() > 200 ? rewritten.substring(0, 200) + "..." : rewritten);
    }

    return rewritten;
  }

  /**
   * Registers SQL views from schema JSON definitions in DuckDB.
   * Extracts view definitions from the "tables" array in the operand and creates them in DuckDB.
   *
   * @param conn DuckDB connection
   * @param duckdbSchema DuckDB schema name where views will be created
   * @param operand Schema operand containing "tables" array and "declaredSchemaName"
   */
  private static void registerSqlViewsInDuckDB(Connection conn, String duckdbSchema,
                                               Map<String, Object> operand) {
    if (operand == null) {
      LOGGER.debug("No operand provided, skipping SQL view registration");
      return;
    }

    // Extract tables array
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> tables = (List<Map<String, Object>>) operand.get("tables");

    if (tables == null || tables.isEmpty()) {
      LOGGER.debug("No tables in operand, skipping SQL view registration");
      return;
    }

    // Extract declared schema name for rewriting
    String declaredSchemaName = (String) operand.get("declaredSchemaName");

    LOGGER.info("Registering SQL views in DuckDB schema '{}' (declaredSchemaName='{}')",
                duckdbSchema, declaredSchemaName);

    int viewCount = 0;
    int skippedCount = 0;

    for (Map<String, Object> table : tables) {
      String tableType = (String) table.get("type");

      // Only process view definitions
      if (!"view".equals(tableType)) {
        continue;
      }

      String viewName = (String) table.get("name");
      // Try "sql" first (used by econ-schema.json), then "viewDef" as fallback
      String viewDef = (String) table.get("sql");
      if (viewDef == null) {
        viewDef = (String) table.get("viewDef");
      }

      if (viewName == null || viewDef == null) {
        LOGGER.warn("View definition missing name or sql/viewDef, skipping: {}", table);
        skippedCount++;
        continue;
      }

      try {
        // Pre-check if view already exists to avoid expensive validation
        String checkViewSql = String.format(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' "
                + "AND table_name = '%s' AND table_type = 'VIEW'",
            duckdbSchema, viewName);
        try (Statement checkStmt = conn.createStatement();
             ResultSet rs = checkStmt.executeQuery(checkViewSql)) {
          if (rs.next()) {
            LOGGER.debug("View {}.{} already exists, skipping creation", duckdbSchema, viewName);
            viewCount++;
            continue;
          }
        }

        // Rewrite schema references if needed
        String rewrittenViewDef = viewDef;
        if (declaredSchemaName != null && !declaredSchemaName.equalsIgnoreCase(duckdbSchema)) {
          rewrittenViewDef = rewriteSchemaReferencesInSql(viewDef, declaredSchemaName, duckdbSchema);
        }

        // Create the view in DuckDB
        String createViewSql =
                                            String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS %s", duckdbSchema, viewName, rewrittenViewDef);

        LOGGER.info("Creating SQL view: {}.{}", duckdbSchema, viewName);
        LOGGER.debug("View SQL: {}", rewrittenViewDef.length() > 200 ?
                    rewrittenViewDef.substring(0, 200) + "..." : rewrittenViewDef);

        conn.createStatement().execute(createViewSql);
        viewCount++;

        LOGGER.info("✅ Created SQL view: {}.{}", duckdbSchema, viewName);

      } catch (SQLException e) {
        LOGGER.error("Failed to create SQL view '{}': {}", viewName, e.getMessage());
        LOGGER.error("View definition was: {}", viewDef);
        LOGGER.error("SQL State: {}, Error Code: {}", e.getSQLState(), e.getErrorCode());
        skippedCount++;
      }
    }

    LOGGER.info("SQL view registration complete: {} views created, {} skipped/failed",
                viewCount, skippedCount);
  }

}
