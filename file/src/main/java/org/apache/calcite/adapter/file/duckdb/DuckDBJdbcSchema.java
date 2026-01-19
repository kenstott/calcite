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

import org.apache.calcite.adapter.file.iceberg.IcebergTable;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Set;
import javax.sql.DataSource;

/**
 * JDBC schema implementation for DuckDB that ensures complete query pushdown.
 * All aggregations, filters, joins, and other operations are executed in DuckDB.
 * Maintains a persistent connection to keep the named in-memory database alive.
 */
public class DuckDBJdbcSchema extends JdbcSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBJdbcSchema.class);

  private final String directoryPath;
  private final boolean recursive;
  private final Connection persistentConnection;
  private final org.apache.calcite.adapter.file.FileSchema fileSchema; // Keep reference for refreshes
  private final String schemaName; // Keep local copy since parent field is package-private

  public DuckDBJdbcSchema(DataSource dataSource, SqlDialect dialect,
                         JdbcConvention convention, String catalog, String schema,
                         String directoryPath, boolean recursive, Connection persistentConnection,
                         org.apache.calcite.adapter.file.FileSchema fileSchema) {
    // DuckDB uses in-memory databases where catalog concept is irrelevant
    // Always pass null as catalog to ensure 2-part naming (schema.table)
    super(dataSource, dialect, convention, null, schema);
    this.directoryPath = directoryPath;
    this.recursive = recursive;
    this.persistentConnection = persistentConnection;
    this.fileSchema = fileSchema; // Keep FileSchema alive for refresh handling
    this.schemaName = schema; // Keep local copy

    LOGGER.info("Created DuckDB JDBC schema for directory: {} (recursive={}) with persistent connection",
                directoryPath, recursive);

    // Register pattern-aware refresh listener to recreate views when parquet files are updated
    if (fileSchema != null) {
      fileSchema.addRefreshListener(new org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener() {
        @Override public void onTableRefreshed(String tableName, File parquetFile) {
          recreateView(tableName, parquetFile);
        }

        @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
          recreateViewWithPattern(tableName, pattern);
        }

        @Override public void onIcebergTableRefreshed(String tableName, String tableLocation) {
          recreateIcebergView(tableName, tableLocation);
        }
      });
      LOGGER.info("Registered pattern-aware refresh listener with FileSchema");
    }
  }

  /**
   * Recreates a DuckDB view when the underlying parquet file has been refreshed.
   * This forces DuckDB to re-read the updated file.
   */
  private void recreateView(String tableName, File parquetFile) {
    try {
      // Support both local file paths and S3 URIs
      String parquetPath = parquetFile.getAbsolutePath();

      String viewSql =
                                    String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s')", schemaName, tableName, parquetPath);

      LOGGER.info("Recreating DuckDB view after refresh: \"{}.{}\" -> {}",
                  schemaName, tableName, parquetFile.getName());

      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(viewSql);
      }

      LOGGER.info("Successfully recreated view for refreshed table '{}'", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to recreate view for table '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Recreates a DuckDB view using a file pattern (glob) instead of explicit file list.
   * Used by DuckDB+Hive optimization where the pattern is sufficient for view recreation.
   * This is more efficient and scalable for large partitioned datasets.
   */
  private void recreateViewWithPattern(String tableName, String pattern) {
    try {
      // Get the record from conversion metadata to check if it's Hive-partitioned
      org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record = null;
      if (fileSchema != null && fileSchema.getConversionMetadata() != null) {
        record = fileSchema.getConversionMetadata().getAllConversions().get(tableName);
      }

      // Check if we should use Hive partitioning
      boolean isHivePartitioned = record != null && record.viewScanPattern != null;

      String viewSql;
      if (isHivePartitioned) {
        // Use Hive-partitioning mode for tables with partition structure
        viewSql =
            String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s', hive_partitioning = true)", schemaName, tableName, pattern);
        LOGGER.info("Recreating DuckDB Hive-partitioned view with pattern: \"{}.{}\" -> {}",
            schemaName, tableName, pattern);
      } else {
        // Standard glob pattern without Hive partitioning
        viewSql =
            String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s')", schemaName, tableName, pattern);
        LOGGER.info("Recreating DuckDB view with pattern: \"{}.{}\" -> {}",
            schemaName, tableName, pattern);
      }

      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(viewSql);
      }

      LOGGER.info("Successfully recreated pattern-based view for table '{}'", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to recreate pattern-based view for table '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Recreates a DuckDB view for an Iceberg table when it has been refreshed or recreated.
   * This is called when ETL recreates an Iceberg table (e.g., due to schema changes),
   * ensuring the DuckDB view stays in sync with the underlying table schema.
   */
  private void recreateIcebergView(String tableName, String tableLocation) {
    try {
      // Use CREATE OR REPLACE to handle schema changes atomically
      String viewSql = String.format(
          "CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM iceberg_scan('%s')",
          schemaName, tableName, tableLocation);

      LOGGER.info("Recreating DuckDB Iceberg view after ETL: \"{}.{}\" -> {}",
                  schemaName, tableName, tableLocation);

      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(viewSql);
      }

      LOGGER.info("Successfully recreated Iceberg view for table '{}'", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to recreate Iceberg view for table '{}': {}", tableName, e.getMessage(), e);
    }
  }


  /**
   * Returns the internal FileSchema that manages file discovery and conversion.
   * This is needed by GovDataSchemaFactory to register custom converters.
   *
   * @return the internal FileSchema, or null if not available
   */
  public org.apache.calcite.adapter.file.FileSchema getFileSchema() {
    return fileSchema;
  }

  /**
   * Returns the schema-level comment from the underlying FileSchema.
   * This allows INFORMATION_SCHEMA queries to see schema comments even when
   * using DuckDB execution engine, by delegating to the FileSchema that
   * implements CommentableSchema.
   *
   * @return the schema comment, or null if not available
   */
  @Override public @org.checkerframework.checker.nullness.qual.Nullable String getComment() {
    if (fileSchema != null) {
      String comment = fileSchema.getComment();
      LOGGER.info("DuckDBJdbcSchema.getComment() returning: {}",
                  comment != null && comment.length() > 80 ? comment.substring(0, 77) + "..." : comment);
      return comment;
    }
    LOGGER.info("DuckDBJdbcSchema.getComment() returning null (fileSchema is null)");
    return null;
  }

  @Override public Set<String> getTableNames() {
    Set<String> tableNames = super.getTableNames();
    LOGGER.debug("DuckDB schema tables available: {}", tableNames);
    return tableNames;
  }

  @Override public Table getTable(String name) {
    LOGGER.info("Looking for table: '{}'", name);
    Table table = super.getTable(name);
    if (table != null) {
      LOGGER.info("Found DuckDB table '{}' - all operations will be pushed to DuckDB", name);

      // Get the original table from FileSchema to access comment metadata
      // ALWAYS wrap the table to provide FileSchema metadata, even if the JDBC table
      // already implements CommentableTable (the JDBC implementation doesn't have our metadata)
      if (fileSchema != null) {
        Table originalTable = fileSchema.tables().get(name);
        LOGGER.info("Found original table for '{}': {} (CommentableTable: {})",
                    name, originalTable != null ? originalTable.getClass().getSimpleName() : "null",
                    originalTable instanceof CommentableTable);
        if (originalTable instanceof CommentableTable) {
          LOGGER.info("Wrapping DuckDB table '{}' with CommentableTable delegation for metadata access", name);
          // Wrap the JDBC table to delegate comment methods to the original table
          return new CommentableJdbcTableWrapper(table, (CommentableTable) originalTable);
        }
      } else {
        LOGGER.info("fileSchema is null for table '{}'", name);
      }
    } else {
      LOGGER.warn("Table '{}' not found in DuckDB schema", name);
      // Try lowercase version
      table = super.getTable(name.toLowerCase());
      if (table != null) {
        LOGGER.info("Found table with lowercase name: '{}'", name.toLowerCase());

        // Apply same wrapping logic for lowercase lookup
        if (table instanceof CommentableTable) {
          return table;
        }

        if (fileSchema != null) {
          Table originalTable = fileSchema.tables().get(name.toLowerCase());
          if (originalTable instanceof CommentableTable) {
            return new CommentableJdbcTableWrapper(table, (CommentableTable) originalTable);
          }
        }
      }
    }
    return table;
  }

  /**
   * Override snapshot() to return this instance.
   *
   * <p>The parent JdbcSchema.snapshot() creates a plain JdbcSchema, which loses
   * the DuckDBJdbcSchema type. This breaks rules like DuckDBIcebergCountStarRule
   * that need to access the FileSchema via the DuckDBJdbcSchema.
   *
   * <p>For DuckDB, we return 'this' because:
   * <ul>
   *   <li>Tables are already registered as views in DuckDB's catalog</li>
   *   <li>The persistentConnection and fileSchema must be shared</li>
   *   <li>Schema mutations are handled via view recreation, not tableMap updates</li>
   * </ul>
   */
  @Override public org.apache.calcite.schema.Schema snapshot(
      org.apache.calcite.schema.SchemaVersion version) {
    LOGGER.debug("DuckDBJdbcSchema.snapshot() called - returning this instance to preserve type");
    return this;
  }

  /**
   * Wrapper that delegates CommentableTable methods to the original FileSchema table
   * while maintaining JdbcTable behavior for query execution.
   * This allows INFORMATION_SCHEMA queries to see table/column comments
   * while still pushing all query execution to DuckDB for performance.
   *
   * <p>IMPORTANT: Must implement TranslatableTable to delegate toRel() to the
   * wrapped JdbcTable. Without this, Calcite creates LogicalTableScan instead of
   * JdbcTableScan, and the JDBC convention rules won't be registered.
   */
  private static class CommentableJdbcTableWrapper
      implements Table, CommentableTable, org.apache.calcite.schema.TranslatableTable {
    private final Table jdbcTable;           // For query execution
    private final CommentableTable commentableTable;  // For metadata

    CommentableJdbcTableWrapper(Table jdbcTable, CommentableTable commentableTable) {
      this.jdbcTable = jdbcTable;
      this.commentableTable = commentableTable;
    }

    @Override public org.apache.calcite.rel.RelNode toRel(
        org.apache.calcite.plan.RelOptTable.ToRelContext context,
        org.apache.calcite.plan.RelOptTable relOptTable) {
      // Delegate to the wrapped JdbcTable to create JdbcTableScan with proper convention
      if (jdbcTable instanceof org.apache.calcite.schema.TranslatableTable) {
        return ((org.apache.calcite.schema.TranslatableTable) jdbcTable).toRel(context, relOptTable);
      }
      throw new IllegalStateException("Wrapped table is not TranslatableTable: " + jdbcTable.getClass());
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return jdbcTable.getRowType(typeFactory);
    }

    @Override public Statistic getStatistic() {
      // Use IcebergTable statistics if available (for row count optimization)
      if (commentableTable instanceof IcebergTable) {
        Statistic icebergStats = ((IcebergTable) commentableTable).getStatistic();
        LOGGER.info("CommentableJdbcTableWrapper.getStatistic() using IcebergTable stats: rowCount={}",
                    icebergStats.getRowCount());
        return icebergStats;
      }
      return jdbcTable.getStatistic();
    }

    @Override public org.apache.calcite.schema.Schema.TableType getJdbcTableType() {
      return jdbcTable.getJdbcTableType();
    }

    @Override public boolean isRolledUp(String column) {
      return jdbcTable.isRolledUp(column);
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                          org.apache.calcite.sql.SqlCall call,
                                                          org.apache.calcite.sql.SqlNode parent,
                                                          org.apache.calcite.config.CalciteConnectionConfig config) {
      return jdbcTable.rolledUpColumnValidInsideAgg(column, call, parent, config);
    }

    // CommentableTable methods - delegate to original table for metadata
    @Override public @org.checkerframework.checker.nullness.qual.Nullable String getTableComment() {
      String comment = commentableTable.getTableComment();
      LOGGER.info("CommentableJdbcTableWrapper.getTableComment() called, returning: {}",
                  comment != null && comment.length() > 50 ? comment.substring(0, 47) + "..." : comment);
      return comment;
    }

    @Override public @org.checkerframework.checker.nullness.qual.Nullable String getColumnComment(String columnName) {
      String comment = commentableTable.getColumnComment(columnName);
      LOGGER.debug("CommentableJdbcTableWrapper.getColumnComment({}) returning: {}", columnName, comment);
      return comment;
    }
  }
}
