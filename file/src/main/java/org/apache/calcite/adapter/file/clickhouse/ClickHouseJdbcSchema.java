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

import org.apache.calcite.adapter.file.iceberg.IcebergTable;
import org.apache.calcite.adapter.file.jdbc.ClickHouseDialect;
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
 * JDBC schema implementation for ClickHouse that ensures query pushdown.
 * All aggregations, filters, joins, and other operations are executed in ClickHouse.
 *
 * <p>Supports both server mode (existing ClickHouse instance) and embedded mode
 * (clickhouse-local subprocess). In embedded mode, the schema manages the lifecycle
 * of the subprocess and destroys it on close.
 */
public class ClickHouseJdbcSchema extends JdbcSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseJdbcSchema.class);

  @SuppressWarnings("UnusedVariable")
  private final String directoryPath;
  @SuppressWarnings("UnusedVariable")
  private final boolean recursive;
  private final Connection persistentConnection;
  private final org.apache.calcite.adapter.file.FileSchema fileSchema;
  private final String schemaName;
  private final Process localProcess; // Non-null only in embedded/local mode

  public ClickHouseJdbcSchema(DataSource dataSource, SqlDialect dialect,
                              JdbcConvention convention, String catalog, String schema,
                              String directoryPath, boolean recursive,
                              Connection persistentConnection,
                              org.apache.calcite.adapter.file.FileSchema fileSchema,
                              Process localProcess) {
    super(dataSource, dialect, convention, catalog, schema);
    this.directoryPath = directoryPath;
    this.recursive = recursive;
    this.persistentConnection = persistentConnection;
    this.fileSchema = fileSchema;
    this.schemaName = schema;
    this.localProcess = localProcess;

    LOGGER.info("Created ClickHouse JDBC schema for directory: {} (recursive={}, mode={})",
                directoryPath, recursive, localProcess != null ? "local" : "server");

    // Register pattern-aware refresh listener to recreate views when files are updated
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
   * Recreates a ClickHouse view when the underlying parquet file has been refreshed.
   */
  private void recreateView(String tableName, File parquetFile) {
    try {
      String path = parquetFile.getAbsolutePath();
      String viewSql =
          ClickHouseDialect.INSTANCE.createParquetViewSql(schemaName, tableName, path, false);

      LOGGER.info("Recreating ClickHouse view after refresh: \"{}.{}\" -> {}",
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
   * Recreates a ClickHouse view using a file pattern (glob).
   */
  private void recreateViewWithPattern(String tableName, String pattern) {
    try {
      String viewSql =
          ClickHouseDialect.INSTANCE.createParquetViewSql(schemaName, tableName, pattern, false);

      LOGGER.info("Recreating ClickHouse view with pattern: \"{}.{}\" -> {}",
                  schemaName, tableName, pattern);

      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(viewSql);
      }

      LOGGER.info("Successfully recreated pattern-based view for table '{}'", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to recreate pattern-based view for table '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Recreates a ClickHouse view for an Iceberg table.
   */
  private void recreateIcebergView(String tableName, String tableLocation) {
    try {
      String viewSql =
          ClickHouseDialect.INSTANCE.createIcebergViewSql(schemaName, tableName, tableLocation);

      LOGGER.info("Recreating ClickHouse Iceberg view: \"{}.{}\" -> {}",
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
   *
   * @return the internal FileSchema, or null if not available
   */
  public org.apache.calcite.adapter.file.FileSchema getFileSchema() {
    return fileSchema;
  }

  @Override public @org.checkerframework.checker.nullness.qual.Nullable String getComment() {
    if (fileSchema != null) {
      String comment = fileSchema.getComment();
      LOGGER.debug("ClickHouseJdbcSchema.getComment() returning: {}",
                  comment != null && comment.length() > 80 ? comment.substring(0, 77) + "..." : comment);
      return comment;
    }
    return null;
  }

  @Override public Set<String> getTableNames() {
    Set<String> tableNames = super.getTableNames();
    LOGGER.debug("ClickHouse schema tables available: {}", tableNames);
    return tableNames;
  }

  @Override public Table getTable(String name) {
    LOGGER.debug("Looking for table: '{}'", name);
    Table table = super.getTable(name);
    if (table != null) {
      LOGGER.debug("Found ClickHouse table '{}' - all operations will be pushed to ClickHouse", name);

      if (fileSchema != null) {
        Table originalTable = fileSchema.tables().get(name);
        if (originalTable instanceof CommentableTable) {
          return new CommentableJdbcTableWrapper(table, (CommentableTable) originalTable);
        }
      }
    } else {
      // Try lowercase version
      table = super.getTable(name.toLowerCase(java.util.Locale.ROOT));
      if (table != null) {
        LOGGER.debug("Found table with lowercase name: '{}'", name.toLowerCase(java.util.Locale.ROOT));

        if (table instanceof CommentableTable) {
          return table;
        }

        if (fileSchema != null) {
          Table originalTable = fileSchema.tables().get(name.toLowerCase(java.util.Locale.ROOT));
          if (originalTable instanceof CommentableTable) {
            return new CommentableJdbcTableWrapper(table, (CommentableTable) originalTable);
          }
        }
      }
    }
    return table;
  }

  /**
   * Returns this instance to preserve schema type for optimizer rules.
   */
  @Override public org.apache.calcite.schema.Schema snapshot(
      org.apache.calcite.schema.SchemaVersion version) {
    LOGGER.debug("ClickHouseJdbcSchema.snapshot() called - returning this instance to preserve type");
    return this;
  }

  /**
   * Closes this schema, shutting down the clickhouse-local process if in embedded mode.
   */
  public void close() {
    if (localProcess != null) {
      LOGGER.info("Shutting down clickhouse-local process");
      localProcess.destroy();
      try {
        boolean exited = localProcess.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
        if (!exited) {
          LOGGER.warn("clickhouse-local did not exit gracefully, force destroying");
          localProcess.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        localProcess.destroyForcibly();
      }
      LOGGER.info("clickhouse-local process shut down");
    }

    if (persistentConnection != null) {
      try {
        persistentConnection.close();
      } catch (Exception e) {
        LOGGER.debug("Error closing persistent connection: {}", e.getMessage());
      }
    }
  }

  /**
   * Wrapper that delegates CommentableTable methods to the original FileSchema table
   * while maintaining JdbcTable behavior for query execution.
   */
  private static class CommentableJdbcTableWrapper
      implements Table, CommentableTable, org.apache.calcite.schema.TranslatableTable {
    private final Table jdbcTable;
    private final CommentableTable commentableTable;

    CommentableJdbcTableWrapper(Table jdbcTable, CommentableTable commentableTable) {
      this.jdbcTable = jdbcTable;
      this.commentableTable = commentableTable;
    }

    @Override public org.apache.calcite.rel.RelNode toRel(
        org.apache.calcite.plan.RelOptTable.ToRelContext context,
        org.apache.calcite.plan.RelOptTable relOptTable) {
      if (jdbcTable instanceof org.apache.calcite.schema.TranslatableTable) {
        return ((org.apache.calcite.schema.TranslatableTable) jdbcTable).toRel(context, relOptTable);
      }
      throw new IllegalStateException("Wrapped table is not TranslatableTable: " + jdbcTable.getClass());
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return jdbcTable.getRowType(typeFactory);
    }

    @Override public Statistic getStatistic() {
      if (commentableTable instanceof IcebergTable) {
        Statistic icebergStats = ((IcebergTable) commentableTable).getStatistic();
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

    @Override public @org.checkerframework.checker.nullness.qual.Nullable String getTableComment() {
      return commentableTable.getTableComment();
    }

    @Override public @org.checkerframework.checker.nullness.qual.Nullable String getColumnComment(String columnName) {
      return commentableTable.getColumnComment(columnName);
    }
  }
}
