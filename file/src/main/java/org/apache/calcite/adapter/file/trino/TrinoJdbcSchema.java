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
package org.apache.calcite.adapter.file.trino;

import org.apache.calcite.adapter.file.iceberg.IcebergTable;
import org.apache.calcite.adapter.file.jdbc.TrinoDialect;
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
import java.util.Locale;
import java.util.Set;
import javax.sql.DataSource;

/**
 * JDBC schema implementation for Trino.
 * All aggregations, filters, joins, and other operations are pushed down to Trino.
 *
 * <p>Trino is always server-mode (no embedded/local mode). The schema manages
 * the persistent connection for session settings and table recreation.
 *
 * <p>Unlike Spark (which creates views), Trino creates external tables directly
 * via {@code CREATE TABLE ... WITH (external_location)}. Table recreation uses
 * {@code DROP TABLE IF EXISTS} followed by a new {@code CREATE TABLE}.
 */
public class TrinoJdbcSchema extends JdbcSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(TrinoJdbcSchema.class);

  @SuppressWarnings("UnusedVariable")
  private final String directoryPath;
  @SuppressWarnings("UnusedVariable")
  private final boolean recursive;
  private final Connection persistentConnection;
  private final org.apache.calcite.adapter.file.FileSchema fileSchema;
  private final String schemaName;

  public TrinoJdbcSchema(DataSource dataSource, SqlDialect dialect,
                          JdbcConvention convention, String catalog, String schema,
                          String directoryPath, boolean recursive,
                          Connection persistentConnection,
                          org.apache.calcite.adapter.file.FileSchema fileSchema) {
    super(dataSource, dialect, convention, catalog, schema);
    this.directoryPath = directoryPath;
    this.recursive = recursive;
    this.persistentConnection = persistentConnection;
    this.fileSchema = fileSchema;
    this.schemaName = schema;

    LOGGER.info("Created Trino JDBC schema for directory: {} (recursive={})",
                directoryPath, recursive);

    // Register pattern-aware refresh listener to recreate tables when files are updated
    if (fileSchema != null) {
      fileSchema.addRefreshListener(new org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener() {
        @Override public void onTableRefreshed(String tableName, File parquetFile) {
          recreateTable(tableName, parquetFile);
        }

        @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
          recreateTableWithPattern(tableName, pattern);
        }

        @Override public void onIcebergTableRefreshed(String tableName, String tableLocation) {
          recreateIcebergTable(tableName, tableLocation);
        }
      });
      LOGGER.info("Registered pattern-aware refresh listener with FileSchema");
    }
  }

  /**
   * Recreates a Trino external table when the underlying parquet file has been refreshed.
   */
  private void recreateTable(String tableName, File parquetFile) {
    try {
      String path = parquetFile.getAbsolutePath();

      // Drop existing table first
      String dropSql = TrinoDialect.INSTANCE.dropViewSql(schemaName, tableName);
      LOGGER.info("Dropping Trino table for recreation: {}.{}", schemaName, tableName);
      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(dropSql);
      }

      // Create new external table
      String createSql =
          TrinoDialect.INSTANCE.createParquetViewSql(schemaName, tableName, path, false);
      LOGGER.info("Recreating Trino external table: \"{}.{}\" -> {}",
                  schemaName, tableName, parquetFile.getName());
      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(createSql);
      }

      LOGGER.info("Successfully recreated table for refreshed table '{}'", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to recreate table for '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Recreates a Trino external table using a file pattern.
   */
  private void recreateTableWithPattern(String tableName, String pattern) {
    try {
      // Drop existing table first
      String dropSql = TrinoDialect.INSTANCE.dropViewSql(schemaName, tableName);
      LOGGER.info("Dropping Trino table for recreation: {}.{}", schemaName, tableName);
      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(dropSql);
      }

      // Create new external table with pattern as location
      String createSql =
          TrinoDialect.INSTANCE.createParquetViewSql(schemaName, tableName, pattern, false);
      LOGGER.info("Recreating Trino table with pattern: \"{}.{}\" -> {}",
                  schemaName, tableName, pattern);
      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(createSql);
      }

      LOGGER.info("Successfully recreated pattern-based table for '{}'", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to recreate pattern-based table for '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Recreates a Trino Iceberg table by re-registering in the catalog.
   */
  private void recreateIcebergTable(String tableName, String tableLocation) {
    try {
      // Re-register the Iceberg table via CALL procedure
      String registerSql =
          TrinoDialect.INSTANCE.createIcebergViewSql(schemaName, tableName, tableLocation);

      LOGGER.info("Re-registering Iceberg table in Trino: {}.{}", schemaName, tableName);
      try (Statement stmt = persistentConnection.createStatement()) {
        stmt.execute(registerSql);
      }

      LOGGER.info("Successfully re-registered Iceberg table '{}'", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to re-register Iceberg table '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Returns the internal FileSchema that manages file discovery and conversion.
   */
  public org.apache.calcite.adapter.file.FileSchema getFileSchema() {
    return fileSchema;
  }

  @Override public @org.checkerframework.checker.nullness.qual.Nullable String getComment() {
    if (fileSchema != null) {
      String comment = fileSchema.getComment();
      LOGGER.debug("TrinoJdbcSchema.getComment() returning: {}",
                  comment != null && comment.length() > 80 ? comment.substring(0, 77) + "..." : comment);
      return comment;
    }
    return null;
  }

  @Override public Set<String> getTableNames() {
    Set<String> tableNames = super.getTableNames();
    LOGGER.debug("Trino schema tables available: {}", tableNames);
    return tableNames;
  }

  @Override public Table getTable(String name) {
    LOGGER.debug("Looking for table: '{}'", name);
    Table table = super.getTable(name);
    if (table != null) {
      LOGGER.debug("Found Trino table '{}' - all operations will be pushed to Trino", name);

      if (fileSchema != null) {
        Table originalTable = fileSchema.tables().get(name);
        if (originalTable instanceof CommentableTable) {
          return new CommentableJdbcTableWrapper(table, (CommentableTable) originalTable);
        }
      }
    } else {
      // Try lowercase version
      table = super.getTable(name.toLowerCase(Locale.ROOT));
      if (table != null) {
        LOGGER.debug("Found table with lowercase name: '{}'", name.toLowerCase(Locale.ROOT));

        if (table instanceof CommentableTable) {
          return table;
        }

        if (fileSchema != null) {
          Table originalTable = fileSchema.tables().get(name.toLowerCase(Locale.ROOT));
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
    LOGGER.debug("TrinoJdbcSchema.snapshot() called - returning this instance to preserve type");
    return this;
  }

  /**
   * Closes this schema, releasing the persistent connection.
   */
  public void close() {
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
