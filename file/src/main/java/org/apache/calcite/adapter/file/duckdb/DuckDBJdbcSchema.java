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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.iceberg.IcebergTable;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
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

  @SuppressWarnings("UnusedVariable")
  private final String directoryPath;
  @SuppressWarnings("UnusedVariable")
  private final boolean recursive;
  private final Connection persistentConnection;
  private final org.apache.calcite.adapter.file.FileSchema fileSchema; // Keep reference for refreshes
  private final String schemaName; // Keep local copy since parent field is package-private
  private final String catalogPath; // DuckDB file path — used as key for pending view flush

  public DuckDBJdbcSchema(DataSource dataSource, SqlDialect dialect,
                         JdbcConvention convention, String catalog, String schema,
                         String directoryPath, boolean recursive, Connection persistentConnection,
                         org.apache.calcite.adapter.file.FileSchema fileSchema, String catalogPath) {
    // DuckDB uses in-memory databases where catalog concept is irrelevant
    // Always pass null as catalog to ensure 2-part naming (schema.table)
    super(dataSource, dialect, convention, null, schema);
    this.directoryPath = directoryPath;
    this.recursive = recursive;
    this.persistentConnection = persistentConnection;
    this.fileSchema = fileSchema; // Keep FileSchema alive for refresh handling
    this.schemaName = schema; // Keep local copy
    this.catalogPath = catalogPath;

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
      String viewSql =
          String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM iceberg_scan('%s', allow_moved_paths=true)",
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
    // Flush deferred views before querying DuckDB metadata so SQL views appear in the result
    if (catalogPath != null && DuckDBPendingViews.hasPending(catalogPath)) {
      DuckDBPendingViews.flush(catalogPath, persistentConnection);
    }
    Set<String> tableNames = new java.util.LinkedHashSet<>(super.getTableNames());
    // Always include tables defined in FileSchema YAML regardless of DuckDB view state.
    // This ensures JDBC metadata (getTables/getColumns) works even when iceberg views
    // haven't been created yet (e.g., first connection before ETL runs).
    if (fileSchema != null) {
      tableNames.addAll(fileSchema.tables()
          .getNames(org.apache.calcite.schema.lookup.LikePattern.any()));
    }
    // SQL views from YAML views: section are included if DuckDB registered them.
    // Cross-schema views that failed to register won't be in super.getTableNames() — no exclusion needed.
    // Views that ARE in DuckDB appear with TABLE_TYPE=VIEW so getTables(type=TABLE) correctly skips them.
    LOGGER.debug("DuckDB schema tables available: {}", tableNames);
    return tableNames;
  }

  @Override public Table getTable(String name) {
    LOGGER.info("Looking for table: '{}'", name);
    // Flush deferred views on first access — all schemas are registered by now
    if (catalogPath != null && DuckDBPendingViews.hasPending(catalogPath)) {
      DuckDBPendingViews.flush(catalogPath, persistentConnection);
    }
    Table table = super.getTable(name);
    if (table != null) {
      LOGGER.info("Found DuckDB table '{}' - all operations will be pushed to DuckDB", name);

      // Get the original table from FileSchema to access comment metadata
      // ALWAYS wrap the table to provide FileSchema metadata, even if the JDBC table
      // already implements CommentableTable (the JDBC implementation doesn't have our metadata)
      if (fileSchema != null) {
        Table originalTable = fileSchema.tables().get(name);
        if (originalTable == null) {
          originalTable = fileSchema.tables().get(name.toLowerCase());
        }
        LOGGER.info("Found original table for '{}': {} (CommentableTable: {})",
                    name, originalTable != null ? originalTable.getClass().getSimpleName() : "null",
                    originalTable instanceof CommentableTable);
        if (originalTable instanceof CommentableTable) {
          LOGGER.info("Wrapping DuckDB table '{}' with CommentableTable delegation for metadata access", name);
          return new CommentableJdbcTableWrapper(table, (CommentableTable) originalTable);
        }
        if (originalTable != null) {
          // FileSchema has the table but it is not a CommentableTable.
          // Use FileSchema's table type as the authoritative type (e.g. ViewTable returns VIEW,
          // other non-comment tables return TABLE). Correct DuckDB's VIEW if FileSchema says TABLE.
          if (originalTable.getJdbcTableType() != org.apache.calcite.schema.Schema.TableType.VIEW) {
            LOGGER.info("Correcting TABLE_TYPE for '{}': FileSchema says {}, DuckDB says VIEW",
                        name, originalTable.getJdbcTableType());
            return new TableTypeCorrectingWrapper(table);
          }
          // FileSchema says VIEW — DuckDB's VIEW type is correct, return as-is.
          return table;
        }
        // originalTable is null: IcebergTable failed to load. Check conversionMetadata.
        if (fileSchema.getConversionMetadata() != null) {
          ConversionMetadata.ConversionRecord record =
              fileSchema.getConversionMetadata().getAllConversions().get(name);
          if (record == null) {
            record = fileSchema.getConversionMetadata().getAllConversions()
                .get(name.toLowerCase());
          }
          if (record != null && "ICEBERG_PARQUET".equals(record.conversionType)) {
            LOGGER.info("Correcting TABLE_TYPE for '{}': conversionMetadata says ICEBERG_PARQUET", name);
            return new TableTypeCorrectingWrapper(table);
          }
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

        if (table instanceof CommentableTable) {
          return table;
        }

        if (fileSchema != null) {
          Table originalTable = fileSchema.tables().get(name.toLowerCase());
          if (originalTable instanceof CommentableTable) {
            return new CommentableJdbcTableWrapper(table, (CommentableTable) originalTable);
          }
          if (originalTable != null) {
            if (originalTable.getJdbcTableType() != org.apache.calcite.schema.Schema.TableType.VIEW) {
              return new TableTypeCorrectingWrapper(table);
            }
            return table;
          }
        }
      }
    }
    // Table not in DuckDB catalog — fall back to FileSchema definition.
    // This allows JDBC metadata to describe tables even when DuckDB views aren't created yet.
    if (table == null && fileSchema != null) {
      Table fsTable = fileSchema.tables().get(name);
      if (fsTable == null) {
        fsTable = fileSchema.tables().get(name.toLowerCase());
      }
      if (fsTable != null) {
        LOGGER.info("Table '{}' not in DuckDB catalog — returning FileSchema table for metadata", name);
        return fsTable;
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
      // For Iceberg-backed tables, use IcebergTable's Calcite SQL types (not DuckDB native types)
      if (commentableTable instanceof IcebergTable) {
        return ((IcebergTable) commentableTable).getRowType(typeFactory);
      }
      return jdbcTable.getRowType(typeFactory);
    }

    @Override public Statistic getStatistic() {
      // FileSchema table is the metadata authority for statistics (row counts, PKs, FKs).
      // DuckDB views carry no constraint metadata, so jdbcTable.getStatistic() is always empty.
      return commentableTable.getStatistic();
    }

    @Override public org.apache.calcite.schema.Schema.TableType getJdbcTableType() {
      // FileSchema table is the metadata authority — DuckDB is execution-only.
      // DuckDB always registers iceberg_scan/parquet_scan wrappers as VIEWs internally,
      // so jdbcTable.getJdbcTableType() always returns VIEW regardless of actual semantics.
      return commentableTable.getJdbcTableType();
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

  /**
   * Minimal wrapper that corrects TABLE_TYPE from VIEW to TABLE.
   *
   * <p>Used when the DuckDB catalog has a view for an iceberg_scan/parquet_scan table
   * but the corresponding FileSchema table is unavailable (e.g. IcebergTable init failed
   * due to Hadoop/Kerberos issues). The view is an execution-layer detail; all such
   * tables are semantically base tables and must report TABLE_TYPE = TABLE.
   */
  private static class TableTypeCorrectingWrapper
      implements Table, org.apache.calcite.schema.TranslatableTable {
    private final Table delegate;

    TableTypeCorrectingWrapper(Table delegate) {
      this.delegate = delegate;
    }

    @Override public org.apache.calcite.rel.RelNode toRel(
        org.apache.calcite.plan.RelOptTable.ToRelContext context,
        org.apache.calcite.plan.RelOptTable relOptTable) {
      if (delegate instanceof org.apache.calcite.schema.TranslatableTable) {
        return ((org.apache.calcite.schema.TranslatableTable) delegate).toRel(context, relOptTable);
      }
      throw new IllegalStateException("Delegate is not TranslatableTable: " + delegate.getClass());
    }

    @Override public org.apache.calcite.schema.Schema.TableType getJdbcTableType() {
      return org.apache.calcite.schema.Schema.TableType.TABLE;
    }

    @Override public org.apache.calcite.rel.type.RelDataType getRowType(
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      return delegate.getRowType(typeFactory);
    }

    @Override public org.apache.calcite.schema.Statistic getStatistic() {
      return delegate.getStatistic();
    }

    @Override public boolean isRolledUp(String column) {
      return delegate.isRolledUp(column);
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
        org.apache.calcite.sql.SqlCall call,
        org.apache.calcite.sql.SqlNode parent,
        org.apache.calcite.config.CalciteConnectionConfig config) {
      return delegate.rolledUpColumnValidInsideAgg(column, call, parent, config);
    }
  }
}
