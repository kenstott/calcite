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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.metadata.TableConstraints;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema wrapper that adds constraint metadata to JdbcTable instances.
 * Acts as a delegating wrapper that intercepts getTable() calls.
 *
 * <p>This wrapper implements CommentableSchema to delegate comment requests
 * to the underlying schema if it supports comments. It also implements Wrapper
 * to ensure proper unwrapping behavior for INFORMATION_SCHEMA views.
 */
public class ConstraintAwareJdbcSchema implements CommentableSchema, Wrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConstraintAwareJdbcSchema.class);

  private final JdbcSchema delegate;
  private final Map<String, Map<String, Object>> constraintMetadata;
  private final FileSchema owner;

  /** Convenience for callers without an owning FileSchema (e.g. unit tests): no owner
   * means no deferred FK validation is triggered from getTable(). */
  public ConstraintAwareJdbcSchema(JdbcSchema delegate,
      Map<String, Map<String, Object>> constraintMetadata) {
    this(delegate, constraintMetadata, null);
  }

  public ConstraintAwareJdbcSchema(JdbcSchema delegate,
      Map<String, Map<String, Object>> constraintMetadata, FileSchema owner) {
    this.delegate = delegate;
    this.constraintMetadata = constraintMetadata != null ? constraintMetadata : new LinkedHashMap<>();
    this.owner = owner;
    LOGGER.info("Created ConstraintAwareJdbcSchema with constraints for tables: {}",
                this.constraintMetadata.keySet());
  }

  /**
   * Gets the delegate JdbcSchema wrapped by this constraint-aware schema.
   * @return the underlying JdbcSchema
   */
  public JdbcSchema getDelegate() {
    return delegate;
  }

  @SuppressWarnings("deprecation")
  @Override public @Nullable Table getTable(String name) {
    // Deferred FK validation: run once here — the lazy consumption point reached only
    // after every sibling schema is registered — so cross-schema FK targets resolve
    // instead of being wrongly stripped during this schema's own construction.
    if (owner != null) {
      owner.ensureForeignKeysValidated();
    }
    Table table = delegate.getTable(name);
    if (table != null && constraintMetadata.containsKey(name)) {
      String schemaName = owner != null ? owner.getName() : null;
      return new ConstraintAwareJdbcTable(table, constraintMetadata.get(name), schemaName, name);
    }
    return table;
  }

  @SuppressWarnings("deprecation")
  @Override public Set<String> getTableNames() {
    return delegate.getTableNames();
  }

  @Override public @Nullable RelProtoDataType getType(String name) {
    return delegate.getType(name);
  }

  @Override public Set<String> getTypeNames() {
    return delegate.getTypeNames();
  }

  @Override public Collection<Function> getFunctions(String name) {
    return delegate.getFunctions(name);
  }

  @Override public Set<String> getFunctionNames() {
    return delegate.getFunctionNames();
  }

  @SuppressWarnings("deprecation")
  @Override public @Nullable Schema getSubSchema(String name) {
    return delegate.getSubSchema(name);
  }

  @SuppressWarnings("deprecation")
  @Override public Set<String> getSubSchemaNames() {
    return delegate.getSubSchemaNames();
  }

  @Override public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
    return delegate.getExpression(parentSchema, name);
  }

  @Override public boolean isMutable() {
    return delegate.isMutable();
  }

  @Override public Schema snapshot(SchemaVersion version) {
    return delegate.snapshot(version);
  }

  /**
   * Unwraps the schema to a given class.
   * Returns this instance when unwrapping to Schema.class to ensure
   * INFORMATION_SCHEMA views can detect and use constraint metadata.
   *
   * @param clazz the class to unwrap to
   * @param <T> the type to unwrap to
   * @return this instance if it's assignable to the class, otherwise delegates to underlying schema
   */
  @Override public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    return delegate.unwrap(clazz);
  }

  /**
   * Returns the schema comment by delegating to the underlying schema
   * if it implements CommentableSchema.
   *
   * @return schema comment, or null if delegate doesn't support comments
   */
  @Override public @Nullable String getComment() {
    if (delegate instanceof CommentableSchema) {
      return ((CommentableSchema) delegate).getComment();
    }
    return null;
  }

  /**
   * Wrapper for any Table that adds constraint metadata.
   * Delegates all methods to the wrapped table except getStatistic().
   *
   * <p>This wrapper implements CommentableTable to delegate comment requests
   * to the underlying table if it supports comments.
   */
  private static class ConstraintAwareJdbcTable implements CommentableTable {
    private final Table wrappedDelegate;
    private final Map<String, Object> constraintConfig;
    private final @Nullable String schemaName;
    private final @Nullable String tableName;
    private Statistic statistic;  // Non-final to allow lazy initialization

    public ConstraintAwareJdbcTable(Table wrappedDelegate,
                                    Map<String, Object> constraintConfig,
                                    @Nullable String schemaName,
                                    @Nullable String tableName) {
      this.wrappedDelegate = wrappedDelegate;
      this.constraintConfig = constraintConfig;
      this.schemaName = schemaName;
      this.tableName = tableName;
      // Delay statistic creation until we have column names
      this.statistic = null;
    }

    @Override public Statistic getStatistic() {
      LOGGER.info("ConstraintAwareJdbcTable.getStatistic() called - statistic is {}",
                  statistic == null ? "null (will create)" : "cached");
      if (statistic == null) {
        // Lazily create the statistic when first requested
        try {
          // Build statistic with constraints
          Map<String, Object> tableConfig = new LinkedHashMap<>();
          tableConfig.put("constraints", constraintConfig);

          LOGGER.info("Constraint config for table: {}", constraintConfig);

          // Column names must match what CalciteMetaImpl.getPrimaryKeys() uses for bit→name mapping:
          // both use wrappedDelegate.getRowType(), so indices are always consistent.
          RelDataTypeFactory typeFactory =
              new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
          RelDataType rowType = wrappedDelegate.getRowType(typeFactory);
          List<String> columnNames = new ArrayList<>();
          for (org.apache.calcite.rel.type.RelDataTypeField field : rowType.getFieldList()) {
            columnNames.add(field.getName());
          }

          LOGGER.info("Creating statistic for table with {} columns: {}",
                       columnNames.size(), columnNames);

          // Get the base statistic from the wrapped table to preserve row count
          Statistic baseStatistic = wrappedDelegate.getStatistic();
          Double rowCount = baseStatistic != null ? baseStatistic.getRowCount() : null;

          // Create statistic with actual column names and row count from base
          // Note: FK validation happens at schema level in FileSchema.validateForeignKeyConstraints()
          // which removes invalid FKs from constraint metadata before tables read it
          statistic = TableConstraints.fromConfig(tableConfig, columnNames, rowCount,
              schemaName, tableName);
          LOGGER.info("Created statistic with {} keys, {} referential constraints, rowCount={}",
                      statistic.getKeys() != null ? statistic.getKeys().size() : 0,
                      statistic.getReferentialConstraints() != null ?
                          statistic.getReferentialConstraints().size() : 0,
                      rowCount);

          // Log detailed FK information
          if (statistic.getReferentialConstraints() != null && !statistic.getReferentialConstraints().isEmpty()) {
            for (org.apache.calcite.rel.RelReferentialConstraint fk : statistic.getReferentialConstraints()) {
              LOGGER.info("FK: {} -> {}", fk.getSourceQualifiedName(),
                          fk.getTargetQualifiedName());
            }
          }
        } catch (Exception e) {
          LOGGER.error("Error creating statistic: {}", e.getMessage(), e);
          // Fall back to wrapped delegate's statistics if there's an error
          statistic = wrappedDelegate.getStatistic();
        }
      }

      LOGGER.info("ConstraintAwareJdbcTable.getStatistic() returning: {} keys, {} referential constraints",
                  statistic.getKeys() != null ? statistic.getKeys().size() : 0,
                  statistic.getReferentialConstraints() != null ? statistic.getReferentialConstraints().size() : 0);
      return statistic;
    }

    @Override public Schema.TableType getJdbcTableType() {
      return wrappedDelegate.getJdbcTableType();
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return wrappedDelegate.getRowType(typeFactory);
    }

    @Override public boolean isRolledUp(String column) {
      return wrappedDelegate.isRolledUp(column);
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
        SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
      return wrappedDelegate.rolledUpColumnValidInsideAgg(column, call, parent, config);
    }

    /**
     * Returns the table comment by delegating to the underlying table
     * if it implements CommentableTable.
     *
     * @return table comment, or null if delegate doesn't support comments
     */
    @Override public @Nullable String getTableComment() {
      if (wrappedDelegate instanceof CommentableTable) {
        return ((CommentableTable) wrappedDelegate).getTableComment();
      }
      return null;
    }

    /**
     * Returns the column comment by delegating to the underlying table
     * if it implements CommentableTable.
     *
     * @param columnName the column name
     * @return column comment, or null if delegate doesn't support comments
     */
    @Override public @Nullable String getColumnComment(String columnName) {
      if (wrappedDelegate instanceof CommentableTable) {
        return ((CommentableTable) wrappedDelegate).getColumnComment(columnName);
      }
      return null;
    }
  }
}
