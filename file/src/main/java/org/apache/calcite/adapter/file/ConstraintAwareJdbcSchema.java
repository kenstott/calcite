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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.metadata.TableConstraints;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
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

  public ConstraintAwareJdbcSchema(JdbcSchema delegate, Map<String, Map<String, Object>> constraintMetadata) {
    this.delegate = delegate;
    this.constraintMetadata = constraintMetadata != null ? constraintMetadata : new LinkedHashMap<>();
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
    Table table = delegate.getTable(name);

    // If this table has constraint metadata, wrap it regardless of type
    // This handles both direct JdbcTable instances and tables already wrapped
    // by DuckDBJdbcSchema with CommentableJdbcTableWrapper
    if (table != null && constraintMetadata.containsKey(name)) {
      LOGGER.info("Wrapping table '{}' (type: {}) with constraint metadata",
                  name, table.getClass().getSimpleName());

      // Try to extract JdbcTable if this is already wrapped by DuckDBJdbcSchema
      JdbcTable jdbcTable = null;
      if (table instanceof JdbcTable) {
        LOGGER.info("Table '{}' is directly a JdbcTable", name);
        jdbcTable = (JdbcTable) table;
      } else {
        LOGGER.info("Table '{}' is not a JdbcTable, trying reflection. Delegate type: {}",
                    name, delegate.getClass().getName());
        // Check if delegate is DuckDBJdbcSchema and can provide the underlying JdbcTable
        // First try to call parent JdbcSchema.getTable() to get the base table
        try {
          java.lang.reflect.Method method = JdbcSchema.class.getDeclaredMethod("getTable", String.class);
          method.setAccessible(true);
          Object unwrapped = method.invoke(delegate, name);
          LOGGER.info("Reflection returned object of type: {}",
                      unwrapped != null ? unwrapped.getClass().getName() : "null");

          if (unwrapped instanceof JdbcTable) {
            jdbcTable = (JdbcTable) unwrapped;
            LOGGER.info("Extracted underlying JdbcTable from wrapped table");
          } else if (unwrapped != null && unwrapped.getClass().getSimpleName().equals("CommentableJdbcTableWrapper")) {
            // The unwrapped object is a CommentableJdbcTableWrapper, extract the jdbcTable field from it
            try {
              java.lang.reflect.Field jdbcTableField = unwrapped.getClass().getDeclaredField("jdbcTable");
              jdbcTableField.setAccessible(true);
              Object extractedJdbcTable = jdbcTableField.get(unwrapped);
              if (extractedJdbcTable instanceof JdbcTable) {
                jdbcTable = (JdbcTable) extractedJdbcTable;
                LOGGER.info("Extracted JdbcTable from CommentableJdbcTableWrapper using reflection");
              }
            } catch (Exception e) {
              LOGGER.warn("Could not extract jdbcTable field from CommentableJdbcTableWrapper: {}", e.getMessage());
            }
          } else {
            LOGGER.warn("Reflection did not return a JdbcTable or CommentableJdbcTableWrapper for table '{}'", name);
          }
        } catch (Exception e) {
          LOGGER.warn("Could not extract JdbcTable from wrapped table: {}", e.getMessage());
        }
      }

      if (jdbcTable != null) {
        LOGGER.info("Successfully created ConstraintAwareJdbcTable for '{}'", name);
        return new ConstraintAwareJdbcTable(jdbcTable, table, constraintMetadata.get(name));
      } else {
        LOGGER.warn("Failed to create ConstraintAwareJdbcTable for '{}' - returning unwrapped table", name);
      }
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
   * Wrapper for JdbcTable that adds constraint metadata.
   * Delegates all methods to the wrapped table except getStatistic().
   *
   * <p>This wrapper implements CommentableTable to delegate comment requests
   * to the underlying table if it supports comments.
   *
   * <p>This class handles both simple JdbcTable instances and tables that are
   * already wrapped by DuckDBJdbcSchema with CommentableJdbcTableWrapper.
   */
  private static class ConstraintAwareJdbcTable implements CommentableTable {
    private final JdbcTable jdbcDelegate;     // For building statistic with metadata
    private final Table wrappedDelegate;      // For delegating other Table methods
    private final Map<String, Object> constraintConfig;
    private Statistic statistic;  // Non-final to allow lazy initialization

    public ConstraintAwareJdbcTable(JdbcTable jdbcDelegate, Table wrappedDelegate,
                                    Map<String, Object> constraintConfig) {
      this.jdbcDelegate = jdbcDelegate;
      this.wrappedDelegate = wrappedDelegate;
      this.constraintConfig = constraintConfig;
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

          // Get column names from the row type using jdbcDelegate
          RelDataTypeFactory typeFactory =
              new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
          RelDataType rowType = jdbcDelegate.getRowType(typeFactory);
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
          statistic = TableConstraints.fromConfig(tableConfig, columnNames, rowCount);
          LOGGER.info("Created statistic with {} keys, {} referential constraints, rowCount={}",
                      statistic.getKeys() != null ? statistic.getKeys().size() : 0,
                      statistic.getReferentialConstraints() != null ?
                          statistic.getReferentialConstraints().size() : 0,
                      rowCount);

          // Log detailed FK information
          if (statistic.getReferentialConstraints() != null && !statistic.getReferentialConstraints().isEmpty()) {
            for (org.apache.calcite.rel.RelReferentialConstraint fk : statistic.getReferentialConstraints()) {
              LOGGER.info("FK found: {} -> {}", fk.getSourceQualifiedName(),
                          fk.getTargetQualifiedName());
            }
          } else {
            LOGGER.warn("No referential constraints found in statistic!");
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
