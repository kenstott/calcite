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
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
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
 */
public class ConstraintAwareJdbcSchema implements Schema {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConstraintAwareJdbcSchema.class);

  private final JdbcSchema delegate;
  private final Map<String, Map<String, Object>> constraintMetadata;

  public ConstraintAwareJdbcSchema(JdbcSchema delegate, Map<String, Map<String, Object>> constraintMetadata) {
    this.delegate = delegate;
    this.constraintMetadata = constraintMetadata != null ? constraintMetadata : new LinkedHashMap<>();
    LOGGER.info("Created ConstraintAwareJdbcSchema with constraints for tables: {}",
                this.constraintMetadata.keySet());
  }

  @SuppressWarnings("deprecation")
  @Override public @Nullable Table getTable(String name) {
    Table table = delegate.getTable(name);
    if (table instanceof JdbcTable && constraintMetadata.containsKey(name)) {
      LOGGER.info("Wrapping JdbcTable '{}' with constraint metadata", name);
      return new ConstraintAwareJdbcTable((JdbcTable) table, constraintMetadata.get(name));
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
   * Wrapper for JdbcTable that adds constraint metadata.
   * Delegates all methods to the original table except getStatistic().
   */
  private static class ConstraintAwareJdbcTable implements Table {
    private final JdbcTable delegate;
    private final Map<String, Object> constraintConfig;
    private Statistic statistic;  // Non-final to allow lazy initialization

    public ConstraintAwareJdbcTable(JdbcTable delegate, Map<String, Object> constraintConfig) {
      this.delegate = delegate;
      this.constraintConfig = constraintConfig;
      // Delay statistic creation until we have column names
      this.statistic = null;
    }

    @Override public Statistic getStatistic() {
      if (statistic == null) {
        // Lazily create the statistic when first requested
        try {
          // Build statistic with constraints
          Map<String, Object> tableConfig = new LinkedHashMap<>();
          tableConfig.put("constraints", constraintConfig);

          // Get column names from the row type
          RelDataTypeFactory typeFactory =
              new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
          RelDataType rowType = delegate.getRowType(typeFactory);
          List<String> columnNames = new ArrayList<>();
          for (org.apache.calcite.rel.type.RelDataTypeField field : rowType.getFieldList()) {
            columnNames.add(field.getName());
          }

          LOGGER.debug("Creating statistic for table with {} columns: {}",
                       columnNames.size(), columnNames);

          // Create statistic with actual column names
          statistic = TableConstraints.fromConfig(tableConfig, columnNames, null);
          LOGGER.info("Created statistic with {} keys and {} referential constraints",
                      statistic.getKeys() != null ? statistic.getKeys().size() : 0,
                      statistic.getReferentialConstraints() != null ?
                          statistic.getReferentialConstraints().size() : 0);
        } catch (Exception e) {
          LOGGER.error("Error creating statistic: {}", e.getMessage(), e);
          // Fall back to unknown statistics if there's an error
          statistic = org.apache.calcite.schema.Statistics.UNKNOWN;
        }
      }
      return statistic;
    }

    @Override public Schema.TableType getJdbcTableType() {
      return delegate.getJdbcTableType();
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return delegate.getRowType(typeFactory);
    }

    @Override public boolean isRolledUp(String column) {
      return delegate.isRolledUp(column);
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
        SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
      return delegate.rolledUpColumnValidInsideAgg(column, call, parent, config);
    }
  }
}
