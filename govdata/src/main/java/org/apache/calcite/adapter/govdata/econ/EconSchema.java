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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.TableCommentDefinitions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper schema for ECON data that adds table/column comment support.
 *
 * <p>This class wraps a FileSchema (which handles the actual data access)
 * and decorates tables with comment metadata from TableCommentDefinitions.
 * This enables information_schema queries to return table and column comments.
 */
public class EconSchema extends AbstractSchema implements CommentableSchema {
  private final Schema delegateSchema;

  public EconSchema(Schema delegateSchema) {
    this.delegateSchema = delegateSchema;
  }

  @SuppressWarnings("deprecation") // getTableNames and getTable are deprecated but still supported
  @Override protected Map<String, Table> getTableMap() {
    // Wrap tables with comment support for known ECON tables
    Map<String, Table> commentableTableMap = new HashMap<>();

    // Iterate through all tables from the delegate schema
    for (String tableName : delegateSchema.getTableNames()) {
      Table originalTable = delegateSchema.getTable(tableName);
      if (originalTable == null) {
        continue;
      }

      // Check if we have comments for this table
      String tableComment = TableCommentDefinitions.getEconTableComment(tableName);
      Map<String, String> columnComments = TableCommentDefinitions.getEconColumnComments(tableName);

      if (tableComment != null || !columnComments.isEmpty()) {
        // Wrap with comment support
        commentableTableMap.put(tableName,
            new CommentableEconTableWrapper(originalTable, tableComment, columnComments));
      } else {
        commentableTableMap.put(tableName, originalTable);
      }
    }
    return ImmutableMap.copyOf(commentableTableMap);
  }

  @Override public @Nullable String getComment() {
    return "U.S. economic data from Bureau of Labor Statistics (BLS), Federal Reserve (FRED), "
        + "U.S. Treasury, Bureau of Economic Analysis (BEA), and World Bank. "
        + "Includes employment statistics, inflation metrics, wage data, interest rates, "
        + "GDP components, trade statistics, and international economic indicators.";
  }

  /**
   * Wrapper that adds comment support to existing ECON Table instances.
   */
  private static class CommentableEconTableWrapper implements CommentableTable {
    private final Table delegate;
    private final @Nullable String tableComment;
    private final Map<String, String> columnComments;

    CommentableEconTableWrapper(Table delegate, @Nullable String tableComment,
        Map<String, String> columnComments) {
      this.delegate = delegate;
      this.tableComment = tableComment;
      this.columnComments = columnComments;
    }

    @Override public @Nullable String getTableComment() {
      return tableComment;
    }

    @Override public @Nullable String getColumnComment(String columnName) {
      return columnComments.get(columnName.toLowerCase());
    }

    // Delegate all other Table methods

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return delegate.getRowType(typeFactory);
    }

    @Override public org.apache.calcite.schema.Statistic getStatistic() {
      return delegate.getStatistic();
    }

    @Override public org.apache.calcite.schema.Schema.TableType getJdbcTableType() {
      return delegate.getJdbcTableType();
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
