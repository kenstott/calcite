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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The tables that describe the org whatever {@code tables} restricts the schema to: what a
 * steward picks from when registering an sObject.
 *
 * <ul>
 *   <li>{@value #OBJECTS}{@code (name, label)}: every queryable sObject, from the one sObject
 *   list call; it reads no describe.
 *   <li>{@value #COLUMNS}{@code (table_name, column_name, data_type, ordinal_position)}: the
 *   columns one sObject has as a table, from its describe. A scan must name the sObject with
 *   {@code table_name = '<name>'}; without it the scan is refused, because answering it would
 *   describe every sObject.
 * </ul>
 *
 * <p>{@code data_type} is the PostgreSQL name the pgwire server reports for the column in
 * {@code information_schema.columns} once the sObject is served.
 */
final class ListingTables {

  static final String OBJECTS = "_salesforce_objects";
  static final String COLUMNS = "_salesforce_columns";

  private ListingTables() {}

  /** {@value #OBJECTS}: one row per queryable sObject, ordered by name. */
  static final class Objects extends AbstractTable implements ScannableTable {
    private final SalesforceSchema schema;

    Objects(SalesforceSchema schema) {
      this.schema = schema;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("name", SqlTypeName.VARCHAR)
          .add("label", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      final List<SalesforceConnection.SObjectBasicInfo> sObjects =
          new ArrayList<>(schema.queryableSObjects());
      sObjects.sort(Comparator.comparing(o -> o.name));
      final List<Object[]> rows = new ArrayList<>(sObjects.size());
      for (SalesforceConnection.SObjectBasicInfo sObject : sObjects) {
        rows.add(new Object[] {sObject.name, sObject.label});
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /** {@value #COLUMNS}: the columns of the one sObject a scan names. */
  static final class Columns extends AbstractTable implements FilterableTable {
    private final SalesforceSchema schema;

    Columns(SalesforceSchema schema) {
      this.schema = schema;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("table_name", SqlTypeName.VARCHAR)
          .add("column_name", SqlTypeName.VARCHAR)
          .add("data_type", SqlTypeName.VARCHAR)
          .add("ordinal_position", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
      final String table = literal(filters);
      if (table == null) {
        throw new IllegalArgumentException(COLUMNS + " lists the columns of one sObject and "
            + "must be queried with table_name = '<sObject>'; without it, it would describe "
            + "every sObject in the org");
      }
      boolean queryable = false;
      for (SalesforceConnection.SObjectBasicInfo sObject : schema.queryableSObjects()) {
        queryable |= sObject.name.equals(table);
      }
      if (!queryable) {
        throw new IllegalArgumentException("'" + table + "' is not a queryable sObject in "
            + "this org");
      }
      final RelDataType rowType =
          new SalesforceTable(schema, table).getRowType(root.getTypeFactory());
      final List<Object[]> rows = new ArrayList<>(rowType.getFieldCount());
      for (RelDataTypeField field : rowType.getFieldList()) {
        rows.add(new Object[] {table, field.getName(),
            postgresName(field.getType().getSqlTypeName()), field.getIndex() + 1});
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * The literal of a {@code table_name = '<literal>'} condition among the filters, or null if
   * there is none. Calcite may pass several conditions as one AND, so each filter's conjuncts are
   * searched. The filters are left as they are for Calcite to evaluate: every row returned for
   * the literal satisfies the condition.
   */
  private static String literal(List<RexNode> filters) {
    for (RexNode filter : filters) {
      for (RexNode condition : RelOptUtil.conjunctions(filter)) {
        if (condition.getKind() != SqlKind.EQUALS) {
          continue;
        }
        final List<RexNode> operands = ((RexCall) condition).getOperands();
        for (int side = 0; side < 2; side++) {
          final RexNode column = operands.get(side);
          final RexNode value = operands.get(1 - side);
          if (column instanceof RexInputRef
              && ((RexInputRef) column).getIndex() == 0
              && value instanceof RexLiteral) {
            final String text = ((RexLiteral) value).getValueAs(String.class);
            if (text != null) {
              return text;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * The PostgreSQL type name the pgwire server reports for a column of this SQL type: its
   * {@code normalize._TYPE_TABLE} type, shown under {@code catalog._PG_TYPE_DISPLAY_NAMES}.
   */
  static String postgresName(SqlTypeName type) {
    switch (type) {
    case VARCHAR:
      return "character varying";
    case INTEGER:
      return "integer";
    case DECIMAL:
      return "numeric";
    case DOUBLE:
      return "double precision";
    case BOOLEAN:
      return "boolean";
    case TIMESTAMP:
      return "timestamp";
    case DATE:
      return "date";
    default:
      throw new IllegalStateException("No PostgreSQL type name for SQL type " + type);
    }
  }
}
