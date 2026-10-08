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
package org.apache.calcite.adapter.servicenow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.servicenow.ServiceNowCatalog.TableDef;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A ServiceNow table, read through the Table API.
 *
 * <p>Projection is pushed down: only the fields the query selects are requested
 * ({@code sysparm_fields}, plus {@code sys_id} for paging). Filters are pushed only when every
 * {@link PushdownCapabilities} entry they need is verified (see {@link EncodedQueryTranslator});
 * with the shipped, empty verification record that is never, and Calcite evaluates every filter
 * over the rows returned. ORDER BY is never pushed: the collation of a ServiceNow sort is
 * undocumented and a sort pushed under a Calcite operator that relies on a different collation
 * would give wrong results, so Calcite sorts. LIMIT is never sent as {@code sysparm_limit}
 * either (the scan is lazy, so a LIMIT above it stops the page fetches once enough rows have been
 * read; that is correct whether the filters were pushed, kept in Calcite or both, so a limit can
 * never cut off rows a filter would have removed).
 *
 * <p>Row type: {@code sys_id} first, then the other columns by name. Every column is nullable
 * without exception: an empty value is NULL for every type, and the dictionary's {@code mandatory}
 * flag is a UI rule that existing rows can violate.
 */
class ServiceNowTable extends AbstractTable implements ProjectableFilterableTable {

  private final ServiceNowSchema schema;
  private final String name;
  private final PredicatePushdown pushdown;
  private final PushdownObserver observer;
  private TableDef definition;
  private RelDataType rowType;

  ServiceNowTable(ServiceNowSchema schema, String name, PredicatePushdown pushdown,
      PushdownObserver observer) {
    this.schema = schema;
    this.name = name;
    this.pushdown = pushdown;
    this.observer = observer;
  }

  /** The table's columns, resolved on first use. */
  synchronized TableDef definition() {
    if (definition == null) {
      definition = schema.catalog().table(name);
    }
    return definition;
  }

  @Override public synchronized RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (ServiceNowColumn column : definition().columns) {
        final RelDataType base;
        if (column.kind.sqlType == SqlTypeName.DECIMAL) {
          base = typeFactory.createSqlType(SqlTypeName.DECIMAL, 19,
              column.kind == ServiceNowColumn.Kind.CURRENCY ? 4 : 2);
        } else if (column.kind == ServiceNowColumn.Kind.GUID
            || column.kind == ServiceNowColumn.Kind.REFERENCE) {
          base = typeFactory.createSqlType(SqlTypeName.VARCHAR, 32);
        } else if (column.kind == ServiceNowColumn.Kind.TEXT && column.maxLength > 0
            && !column.display) {
          base = typeFactory.createSqlType(SqlTypeName.VARCHAR, column.maxLength);
        } else {
          base = typeFactory.createSqlType(column.kind.sqlType);
        }
        builder.add(column.name, base).nullable(true);
      }
      rowType = builder.build();
    }
    return rowType;
  }

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
      int[] projects) {
    final List<ServiceNowColumn> columns = definition().columns;
    final int[] selected = projects != null ? projects : allColumns(columns.size());
    // What is taken goes into sysparm_query and is removed from filters; the rest stays there and
    // is evaluated by Calcite. With no verified pushdown entry nothing is taken.
    final PredicatePushdown.Result pushed = pushdown.push(filters, columns);
    final String pushedQuery = pushed.query;
    if (observer != null) {
      observer.scan(name, pushedQuery, pushed.entries, filters.size());
    }

    // The fields to request: those behind the selected columns, plus sys_id for paging
    final Set<String> fields = new LinkedHashSet<>();
    fields.add("sys_id");
    boolean displayValues = false;
    for (int index : selected) {
      fields.add(columns.get(index).field);
      displayValues |= columns.get(index).display;
    }
    final boolean display = displayValues;
    final AtomicBoolean cancel = DataContext.Variable.CANCEL_FLAG.get(root);

    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new RowEnumerator(
            new TableReader(schema.connection(), name, new ArrayList<>(fields), display,
                schema.pageSize(), pushedQuery),
            columns, selected, display, name, cancel);
      }
    };
  }

  private static int[] allColumns(int count) {
    final int[] all = new int[count];
    for (int i = 0; i < count; i++) {
      all[i] = i;
    }
    return all;
  }

  /** Converts rows as the reader delivers them. */
  private static final class RowEnumerator implements Enumerator<Object[]> {
    private final TableReader reader;
    private final List<ServiceNowColumn> columns;
    private final int[] selected;
    private final boolean displayMode;
    private final String table;
    private final AtomicBoolean cancel;
    private Object[] current;

    RowEnumerator(TableReader reader, List<ServiceNowColumn> columns, int[] selected,
        boolean displayMode, String table, AtomicBoolean cancel) {
      this.reader = reader;
      this.columns = columns;
      this.selected = selected;
      this.displayMode = displayMode;
      this.table = table;
      this.cancel = cancel;
    }

    @Override public Object[] current() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override public boolean moveNext() {
      if (cancel != null && cancel.get()) {
        throw new ServiceNowException("Scan of " + table + " was cancelled");
      }
      if (!reader.hasNext()) {
        current = null;
        return false;
      }
      final JsonNode row = reader.next();
      final String sysId = Rows.stored(row, "sys_id", table, displayMode, false);
      final Object[] values = new Object[selected.length];
      for (int i = 0; i < selected.length; i++) {
        final ServiceNowColumn column = columns.get(selected[i]);
        final String text;
        if (column.display) {
          text = Rows.displayText(row, column.field, table);
        } else {
          text = Rows.stored(row, column.field, table, displayMode,
              column.kind == ServiceNowColumn.Kind.REFERENCE);
        }
        values[i] = ValueConverter.convert(column, text, table, sysId);
      }
      current = values;
      return true;
    }

    @Override public void reset() {
      throw new UnsupportedOperationException("A ServiceNow scan cannot be restarted");
    }

    @Override public void close() {
      // Each page is a complete HTTP exchange; nothing is held open between pages
    }
  }
}
