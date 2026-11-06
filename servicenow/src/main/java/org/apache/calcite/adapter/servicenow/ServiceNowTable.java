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
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.servicenow.ServiceNowCatalog.TableDef;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
 *
 * <p>INSERT, UPDATE and DELETE are executed by {@link ServiceNowTableModify} through
 * {@link ServiceNowWriter}. UPDATE and DELETE find their rows through this read path, so a
 * verified filter is pushed for them as for a query.
 */
public class ServiceNowTable extends AbstractQueryableTable
    implements ProjectableFilterableTable, ModifiableTable, TranslatableTable {

  private final ServiceNowSchema schema;
  private final String name;
  private final PredicatePushdown pushdown;
  private final PushdownObserver observer;
  /** The staging table INSERTs go through, or null to insert directly. */
  private final @Nullable String stagingTable;
  private final List<String> insertedKeys = new ArrayList<>();
  private TableDef definition;
  private RelDataType rowType;
  private ImportBinding binding;

  ServiceNowTable(ServiceNowSchema schema, String name, PredicatePushdown pushdown,
      PushdownObserver observer, @Nullable String stagingTable) {
    super(Object[].class);
    this.schema = schema;
    this.name = name;
    this.pushdown = pushdown;
    this.observer = observer;
    this.stagingTable = stagingTable;
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
    return read(filters, projects, DataContext.Variable.CANCEL_FLAG.get(root));
  }

  /**
   * Reads the table.
   *
   * @param filters  the query's filters; those pushed to ServiceNow are removed from the list
   * @param projects the columns to return, or null for all
   * @param cancel   set when the statement is cancelled, or null if it cannot be
   */
  private Enumerable<Object[]> read(List<RexNode> filters, int @Nullable [] projects,
      @Nullable AtomicBoolean cancel) {
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

    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new RowEnumerator(
            new TableReader(schema.connection(), name, new ArrayList<>(fields), display,
                schema.pageSize(), pushedQuery),
            columns, selected, display, name, cancel);
      }
    };
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      // The plan of a query that needs no column (SELECT COUNT(*)) reads through here; it is the
      // same scan as ProjectableFilterableTable's, of every column and with no filter
      @SuppressWarnings("unchecked")
      @Override public Enumerator<T> enumerator() {
        return (Enumerator<T>) read(new ArrayList<>(), null, null).enumerator();
      }
    };
  }

  /**
   * The same logical scan Calcite makes of any table, after registering
   * {@link ServiceNowTableModify#RULE}: Calcite calls {@link #toModificationRel} only for an
   * INSERT, and an UPDATE or DELETE always scans the table it writes.
   */
  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    context.getCluster().getPlanner().addRule(ServiceNowTableModify.RULE);
    return LogicalTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
  }

  @Override public @Nullable Collection getModifiableCollection() {
    // Writes are executed by ServiceNowTableModify, not through a collection
    return null;
  }

  @Override public TableModify toModificationRel(RelOptCluster cluster,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode input,
      TableModify.Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    cluster.getPlanner().addRule(ServiceNowTableModify.RULE);
    return new LogicalTableModify(cluster, cluster.traitSetOf(Convention.NONE),
        table, catalogReader, input, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  /**
   * Name of the column that identifies a row. With {@link #takeInsertedKeys} this is what a
   * server needs to answer {@code INSERT/UPDATE/DELETE ... RETURNING}: Calcite has no RETURNING,
   * so the rows are read back by key. pgwire-calcite looks these two methods up by name.
   */
  public String getKeyColumn() {
    return "sys_id";
  }

  /**
   * Returns the sys_ids of the records written by INSERTs through this table since the last call,
   * in order, and forgets them. A row imported through an import set gives the sys_id of the
   * record its transform map wrote, inserted or (by coalesce) updated.
   */
  public List<String> takeInsertedKeys() {
    synchronized (insertedKeys) {
      final List<String> keys = new ArrayList<>(insertedKeys);
      insertedKeys.clear();
      return keys;
    }
  }

  /**
   * Executes an INSERT, UPDATE or DELETE. Called from code generated by
   * {@link ServiceNowTableModify}.
   *
   * @param operation     INSERT, UPDATE or DELETE
   * @param rows          for INSERT, one value per table column; for DELETE, the table row; for
   *                      UPDATE, the table row followed by one new value per update column
   * @param updateColumns comma-separated update column names (UPDATE only)
   * @return single-element enumerable holding the number of records written
   */
  public Enumerable<Long> modify(String operation, Enumerable<Object[]> rows,
      String updateColumns) {
    final List<Object[]> rowList = rows.toList();
    final List<ServiceNowColumn> columns = definition().columns;
    final long written;
    switch (TableModify.Operation.valueOf(operation)) {
    case INSERT:
      final List<String> keys = new ArrayList<>();
      try {
        written = new ServiceNowWriter(schema.connection(), name, columns, binding())
            .insert(rowList, keys);
      } finally {
        synchronized (insertedKeys) {
          insertedKeys.addAll(keys);
        }
      }
      break;
    case UPDATE:
      written = new ServiceNowWriter(schema.connection(), name, columns, null)
          .update(rowList, Arrays.asList(updateColumns.split(",")));
      break;
    case DELETE:
      written = new ServiceNowWriter(schema.connection(), name, columns, null).delete(rowList);
      break;
    default:
      throw new UnsupportedOperationException(
          operation + " is not supported on ServiceNow tables");
    }
    return Linq4j.singletonEnumerable(written);
  }

  /** The import set INSERTs go through, read on first use; null if the table is not bound. */
  private synchronized @Nullable ImportBinding binding() {
    if (stagingTable == null) {
      return null;
    }
    if (binding == null) {
      binding = ImportBinding.load(schema.connection(), schema.metadataPageSize(), name,
          stagingTable);
    }
    return binding;
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
