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
package org.apache.calcite.adapter.jira;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Jira table that is read with projection and filters handed to it, and written by
 * {@link JiraTableModify}.
 */
public abstract class AbstractJiraTable extends AbstractQueryableTable
    implements ProjectableFilterableTable, ModifiableTable, TranslatableTable, JiraWritable {

  private final List<String> insertedKeys = new ArrayList<>();

  AbstractJiraTable() {
    super(Object[].class);
  }

  /**
   * Reads the table.
   *
   * @param filters  the query's filters, all of which Calcite still evaluates
   * @param projects the columns to return, or null for all
   * @param cancel   set when the statement is cancelled, or null if it cannot be
   */
  abstract Enumerable<Object[]> read(List<RexNode> filters, int @Nullable [] projects,
      @Nullable AtomicBoolean cancel);

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
      int[] projects) {
    return read(filters, projects, DataContext.Variable.CANCEL_FLAG.get(root));
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      // The plan of a query that needs no column (SELECT COUNT(*)) reads through here
      @SuppressWarnings("unchecked")
      @Override public Enumerator<T> enumerator() {
        return (Enumerator<T>) read(new ArrayList<>(), null, null).enumerator();
      }
    };
  }

  /**
   * The same logical scan Calcite makes of any table, after registering
   * {@link JiraTableModify#RULE}: Calcite calls {@link #toModificationRel} only for an INSERT,
   * and an UPDATE or DELETE always scans the table it writes.
   */
  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    context.getCluster().getPlanner().addRule(JiraTableModify.RULE);
    return LogicalTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
  }

  @Override public @Nullable Collection getModifiableCollection() {
    // Writes are executed by JiraTableModify, not through a collection
    return null;
  }

  @Override public TableModify toModificationRel(RelOptCluster cluster,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode input,
      TableModify.Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    cluster.getPlanner().addRule(JiraTableModify.RULE);
    return new LogicalTableModify(cluster, cluster.traitSetOf(Convention.NONE),
        table, catalogReader, input, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  /**
   * Name of the column that identifies a row. With {@link #takeInsertedKeys} this is what a
   * server needs to answer {@code INSERT/UPDATE/DELETE ... RETURNING}: Calcite has no RETURNING,
   * so the rows are read back by key. pgwire-calcite looks these two methods up by name.
   */
  public abstract String getKeyColumn();

  /** The keys of the rows INSERTs wrote since the last call, in order; forgets them. */
  public List<String> takeInsertedKeys() {
    synchronized (insertedKeys) {
      final List<String> keys = new ArrayList<>(insertedKeys);
      insertedKeys.clear();
      return keys;
    }
  }

  void recordInserted(List<String> keys) {
    synchronized (insertedKeys) {
      insertedKeys.addAll(keys);
    }
  }

  static int[] allColumns(int count) {
    final int[] all = new int[count];
    for (int i = 0; i < count; i++) {
      all[i] = i;
    }
    return all;
  }

  /** Throws if the statement was cancelled. */
  static void checkCancel(@Nullable AtomicBoolean cancel, String table) {
    if (cancel != null && cancel.get()) {
      throw new JiraException("Scan of " + table + " was cancelled");
    }
  }
}
