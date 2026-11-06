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

import org.apache.calcite.adapter.jira.JiraCatalog.Form;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;

import com.fasterxml.jackson.databind.JsonNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A form's issues: one project, one issue type, the chosen fields as columns.
 *
 * <p>Read by a JQL search over the form, asking only for the fields the query selects; see
 * {@link Jql} for what of the WHERE clause narrows it. ORDER BY is evaluated by Calcite: JQL
 * orders text by its own collation, which a sort above the scan would not share. The scan is
 * lazy, so a LIMIT stops the page requests once it has its rows. Every column is nullable.
 *
 * <p>Written by {@link IssueWriter}.
 */
public class JiraFormTable extends AbstractJiraTable {

  private final JiraSchema schema;
  final Form form;
  private @Nullable RelDataType rowType;

  JiraFormTable(JiraSchema schema, Form form) {
    this.schema = schema;
    this.form = form;
  }

  @Override public synchronized RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (JiraColumn column : form.columns) {
        builder.add(column.name, column.kind.sqlType).nullable(true);
      }
      rowType = builder.build();
    }
    return rowType;
  }

  @Override Enumerable<Object[]> read(List<RexNode> filters, int @Nullable [] projects,
      @Nullable AtomicBoolean cancel) {
    final List<JiraColumn> columns = form.columns;
    final int[] selected = projects != null ? projects : allColumns(columns.size());
    final Set<String> fields = new LinkedHashSet<>();
    for (int index : selected) {
      final String fieldId = columns.get(index).fieldId;
      if (fieldId != null) {
        fields.add(fieldId);
      }
    }
    final Jql.Plan plan = Jql.plan(filters, columns);
    final boolean documentFormat = schema.connection().flavor().documentFormat;
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        final IssueReader reader = plan.keys != null
            ? IssueReader.fetch(schema.connection(), Collections.singletonList(form), fields,
                plan.keys)
            : IssueReader.search(schema.connection(), Collections.singletonList(form), fields,
                schema.pageSize(), plan.clauses);
        return new Enumerator<Object[]>() {
          private Object @Nullable [] current;

          @Override public Object[] current() {
            if (current == null) {
              throw new NoSuchElementException();
            }
            return current;
          }

          @Override public boolean moveNext() {
            checkCancel(cancel, form.name);
            if (!reader.hasNext()) {
              current = null;
              return false;
            }
            final JsonNode issue = reader.next();
            final Object[] row = new Object[selected.length];
            for (int i = 0; i < selected.length; i++) {
              row[i] = columns.get(selected[i]).read(issue, documentFormat);
            }
            current = row;
            return true;
          }

          @Override public void reset() {
            throw new UnsupportedOperationException("A Jira scan cannot be restarted");
          }

          @Override public void close() {
            // Each page is a complete HTTP exchange; nothing is held open between pages
          }
        };
      }
    };
  }

  /** The issue key. */
  @Override public String getKeyColumn() {
    return "key";
  }

  @Override public Enumerable<Long> modify(String operation, Enumerable<Object[]> rows,
      String updateColumns) {
    final List<Object[]> rowList = rows.toList();
    final IssueWriter writer = new IssueWriter(schema.connection(), form);
    final long written;
    switch (TableModify.Operation.valueOf(operation)) {
    case INSERT:
      final List<String> keys = new ArrayList<>();
      try {
        written = writer.insert(rowList, keys);
      } finally {
        recordInserted(keys);
      }
      break;
    case UPDATE:
      written = writer.update(rowList, Arrays.asList(updateColumns.split(",")));
      break;
    case DELETE:
      written = writer.delete(rowList);
      break;
    default:
      throw new UnsupportedOperationException(operation + " is not supported on Jira tables");
    }
    return Linq4j.singletonEnumerable(written);
  }
}
