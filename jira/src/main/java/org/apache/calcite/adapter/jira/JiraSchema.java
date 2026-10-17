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
import org.apache.calcite.adapter.jira.JiraCatalog.FormSpec;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Schema holding the site's listing tables, its issue links and one table per form.
 *
 * <p>{@link ListingTables#FORMS}, {@link ListingTables#FIELDS} and {@link IssueLinksTable#NAME}
 * are always present. Only the forms named in the {@code forms} operand are tables of issues;
 * with none there are none. Forms are resolved against the site's metadata on first use.
 */
public class JiraSchema extends AbstractSchema {

  private final JiraConnection connection;
  private final int pageSize;
  private final List<FormSpec> formSpecs;
  private final JiraCatalog catalog;
  private @Nullable List<Form> forms;
  private @Nullable Map<String, Table> tableMap;

  JiraSchema(JiraConnection connection, int pageSize, List<FormSpec> formSpecs) {
    this.connection = connection;
    this.pageSize = pageSize;
    this.formSpecs = formSpecs;
    this.catalog = new JiraCatalog(connection);
  }

  JiraConnection connection() {
    return connection;
  }

  int pageSize() {
    return pageSize;
  }

  JiraCatalog catalog() {
    return catalog;
  }

  /** The forms, resolved on first use. */
  synchronized List<Form> forms() {
    if (forms == null) {
      final Set<String> names = new HashSet<>();
      final List<Form> resolved = new ArrayList<>();
      for (FormSpec spec : formSpecs) {
        final String lower = spec.name.toLowerCase(Locale.ROOT);
        if (lower.equals(ListingTables.FORMS) || lower.equals(ListingTables.FIELDS)
            || lower.equals(IssueLinksTable.NAME)) {
          throw new JiraException("The 'forms' operand names a form '" + spec.name + "', which "
              + "is the adapter's own table");
        }
        if (!names.add(lower)) {
          throw new JiraException("The 'forms' operand names the form '" + spec.name
              + "' twice");
        }
        resolved.add(catalog.resolve(spec));
      }
      forms = resolved;
    }
    return forms;
  }

  @Override protected synchronized Map<String, Table> getTableMap() {
    if (tableMap == null) {
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
      builder.put(ListingTables.FORMS, new ListingTables.Forms(this));
      builder.put(ListingTables.FIELDS, new ListingTables.Fields(this));
      builder.put(IssueLinksTable.NAME, new IssueLinksTable(this));
      for (Form form : forms()) {
        builder.put(form.name, new JiraFormTable(this, form));
      }
      tableMap = builder.build();
    }
    return tableMap;
  }
}
