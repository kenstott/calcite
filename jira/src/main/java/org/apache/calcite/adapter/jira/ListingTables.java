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
import org.apache.calcite.adapter.jira.JiraCatalog.FieldMeta;
import org.apache.calcite.adapter.jira.JiraCatalog.IssueType;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The tables that describe the site without serving its issues: what a steward picks from when
 * registering a form.
 *
 * <ul>
 *   <li>{@value #FORMS}{@code (project_key, project_name, issue_type, issue_type_id, subtask,
 *   name)}: every project and issue type the account can create issues of. {@code name} is the
 *   table name the form would get by default, {@code <project key>_<issue type>} snake-cased. A
 *   condition {@code project_key = '<key>'} reads that project's issue types only.
 *   <li>{@value #FIELDS}{@code (project_key, issue_type, field_id, field_name, column_name,
 *   data_type, required, read_only, ordinal_position)}: the fields of one issue type's create
 *   screen. A scan
 *   must name it with {@code project_key = '<key>' AND issue_type = '<name or id>'}. {@code
 *   column_name} is the column the field gets by default; {@code data_type} is the PostgreSQL
 *   name of that column's type. A user field is followed by its display name,
 *   {@code <id>.displayName}, read-only. A read-only field is one no INSERT or UPDATE may set.
 * </ul>
 */
final class ListingTables {

  static final String FORMS = "_jira_forms";
  static final String FIELDS = "_jira_fields";

  private ListingTables() {}

  /** {@value #FORMS}: one row per project and issue type. */
  static final class Forms extends AbstractTable implements FilterableTable {
    private final JiraSchema schema;

    Forms(JiraSchema schema) {
      this.schema = schema;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("project_key", SqlTypeName.VARCHAR)
          .add("project_name", SqlTypeName.VARCHAR)
          .add("issue_type", SqlTypeName.VARCHAR)
          .add("issue_type_id", SqlTypeName.VARCHAR)
          .add("subtask", SqlTypeName.BOOLEAN)
          .add("name", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
      final String only = literal(filters, 0);
      final List<Object[]> rows = new ArrayList<>();
      for (Map.Entry<String, String> project : schema.catalog().projects().entrySet()) {
        if (only != null && !only.equals(project.getKey())) {
          continue;
        }
        for (IssueType type : schema.catalog().issueTypes(project.getKey())) {
          rows.add(new Object[] {project.getKey(), project.getValue(), type.name, type.id,
              type.subtask,
              JiraColumn.columnName(project.getKey().toLowerCase(Locale.ROOT) + " "
                  + type.name)});
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /** {@value #FIELDS}: the create-screen fields of the one issue type a scan names. */
  static final class Fields extends AbstractTable implements FilterableTable {
    private final JiraSchema schema;

    Fields(JiraSchema schema) {
      this.schema = schema;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("project_key", SqlTypeName.VARCHAR)
          .add("issue_type", SqlTypeName.VARCHAR)
          .add("field_id", SqlTypeName.VARCHAR)
          .add("field_name", SqlTypeName.VARCHAR)
          .add("column_name", SqlTypeName.VARCHAR)
          .add("data_type", SqlTypeName.VARCHAR)
          .add("required", SqlTypeName.BOOLEAN)
          .add("read_only", SqlTypeName.BOOLEAN)
          .add("ordinal_position", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
      final String project = literal(filters, 0);
      final String issueType = literal(filters, 1);
      if (project == null || issueType == null) {
        throw new JiraException(FIELDS + " lists the fields of one issue type and must be "
            + "queried with project_key = '<key>' AND issue_type = '<issue type>'");
      }
      final IssueType type = schema.catalog().issueType(project, issueType);
      final List<Object[]> rows = new ArrayList<>();
      int ordinal = 0;
      for (FieldMeta field : schema.catalog().createFields(project, type.id)) {
        // project and issuetype are the form itself, not columns of it
        if (field.id.equals("project") || field.id.equals("issuetype")) {
          continue;
        }
        final JiraColumn.Kind kind = field.kind();
        final String column = JiraColumn.columnName(field.name);
        rows.add(new Object[] {project, issueType, field.id, field.name, column,
            postgresName(kind.sqlType), field.required, !JiraColumn.writable(kind), ++ordinal});
        if (kind == JiraColumn.Kind.USER) {
          rows.add(new Object[] {project, issueType, field.id + JiraCatalog.DISPLAY_NAME,
              field.name + " (display name)", column + "_name", postgresName(SqlTypeName.VARCHAR),
              false, true, ++ordinal});
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * The literal of a {@code <column> = '<literal>'} condition among the filters, or null. The
   * filters stay for Calcite: every row returned for the literal satisfies the condition.
   */
  private static @Nullable String literal(List<RexNode> filters, int columnIndex) {
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
              && ((RexInputRef) column).getIndex() == columnIndex
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

  /** The PostgreSQL type name the pgwire server reports for a column of this SQL type. */
  static String postgresName(SqlTypeName type) {
    switch (type) {
    case VARCHAR:
      return "character varying";
    case DOUBLE:
      return "double precision";
    case DATE:
      return "date";
    case TIMESTAMP:
      return "timestamp";
    case BOOLEAN:
      return "boolean";
    default:
      throw new JiraException("No PostgreSQL type name for SQL type " + type);
    }
  }
}
