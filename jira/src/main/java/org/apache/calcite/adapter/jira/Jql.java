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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * What of a query's WHERE clause is sent to Jira.
 *
 * <p>Every filter stays in Calcite as well: what is sent only narrows what Jira returns, so its
 * comparison rules never decide a row. Two kinds of condition are sent:
 * <ul>
 *   <li>{@code key = '<key>'}, or several ORed or as IN: the issues are fetched by key
 *   ({@code POST /issue/bulkfetch}), so a key that does not exist is an absent row rather than
 *   the JQL error a {@code key = } clause would give.
 *   <li>{@code <, <=, >, >=, =} between a date, datetime or number column and a literal, as a
 *   JQL clause. A datetime is compared by JQL to the minute and in the account's time zone, so
 *   its bound is widened by a day each way; a date and a number are exact.
 * </ul>
 * Equality on text, options, statuses and users is not sent: JQL refuses a value that names no
 * option, status or user, where SQL finds no row.
 */
final class Jql {

  private static final JavaTypeFactory TYPES = new JavaTypeFactoryImpl();

  private static final RexBuilder REX = new RexBuilder(TYPES);

  private Jql() {}

  /** The issues a scan reads. */
  static final class Plan {
    /** The keys to fetch, or null to search. */
    final @Nullable Set<String> keys;
    /** JQL clauses ANDed to the form's own, when searching. */
    final List<String> clauses;

    Plan(@Nullable Set<String> keys, List<String> clauses) {
      this.keys = keys;
      this.clauses = clauses;
    }
  }

  static Plan plan(List<RexNode> filters, List<JiraColumn> columns) {
    Set<String> keys = null;
    final List<String> clauses = new ArrayList<>();
    for (RexNode filter : filters) {
      for (RexNode condition
          : RelOptUtil.conjunctions(RexUtil.expandSearch(REX, null, filter))) {
        final Set<String> pinned = keys(condition, columns);
        if (pinned != null) {
          if (keys == null) {
            keys = pinned;
          } else {
            keys.retainAll(pinned);
          }
          continue;
        }
        final String clause = clause(condition, columns);
        if (clause != null) {
          clauses.add(clause);
        }
      }
    }
    return new Plan(keys, clauses);
  }

  /** The keys a condition pins the key column to, or null if it does not. */
  private static @Nullable Set<String> keys(RexNode condition, List<JiraColumn> columns) {
    final Set<String> keys = new LinkedHashSet<>();
    for (RexNode disjunct : RelOptUtil.disjunctions(condition)) {
      if (disjunct.getKind() != SqlKind.EQUALS) {
        return null;
      }
      final Comparison comparison = Comparison.of((RexCall) disjunct);
      if (comparison == null
          || columns.get(comparison.column).kind != JiraColumn.Kind.KEY) {
        return null;
      }
      final String key = comparison.literal.getValueAs(String.class);
      if (key == null) {
        return null;
      }
      keys.add(key);
    }
    return keys;
  }

  private static @Nullable String clause(RexNode condition, List<JiraColumn> columns) {
    if (!(condition instanceof RexCall)) {
      return null;
    }
    final Comparison comparison = Comparison.of((RexCall) condition);
    if (comparison == null) {
      return null;
    }
    final JiraColumn column = columns.get(comparison.column);
    if (column.fieldId == null) {
      return null;
    }
    final String field = field(column.fieldId);
    final SqlKind op = comparison.op;
    switch (column.kind) {
    case NUMBER: {
      final BigDecimal value = comparison.literal.getValueAs(BigDecimal.class);
      return value == null ? null : field + " " + operator(op) + " " + value.toPlainString();
    }
    case DATE: {
      final Integer days = comparison.literal.getValueAs(Integer.class);
      return days == null ? null
          : field + " " + operator(op) + " " + quote(LocalDate.ofEpochDay(days).toString());
    }
    case DATETIME: {
      final Long millis = comparison.literal.getValueAs(Long.class);
      if (millis == null) {
        return null;
      }
      final LocalDate day = Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).toLocalDate();
      final String from = quote(day.minusDays(1).toString());
      final String before = quote(day.plusDays(2).toString());
      switch (op) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return field + " >= " + from;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return field + " < " + before;
      case EQUALS:
        return field + " >= " + from + " AND " + field + " < " + before;
      default:
        return null;
      }
    }
    default:
      return null;
    }
  }

  private static String operator(SqlKind op) {
    switch (op) {
    case EQUALS:
      return "=";
    case LESS_THAN:
      return "<";
    case LESS_THAN_OR_EQUAL:
      return "<=";
    case GREATER_THAN:
      return ">";
    case GREATER_THAN_OR_EQUAL:
      return ">=";
    default:
      throw new IllegalArgumentException("No JQL operator for " + op);
    }
  }

  /** How JQL names a field: {@code cf[10016]} for {@code customfield_10016}, else its id. */
  static String field(String fieldId) {
    return fieldId.startsWith("customfield_")
        ? "cf[" + fieldId.substring("customfield_".length()) + "]" : fieldId;
  }

  /** A JQL string literal. */
  static String quote(String text) {
    return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  /** {@code <column> <op> <literal>}, with the literal on the right. */
  private static final class Comparison {
    final int column;
    final SqlKind op;
    final RexLiteral literal;

    private Comparison(int column, SqlKind op, RexLiteral literal) {
      this.column = column;
      this.op = op;
      this.literal = literal;
    }

    static @Nullable Comparison of(RexCall call) {
      final SqlKind kind = call.getKind();
      switch (kind) {
      case EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        break;
      default:
        return null;
      }
      final RexNode left = call.getOperands().get(0);
      final RexNode right = call.getOperands().get(1);
      if (left instanceof RexInputRef && right instanceof RexLiteral) {
        return new Comparison(((RexInputRef) left).getIndex(), kind, (RexLiteral) right);
      }
      if (right instanceof RexInputRef && left instanceof RexLiteral) {
        return new Comparison(((RexInputRef) right).getIndex(), kind.reverse(),
            (RexLiteral) left);
      }
      return null;
    }
  }
}
