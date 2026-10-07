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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Builds SOQL queries from Rex expressions.
 */
public class SOQLBuilder {

  /**
   * Delimits the control tokens of a WHERE clause template. A template is plain SOQL interleaved
   * with tokens, each wrapped in a pair of marks:
   *
   * <ul>
   * <li>{@code GF} / {@code GT} opens a comparison that has a bind parameter and {@code E} closes
   * it. The letter is the SOQL constant the comparison becomes when a parameter is null: such a
   * comparison is UNKNOWN in SQL, which a filter treats as false, or as true below an odd number
   * of NOTs.
   * <li>{@code P<index>:<type>} stands for the value of bind parameter {@code ?<index>}.
   * </ul>
   *
   * <p>String literals containing the mark are not translated, so a mark in a template is always
   * a token delimiter.
   */
  static final char MARK = '\uE000';

  /** Always-false and always-true SOQL conditions; every sObject has a non-null Id. */
  private static final String SOQL_FALSE = "Id = null";
  private static final String SOQL_TRUE = "Id != null";

  private static final DateTimeFormatter SOQL_DATETIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT)
          .withZone(ZoneOffset.UTC);

  private SOQLBuilder() {}

  /**
   * Replaces the bind parameter tokens of a SOQL template with the values of the statement being
   * executed.
   *
   * @param template SOQL whose WHERE clause came from {@link #buildWhereClause}
   * @param root     execution context holding the parameter values
   */
  public static String bind(String template, DataContext root) {
    if (template.indexOf(MARK) < 0) {
      return template;
    }
    final String[] parts = template.split(String.valueOf(MARK), -1);
    final StringBuilder soql = new StringBuilder();
    final StringBuilder comparison = new StringBuilder();
    boolean nullParam = false;
    String ifNull = null;
    // Even parts are SOQL text, odd parts are tokens
    for (int i = 0; i < parts.length; i++) {
      final String part = parts[i];
      if (i % 2 == 0) {
        (ifNull == null ? soql : comparison).append(part);
      } else if (part.equals("GF") || part.equals("GT")) {
        ifNull = part.equals("GT") ? SOQL_TRUE : SOQL_FALSE;
        nullParam = false;
        comparison.setLength(0);
      } else if (part.equals("E")) {
        soql.append(nullParam ? ifNull : comparison);
        ifNull = null;
      } else {
        final int colon = part.indexOf(':');
        final String name = "?" + part.substring(1, colon);
        final Object value = root.get(name);
        if (value == null) {
          nullParam = true;
        } else {
          comparison.append(
              paramLiteral(name, value, SqlTypeName.valueOf(part.substring(colon + 1))));
        }
      }
    }
    return soql.toString();
  }

  /** Renders a bind parameter value, in Calcite's internal representation, as a SOQL literal. */
  private static String paramLiteral(String name, Object value, SqlTypeName typeName) {
    switch (typeName) {
    case VARCHAR:
    case CHAR:
      if (value instanceof String) {
        return quote((String) value);
      }
      break;

    case BOOLEAN:
      if (value instanceof Boolean) {
        return value.toString();
      }
      break;

    case INTEGER:
    case BIGINT:
    case SMALLINT:
    case TINYINT:
    case DECIMAL:
    case DOUBLE:
    case FLOAT:
    case REAL:
      if (value instanceof BigDecimal) {
        return ((BigDecimal) value).toPlainString();
      }
      if (value instanceof Number) {
        return value.toString();
      }
      break;

    case DATE:
      // Days since the epoch
      if (value instanceof Number) {
        return LocalDate.ofEpochDay(((Number) value).longValue()).toString();
      }
      break;

    case TIMESTAMP:
      // Milliseconds since the epoch
      if (value instanceof Number) {
        return SOQL_DATETIME.format(Instant.ofEpochMilli(((Number) value).longValue()));
      }
      break;

    default:
      break;
    }
    throw new IllegalStateException("Bind parameter " + name + " of type " + typeName
        + " has a value of unexpected class " + value.getClass().getName());
  }

  /** Quotes a string as a SOQL literal, escaping backslashes and single quotes. */
  private static String quote(String value) {
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  /**
   * Convert a filter condition to a SOQL WHERE clause. If the condition has bind parameters the
   * result is a template (see {@link #MARK}) to pass through {@link #bind}.
   *
   * @param rexBuilder builder of the condition's cluster
   * @param condition  filter condition
   * @param fieldNames SOQL field name for each input column
   * @throws UnsupportedOperationException if the condition has no SOQL form
   */
  public static String buildWhereClause(RexBuilder rexBuilder, RexNode condition,
      List<String> fieldNames) {
    // Calcite folds IN lists and ranges into SEARCH(field, Sarg); SOQL has neither, so
    // they are spelled out as comparisons first
    return translate(RexUtil.expandSearch(rexBuilder, null, condition),
        new SOQLFilterTranslator(fieldNames));
  }

  /**
   * Translates one expression. {@link RexVisitorImpl} answers null for every node kind the
   * translator does not override (dynamic parameters, field accesses, sub-queries, ...); none of
   * those has a SOQL form.
   */
  private static String translate(RexNode node, SOQLFilterTranslator translator) {
    String soql = node.accept(translator);
    if (soql == null) {
      throw new UnsupportedOperationException(
          "Expression not supported in SOQL: " + node);
    }
    return soql;
  }

  /**
   * Visitor that translates Rex expressions to SOQL.
   */
  private static class SOQLFilterTranslator extends RexVisitorImpl<String> {

    private final List<String> fieldNames;
    /** Whether the expression being translated is below an odd number of NOTs. */
    private boolean negated;

    protected SOQLFilterTranslator(List<String> fieldNames) {
      super(true);
      this.fieldNames = fieldNames;
    }

    @Override public String visitCall(RexCall call) {
      SqlKind kind = call.getKind();

      switch (kind) {
      case EQUALS:
        return comparison(call, " = ");

      case NOT_EQUALS:
        return comparison(call, " != ");

      case GREATER_THAN:
        return comparison(call, " > ");

      case GREATER_THAN_OR_EQUAL:
        return comparison(call, " >= ");

      case LESS_THAN:
        return comparison(call, " < ");

      case LESS_THAN_OR_EQUAL:
        return comparison(call, " <= ");

      case LIKE:
        // SOQL uses LIKE with % wildcards; an ESCAPE operand has no SOQL form
        if (call.getOperands().size() != 2) {
          throw new UnsupportedOperationException("LIKE ... ESCAPE not supported in SOQL");
        }
        return comparison(call, " LIKE ");

      case IN:
        return in(call);

      case NOT:
        negated = !negated;
        try {
          return "NOT (" + translate(call.getOperands().get(0), this) + ")";
        } finally {
          negated = !negated;
        }

      default:
        break;
      }

      List<String> operands = new ArrayList<>();
      for (RexNode operand : call.getOperands()) {
        operands.add(translate(operand, this));
      }

      switch (kind) {
      case AND:
        return "(" + String.join(" AND ", operands) + ")";

      case OR:
        return "(" + String.join(" OR ", operands) + ")";

      case IS_NULL:
        return operands.get(0) + " = null";

      case IS_NOT_NULL:
        return operands.get(0) + " != null";

      default:
        throw new UnsupportedOperationException(
            "Operator not supported in SOQL: " + kind);
      }
    }

    /** Translates a binary comparison, either of whose operands may be a bind parameter. */
    private String comparison(RexCall call, String operator) {
      final RexNode left = call.getOperands().get(0);
      final RexNode right = call.getOperands().get(1);
      final String soql = comparand(left) + operator + comparand(right);
      if (left instanceof RexDynamicParam || right instanceof RexDynamicParam) {
        return guard(soql);
      }
      return soql;
    }

    /**
     * Translates IN. The first operand is the field, the rest are values. With bind parameters it
     * becomes a disjunction, so that a null parameter drops out on its own.
     */
    private String in(RexCall call) {
      final List<RexNode> values = call.getOperands().subList(1, call.getOperands().size());
      final String field = translate(call.getOperands().get(0), this);
      boolean hasParam = false;
      for (RexNode value : values) {
        hasParam |= value instanceof RexDynamicParam;
      }
      final List<String> terms = new ArrayList<>();
      for (RexNode value : values) {
        if (!hasParam) {
          terms.add(translate(value, this));
        } else if (value instanceof RexDynamicParam) {
          terms.add(guard(field + " = " + comparand(value)));
        } else {
          terms.add(field + " = " + translate(value, this));
        }
      }
      if (hasParam) {
        return "(" + String.join(" OR ", terms) + ")";
      }
      return field + " IN (" + String.join(", ", terms) + ")";
    }

    private String comparand(RexNode node) {
      if (!(node instanceof RexDynamicParam)) {
        return translate(node, this);
      }
      final RexDynamicParam param = (RexDynamicParam) node;
      final SqlTypeName typeName = param.getType().getSqlTypeName();
      switch (typeName) {
      case VARCHAR:
      case CHAR:
      case BOOLEAN:
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case TINYINT:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case REAL:
      case DATE:
      case TIMESTAMP:
        return MARK + "P" + param.getIndex() + ":" + typeName + MARK;

      default:
        throw new UnsupportedOperationException(
            "Bind parameter type not supported in SOQL: " + typeName);
      }
    }

    /** Wraps a comparison that has a bind parameter; see {@link SOQLBuilder#MARK}. */
    private String guard(String comparison) {
      return MARK + (negated ? "GT" : "GF") + MARK + comparison + MARK + "E" + MARK;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return fieldNames.get(inputRef.getIndex());
    }

    @Override public String visitLiteral(RexLiteral literal) {
      if (literal.isNull()) {
        return "null";
      }

      SqlTypeName typeName = literal.getType().getSqlTypeName();
      Object value = literal.getValue();

      switch (typeName) {
      case VARCHAR:
      case CHAR:
        String str = literal.getValueAs(String.class);
        if (str.indexOf(MARK) >= 0) {
          throw new UnsupportedOperationException(
              "String literal contains the SOQL template mark");
        }
        return quote(str);

      case BOOLEAN:
        return value.toString();

      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case TINYINT:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
        return value.toString();

      case DATE:
        // Format: YYYY-MM-DD
        Calendar cal = (Calendar) value;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(cal.getTime());

      case TIMESTAMP:
        // Format: YYYY-MM-DDTHH:MM:SS.sssZ
        Calendar tsCal = (Calendar) value;
        SimpleDateFormat tsFormat =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT);
        tsFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return tsFormat.format(tsCal.getTime());

      default:
        throw new UnsupportedOperationException(
            "Literal type not supported in SOQL: " + typeName);
      }
    }
  }
}
