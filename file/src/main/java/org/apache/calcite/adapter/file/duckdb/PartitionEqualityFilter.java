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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Recognises the one predicate shape a manifest-only {@code COUNT(*)} can answer: a conjunction of
 * equality and {@code IN} tests, each against a column and constants.
 *
 * <p>Two producers of that shape are handled — a planner {@link org.apache.calcite.rex.RexNode}
 * from an explicit {@code WHERE}, and the {@code WHERE} of a DuckDB SQL view's defining query — and
 * both reduce to the same column-to-accepted-values map. Nothing here decides whether those columns
 * are <em>partition</em> columns; that judgement belongs to Iceberg and is made in
 * {@link org.apache.calcite.adapter.file.iceberg.IcebergPartitionRowCount}, which refuses any
 * predicate its manifests cannot settle. This class only rejects shapes that could never be settled
 * by a partition tuple — ranges, {@code LIKE}, {@code IS NULL}, negation, column-to-column
 * comparisons, subqueries — so the manifest read is not attempted pointlessly.
 *
 * <p>Every method returns null rather than throwing when the shape does not match. A predicate this
 * class cannot describe is the ordinary case, not an error.
 */
final class PartitionEqualityFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionEqualityFilter.class);

  private PartitionEqualityFilter() {
  }

  /** A SQL view reduced to the base table it reads and the constants it pins. */
  static final class ViewDefinition {
    /** Unqualified name of the single table the view selects from. */
    final String baseTableName;
    /** Column to accepted values, from the view's own {@code WHERE}. */
    final Map<String, List<Object>> acceptedValues;

    ViewDefinition(String baseTableName, Map<String, List<Object>> acceptedValues) {
      this.baseTableName = baseTableName;
      this.acceptedValues = acceptedValues;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Planner predicates
  // ---------------------------------------------------------------------------------------------

  /**
   * Reduces a Filter's condition to column-to-accepted-values, or null when it is not a pure
   * conjunction of equality/{@code IN} tests against constants.
   *
   * @param condition the Filter condition
   * @param fieldNames field names of the Filter's input, indexed as its {@code RexInputRef}s are
   * @param rexBuilder used to expand {@code SEARCH}/{@code Sarg} back into {@code OR} of equalities
   */
  static Map<String, List<Object>> fromRex(RexNode condition, List<String> fieldNames,
      RexBuilder rexBuilder) {
    // Calcite folds `IN (a, b)` and equality runs into a single SEARCH over a Sarg. Expanding it
    // first means only OR/= have to be recognised below.
    RexNode expanded = RexUtil.expandSearch(rexBuilder, null, condition);
    Map<String, List<Object>> accepted = new LinkedHashMap<>();
    return collectRex(expanded, fieldNames, accepted) ? accepted : null;
  }

  private static boolean collectRex(RexNode node, List<String> fieldNames,
      Map<String, List<Object>> accepted) {
    if (node.getKind() == SqlKind.AND) {
      for (RexNode operand : ((RexCall) node).getOperands()) {
        if (!collectRex(operand, fieldNames, accepted)) {
          return false;
        }
      }
      return true;
    }
    if (node.getKind() == SqlKind.EQUALS) {
      RexCall call = (RexCall) node;
      String column = columnOf(call.getOperands().get(0), call.getOperands().get(1), fieldNames);
      Object value = literalOf(call.getOperands().get(0), call.getOperands().get(1));
      if (column == null || value == null) {
        return false;
      }
      return add(accepted, column, java.util.Collections.singletonList(value));
    }
    if (node.getKind() == SqlKind.OR) {
      // An OR is usable only when every branch pins the same column: that is an IN list.
      String column = null;
      List<Object> values = new ArrayList<>();
      for (RexNode operand : ((RexCall) node).getOperands()) {
        if (operand.getKind() != SqlKind.EQUALS) {
          return false;
        }
        RexCall call = (RexCall) operand;
        String branchColumn =
            columnOf(call.getOperands().get(0), call.getOperands().get(1), fieldNames);
        Object value = literalOf(call.getOperands().get(0), call.getOperands().get(1));
        if (branchColumn == null || value == null) {
          return false;
        }
        if (column == null) {
          column = branchColumn;
        } else if (!column.equals(branchColumn)) {
          return false;
        }
        values.add(value);
      }
      return column != null && add(accepted, column, values);
    }
    LOGGER.debug("Predicate term {} is not an equality or IN test", node);
    return false;
  }

  /** The column named by whichever side of a comparison is a plain input reference. */
  private static String columnOf(RexNode left, RexNode right, List<String> fieldNames) {
    RexInputRef ref = left instanceof RexInputRef ? (RexInputRef) left
        : right instanceof RexInputRef ? (RexInputRef) right : null;
    if (ref == null || ref.getIndex() >= fieldNames.size()) {
      return null;
    }
    // Both sides being refs (a column-to-column comparison) is caught by literalOf returning null.
    return fieldNames.get(ref.getIndex());
  }

  /** The constant named by whichever side of a comparison is a literal. */
  private static Object literalOf(RexNode left, RexNode right) {
    RexLiteral literal = left instanceof RexLiteral ? (RexLiteral) left
        : right instanceof RexLiteral ? (RexLiteral) right : null;
    if (literal == null || literal.isNull()) {
      return null;
    }
    SqlTypeName type = literal.getType().getSqlTypeName();
    if (SqlTypeName.CHAR_TYPES.contains(type)) {
      return literal.getValueAs(String.class);
    }
    if (SqlTypeName.INT_TYPES.contains(type)) {
      return literal.getValueAs(Long.class);
    }
    if (type == SqlTypeName.BOOLEAN) {
      return literal.getValueAs(Boolean.class);
    }
    if (type == SqlTypeName.DECIMAL) {
      return literal.getValueAs(BigDecimal.class);
    }
    // Dates, times, intervals and the rest: an Iceberg partition literal for them would have to
    // agree with Calcite's internal encoding, which is not worth guessing at for a count.
    LOGGER.debug("Literal type {} is not carried into an Iceberg predicate", type);
    return null;
  }

  // ---------------------------------------------------------------------------------------------
  // View definitions
  // ---------------------------------------------------------------------------------------------

  /**
   * Reduces a DuckDB SQL view's defining query to its base table and pinned constants, or null when
   * the view is anything more than {@code SELECT <plain columns> FROM <one table> WHERE <equalities>}.
   *
   * <p>The select list must be {@code *} or bare column identifiers with no aliases. That keeps the
   * view's column names identical to the base table's, which is what lets a caller's own
   * {@code WHERE} — written against the view — name base-table columns directly.
   */
  static ViewDefinition fromViewSql(String viewSql) {
    SqlNode parsed;
    try {
      // Babel's parser, not core's. Core's reserves `year` and `month`, which are ordinary
      // partition columns across govdata's lake, so it cannot read the very views this exists for.
      SqlParser.Config config = SqlParser.config()
          .withLex(Lex.ORACLE)
          .withParserFactory(org.apache.calcite.sql.parser.babel.SqlBabelParserImpl.FACTORY)
          .withUnquotedCasing(Casing.TO_LOWER)
          .withCaseSensitive(false);
      parsed = SqlParser.create(viewSql, config).parseQuery();
    } catch (org.apache.calcite.sql.parser.SqlParseException e) {
      LOGGER.debug("View SQL is not parseable by Calcite, declining: {}", e.getMessage());
      return null;
    }
    if (!(parsed instanceof SqlSelect)) {
      // ORDER BY (SqlOrderBy), UNION, VALUES, WITH ... — none is a plain filtered projection.
      return null;
    }
    SqlSelect select = (SqlSelect) parsed;
    if (select.isDistinct()
        || select.getGroup() != null
        || select.getHaving() != null
        || select.getFetch() != null
        || select.getOffset() != null
        || (select.getWindowList() != null && !select.getWindowList().isEmpty())) {
      return null;
    }
    if (!isPlainColumnList(select.getSelectList())) {
      return null;
    }
    if (!(select.getFrom() instanceof SqlIdentifier)) {
      // A join, a subquery or an aliased table reference: more than one row source, or a name
      // mapping this class does not follow.
      return null;
    }
    SqlIdentifier from = (SqlIdentifier) select.getFrom();
    String baseTableName = from.names.get(from.names.size() - 1).toLowerCase(Locale.ROOT);
    if (select.getWhere() == null) {
      return new ViewDefinition(baseTableName, new LinkedHashMap<>());
    }
    Map<String, List<Object>> accepted = new LinkedHashMap<>();
    if (!collectSql(select.getWhere(), accepted)) {
      return null;
    }
    return new ViewDefinition(baseTableName, accepted);
  }

  private static boolean isPlainColumnList(SqlNodeList selectList) {
    if (selectList == null) {
      return false;
    }
    for (SqlNode item : selectList) {
      if (!(item instanceof SqlIdentifier)) {
        // An expression or an AS alias: the view's column names would stop matching the base
        // table's, so a caller's predicate could no longer be read as base-table columns.
        return false;
      }
    }
    return true;
  }

  private static boolean collectSql(SqlNode node, Map<String, List<Object>> accepted) {
    if (node.getKind() == SqlKind.AND) {
      List<SqlNode> operands = ((SqlBasicCall) node).getOperandList();
      for (SqlNode operand : operands) {
        if (!collectSql(operand, accepted)) {
          return false;
        }
      }
      return true;
    }
    if (node.getKind() == SqlKind.EQUALS) {
      List<SqlNode> operands = ((SqlBasicCall) node).getOperandList();
      String column = sqlColumnOf(operands.get(0), operands.get(1));
      Object value = sqlLiteralOf(operands.get(0), operands.get(1));
      if (column == null || value == null) {
        return false;
      }
      return add(accepted, column, java.util.Collections.singletonList(value));
    }
    if (node.getKind() == SqlKind.IN) {
      List<SqlNode> operands = ((SqlBasicCall) node).getOperandList();
      if (!(operands.get(0) instanceof SqlIdentifier) || !(operands.get(1) instanceof SqlNodeList)) {
        return false;
      }
      String column = simpleColumnName((SqlIdentifier) operands.get(0));
      if (column == null) {
        return false;
      }
      List<Object> values = new ArrayList<>();
      for (SqlNode item : (SqlNodeList) operands.get(1)) {
        Object value = sqlLiteralValue(item);
        if (value == null) {
          return false;
        }
        values.add(value);
      }
      return !values.isEmpty() && add(accepted, column, values);
    }
    LOGGER.debug("View WHERE term {} is not an equality or IN test", node);
    return false;
  }

  private static String sqlColumnOf(SqlNode left, SqlNode right) {
    SqlIdentifier id = left instanceof SqlIdentifier ? (SqlIdentifier) left
        : right instanceof SqlIdentifier ? (SqlIdentifier) right : null;
    return id == null ? null : simpleColumnName(id);
  }

  private static Object sqlLiteralOf(SqlNode left, SqlNode right) {
    Object value = sqlLiteralValue(left);
    return value != null ? value : sqlLiteralValue(right);
  }

  private static Object sqlLiteralValue(SqlNode node) {
    if (!(node instanceof SqlLiteral)) {
      return null;
    }
    SqlLiteral literal = (SqlLiteral) node;
    if (literal.getTypeName() == SqlTypeName.NULL) {
      return null;
    }
    if (SqlTypeName.CHAR_TYPES.contains(literal.getTypeName())) {
      return literal.getValueAs(String.class);
    }
    if (literal instanceof SqlNumericLiteral) {
      SqlNumericLiteral numeric = (SqlNumericLiteral) literal;
      BigDecimal decimal = numeric.getValueAs(BigDecimal.class);
      return numeric.isInteger() ? (Object) decimal.longValueExact() : (Object) decimal;
    }
    if (literal.getTypeName() == SqlTypeName.BOOLEAN) {
      return literal.getValueAs(Boolean.class);
    }
    return null;
  }

  /** The column name of an unqualified or table-qualified identifier; null for {@code *}. */
  private static String simpleColumnName(SqlIdentifier identifier) {
    if (identifier.isStar()) {
      return null;
    }
    return identifier.names.get(identifier.names.size() - 1).toLowerCase(Locale.ROOT);
  }

  /**
   * Records accepted values for one column, refusing a second mention of it.
   *
   * <p>Two tests on one column ({@code a = 1 AND a = 2}, or {@code a = 1 AND a IN (1, 2)}) would
   * have to be intersected. Refusing costs only the fast path on a predicate nobody writes, where
   * getting the intersection subtly wrong would cost a wrong count.
   */
  private static boolean add(Map<String, List<Object>> accepted, String column,
      List<Object> values) {
    if (accepted.containsKey(column)) {
      LOGGER.debug("Column {} is constrained twice; declining rather than intersecting", column);
      return false;
    }
    accepted.put(column, values);
    return true;
  }
}
