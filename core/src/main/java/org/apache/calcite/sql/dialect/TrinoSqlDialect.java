/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.RelToSqlConverterUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/**
 * A <code>SqlDialect</code> implementation for the Trino database.
 *
 * <p>Where Trino computes a different answer than Calcite for the same
 * expression, the dialect either renders an equivalent Trino expression or
 * declares the call unsupported, so that the JDBC adapter evaluates it in
 * Calcite:
 *
 * <ul>
 * <li>{@code SUBSTRING} with a start below 1: Trino returns an empty string
 *     for start 0 and counts a negative start from the end;
 * <li>{@code EXTRACT(DOW ...)}: Trino's day of week is ISO (Monday = 1),
 *     Calcite's is Sunday = 1; Trino has no {@code ISODOW}, {@code ISOYEAR},
 *     {@code MILLISECOND}, {@code MICROSECOND}, {@code NANOSECOND},
 *     {@code EPOCH}, {@code DECADE}, {@code CENTURY} or {@code MILLENNIUM}
 *     unit;
 * <li>{@code FLOOR(x TO WEEK)}: Trino's week starts on Monday, Calcite's on
 *     Sunday;
 * <li>{@code CAST} of a DECIMAL or floating-point value to an integer type:
 *     Trino rounds, Calcite truncates;
 * <li>{@code SOME}: Trino's name is {@code BOOL_OR};
 * <li>{@code CURRENT_TIMESTAMP} and the other current-time functions: Trino
 *     evaluates them in the client JVM's time zone, Calcite in the
 *     connection's, so they stay in Calcite.
 * </ul>
 */
public class TrinoSqlDialect extends PrestoSqlDialect {
  public static final Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.TRINO)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.UNCHANGED)
      .withNullCollation(NullCollation.LAST)
      .withDataTypeSystem(TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new TrinoSqlDialect(DEFAULT_CONTEXT);

  /** Scalar functions and operators, beyond those matched by kind in
   * {@link #supportsFunction}, that Trino runs with Calcite's answer. */
  private static final Set<SqlOperator> SUPPORTED_OPERATORS =
      ImmutableSet.<SqlOperator>builder()
          .add(SqlStdOperatorTable.ABS)
          .add(SqlStdOperatorTable.ACOS)
          .add(SqlStdOperatorTable.ASIN)
          .add(SqlStdOperatorTable.ATAN)
          .add(SqlStdOperatorTable.ATAN2)
          .add(SqlStdOperatorTable.CBRT)
          .add(SqlStdOperatorTable.CHAR_LENGTH)
          .add(SqlStdOperatorTable.CHARACTER_LENGTH)
          .add(SqlStdOperatorTable.CONCAT)
          .add(SqlStdOperatorTable.COS)
          .add(SqlStdOperatorTable.DEGREES)
          .add(SqlStdOperatorTable.EXP)
          .add(SqlStdOperatorTable.LN)
          .add(SqlStdOperatorTable.LOG10)
          .add(SqlStdOperatorTable.LOWER)
          .add(SqlStdOperatorTable.NOT_LIKE)
          .add(SqlStdOperatorTable.PI)
          .add(SqlStdOperatorTable.POWER)
          .add(SqlStdOperatorTable.RADIANS)
          .add(SqlStdOperatorTable.REPLACE)
          .add(SqlStdOperatorTable.ROUND)
          .add(SqlStdOperatorTable.SIGN)
          .add(SqlStdOperatorTable.SIN)
          .add(SqlStdOperatorTable.SQRT)
          .add(SqlStdOperatorTable.SUBSTRING)
          .add(SqlStdOperatorTable.TAN)
          .add(SqlStdOperatorTable.TRUNCATE)
          .add(SqlStdOperatorTable.UPPER)
          .add(SqlLibraryOperators.CHR)
          .add(SqlLibraryOperators.COSH)
          .add(SqlLibraryOperators.STRPOS)
          .add(SqlLibraryOperators.TANH)
          .build();

  private static final SqlOperator DAY_OF_WEEK = trinoFunction("DAY_OF_WEEK");
  private static final SqlOperator YEAR_OF_WEEK = trinoFunction("YEAR_OF_WEEK");
  private static final SqlOperator YEAR = trinoFunction("YEAR");
  private static final SqlOperator BOOL_OR = trinoFunction("BOOL_OR");
  private static final SqlOperator SECOND = trinoFunction("SECOND");
  private static final SqlOperator MILLISECOND = trinoFunction("MILLISECOND");
  private static final SqlOperator DATE_DIFF = trinoFunction("DATE_DIFF");
  private static final SqlOperator DATE_TRUNC = trinoFunction("DATE_TRUNC");

  /**
   * Creates a TrinoSqlDialect.
   */
  public TrinoSqlDialect(Context context) {
    super(context);
  }

  private static SqlOperator trinoFunction(String name) {
    return SqlBasicFunction.create(name, ReturnTypes.ARG0, OperandTypes.ANY);
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingAnsi(writer, offset, fetch);
  }

  /** {@inheritDoc}
   *
   * <p>Trino has no SEMI, ANTI or ASOF join syntax. */
  @Override public boolean supportsJoinType(JoinRelType joinType) {
    switch (joinType) {
    case INNER:
    case LEFT:
    case RIGHT:
    case FULL:
      return true;
    default:
      return false;
    }
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case ANY_VALUE:
    case AVG:
    case COUNT:
    case LITERAL_AGG:
    case MAX:
    case MIN:
    case SUM:
    case SUM0:
    case STDDEV_POP:
    case STDDEV_SAMP:
    case VAR_POP:
    case VAR_SAMP:
      return true;
    default:
      return super.supportsAggregateFunction(kind);
    }
  }

  @Override public boolean supportsFunction(SqlOperator operator,
      RelDataType type, List<RelDataType> paramTypes) {
    switch (operator.kind) {
    case AND:
    case BETWEEN:
    case CASE:
    case CAST:
    case COALESCE:
    case DIVIDE:
    case EQUALS:
    case EXTRACT:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case IN:
    case IS_DISTINCT_FROM:
    case IS_FALSE:
    case IS_NOT_DISTINCT_FROM:
    case IS_NOT_FALSE:
    case IS_NOT_NULL:
    case IS_NOT_TRUE:
    case IS_NULL:
    case IS_TRUE:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case LIKE:
    case MINUS:
    case MINUS_PREFIX:
    case MOD:
    case NOT:
    case NOT_EQUALS:
    case NOT_IN:
    case NULLIF:
    case OR:
    case PLUS:
    case PLUS_PREFIX:
    case POSITION:
    case ROW:
    case SEARCH:
    case TIMES:
    case TRIM:
    case CHECKED_PLUS:
    case CHECKED_MINUS:
    case CHECKED_TIMES:
    case CHECKED_DIVIDE:
      return true;
    case FLOOR:
    case CEIL:
      // FLOOR(datetime TO unit) is rendered as DATE_TRUNC; CEIL of a
      // datetime has no Trino form.
      return operator.kind == SqlKind.FLOOR
          || paramTypes.isEmpty()
          || !SqlTypeFamily.DATETIME.contains(paramTypes.get(0));
    default:
      break;
    }
    if (operator == SqlLibraryOperators.LPAD
        || operator == SqlLibraryOperators.RPAD) {
      // Trino's LPAD and RPAD require the padding string.
      return paramTypes.size() == 3;
    }
    if (operator == SqlLibraryOperators.REGEXP_LIKE) {
      // Trino's REGEXP_LIKE has no match-parameter argument.
      return paramTypes.size() == 2;
    }
    return SUPPORTED_OPERATORS.contains(operator);
  }

  /** {@inheritDoc}
   *
   * <p>Trino's MAP and ROW types take no {@code NULL} or {@code NOT NULL} on
   * their components, so the nullability Calcite writes there is dropped. */
  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    final SqlNode spec = super.getCastSpec(type);
    if (spec instanceof SqlDataTypeSpec) {
      return withoutNullability((SqlDataTypeSpec) spec);
    }
    return spec;
  }

  private static SqlDataTypeSpec withoutNullability(SqlDataTypeSpec spec) {
    final SqlParserPos pos = spec.getParserPosition();
    final SqlTypeNameSpec name = spec.getTypeNameSpec();
    if (name instanceof SqlMapTypeNameSpec) {
      final SqlMapTypeNameSpec map = (SqlMapTypeNameSpec) name;
      return new SqlDataTypeSpec(
          new SqlMapTypeNameSpec(withoutNullability(map.getKeyType()),
              withoutNullability(map.getValType()), map.getParserPos()),
          spec.getTimeZone(), pos);
    }
    if (name instanceof SqlRowTypeNameSpec) {
      final SqlRowTypeNameSpec row = (SqlRowTypeNameSpec) name;
      return new SqlDataTypeSpec(
          new SqlRowTypeNameSpec(row.getParserPos(), row.getFieldNames(),
              Util.transform(row.getFieldTypes(),
                  TrinoSqlDialect::withoutNullability)),
          spec.getTimeZone(), pos);
    }
    return new SqlDataTypeSpec(name, spec.getTimeZone(), pos);
  }

  /** {@inheritDoc}
   *
   * <p>Trino rounds when it casts a DECIMAL or floating-point value to an
   * integer type, Calcite truncates; the cast is rendered over
   * {@code TRUNCATE}. */
  @Override public RexNode prepareUnparse(RexNode rexNode) {
    final RexNode node = super.prepareUnparse(rexNode);
    if (node.getKind() != SqlKind.CAST) {
      return node;
    }
    final RexCall cast = (RexCall) node;
    final RexNode operand = cast.getOperands().get(0);
    if (!isIntegerType(cast.getType().getSqlTypeName())
        || !hasFraction(operand.getType())) {
      return node;
    }
    final RexNode truncated =
        RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.TRUNCATE, operand);
    return cast.clone(cast.getType(), ImmutableList.of(truncated));
  }

  private static boolean isIntegerType(SqlTypeName typeName) {
    switch (typeName) {
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return true;
    default:
      return false;
    }
  }

  private static boolean hasFraction(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case DECIMAL:
      return type.getScale() > 0;
    case DOUBLE:
    case FLOAT:
    case REAL:
      return true;
    default:
      return false;
    }
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      RelToSqlConverterUtil.specialOperatorByName("SUBSTRING")
          .unparse(writer, substringFromOne(call), 0, 0);
    } else if (call.getOperator() == SqlStdOperatorTable.SOME) {
      BOOL_OR.createCall(SqlParserPos.ZERO, call.getOperandList())
          .unparse(writer, leftPrec, rightPrec);
    } else if (call.getKind() == SqlKind.EXTRACT) {
      unparseExtract(writer, call, leftPrec, rightPrec);
    } else if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2
        && timeUnit(call.operand(1)) == TimeUnitRange.WEEK) {
      unparseFloorToWeek(writer, call);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /** Rewrites {@code SUBSTRING(s FROM start [FOR length])} so that a start
   * below 1 behaves as in standard SQL: the characters before position 1
   * count against the length. A start that is a literal of at least 1 is
   * kept as is. */
  private static SqlCall substringFromOne(SqlCall call) {
    final SqlNode start = call.operand(1);
    if (start instanceof SqlNumericLiteral
        && ((SqlNumericLiteral) start).intValue(true) >= 1) {
      return call;
    }
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNode one = SqlLiteral.createExactNumeric("1", pos);
    final SqlNode clampedStart =
        SqlLibraryOperators.GREATEST.createCall(pos, start, one);
    if (call.operandCount() == 2) {
      return SqlStdOperatorTable.SUBSTRING.createCall(pos, call.operand(0),
          clampedStart);
    }
    // length' = max(start + length - max(start, 1), 0)
    final SqlNode end =
        SqlStdOperatorTable.PLUS.createCall(pos, start, call.operand(2));
    final SqlNode length =
        SqlLibraryOperators.GREATEST.createCall(pos,
            SqlStdOperatorTable.MINUS.createCall(pos, end, clampedStart),
            SqlLiteral.createExactNumeric("0", pos));
    return SqlStdOperatorTable.SUBSTRING.createCall(pos, call.operand(0),
        clampedStart, length);
  }

  /** Renders {@code FLOOR(x TO WEEK)}. Calcite's week starts on Sunday,
   * Trino's {@code DATE_TRUNC('WEEK', ...)} on Monday, so the value is moved
   * forward a day, truncated, and moved back:
   * {@code DATE_TRUNC('WEEK', x + INTERVAL '1' DAY) - INTERVAL '1' DAY}. */
  private static void unparseFloorToWeek(SqlWriter writer, SqlCall call) {
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNode oneDay =
        SqlLiteral.createInterval(1, "1",
            new SqlIntervalQualifier(TimeUnit.DAY, null, pos), pos);
    final SqlNode node =
        SqlStdOperatorTable.MINUS.createCall(pos,
            DATE_TRUNC.createCall(pos,
                SqlLiteral.createCharString("WEEK", pos),
                SqlStdOperatorTable.PLUS.createCall(pos, call.operand(0),
                    oneDay)),
            oneDay);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    node.unparse(writer, 0, 0);
    writer.endList(frame);
  }

  private void unparseExtract(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNode arg = call.operand(1);
    final SqlNode node;
    switch (timeUnit(call.operand(0))) {
    case DOW:
      // Calcite: Sunday = 1 .. Saturday = 7; Trino: Monday = 1 .. Sunday = 7
      node =
          SqlStdOperatorTable.PLUS.createCall(pos,
              SqlStdOperatorTable.MOD.createCall(pos,
                  DAY_OF_WEEK.createCall(pos, arg),
                  SqlLiteral.createExactNumeric("7", pos)),
              SqlLiteral.createExactNumeric("1", pos));
      break;
    case ISODOW:
      node = DAY_OF_WEEK.createCall(pos, arg);
      break;
    case MILLISECOND:
      // Calcite: the seconds field in milliseconds, 30.123 s -> 30123
      node = secondsFieldTimes(arg, 1L);
      break;
    case MICROSECOND:
      node = secondsFieldTimes(arg, 1_000L);
      break;
    case NANOSECOND:
      node = secondsFieldTimes(arg, 1_000_000L);
      break;
    case EPOCH:
      // Whole seconds since 1970-01-01 00:00:00, with no time zone applied
      node =
          DATE_DIFF.createCall(pos, SqlLiteral.createCharString("second", pos),
              SqlLiteral.createTimestamp(SqlTypeName.TIMESTAMP,
                  new TimestampString(1970, 1, 1, 0, 0, 0), 0, pos),
              SqlStdOperatorTable.CAST.createCall(pos, arg,
                  new SqlDataTypeSpec(
                      new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, pos),
                      pos)));
      break;
    case ISOYEAR:
      node = YEAR_OF_WEEK.createCall(pos, arg);
      break;
    case DECADE:
      node =
          SqlStdOperatorTable.DIVIDE.createCall(pos, YEAR.createCall(pos, arg),
              SqlLiteral.createExactNumeric("10", pos));
      break;
    case CENTURY:
      node = yearsDividedRoundingAway(arg, 100);
      break;
    case MILLENNIUM:
      node = yearsDividedRoundingAway(arg, 1000);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
      return;
    }
    final SqlWriter.Frame frame = writer.startList("(", ")");
    node.unparse(writer, 0, 0);
    writer.endList(frame);
  }

  /** Returns {@code (SECOND(arg) * 1000 + MILLISECOND(arg)) * factor}. */
  private static SqlNode secondsFieldTimes(SqlNode arg, long factor) {
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNode millis =
        SqlStdOperatorTable.PLUS.createCall(pos,
            SqlStdOperatorTable.MULTIPLY.createCall(pos,
                SECOND.createCall(pos, arg),
                SqlLiteral.createExactNumeric("1000", pos)),
            MILLISECOND.createCall(pos, arg));
    if (factor == 1L) {
      return millis;
    }
    return SqlStdOperatorTable.MULTIPLY.createCall(pos, millis,
        SqlLiteral.createExactNumeric(Long.toString(factor), pos));
  }

  /** Returns {@code CASE WHEN YEAR(arg) > 0 THEN (YEAR(arg) + n - 1) / n
   * ELSE (YEAR(arg) - n + 1) / n END}, Calcite's CENTURY (n = 100) and
   * MILLENNIUM (n = 1000). */
  private static SqlNode yearsDividedRoundingAway(SqlNode arg, int n) {
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNode year = YEAR.createCall(pos, arg);
    final SqlNode nLiteral =
        SqlLiteral.createExactNumeric(Integer.toString(n), pos);
    final SqlNode nMinusOne =
        SqlLiteral.createExactNumeric(Integer.toString(n - 1), pos);
    final SqlNode positive =
        SqlStdOperatorTable.DIVIDE.createCall(pos,
            SqlStdOperatorTable.PLUS.createCall(pos, year, nMinusOne),
            nLiteral);
    final SqlNode negative =
        SqlStdOperatorTable.DIVIDE.createCall(pos,
            SqlStdOperatorTable.MINUS.createCall(pos, year, nMinusOne),
            nLiteral);
    return SqlStdOperatorTable.CASE.createCall(null, pos, null,
        SqlNodeList.of(
            SqlStdOperatorTable.GREATER_THAN.createCall(pos, year,
                SqlLiteral.createExactNumeric("0", pos))),
        SqlNodeList.of(positive), negative);
  }

  private static TimeUnitRange timeUnit(SqlNode node) {
    if (node instanceof SqlIntervalQualifier) {
      return ((SqlIntervalQualifier) node).timeUnitRange;
    }
    return ((SqlLiteral) node).getValueAs(TimeUnitRange.class);
  }
}
