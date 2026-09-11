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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A <code>SqlDialect</code> implementation for the DuckDB database.
 *
 * <p>DuckDB is PostgreSQL-compatible, so we extend PostgresqlSqlDialect to inherit
 * appropriate behaviors like type coercion rules, identifier casing, and SQL syntax.
 */
public class DuckDBSqlDialect extends PostgresqlSqlDialect {
  public static final RelDataTypeSystem TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {

        // We can refer to document of DuckDB 1.2.x:
        // https://duckdb.org/docs/stable/sql/data_types/numeric#fixed-point-decimals
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxPrecision(typeName);
          }
        }

        @Override public int getMaxScale(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxScale(typeName);
          }
        }

        @Override public int getMaxNumericScale() {
          return getMaxScale(SqlTypeName.DECIMAL);
        }
      };

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.DUCKDB)
      .withIdentifierQuoteString("\"")
      // Refer to document: https://duckdb.org/docs/stable/sql/query_syntax/orderby.html
      .withNullCollation(NullCollation.LAST)
      .withDataTypeSystem(TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new DuckDBSqlDialect(DEFAULT_CONTEXT);

  /** Creates a DuckDBSqlDialect. */
  public DuckDBSqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  // Note: supportsImplicitTypeCoercion is inherited from PostgresqlSqlDialect
  // which correctly handles DuckDB's PostgreSQL-compatible type coercion rules

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case CAST:
    case SAFE_CAST:
      // DuckDB requires CAST without extra parentheses around type
      // Handle CAST specially to avoid issues with parentheses.
      //
      // RexBuilder.makeCast(..., safe=true) always rebuilds a safe cast using the
      // SqlLibraryOperators.SAFE_CAST singleton (name "SAFE_CAST"), even when the original
      // SQL text used TRY_CAST (SqlLibraryOperators.TRY_CAST, same SqlKind.SAFE_CAST, different
      // operator instance/name) -- the two collapse to one Rex-level representation, so the
      // dialect cannot tell which keyword the caller wrote and must not rely on
      // call.getOperator().getName(). DuckDB's parser only recognizes the CAST-style
      // "TRY_CAST(expr AS type)" grammar, not "SAFE_CAST(...)" (that name isn't a keyword to
      // DuckDB's parser, so unparsing SqlKind.SAFE_CAST as literal "SAFE_CAST" fails with a
      // parser error at "AS"), so always emit the DuckDB-native keyword here.
      writer.keyword(call.getKind() == SqlKind.SAFE_CAST ? "TRY_CAST" : "CAST");
      writer.print("(");
      call.operand(0).unparse(writer, 0, 0);
      writer.sep("AS");
      // For the type operand, we need to avoid extra parentheses
      // DuckDB doesn't accept CAST(x AS (INTEGER))
      String typeStr = call.operand(1).toString();
      // Remove any outer parentheses from the type specification
      if (typeStr.startsWith("(") && typeStr.endsWith(")")) {
        typeStr = typeStr.substring(1, typeStr.length() - 1);
      }
      writer.print(typeStr);
      writer.print(")");
      break;
    case MAP_VALUE_CONSTRUCTOR:
      writer.keyword(call.getOperator().getName());
      final SqlWriter.Frame mapFrame = writer.startList("{", "}");
      for (int i = 0; i < call.operandCount(); i++) {
        String sep = i % 2 == 0 ? "," : ":";
        writer.sep(sep);
        call.operand(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(mapFrame);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      unparseFloor(writer, call);
      break;
    case DATE_TRUNC:
      // SqlLibraryOperators.DATE_TRUNC (the BigQuery-library operator; this is the only
      // DATE_TRUNC registered in Calcite's operator tables) validates as
      // DATE_TRUNC(<DATE_OR_TIMESTAMP>, <DATETIME_INTERVAL>), with its unit operand
      // converted by OperandHandlers.OPERAND_1_MIGHT_BE_TIME_FRAME from a plain identifier
      // (MONTH, YEAR, ...) into a SqlIntervalQualifier at validation time. But this call
      // reaches unparseCall only after a round trip through RelNode/RexNode for JDBC
      // pushdown (SqlImplementor.toSql), which re-materializes that unit as a plain
      // SqlLiteral -- same representation FLOOR's unit operand uses (see unparseFloor) --
      // wrapping a TimeUnitRange value, not the original SqlIntervalQualifier. Confirmed
      // live 2026-09-11 in two stages: first that operand(1)'s runtime class was
      // org.apache.calcite.sql.SqlLiteral (toString "MONTH"), then that
      // getValueAs(TimeUnit.class) throws ("cannot cast MONTH as class ... TimeUnit") where
      // getValueAs(TimeUnitRange.class) is what the literal actually stores -- TimeUnitRange
      // has its own singular MONTH/YEAR/DAY/... constants distinct from TimeUnit's.
      //
      // Left unhandled (falling to the default branch below), the call unparses exactly as
      // received -- operand order unchanged and the unit rendered as a bare, unquoted
      // identifier ("MONTH") -- which is neither of DuckDB's own accepted DATE_TRUNC forms:
      // DuckDB's binder rejected the unparsed "DATE_TRUNC(CAST(filing_date AS TIMESTAMP(0)),
      // MONTH)" with "Referenced column "MONTH" not found in FROM clause". DuckDB (like
      // PostgreSQL) only accepts the unit FIRST as a quoted string:
      // DATE_TRUNC('month', date_expr).
      if (call.operandCount() == 2) {
        TimeUnitRange unitProbe = null;
        if (call.operand(1) instanceof SqlLiteral) {
          unitProbe = ((SqlLiteral) call.operand(1)).getValueAs(TimeUnitRange.class);
        } else if (call.operand(1) instanceof SqlIntervalQualifier) {
          TimeUnit u = ((SqlIntervalQualifier) call.operand(1)).getStartUnit();
          try {
            unitProbe = TimeUnitRange.valueOf(u.name());
          } catch (IllegalArgumentException ignored) {
            // No singular TimeUnitRange constant shares this TimeUnit's name (e.g. a
            // custom time frame); fall through to the default unparse below.
          }
        }
        if (unitProbe != null) {
          unparseDateTrunc(writer, call, unitProbe);
          break;
        }
      }
      super.unparseCall(writer, call, leftPrec, rightPrec);
      break;
    case CHAR_LENGTH:
      SqlCall lengthCall = SqlLibraryOperators.LENGTH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private static void unparseFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = node.getValueAs(TimeUnitRange.class);

    String format;
    switch (unit) {
    case YEAR:
      format = "year";
      break;
    case QUARTER:
      format = "quarter";
      break;
    case MONTH:
      format = "month";
      break;
    case WEEK:
      format = "week";
      break;
    case DAY:
      format = "day";
      break;
    case HOUR:
      format = "hour";
      break;
    case MINUTE:
      format = "minute";
      break;
    case SECOND:
      format = "second";
      break;
    case MILLISECOND:
      format = "milliseconds";
      break;
    case MICROSECOND:
      format = "microseconds";
      break;
    default:
      throw new AssertionError("DUCKDB does not support FLOOR for time unit: "
          + unit);
    }

    // Refer to document: https://duckdb.org/docs/stable/sql/functions/date#date_truncpart-date
    writer.print("DATETRUNC");
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.print("'" + format + "'");
    writer.sep(",", true);
    call.operand(0).unparse(writer, 0, 0);
    writer.endList(frame);
  }

  /** As {@link #unparseFloor}, for {@code SqlLibraryOperators.DATE_TRUNC} (the BigQuery-
   *  library operator) -- same target DuckDB syntax, but validated in the opposite argument
   *  order (date first, unit second) from what DuckDB's own {@code DATE_TRUNC('unit', date)}
   *  expects, so operand(0) and the caller-supplied unit trade places on the way out. The
   *  unit itself is extracted by the caller (see the {@code DATE_TRUNC} case above) rather
   *  than here, since its SqlNode representation varies by call path (a validated-but-not-
   *  pushed-down call sees a SqlIntervalQualifier; one round-tripped through RelNode/RexNode
   *  for JDBC pushdown sees a SqlLiteral&lt;TimeUnit&gt; instead). */
  private static void unparseDateTrunc(SqlWriter writer, SqlCall call, TimeUnitRange unit) {
    String format;
    switch (unit) {
    case YEAR:
      format = "year";
      break;
    case QUARTER:
      format = "quarter";
      break;
    case MONTH:
      format = "month";
      break;
    case WEEK:
      format = "week";
      break;
    case DAY:
      format = "day";
      break;
    case HOUR:
      format = "hour";
      break;
    case MINUTE:
      format = "minute";
      break;
    case SECOND:
      format = "second";
      break;
    case MILLISECOND:
      format = "milliseconds";
      break;
    case MICROSECOND:
      format = "microseconds";
      break;
    default:
      throw new AssertionError("DUCKDB does not support DATE_TRUNC for time unit: "
          + unit);
    }

    // Refer to document: https://duckdb.org/docs/stable/sql/functions/date#date_truncpart-date
    writer.print("DATETRUNC");
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.print("'" + format + "'");
    writer.sep(",", true);
    call.operand(0).unparse(writer, 0, 0);
    writer.endList(frame);
  }

}
