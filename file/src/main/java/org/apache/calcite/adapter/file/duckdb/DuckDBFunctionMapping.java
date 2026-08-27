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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps Calcite SQL functions to their DuckDB equivalents.
 * This ensures proper function pushdown and SQL generation.
 */
public class DuckDBFunctionMapping {

  // Map of Calcite function names to DuckDB function names
  private static final Map<String, String> FUNCTION_MAP = new HashMap<>();

  static {
    // String functions
    FUNCTION_MAP.put("CHAR_LENGTH", "LENGTH");
    FUNCTION_MAP.put("CHARACTER_LENGTH", "LENGTH");
    FUNCTION_MAP.put("SUBSTRING", "SUBSTR");
    FUNCTION_MAP.put("LOCATE", "INSTR");

    // Date/Time functions
    FUNCTION_MAP.put("CURRENT_TIMESTAMP", "NOW");
    FUNCTION_MAP.put("CURRENT_DATE", "TODAY");
    FUNCTION_MAP.put("DAYOFWEEK", "DAYOFWEEK");
    FUNCTION_MAP.put("DAYOFMONTH", "DAY");
    FUNCTION_MAP.put("DAYOFYEAR", "DAYOFYEAR");
    FUNCTION_MAP.put("YEAR", "YEAR");
    FUNCTION_MAP.put("MONTH", "MONTH");
    FUNCTION_MAP.put("DAY", "DAY");
    FUNCTION_MAP.put("HOUR", "HOUR");
    FUNCTION_MAP.put("MINUTE", "MINUTE");
    FUNCTION_MAP.put("SECOND", "SECOND");

    // Math functions
    FUNCTION_MAP.put("TRUNCATE", "TRUNC");
    FUNCTION_MAP.put("LOG10", "LOG10");
    FUNCTION_MAP.put("LOG", "LN");
    FUNCTION_MAP.put("POWER", "POW");

    // Aggregate functions
    FUNCTION_MAP.put("STDDEV", "STDDEV_SAMP");
    FUNCTION_MAP.put("VARIANCE", "VAR_SAMP");
    FUNCTION_MAP.put("GROUP_CONCAT", "STRING_AGG");
    FUNCTION_MAP.put("LISTAGG", "STRING_AGG");

    // Reserved regression aggregates: their real names are reserved Calcite parser
    // keywords, so they are registered/parsed under non-reserved AGG_* aliases (via a
    // pre-parse rewrite) and mapped back to the native DuckDB name here on unparse.
    FUNCTION_MAP.put("AGG_CORR", "corr");
    FUNCTION_MAP.put("AGG_REGR_SLOPE", "regr_slope");
    FUNCTION_MAP.put("AGG_REGR_INTERCEPT", "regr_intercept");
    FUNCTION_MAP.put("AGG_REGR_R2", "regr_r2");
    FUNCTION_MAP.put("AGG_REGR_AVGX", "regr_avgx");
    FUNCTION_MAP.put("AGG_REGR_AVGY", "regr_avgy");
    FUNCTION_MAP.put("AGG_REGR_SXY", "regr_sxy");

    // Window functions
    FUNCTION_MAP.put("RANK", "RANK");
    FUNCTION_MAP.put("DENSE_RANK", "DENSE_RANK");
    FUNCTION_MAP.put("ROW_NUMBER", "ROW_NUMBER");
    FUNCTION_MAP.put("NTILE", "NTILE");
    FUNCTION_MAP.put("PERCENT_RANK", "PERCENT_RANK");
    FUNCTION_MAP.put("CUME_DIST", "CUME_DIST");

    // DuckDB-specific functions for files
    FUNCTION_MAP.put("READ_CSV", "read_csv_auto");
    FUNCTION_MAP.put("READ_PARQUET", "read_parquet");
    FUNCTION_MAP.put("READ_JSON", "read_json_auto");

    // JSON functions: DuckDB has no ANSI JSON_VALUE syntax, but does support
    // json_extract(json, path) for simple two-argument extraction. JSON_EXTRACT is the
    // file adapter's own declaration (see DuckDBJsonFunctions) registered under DuckDB's
    // native spelling; JSON_VALUE is Calcite's real ANSI operator, remapped here only for
    // the plain two-operand form (see the JSON_VALUE case in unparseCall below) — a
    // RETURNING/ON ERROR/ON EMPTY clause has no json_extract equivalent and is left to
    // unparse in ANSI form, so pushdown fails loudly at DuckDB instead of mistranslating.
    FUNCTION_MAP.put("JSON_EXTRACT", "json_extract");
    FUNCTION_MAP.put("JSON_VALUE", "json_extract");

    // STRING_SPLIT is the file adapter's own declaration (see DuckDBStringFunctions)
    // registered under DuckDB's native spelling.
    FUNCTION_MAP.put("STRING_SPLIT", "string_split");

    // Vector similarity functions - map to DuckDB's native array functions
    FUNCTION_MAP.put("COSINE_SIMILARITY", "array_cosine_similarity");
    FUNCTION_MAP.put("COSINE_DISTANCE", "array_cosine_distance");
  }

  /**
   * Gets the DuckDB equivalent of a Calcite function.
   */
  public static String getDuckDBFunction(String calciteFunction) {
    String upperFunc = calciteFunction.toUpperCase();
    return FUNCTION_MAP.getOrDefault(upperFunc, calciteFunction);
  }

  /**
   * Checks if a function needs special handling in DuckDB.
   */
  public static boolean needsSpecialHandling(SqlOperator operator) {
    String name = operator.getName().toUpperCase();
    return FUNCTION_MAP.containsKey(name);
  }

  /**
   * Writes a function call with DuckDB-specific syntax.
   */
  public static void unparseCall(SqlWriter writer, SqlCall call,
                                int leftPrec, int rightPrec) {
    SqlOperator operator = call.getOperator();

    // Special handling for CAST to avoid parentheses issues in DuckDB
    if (operator.getKind() == SqlKind.CAST) {
      // Don't handle CAST here - let the base dialect handle it
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      return;
    }

    String funcName = operator.getName().toUpperCase();

    if (FUNCTION_MAP.containsKey(funcName)) {
      String duckdbFunc = FUNCTION_MAP.get(funcName);

      // Special handling for specific functions
      switch (funcName) {
      case "GROUP_CONCAT":
      case "LISTAGG":
        // DuckDB's STRING_AGG has different syntax: STRING_AGG(expr, separator)
        writer.keyword(duckdbFunc);
        writer.print("(");
        call.operand(0).unparse(writer, 0, 0);
        if (call.operandCount() > 1) {
          writer.print(", ");
          call.operand(1).unparse(writer, 0, 0);
        } else {
          writer.print(", ','");  // Default separator
        }
        writer.print(")");
        break;

      case "SUBSTRING":
        // DuckDB's SUBSTR has same syntax as SUBSTRING
        writer.keyword(duckdbFunc);
        writer.print("(");
        for (int i = 0; i < call.operandCount(); i++) {
          if (i > 0) writer.print(", ");
          call.operand(i).unparse(writer, 0, 0);
        }
        writer.print(")");
        break;

      case "JSON_VALUE":
        if (call.operandCount() != 2) {
          // RETURNING / ON ERROR / ON EMPTY clauses have no json_extract equivalent;
          // let the ANSI form through so pushdown fails loudly instead of mistranslating.
          call.getOperator().unparse(writer, call, leftPrec, rightPrec);
          break;
        }
        writer.keyword(duckdbFunc);
        writer.print("(");
        for (int i = 0; i < call.operandCount(); i++) {
          if (i > 0) writer.print(", ");
          call.operand(i).unparse(writer, 0, 0);
        }
        writer.print(")");
        break;

      case "COSINE_SIMILARITY":
      case "COSINE_DISTANCE":
        // DuckDB's array functions work directly with array columns
        // Remove any CAST to VARCHAR operations and use arrays directly
        writer.keyword(duckdbFunc);
        writer.print("(");
        for (int i = 0; i < call.operandCount(); i++) {
          if (i > 0) writer.print(", ");

          // Skip CAST operations for embedding arrays
          SqlNode operand = call.operand(i);
          if (operand instanceof SqlCall &&
              ((SqlCall) operand).getOperator().getKind() == SqlKind.CAST) {
            // If this is a CAST operation, just use the inner expression
            SqlCall castCall = (SqlCall) operand;
            castCall.operand(0).unparse(writer, 0, 0);
          } else {
            operand.unparse(writer, 0, 0);
          }
        }
        writer.print(")");
        break;

      default:
        // Standard function replacement
        writer.keyword(duckdbFunc);
        writer.print("(");
        for (int i = 0; i < call.operandCount(); i++) {
          if (i > 0) writer.print(", ");
          call.operand(i).unparse(writer, 0, 0);
        }
        writer.print(")");
      }
    } else {
      // Let default handling take care of it
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * True if any expression rooted at {@code node} calls a user-defined function that this
   * class does not know how to render for DuckDB (see {@link #needsSpecialHandling}). Used by
   * {@link DuckDBProjectRule}/{@link DuckDBFilterRule} to generalize the stock
   * {@code JdbcProjectRule}/{@code JdbcFilterRule}'s blanket refusal to push down a
   * Project/Filter containing any user-defined function at all -- those core Calcite rules
   * block unconditionally without ever consulting the dialect, which meant this project's own
   * DuckDB-pushdown stub UDFs (JSON_EXTRACT, STRING_SPLIT, ...) could never reach DuckDB even
   * though {@link #unparseCall} already knows exactly how to render them. A call to any OTHER
   * (unrecognized) user-defined function still makes this return true, preserving the stock
   * rules' safety for anything this class doesn't have a mapping for.
   */
  public static boolean hasUnsupportedUserDefinedFunction(RexNode node) {
    UnsupportedUdfVisitor visitor = new UnsupportedUdfVisitor();
    node.accept(visitor);
    return visitor.found;
  }

  private static final class UnsupportedUdfVisitor extends RexVisitorImpl<Void> {
    private boolean found = false;

    UnsupportedUdfVisitor() {
      super(true);
    }

    @Override public Void visitCall(RexCall call) {
      SqlOperator operator = call.getOperator();
      if (operator instanceof SqlFunction
          && ((SqlFunction) operator).getFunctionType().isUserDefined()
          && !needsSpecialHandling(operator)) {
        found = true;
        return null;
      }
      return super.visitCall(call);
    }
  }

  /**
   * Checks if DuckDB supports a specific SQL kind.
   */
  public static boolean supportsSqlKind(SqlKind kind) {
    switch (kind) {
    // Basic operations
    case SELECT:
    case INSERT:
    case UPDATE:
    case DELETE:
    case VALUES:
    case ORDER_BY:
    case WITH:
    case UNION:
    case EXCEPT:
    case INTERSECT:

    // Joins
    case JOIN:

    // Aggregates
    case COUNT:
    case SUM:
    case AVG:
    case MIN:
    case MAX:
    case STDDEV_POP:
    case STDDEV_SAMP:
    case VAR_POP:
    case VAR_SAMP:
    case COLLECT:
    case LISTAGG:

    // Window functions
    case OVER:
    case RANK:
    case DENSE_RANK:
    case ROW_NUMBER:
    case FIRST_VALUE:
    case LAST_VALUE:
    case LEAD:
    case LAG:
    case NTILE:

    // Expressions
    case CASE:
    case CAST:
    case LIKE:
    case BETWEEN:
    case IN:
    case NOT_IN:
    case EXISTS:
    case IS_NULL:
    case IS_NOT_NULL:

    // Arithmetic
    case PLUS:
    case MINUS:
    case TIMES:
    case DIVIDE:
    case MOD:

    // Comparison
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:

    // Logical
    case AND:
    case OR:
    case NOT:
      return true;

    default:
      return false;
    }
  }
}
