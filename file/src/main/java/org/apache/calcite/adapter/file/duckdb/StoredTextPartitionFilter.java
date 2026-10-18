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

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;

/**
 * Rewrites a filter on a stored-text partition column served as its declared integer type
 * ({@link StoredTextPartitions}) to compare the stored text, so DuckDB prunes partitions by it.
 * {@link DuckDBFilterRule} applies it to every filter it pushes to DuckDB: the cast form never
 * reaches DuckDB, so it cannot be chosen over the rewritten one.
 *
 * <p>{@code CAST(year AS INTEGER) = 2023} becomes {@code year = '2023'}, and an {@code IN} list
 * becomes the list of stored texts. The rewrite holds because the writer stores each value as the
 * decimal text of its integer: unpadded, or zero-padded to the column's declared digits.
 *
 * <ul>
 *   <li>A column whose every value has the same number of digits (a year) sorts as text the way
 *       its integers sort, so a range ({@code >}, {@code >=}, {@code <}, {@code <=},
 *       {@code BETWEEN}) becomes the same range over the text.
 *   <li>An unpadded column (a month 1 to 12) does not, so a bounded range becomes the list of
 *       the texts it holds, up to {@link #MAX_POINTS}; an unbounded or longer one keeps the cast.
 * </ul>
 *
 * <p>A literal no stored value can hold (negative, fractional, or wider than the column's digits)
 * leaves its comparison to the cast, which answers it correctly without pruning.
 */
final class StoredTextPartitionFilter {

  /** Most texts a range over an unpadded column is expanded to. */
  static final int MAX_POINTS = 1000;

  private StoredTextPartitionFilter() {
  }

  /**
   * The condition of {@code filter} with each comparison of a stored-text partition column of
   * the scan below it made against the stored text; the condition itself when there is none.
   */
  static RexNode condition(Filter filter) {
    RexNode condition = filter.getCondition();
    JdbcTableScan scan = scanBelow(filter.getInput());
    if (scan == null) {
      return condition;
    }
    StoredTextPartitions partitions = scan.getTable().unwrap(StoredTextPartitions.class);
    if (partitions == null || partitions.isEmpty()) {
      return condition;
    }
    return condition.accept(new ToStoredText(filter.getCluster().getRexBuilder(), partitions));
  }

  /** The table scan {@code input} is, or that its planner set holds, or null. */
  private static @Nullable JdbcTableScan scanBelow(RelNode input) {
    if (input instanceof JdbcTableScan) {
      return (JdbcTableScan) input;
    }
    if (input instanceof RelSubset) {
      for (RelNode rel : ((RelSubset) input).getRelList()) {
        if (rel instanceof JdbcTableScan) {
          return (JdbcTableScan) rel;
        }
      }
    }
    return null;
  }

  /** Replaces each comparison of a cast stored-text column with one of its stored text. */
  private static final class ToStoredText extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final StoredTextPartitions partitions;

    ToStoredText(RexBuilder rexBuilder, StoredTextPartitions partitions) {
      this.rexBuilder = rexBuilder;
      this.partitions = partitions;
    }

    @Override public RexNode visitCall(RexCall call) {
      RexNode rewritten = rewrite(call);
      return rewritten != null ? rewritten : super.visitCall(call);
    }

    private @Nullable RexNode rewrite(RexCall call) {
      switch (call.getKind()) {
      case SEARCH:
        return search(call);
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return comparison(call);
      default:
        return null;
      }
    }

    /** The stored column {@code node} casts to an integer, or null. */
    private @Nullable RexInputRef storedRef(RexNode node) {
      if (node.getKind() != SqlKind.CAST
          || !SqlTypeName.INT_TYPES.contains(node.getType().getSqlTypeName())) {
        return null;
      }
      RexNode operand = ((RexCall) node).getOperands().get(0);
      if (operand instanceof RexInputRef
          && partitions.column(((RexInputRef) operand).getIndex()) != null) {
        return (RexInputRef) operand;
      }
      return null;
    }

    private @Nullable RexNode comparison(RexCall call) {
      RexNode left = call.getOperands().get(0);
      RexNode right = call.getOperands().get(1);
      SqlOperator op = call.getOperator();
      RexInputRef ref = storedRef(left);
      if (ref == null) {
        ref = storedRef(right);
        if (ref == null || !(left instanceof RexLiteral)) {
          return null;
        }
        right = left;
        op = op.reverse();
        if (op == null) {
          return null;
        }
      } else if (!(right instanceof RexLiteral)) {
        return null;
      }
      StoredTextPartitions.Column column = partitions.column(ref.getIndex());
      BigDecimal value = ((RexLiteral) right).getValueAs(BigDecimal.class);
      if (column == null || value == null) {
        return null;
      }
      boolean equality = op.getKind() == SqlKind.EQUALS || op.getKind() == SqlKind.NOT_EQUALS;
      if (!equality && column.digits == null) {
        return null;
      }
      String text = column.text(value);
      if (text == null) {
        return null;
      }
      return rexBuilder.makeCall(op, ref, rexBuilder.makeCharLiteral(nls(text, ref.getType())));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private @Nullable RexNode search(RexCall call) {
      RexInputRef ref = storedRef(call.getOperands().get(0));
      if (ref == null) {
        return null;
      }
      StoredTextPartitions.Column column = partitions.column(ref.getIndex());
      Sarg sarg = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);
      if (column == null || sarg == null) {
        return null;
      }
      ImmutableRangeSet.Builder<NlsString> texts = ImmutableRangeSet.builder();
      int points = 0;
      for (Object o : sarg.rangeSet.asRanges()) {
        Range<BigDecimal> range = (Range<BigDecimal>) o;
        if (column.digits != null) {
          Range<NlsString> text = textRange(column, range, ref.getType());
          if (text == null) {
            return null;
          }
          texts.add(text);
          continue;
        }
        if (!range.hasLowerBound() || !range.hasUpperBound()) {
          return null;
        }
        BigInteger first = first(range);
        BigInteger last = last(range);
        if (first == null || last == null) {
          return null;
        }
        for (BigInteger v = first; v.compareTo(last) <= 0; v = v.add(BigInteger.ONE)) {
          if (++points > MAX_POINTS) {
            return null;
          }
          String text = column.text(new BigDecimal(v));
          if (text == null) {
            return null;
          }
          texts.add(Range.singleton(nls(text, ref.getType())));
        }
      }
      Sarg<NlsString> textSarg = Sarg.of(sarg.nullAs, texts.build());
      return rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, ref,
          rexBuilder.makeSearchArgumentLiteral(textSarg, ref.getType()));
    }

    /** The range over fixed-width text that holds the same values as {@code range}. */
    private static @Nullable Range<NlsString> textRange(StoredTextPartitions.Column column,
        Range<BigDecimal> range, RelDataType type) {
      NlsString lower = null;
      NlsString upper = null;
      if (range.hasLowerBound()) {
        String text = column.text(range.lowerEndpoint());
        if (text == null) {
          return null;
        }
        lower = nls(text, type);
      }
      if (range.hasUpperBound()) {
        String text = column.text(range.upperEndpoint());
        if (text == null) {
          return null;
        }
        upper = nls(text, type);
      }
      if (lower != null && upper != null) {
        return Range.range(lower, range.lowerBoundType(), upper, range.upperBoundType());
      }
      if (lower != null) {
        return Range.downTo(lower, range.lowerBoundType());
      }
      if (upper != null) {
        return Range.upTo(upper, range.upperBoundType());
      }
      return Range.all();
    }

    /** The least integer in {@code range}, which has a lower bound; null if it is fractional. */
    private static @Nullable BigInteger first(Range<BigDecimal> range) {
      BigDecimal bound = range.lowerEndpoint();
      try {
        BigInteger value = bound.toBigIntegerExact();
        return range.lowerBoundType() == BoundType.CLOSED ? value : value.add(BigInteger.ONE);
      } catch (ArithmeticException fractional) {
        return null;
      }
    }

    /** The greatest integer in {@code range}, which has an upper bound; null if fractional. */
    private static @Nullable BigInteger last(Range<BigDecimal> range) {
      BigDecimal bound = range.upperEndpoint();
      try {
        BigInteger value = bound.toBigIntegerExact();
        return range.upperBoundType() == BoundType.CLOSED ? value : value.subtract(BigInteger.ONE);
      } catch (ArithmeticException fractional) {
        return null;
      }
    }

    private static NlsString nls(String text, RelDataType type) {
      Charset charset = type.getCharset();
      return new NlsString(text, charset == null ? null : charset.name(), type.getCollation());
    }
  }
}
