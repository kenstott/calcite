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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests the predicate recogniser behind the manifest-only {@code COUNT(*)}.
 *
 * <p>Two producers feed it: an explicit {@code WHERE} that reaches the planner as a Filter
 * condition, and the {@code WHERE} of a DuckDB SQL view, which under the DuckDB engine is the only
 * place a view's predicate survives — the plan tree just scans the view.
 *
 * <p>Which columns are partition columns is not this class's judgement, so these tests are about
 * <em>shape</em>: what reduces to column-to-constants, and what must be refused outright because no
 * partition tuple could ever settle it.
 */
@Tag("unit")
public class PartitionEqualityFilterTest {

  /** Field names of a scan, in the order a {@code RexInputRef} indexes them. */
  private static final List<String> FIELDS =
      Arrays.asList("dissemination_id", "cleared", "asset_class", "year");

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  // -----------------------------------------------------------------------------------------
  // Filter conditions
  // -----------------------------------------------------------------------------------------

  @Test void readsASingleEquality() {
    Map<String, List<Object>> accepted =
        PartitionEqualityFilter.fromRex(eq(2, "RATES"), FIELDS, rexBuilder);
    assertNotNull(accepted);
    assertEquals(1, accepted.size());
    assertEquals(Arrays.<Object>asList("RATES"), accepted.get("asset_class"));
  }

  @Test void readsAConjunctionOfEqualitiesOnDifferentColumns() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.AND, eq(2, "RATES"), eq(3, 2024));
    Map<String, List<Object>> accepted =
        PartitionEqualityFilter.fromRex(condition, FIELDS, rexBuilder);
    assertNotNull(accepted);
    assertEquals(2, accepted.size());
    assertEquals(Arrays.<Object>asList("RATES"), accepted.get("asset_class"));
    assertEquals(Arrays.<Object>asList(2024L), accepted.get("year"));
  }

  @Test void readsAnInListAsSeveralAcceptedValues() {
    // Calcite folds `IN (a, b)` into a SEARCH over a Sarg; the recogniser expands it first.
    RexNode condition =
        rexBuilder.makeIn(rexBuilder.makeInputRef(varchar(), 2),
            Arrays.asList(rexBuilder.makeLiteral("RATES"), rexBuilder.makeLiteral("CREDITS")));
    Map<String, List<Object>> accepted =
        PartitionEqualityFilter.fromRex(condition, FIELDS, rexBuilder);
    assertNotNull(accepted);
    // Calcite normalises an IN list into a sorted Sarg, so compare as a set.
    assertEquals(new java.util.HashSet<Object>(Arrays.asList("RATES", "CREDITS")),
        new java.util.HashSet<Object>(accepted.get("asset_class")));
  }

  @Test void refusesARangePredicate() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(integer(), 3),
            rexBuilder.makeLiteral(2024L, integer(), false));
    assertNull(PartitionEqualityFilter.fromRex(condition, FIELDS, rexBuilder));
  }

  @Test void refusesIsNull() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef(varchar(), 2));
    assertNull(PartitionEqualityFilter.fromRex(condition, FIELDS, rexBuilder));
  }

  @Test void refusesAColumnToColumnComparison() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(varchar(), 1), rexBuilder.makeInputRef(varchar(), 2));
    assertNull(PartitionEqualityFilter.fromRex(condition, FIELDS, rexBuilder));
  }

  @Test void refusesAnOrAcrossTwoColumns() {
    // Not an IN list: a disjunction over different columns selects a union of partitions that
    // simple accepted-value sets cannot express.
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.OR, eq(2, "RATES"), eq(1, "Y"));
    assertNull(PartitionEqualityFilter.fromRex(condition, FIELDS, rexBuilder));
  }

  @Test void refusesTheSameColumnConstrainedTwice() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.AND, eq(2, "RATES"), eq(2, "CREDITS"));
    assertNull(PartitionEqualityFilter.fromRex(condition, FIELDS, rexBuilder));
  }

  @Test void namesANonPartitionColumnAsReadily() {
    // The recogniser does not know which columns are partition columns and must not pretend to:
    // it reports `cleared` just like any other, and Iceberg's residual is what refuses the count.
    Map<String, List<Object>> accepted =
        PartitionEqualityFilter.fromRex(eq(1, "Y"), FIELDS, rexBuilder);
    assertNotNull(accepted);
    assertEquals(Arrays.<Object>asList("Y"), accepted.get("cleared"));
  }

  // -----------------------------------------------------------------------------------------
  // View definitions
  // -----------------------------------------------------------------------------------------

  @Test void readsAPartitionFilteringView() {
    // cftc.commodity_derivatives, the view that motivated the rule.
    PartitionEqualityFilter.ViewDefinition view =
        PartitionEqualityFilter.fromViewSql(
            "SELECT dissemination_id, cleared, year FROM cftc.cftc_trades "
                + "WHERE asset_class = 'COMMODITIES'");
    assertNotNull(view);
    assertEquals("cftc_trades", view.baseTableName);
    assertEquals(Arrays.<Object>asList("COMMODITIES"), view.acceptedValues.get("asset_class"));
  }

  @Test void readsAViewWhoseColumnsAreSqlReservedWords() {
    // `year` and `month` are partition columns across govdata's lake and reserved words to
    // Calcite's own parser. A view listing them must still be readable, or the rewrite would
    // never fire on the tables it was written for.
    PartitionEqualityFilter.ViewDefinition view =
        PartitionEqualityFilter.fromViewSql(
            "SELECT dissemination_id, trade_date, year, month FROM cftc.cftc_trades "
                + "WHERE asset_class = 'COMMODITIES'");
    assertNotNull(view);
    assertEquals("cftc_trades", view.baseTableName);
    assertEquals(Arrays.<Object>asList("COMMODITIES"), view.acceptedValues.get("asset_class"));
  }

  @Test void readsAStarSelect() {
    PartitionEqualityFilter.ViewDefinition view =
        PartitionEqualityFilter.fromViewSql("SELECT * FROM cftc_trades WHERE asset_class = 'RATES'");
    assertNotNull(view);
    assertEquals("cftc_trades", view.baseTableName);
    assertEquals(Arrays.<Object>asList("RATES"), view.acceptedValues.get("asset_class"));
  }

  @Test void readsAnInListInAView() {
    PartitionEqualityFilter.ViewDefinition view =
        PartitionEqualityFilter.fromViewSql(
            "SELECT * FROM cftc_trades WHERE asset_class IN ('RATES', 'CREDITS')");
    assertNotNull(view);
    assertEquals(Arrays.<Object>asList("RATES", "CREDITS"),
        view.acceptedValues.get("asset_class"));
  }

  @Test void readsANumericEqualityInAView() {
    PartitionEqualityFilter.ViewDefinition view =
        PartitionEqualityFilter.fromViewSql("SELECT * FROM cftc_trades WHERE year = 2024");
    assertNotNull(view);
    assertEquals(Arrays.<Object>asList(2024L), view.acceptedValues.get("year"));
  }

  @Test void readsAnUnfilteredViewAsTheWholeTable() {
    PartitionEqualityFilter.ViewDefinition view =
        PartitionEqualityFilter.fromViewSql("SELECT dissemination_id FROM cftc_trades");
    assertNotNull(view);
    assertEquals("cftc_trades", view.baseTableName);
    assertEquals(0, view.acceptedValues.size());
  }

  @Test void refusesAnAggregatingView() {
    // cftc.swap_activity: COUNT(*) over it is a count of groups, not of base rows.
    assertNull(PartitionEqualityFilter.fromViewSql(
        "SELECT trade_date, COUNT(*) AS trade_count FROM cftc_trades GROUP BY trade_date"));
  }

  @Test void refusesAnAliasedSelectList() {
    // An alias breaks the assumption that the view's column names are the base table's, which is
    // what lets a caller's own WHERE over the view be read as base-table columns.
    assertNull(PartitionEqualityFilter.fromViewSql(
        "SELECT dissemination_id AS id FROM cftc_trades WHERE asset_class = 'RATES'"));
  }

  @Test void refusesAJoiningView() {
    assertNull(PartitionEqualityFilter.fromViewSql(
        "SELECT t.dissemination_id FROM cftc_trades t JOIN counterparties c "
            + "ON t.dissemination_id = c.dissemination_id WHERE t.asset_class = 'RATES'"));
  }

  @Test void refusesADistinctView() {
    assertNull(PartitionEqualityFilter.fromViewSql(
        "SELECT DISTINCT asset_class FROM cftc_trades WHERE asset_class = 'RATES'"));
  }

  @Test void refusesAViewWithARangePredicate() {
    assertNull(PartitionEqualityFilter.fromViewSql(
        "SELECT * FROM cftc_trades WHERE year > 2020"));
  }

  @Test void refusesAViewWithALikePredicate() {
    assertNull(PartitionEqualityFilter.fromViewSql(
        "SELECT * FROM cftc_trades WHERE asset_class LIKE 'RATE%'"));
  }

  @Test void refusesAUnionView() {
    assertNull(PartitionEqualityFilter.fromViewSql(
        "SELECT * FROM cftc_trades WHERE asset_class = 'RATES' "
            + "UNION ALL SELECT * FROM cftc_trades WHERE asset_class = 'CREDITS'"));
  }

  @Test void refusesUnparseableSql() {
    assertNull(PartitionEqualityFilter.fromViewSql("this is not sql"));
  }

  // -----------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------

  private RelDataType varchar() {
    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
  }

  private RelDataType integer() {
    return typeFactory.createSqlType(SqlTypeName.INTEGER);
  }

  private RexNode eq(int fieldIndex, String value) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(varchar(), fieldIndex), rexBuilder.makeLiteral(value));
  }

  private RexNode eq(int fieldIndex, int value) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(integer(), fieldIndex),
        rexBuilder.makeLiteral((long) value, integer(), false));
  }
}
