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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code fetch_aligned_series} must align on a time grain whether the underlying date column is
 * DATE or VARCHAR.
 *
 * <p>Most date-bearing columns across these schemas are declared {@code type: string} and hold
 * ISO-8601 text — {@code econ}, {@code weather}, {@code sec}, {@code health} and
 * {@code environment} all have a bare {@code date} column of that shape. {@code date_trunc} over a
 * VARCHAR fails outright, so every time-grain alignment failed while the geography path, which
 * passes its key column through untouched, kept working. These tests run the generated key
 * expression against a real DuckDB table of each typing.
 */
@Tag("unit")
class McpServerAlignedSeriesKeyTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static ObjectNode spec(String field, String value) {
    ObjectNode o = MAPPER.createObjectNode();
    o.put(field, value);
    return o;
  }

  /** Runs {@code SELECT <expr> FROM t} against a one-row DuckDB table and returns the value. */
  private static String evaluate(String columnDdl, String columnValue, String expr)
      throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (" + columnDdl + ")");
      st.execute("INSERT INTO t VALUES (" + columnValue + ")");
      try (ResultSet rs = st.executeQuery("SELECT " + expr + " AS k FROM t")) {
        assertTrue(rs.next(), "expected one row");
        return rs.getString(1);
      }
    }
  }

  @Test @DisplayName("time_col is cast, so a VARCHAR ISO date aligns")
  void timeColCastAllowsVarcharDate() throws Exception {
    String expr = McpServer.keyExpr(spec("time_col", "d"), "month", "s0");
    assertTrue(expr.contains("CAST(d AS DATE)"),
        "time_col must be cast before date_trunc; got " + expr);
    assertEquals("2024-03-01", evaluate("d VARCHAR", "'2024-03-17'", expr));
  }

  @Test @DisplayName("the same expression still works on a real DATE column")
  void timeColCastIsANoOpOnDate() throws Exception {
    String expr = McpServer.keyExpr(spec("time_col", "d"), "month", "s0");
    assertEquals("2024-03-01", evaluate("d DATE", "DATE '2024-03-17'", expr));
  }

  @Test @DisplayName("quarter grain truncates a VARCHAR date")
  void quarterGrainOnVarchar() throws Exception {
    String expr = McpServer.keyExpr(spec("time_col", "d"), "quarter", "s0");
    assertEquals("2024-07-01", evaluate("d VARCHAR", "'2024-08-02'", expr));
  }

  @Test @DisplayName("year_only_col is cast, so a VARCHAR year aligns")
  void yearOnlyColCastAllowsVarcharYear() throws Exception {
    String expr = McpServer.keyExpr(spec("year_only_col", "y"), "year", "s0");
    assertTrue(expr.contains("CAST(y AS INTEGER)"),
        "year_only_col must be cast for make_date; got " + expr);
    assertEquals("2019-01-01", evaluate("y VARCHAR", "'2019'", expr));
  }

  @Test @DisplayName("year_col + period_col align when the year is VARCHAR")
  void yearAndPeriodColWithVarcharYear() throws Exception {
    ObjectNode s = MAPPER.createObjectNode();
    s.put("year_col", "y");
    s.put("period_col", "p");
    String expr = McpServer.keyExpr(s, "month", "s0");
    assertTrue(expr.contains("CAST(y AS INTEGER)"),
        "year_col must be cast for make_date; got " + expr);
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (y VARCHAR, p VARCHAR)");
      st.execute("INSERT INTO t VALUES ('2021', 'M07')");
      try (ResultSet rs = st.executeQuery("SELECT " + expr + " AS k FROM t")) {
        assertTrue(rs.next());
        assertEquals("2021-07-01", rs.getString(1));
      }
    }
  }

  /**
   * The geography path must keep passing its key column through unchanged — it already worked, and
   * a FIPS code is a string join key, not a date.
   */
  @Test @DisplayName("geo_col is passed through untouched")
  void geoColUnchanged() {
    assertEquals("county_fips",
        McpServer.keyExpr(spec("geo_col", "county_fips"), "county", "s0"));
  }
}
