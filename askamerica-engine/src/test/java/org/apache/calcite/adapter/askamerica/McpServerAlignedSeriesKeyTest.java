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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code fetch_aligned_series} must align on a time grain whether the underlying date column is
 * DATE or VARCHAR — and the generated expression must actually validate under Calcite, not just
 * under raw DuckDB.
 *
 * <p>Most date-bearing columns across these schemas are declared {@code type: string} and hold
 * ISO-8601 text — {@code econ}, {@code weather}, {@code sec}, {@code health} and
 * {@code environment} all have a bare {@code date} column of that shape. {@code date_trunc}/{@code
 * make_date} over a VARCHAR fail outright on DuckDB, so casting the raw column matters — but the
 * earlier version of these tests ran the generated expression directly against a bare {@code
 * jdbc:duckdb:} connection, bypassing Calcite entirely. DuckDB understands {@code date_trunc}/
 * {@code make_date} natively, so those tests kept passing even though the actual production
 * connection never got that far: neither is registered under this connection's function libraries
 * (fun=standard,postgresql,spatial per GovDataDriver) — {@code make_date} isn't registered under
 * any library at all, and {@code date_trunc} exists only under {@code bigquery}, with date and
 * unit swapped from Postgres/DuckDB order — so every time-grain call failed validation with "No
 * match found for function signature".
 *
 * <p>The fix uses only genuine Calcite-standard operators instead: {@code FLOOR(x TO unit)},
 * {@code CAST}, {@code ||}, {@code CASE}, and ANSI {@code SUBSTRING(x FROM n [FOR m])}. This is
 * deliberate, not just a style choice: registering {@code make_date}/{@code date_trunc} as schema
 * UDFs (the same pattern already used for {@code corr}/{@code regr_*}, which validates and pushes
 * down fine) does NOT work here, because Calcite's {@code JdbcProjectRule} unconditionally refuses
 * to push a {@code Project} down to a JDBC source at all if it contains a user-defined function —
 * and this key expression always sits inside a Project (the {@code GROUP BY} key), never inside
 * an Aggregate the way {@code corr}/{@code regr_*} do. Built-in operators are never flagged
 * user-defined, so they push through, and Calcite's own dialect-aware unparse for {@code FLOOR}
 * expands it into DuckDB's native {@code date_trunc} call at SQL-generation time.
 *
 * <p>These tests run the generated expression through a real Calcite connection over a genuine
 * DuckDB-engine {@code FileSchema} table (not a bare in-memory VALUES row), projecting the
 * expression once in an inner subquery and aggregating {@code GROUP BY} the resulting column —
 * the same shape {@code buildAlignedSql} generates. That shape matters on its own: repeating the
 * key expression's literal text in both {@code SELECT} and {@code GROUP BY} instead (the original
 * form of this query) makes Calcite's {@code GROUP BY} expander fail to resolve identifiers inside
 * the second copy for some expression shapes — {@code CASE} inside {@code SUBSTRING}
 * (quarter_col), and a join-derived {@code COALESCE} (the state grain) — even though the identical
 * {@code SELECT}-list copy resolves fine. So a function Calcite can't resolve, an identifier its
 * parser rejects, an expression that fails to push down, or a {@code GROUP BY} shape it can't
 * resolve all fail the test the same way they fail a real MCP call.
 */
@Tag("unit")
class McpServerAlignedSeriesKeyTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private File tempDir;
  private File modelFile;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = java.nio.file.Files.createTempDirectory("mcp-aligned-key-test").toFile();
    tempDir.deleteOnExit();

    // A second "geo" schema with a state_ref table, mirroring the real geo.state_ref used to
    // normalize state-grain geo keys — needed only by the geo-normalization test below.
    File geoDir = new File(tempDir, "geo_data");
    geoDir.mkdirs();
    try (FileWriter writer = new FileWriter(new File(geoDir, "state_ref.json"))) {
      writer.write("[\n"
          + "  {\"state_fips\": \"06\", \"state_abbr\": \"CA\"},\n"
          + "  {\"state_fips\": \"36\", \"state_abbr\": \"NY\"}\n"
          + "]\n");
    }

    modelFile = new File(tempDir, "model.json");
    try (FileWriter writer = new FileWriter(modelFile)) {
      writer.write("{\n"
          + "  \"version\": \"1.0\",\n"
          + "  \"defaultSchema\": \"TEST\",\n"
          + "  \"schemas\": [{\n"
          + "    \"name\": \"TEST\",\n"
          + "    \"type\": \"custom\",\n"
          + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
          + "    \"operand\": {\n"
          + "      \"directory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n"
          + "      \"executionEngine\": \"duckdb\",\n"
          + "      \"ephemeralCache\": true\n"
          + "    }\n"
          + "  }, {\n"
          + "    \"name\": \"geo\",\n"
          + "    \"type\": \"custom\",\n"
          + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
          + "    \"operand\": {\n"
          + "      \"directory\": \"" + geoDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n"
          + "      \"executionEngine\": \"duckdb\",\n"
          + "      \"ephemeralCache\": true\n"
          + "    }\n"
          + "  }]\n"
          + "}\n");
    }
  }

  private static ObjectNode spec(String field, String value) {
    ObjectNode o = MAPPER.createObjectNode();
    o.put(field, value);
    return o;
  }

  /**
   * Writes a one-row JSON table {@code t(<columnNames>)} (each value a raw JSON literal — quote
   * string values yourself) into the schema directory, then projects {@code expr} once in an
   * inner subquery and runs {@code SELECT k, COUNT(*) AS n FROM (SELECT <expr> AS k FROM t) p
   * GROUP BY k} through a real Calcite+DuckDB-engine connection, returning the value. This is the
   * exact shape {@code buildAlignedSql} generates — project the key once, group by the resulting
   * column — never the key expression's own text repeated in both SELECT and GROUP BY, which
   * Calcite's GROUP BY expander fails to resolve for some expression shapes (see
   * {@link #quarterColMapsToFirstMonthOfQuarter} and {@link #stateGeoColNormalizesFipsAndAbbreviation}).
   * The real table scan is what forces the whole query to push down to DuckDB.
   */
  private String evaluate(String[] columnNames, String[] jsonValues, String expr)
      throws Exception {
    File jsonFile = new File(tempDir, "t.json");
    StringBuilder row = new StringBuilder("{");
    for (int i = 0; i < columnNames.length; i++) {
      if (i > 0) {
        row.append(", ");
      }
      row.append('"').append(columnNames[i]).append("\": ").append(jsonValues[i]);
    }
    row.append('}');
    try (FileWriter writer = new FileWriter(jsonFile)) {
      writer.write("[\n  " + row + "\n]\n");
    }
    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath()
        + ";lex=ORACLE;unquotedCasing=TO_LOWER;fun=standard,postgresql,spatial";
    try (Connection c = DriverManager.getConnection(url);
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT k, COUNT(*) AS n FROM (SELECT " + expr
             + " AS k FROM \"TEST\".\"t\") p GROUP BY k")) {
      assertTrue(rs.next(), "expected one row");
      return rs.getString(1);
    }
  }

  private String evaluate(String columnName, String jsonValue, String expr) throws Exception {
    return evaluate(new String[] {columnName}, new String[] {jsonValue}, expr);
  }

  @Test @DisplayName("time_col is cast and quoted, so a VARCHAR ISO date aligns")
  void timeColCastAllowsVarcharDate() throws Exception {
    String expr = McpServer.keyExpr(spec("time_col", "d"), "month", "s0");
    assertTrue(expr.contains("CAST(\"d\" AS DATE)"),
        "time_col must be quoted and cast before FLOOR; got " + expr);
    assertEquals("2024-03-01", evaluate("d", "\"2024-03-17\"", expr));
  }

  @Test @DisplayName("the same expression still works on a real DATE column")
  void timeColCastIsANoOpOnDate() throws Exception {
    // Casts the JSON-inferred VARCHAR to a genuine DATE in a subquery first, so the outer
    // expression's own CAST(... AS DATE) runs against an already-DATE column, not text.
    String expr = McpServer.keyExpr(spec("time_col", "d"), "month", "s0");
    File jsonFile = new File(tempDir, "t.json");
    try (FileWriter writer = new FileWriter(jsonFile)) {
      writer.write("[\n  {\"d\": \"2024-03-17\"}\n]\n");
    }
    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath()
        + ";lex=ORACLE;unquotedCasing=TO_LOWER;fun=standard,postgresql,spatial";
    try (Connection c = DriverManager.getConnection(url);
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT k, COUNT(*) AS n FROM (SELECT " + expr
             + " AS k FROM (SELECT CAST(\"d\" AS DATE) AS d FROM \"TEST\".\"t\") AS dt) p"
             + " GROUP BY k")) {
      assertTrue(rs.next());
      assertEquals("2024-03-01", rs.getString(1));
    }
  }

  @Test @DisplayName("time_col named after a reserved word still parses")
  void timeColReservedWord() throws Exception {
    // "date" is a genuine reserved identifier in this catalog (RESERVED_IDENTIFIERS) and a real
    // column name (e.g. econ.fred_indicators.date) — the unquoted bare reference this generator
    // used to emit failed Calcite's parser outright ("Encountered "date AS"").
    String expr = McpServer.keyExpr(spec("time_col", "date"), "month", "s0");
    assertEquals("2024-03-01", evaluate("date", "\"2024-03-17\"", expr));
  }

  @Test @DisplayName("quarter grain truncates a VARCHAR date")
  void quarterGrainOnVarchar() throws Exception {
    // Previously observed returning a stale value (2024-01-01 instead of 2024-07-01) under the
    // old query shape, which repeated this CASE/SUBSTRING expression's literal text in both
    // SELECT and GROUP BY. Projecting the key once in a subquery and grouping by the resulting
    // column (see #evaluate) resolved it — same root cause as the "Column not found" failure in
    // #quarterColMapsToFirstMonthOfQuarter and #stateGeoColNormalizesFipsAndAbbreviation: Calcite's
    // GROUP BY validation of a duplicated complex expression, not an error in the expression itself.
    String expr = McpServer.keyExpr(spec("time_col", "d"), "quarter", "s0");
    assertEquals("2024-07-01", evaluate("d", "\"2024-08-02\"", expr));
  }

  @Test @DisplayName("year_only_col is cast, so a VARCHAR year aligns")
  void yearOnlyColCastAllowsVarcharYear() throws Exception {
    String expr = McpServer.keyExpr(spec("year_only_col", "y"), "year", "s0");
    assertTrue(expr.contains("CAST(\"y\" AS INTEGER)"),
        "year_only_col must be quoted and cast; got " + expr);
    assertEquals("2019-01-01", evaluate("y", "\"2019\"", expr));
  }

  @Test @DisplayName("year_col + period_col align when the year is VARCHAR")
  void yearAndPeriodColWithVarcharYear() throws Exception {
    ObjectNode s = MAPPER.createObjectNode();
    s.put("year_col", "y");
    s.put("period_col", "p");
    String expr = McpServer.keyExpr(s, "month", "s0");
    assertTrue(expr.contains("CAST(\"y\" AS INTEGER)"),
        "year_col must be quoted and cast; got " + expr);
    assertEquals("2021-07-01",
        evaluate(new String[] {"y", "p"}, new String[] {"\"2021\"", "\"M07\""}, expr));
  }

  @Test @DisplayName("quarter_col maps each quarter digit to its first month")
  void quarterColMapsToFirstMonthOfQuarter() throws Exception {
    String expr = McpServer.keyExpr(spec("quarter_col", "q"), "quarter", "s0");
    assertEquals("2023-07-01", evaluate("q", "\"2023Q3\"", expr));
  }

  /**
   * The geography path must keep passing its key column through as a plain identifier — it
   * already worked, and a FIPS code is a string join key, not a date — but it must now be quoted
   * the same as the time-grain columns, since a geo_col could just as easily collide with a
   * reserved word.
   */
  @Test @DisplayName("geo_col is quoted but otherwise passed through untouched")
  void geoColQuoted() {
    assertEquals("\"county_fips\"",
        McpServer.keyExpr(spec("geo_col", "county_fips"), "county", "s0"));
  }

  /**
   * Reproduces the {@code fetch_aligned_series} state-grain bug: two series whose {@code geo_col}
   * use different conventions (one a 2-digit FIPS code, the other a USPS abbreviation) produced a
   * disjoint {@code FULL OUTER JOIN} where every row was half-NULL, because the raw key values
   * never compared equal. Both forms must now resolve to the same canonical {@code state_fips} via
   * {@code geo.state_ref}, so a FIPS-keyed series and an abbreviation-keyed series actually align.
   */
  @Test @DisplayName("state geo_col normalizes both FIPS and USPS-abbreviation input to the same key")
  void stateGeoColNormalizesFipsAndAbbreviation() throws Exception {
    ObjectNode s = spec("geo_col", "state");
    String expr = McpServer.keyExpr(s, "state", "s0");
    String join = McpServer.stateGeoJoin(s, "state", "s0");
    assertTrue(join != null && join.contains("geo.state_ref"),
        "expected a geo.state_ref join fragment; got " + join);

    String fromFips = evaluateWithJoin("state", "\"06\"", expr, join);
    String fromAbbr = evaluateWithJoin("state", "\"CA\"", expr, join);

    assertEquals("06", fromFips);
    assertEquals(fromFips, fromAbbr,
        "a FIPS-keyed series and an abbreviation-keyed series must normalize to the same key");
  }

  /** Like {@link #evaluate}, but appends a join fragment to the FROM clause — needed for the
   *  state geo_col case, whose key expression references an alias only the join introduces. */
  private String evaluateWithJoin(String columnName, String jsonValue, String expr, String join)
      throws Exception {
    File jsonFile = new File(tempDir, "t.json");
    try (FileWriter writer = new FileWriter(jsonFile)) {
      writer.write("[\n  {\"" + columnName + "\": " + jsonValue + "}\n]\n");
    }
    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath()
        + ";lex=ORACLE;unquotedCasing=TO_LOWER;fun=standard,postgresql,spatial";
    try (Connection c = DriverManager.getConnection(url);
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT k, COUNT(*) AS n FROM (SELECT " + expr
             + " AS k FROM \"TEST\".\"t\"" + join + ") p GROUP BY k")) {
      assertTrue(rs.next(), "expected one row");
      return rs.getString(1);
    }
  }
}
