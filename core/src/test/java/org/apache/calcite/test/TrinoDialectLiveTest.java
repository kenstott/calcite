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
package org.apache.calcite.test;

import org.apache.calcite.runtime.Hook;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs the SQL that {@link org.apache.calcite.sql.dialect.TrinoSqlDialect}
 * generates against a real Trino server in a Docker container, through
 * Calcite's JDBC adapter.
 *
 * <p>Each expression is evaluated twice: pushed to Trino, and by Calcite
 * over a VALUES view holding the same rows. The two answers must agree, and
 * the expression must be pushed (or kept in Calcite) as the dialect
 * declares. Run by the {@code :core:integTestTrino} task, which needs Docker;
 * the default {@code :core:test} task does not include it, since its
 * Windows and macOS CI jobs have no Linux container runtime.
 */
@Tag("trino")
@Execution(ExecutionMode.SAME_THREAD)
class TrinoDialectLiveTest {
  /** Matches the {@code trino.version} the build uses for its Trino
   * plugins and the {@code trino-jdbc} driver of this task. */
  private static final String IMAGE = "trinodb/trino:481";

  private static String containerId;
  private static int port;

  /** Rows of table {@code t}, in Trino syntax, and the same rows as a
   * Calcite VALUES list. */
  private static final String TRINO_ROWS = "(1, 'alpha', 12.50, 1.5,"
      + " TIMESTAMP '2024-03-15 10:20:30.123', DATE '2024-03-15', true, 100),"
      + " (2, 'Beta Gamma', -3.25, -2.75,"
      + " TIMESTAMP '2023-12-31 23:59:59.999', DATE '2023-12-31', false, -7),"
      + " (3, '  pad  ', 0.00, 0.0,"
      + " TIMESTAMP '2024-02-29 00:00:00.000', DATE '2024-02-29', true, 0),"
      + " (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";

  private static final String CALCITE_ROWS = "(1, CAST('alpha' AS VARCHAR),"
      + " CAST(12.50 AS DECIMAL(10, 2)), CAST(1.5 AS DOUBLE),"
      + " CAST(TIMESTAMP '2024-03-15 10:20:30.123' AS TIMESTAMP(3)),"
      + " DATE '2024-03-15', TRUE, CAST(100 AS BIGINT)),"
      + " (2, CAST('Beta Gamma' AS VARCHAR), CAST(-3.25 AS DECIMAL(10, 2)),"
      + " CAST(-2.75 AS DOUBLE),"
      + " CAST(TIMESTAMP '2023-12-31 23:59:59.999' AS TIMESTAMP(3)),"
      + " DATE '2023-12-31', FALSE, CAST(-7 AS BIGINT)),"
      + " (3, CAST('  pad  ' AS VARCHAR), CAST(0.00 AS DECIMAL(10, 2)),"
      + " CAST(0.0 AS DOUBLE),"
      + " CAST(TIMESTAMP '2024-02-29 00:00:00.000' AS TIMESTAMP(3)),"
      + " DATE '2024-02-29', TRUE, CAST(0 AS BIGINT)),"
      + " (4, CAST(NULL AS VARCHAR), CAST(NULL AS DECIMAL(10, 2)),"
      + " CAST(NULL AS DOUBLE), CAST(NULL AS TIMESTAMP(3)),"
      + " CAST(NULL AS DATE), CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT))";

  private static final String COLUMNS =
      "id, name, amount, x, ts, dt, flag, n";

  @BeforeAll static void startTrino() throws Exception {
    containerId =
        docker("run", "-d", "--rm", "--memory", "3g", "-p", "127.0.0.1::8080",
            IMAGE).trim();
    final String mapping = docker("port", containerId, "8080/tcp").trim();
    port =
        Integer.parseInt(
            mapping.split("\n")[0].substring(mapping.lastIndexOf(':') + 1)
                .trim());
    awaitStarted();
    try (Connection c = trino(); Statement s = c.createStatement()) {
      s.execute("CREATE TABLE memory.default.t (id integer, name varchar,"
          + " amount decimal(10, 2), x double, ts timestamp(3), dt date,"
          + " flag boolean, n bigint)");
      s.execute("INSERT INTO memory.default.t VALUES " + TRINO_ROWS);
      s.execute("CREATE TABLE memory.default.u (id integer, label varchar)");
      s.execute("INSERT INTO memory.default.u VALUES (1, 'ALPHA'),"
          + " (2, 'beta gamma'), (5, 'none')");
      s.execute("CREATE TABLE memory.default.types_t (id integer,"
          + " r row(a integer, b varchar), m map(varchar, integer),"
          + " arr array(integer), tstz timestamp(3) with time zone,"
          + " ts6 timestamp(6), tm time(3), vb varbinary, c char(5),"
          + " dcm decimal(38, 10), real_col real, tiny tinyint,"
          + " small smallint)");
      s.execute("INSERT INTO memory.default.types_t VALUES (1,"
          + " ROW(7, 'seven'), MAP(ARRAY['k1', 'k2'], ARRAY[1, 2]),"
          + " ARRAY[1, 2, 3],"
          + " TIMESTAMP '2024-03-15 10:20:30.123 Asia/Kolkata',"
          + " TIMESTAMP '2024-03-15 10:20:30.123456', TIME '10:20:30.123',"
          + " X'CAFE', 'ab', 12345678901234567890.0123456789,"
          + " REAL '1.25', TINYINT '7', SMALLINT '300')");
    }
  }

  @AfterAll static void stopTrino() throws Exception {
    if (containerId != null) {
      docker("rm", "-f", containerId);
    }
  }

  /** Expressions whose pushed form Trino runs with Calcite's answer. */
  private static final List<String> PUSHED =
      Arrays.asList(
          // string
          "upper(name)", "lower(name)", "char_length(name)",
          "character_length(name)", "trim(name)",
          "trim(leading ' ' from name)", "trim(trailing ' ' from name)",
          "trim(both 'a' from name)", "substring(name from 2 for 3)",
          "substring(name from 2)", "substring(name from 0 for 2)",
          "substring(name from id - 2 for 2)", "substring(name from id - 2)",
          "position('a' in name)", "name || '!'", "replace(name, 'a', 'o')",
          "name like 'a%'", "name not like '%a%'", "name in ('alpha', 'x')",
          "name > 'B'", "name = 'alpha'",
          // numeric
          "strpos(name, 'a')", "lpad(name, 8, '*')",
          "rpad(name, 3, '*')", "chr(65 + id)",
          "regexp_like(name, '^[a-z]+$')",
          "abs(amount)", "abs(x)", "atan2(x, 2)", "degrees(x)", "radians(x)",
          "truncate(x)", "truncate(amount, 1)", "tanh(x)", "cosh(x)", "ceil(x)", "floor(x)", "ceil(amount)",
          "floor(amount)", "round(amount, 1)", "round(x)", "mod(n, 3)",
          "sqrt(abs(x))", "ln(abs(x) + 1)", "log10(abs(x) + 1)", "exp(x)",
          "power(x, 2)", "sin(x)", "cos(x)", "tan(x)", "asin(x / 10)",
          "acos(x / 10)", "atan(x)", "cbrt(x)", "sign(x)", "pi",
          "amount * 2", "amount / 4", "n / 3", "-n", "n + id",
          "amount between 0 and 20",
          // datetime
          "extract(year from ts)", "extract(quarter from ts)",
          "extract(month from ts)", "extract(week from ts)",
          "extract(day from ts)", "extract(hour from ts)",
          "extract(minute from ts)", "extract(second from ts)",
          "extract(year from dt)", "extract(month from dt)",
          "extract(day from dt)", "extract(dow from dt)",
          "extract(doy from dt)", "extract(isodow from dt)",
          "extract(isoyear from dt)",
          "floor(ts to year)", "floor(ts to month)", "floor(ts to day)",
          "floor(ts to hour)", "floor(dt to month)", "floor(dt to week)",
          "floor(ts to quarter)", "floor(ts to minute)",
          "extract(decade from dt)", "extract(century from dt)",
          "extract(millennium from dt)", "extract(week from dt)",
          "extract(millisecond from ts)", "extract(microsecond from ts)",
          "extract(nanosecond from ts)", "extract(epoch from ts)",
          "extract(epoch from dt)", "timestampadd(day, 3, ts)",
          "timestampadd(month, 1, dt)",
          "ts > timestamp '2024-01-01 00:00:00'",
          "dt = date '2024-03-15'", "ts + interval '1' day",
          "dt + interval '1' month", "dt - interval '2' year",
          // casts
          "cast(n as varchar)", "cast(id as double)", "cast(n as decimal(12, 3))",
          "cast(amount as integer)", "cast(x as bigint)", "cast(x as integer)",
          "cast(ts as date)", "cast(dt as timestamp)",
          "cast(ts as timestamp(0))", "cast(name as char(3))",
          "cast(n as smallint)",
          // conditional
          "case when n > 0 then 'pos' when n < 0 then 'neg' else 'zero' end",
          "coalesce(name, '?')", "nullif(n, 0)", "name is null",
          "flag is true", "flag is not false", "not flag",
          "flag and n > 0", "flag or n < 0");

  @Test void pushedExpressions() throws Exception {
    final List<String> failures = new ArrayList<>();
    for (String e : PUSHED) {
      check(e, true, failures);
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  /** Expressions Trino cannot run with Calcite's answer, so they stay in
   * Calcite; the answer must still be right. */
  private static final List<String> KEPT =
      Arrays.asList("cot(x + 10)", "current_timestamp > ts",
          "name similar to 'a%'", "left(name, 2)", "md5(name)",
          "split_part(name, ' ', 2)", "ceil(ts to month)",
          "regexp_replace(name, 'a', 'o')", "sinh(x)",
          "timestampdiff(day, ts, timestamp '2025-01-01 00:00:00')");

  @Test void keptExpressions() throws Exception {
    final List<String> failures = new ArrayList<>();
    for (String e : KEPT) {
      check(e, false, failures);
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  private static void check(String expr, boolean pushed, List<String> failures)
      throws SQLException {
    // No ORDER BY: rows are sorted by id here, so the plan shows only the
    // expression's own pushdown.
    final String select = "select id, " + expr + " as e from ";
    final List<String> sqls = new ArrayList<>();
    final List<List<Object>> actual;
    final String plan;
    try (Connection c = calcite()) {
      try (Hook.Closeable ignored =
               Hook.QUERY_PLAN.addThread((Consumer<String>) sqls::add)) {
        actual = rows(c, select + "trino.t");
      } catch (SQLException ex) {
        failures.add(expr + ": FAILED pushed " + sqls + ": " + ex);
        System.out.println("PUSHDOWN|" + expr + "|error|" + sqls);
        return;
      }
      plan = plan(c, select + "trino.t");
      final List<List<Object>> expected =
          rows(c, select + "calc.t");
      final boolean isPushed = plan.startsWith("JdbcToEnumerableConverter");
      System.out.println("PUSHDOWN|" + expr + "|" + (isPushed ? "pushed" : "calcite")
          + "|" + sqls);
      if (!same(expected, actual)) {
        failures.add(expr + ": expected " + expected + " got " + actual
            + " via " + sqls);
      }
      if (isPushed != pushed) {
        failures.add(expr + ": pushed=" + isPushed + ", expected " + pushed
            + "; plan " + plan);
      }
    }
  }

  @Test void projectionFilterPushed() throws Exception {
    assertPlan("select id, name from trino.t where amount > 0 and n > 10",
        "[[1, alpha]]",
        "\nWHERE ");
  }

  @Test void joinPushed() throws Exception {
    assertPlan("select t.id, u.label from trino.t join trino.u"
            + " on t.id = u.id where u.label <> 'x'",
        "[[1, ALPHA], [2, beta gamma]]",
        "INNER JOIN");
  }

  @Test void aggregatePushed() throws Exception {
    assertPlan("select flag, count(*) as c, sum(amount) as s, avg(x) as a,"
            + " min(name) as mn, max(ts) as mx, count(distinct n) as d"
            + " from trino.t group by flag",
        "[[false, 1, -3.25, -2.75, Beta Gamma, 2023-12-31 23:59:59.999, 1],"
            + " [null, 1, null, null, null, null, 0],"
            + " [true, 2, 12.50, 0.75,   pad  , 2024-03-15 10:20:30.123, 2]]",
        "GROUP BY \"flag\"");
  }

  /** AggregateReduceFunctionsRule rewrites STDDEV and VAR into SUM and
   * COUNT before the JDBC rules see them; the rewritten form is pushed. */
  @Test void statisticalAggregatesPushed() throws Exception {
    final List<String> failures = new ArrayList<>();
    for (String e : Arrays.asList("stddev_pop(x)", "stddev_samp(x)",
        "var_pop(x)", "var_samp(x)", "avg(amount)", "sum(n)",
        "count(distinct flag)", "min(ts)", "max(dt)", "every(flag)",
        "some(flag)")) {
      checkAggregate(e, failures);
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  private static void checkAggregate(String agg, List<String> failures)
      throws SQLException {
    final String sql = "select " + agg + " as e from ";
    final List<String> sqls = new ArrayList<>();
    try (Connection c = calcite()) {
      final List<List<Object>> actual;
      try (Hook.Closeable ignored =
               Hook.QUERY_PLAN.addThread((Consumer<String>) sqls::add)) {
        actual = rows(c, sql + "trino.t");
      } catch (SQLException ex) {
        failures.add(agg + ": FAILED pushed " + sqls + ": " + ex);
        return;
      }
      final String plan = plan(c, sql + "trino.t");
      final List<List<Object>> expected =
          rows(c, sql + "calc.t");
      final boolean isPushed = plan.startsWith("JdbcToEnumerable")
          || !plan.contains("EnumerableAggregate");
      System.out.println("AGGREGATE|" + agg + "|"
          + (isPushed ? "pushed" : "calcite") + "|" + sqls);
      if (!same(expected, actual)) {
        failures.add(agg + ": expected " + expected + " got " + actual);
      }
      if (!isPushed) {
        failures.add(agg + ": not pushed; plan " + plan);
      }
    }
  }

  @Test void topNPushed() throws Exception {
    assertPlan("select id, amount from trino.t order by amount desc"
            + " nulls last limit 2",
        "[[1, 12.50], [3, 0.00]]", "FETCH NEXT 2 ROWS ONLY");
    assertPlan("select id from trino.t order by id offset 1 rows"
            + " fetch next 2 rows only",
        "[[2], [3]]", "OFFSET 1 ROWS");
  }

  /** EXISTS and IN become a SEMI join, and JdbcJoinRule declines SEMI and
   * ANTI joins (they have fewer columns than a join), so Calcite runs the
   * semi-join over two pushed scans. */
  @Test void existsAndInStayInCalciteAsSemiJoin() throws Exception {
    assertKept("select id from trino.t where exists"
            + " (select 1 from trino.u where u.id = t.id)",
        "[[1], [2]]");
    assertKept("select id from trino.t where id in (select id from trino.u)",
        "[[1], [2]]");
  }

  /** NOT EXISTS is decorrelated into a LEFT JOIN on an aggregate, which is
   * pushed whole. */
  @Test void notExistsPushedAsLeftJoin() throws Exception {
    assertPlan("select id from trino.t where not exists"
            + " (select 1 from trino.u where u.id = t.id)",
        "[[3], [4]]", "LEFT JOIN");
  }

  /** A join condition that calls a function is pushed: the function is
   * projected below the join, and the join compares the projected columns. */
  @Test void functionJoinConditionPushedThroughProject() throws Exception {
    assertPlan("select t.id, u.label from trino.t join trino.u"
            + " on upper(t.name) = upper(u.label)",
        "[[1, ALPHA], [2, beta gamma]]", "UPPER(\"name\")");
  }

  /** GROUPING SETS: JdbcAggregateRule declines an aggregate with more than
   * one group set ([CALCITE-734]). */
  @Test void groupingSetsStayInCalcite() throws Exception {
    assertKept("select flag, n > 0 as p, count(*) as c from trino.t"
            + " group by grouping sets ((flag), (n > 0))",
        null);
  }

  /** A join condition with a function over columns of both inputs cannot
   * be projected below the join, and JdbcJoinRule.canJoinOnCondition accepts
   * only comparisons, AND/OR, IS [NOT] NULL/TRUE/FALSE and CAST, so Calcite
   * runs the join. */
  @Test void functionOfBothSidesJoinConditionStaysInCalcite()
      throws Exception {
    assertKept("select t.id, u.label from trino.t join trino.u"
            + " on position(lower(u.label) in lower(t.name)) > 0",
        "[[1, ALPHA], [2, beta gamma]]");
  }

  @Test void typeRoundTrips() throws Exception {
    final List<String> failures = new ArrayList<>();
    final String[][] cases = {
        // ROW and MAP are typed ANY by JdbcSchema and pass through as the
        // driver's objects, field names included.
        {"r", "{a=7, b=seven}"},
        {"m", "{k1=1, k2=2}"},
        {"arr", "[1, 2, 3]"},
        {"tm", "10:20:30.123"},
        // Read as an instant: 10:20:30.123 at +05:30
        {"tstz", "2024-03-15T04:50:30.123Z"},
        {"vb", "cafe"},
        {"c", "ab   "},
        {"dcm", "12345678901234567890.0123456789"},
        {"real_col", "1.25"},
        {"tiny", "7"},
        {"small", "300"},
    };
    try (Connection c = calcite()) {
      for (String[] cs : cases) {
        final String sql = "select " + cs[0] + " from trino.types_t";
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
          rs.next();
          final Object o = rs.getObject(1);
          final String type = rs.getMetaData().getColumnTypeName(1);
          final String got = cs[0].equals("tstz")
              ? ((java.sql.Timestamp) o).toInstant().toString()
              : render(o);
          System.out.println("ROUNDTRIP|" + cs[0] + "|" + type + "|"
              + (o == null ? "null" : o.getClass().getName()) + "|" + got);
          if (!cs[1].equals(got)) {
            failures.add(cs[0] + " (" + type + "): expected " + cs[1]
                + " got " + got);
          }
        } catch (SQLException e) {
          System.out.println("ROUNDTRIP|" + cs[0] + "|error|" + e);
          failures.add(cs[0] + ": " + e);
        }
      }
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  /** Casts to Trino's nested and zoned types, pushed through getCastSpec. */
  @Test void nestedAndZonedCastsPushed() throws Exception {
    final String[][] cases = {
        {"cast(arr as bigint array)", "BIGINT ARRAY", "[1, 2, 3]"},
        {"cast(m as map<varchar, bigint>)", "MAP<", "{k1=1, k2=2}"},
        {"cast(ts6 as varchar)", "VARCHAR", "2024-03-15 10:20:30.123456"},
        {"cast(c as varchar(5))", "VARCHAR(5)", "ab   "},
        {"cast(tm as varchar)", "VARCHAR", "10:20:30.123"},
    };
    final List<String> failures = new ArrayList<>();
    try (Connection c = calcite()) {
      for (String[] cs : cases) {
        final List<String> sqls = new ArrayList<>();
        final String sql = "select " + cs[0] + " from trino.types_t";
        try (Hook.Closeable ignored =
                 Hook.QUERY_PLAN.addThread((Consumer<String>) sqls::add);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
          rs.next();
          final String got = render(rs.getObject(1));
          System.out.println("CAST|" + cs[0] + "|" + got + "|" + sqls);
          if (!cs[2].equals(got)
              || sqls.stream().noneMatch(q -> q.contains(cs[1]))) {
            failures.add(cs[0] + ": got " + got + " via " + sqls);
          }
        } catch (SQLException e) {
          System.out.println("CAST|" + cs[0] + "|error|" + sqls + "|" + e);
          failures.add(cs[0] + ": " + sqls + " " + e);
        }
      }
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  /** A cast to ROW renders as Trino SQL that Trino runs, but the JDBC
   * adapter cannot read the result: it reads a ROW-typed column's fields by
   * reflection on the driver's {@code io.trino.jdbc.Row}, which has no such
   * fields. The test runs the pushed SQL on Trino directly and pins the
   * adapter's failure, so that a fix in the adapter shows up here. */
  @Test void rowCastRunsOnTrinoButAdapterCannotReadRow() throws Exception {
    final List<String> sqls = new ArrayList<>();
    String adapterError = null;
    try (Connection c = calcite();
         Hook.Closeable ignored =
             Hook.QUERY_PLAN.addThread((Consumer<String>) sqls::add);
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery("select cast(r as row(a bigint, b varchar))"
             + " from trino.types_t")) {
      rs.next();
      rs.getObject(1);
    } catch (SQLException e) {
      adapterError = e.getMessage();
    }
    assertEquals("[SELECT CAST(\"r\" AS ROW(\"a\" BIGINT, \"b\" VARCHAR))\n"
        + "FROM \"memory\".\"default\".\"types_t\"]", sqls.toString());
    try (Connection t = trino(); Statement s = t.createStatement();
         ResultSet rs = s.executeQuery(sqls.get(0))) {
      rs.next();
      assertEquals("{a=7, b=seven}", render(rs.getObject(1)));
    }
    System.out.println("ROUNDTRIP-GAP|row cast|adapter error " + adapterError);
    assertTrue(adapterError != null
            && adapterError.contains("NoSuchFieldException: a"),
        () -> "the adapter now reads the ROW result; move this case to"
            + " nestedAndZonedCastsPushed");
  }

  /** Values the JDBC adapter reads wrongly for reasons outside the dialect.
   * Each row is the column, the value Trino holds, the value Calcite reads
   * today, and the reason; the test pins today's value so that a fix in the
   * adapter shows up here and moves the column to {@link #typeRoundTrips}. */
  private static final String[][] ADAPTER_GAPS = {
      {"ts6", "2024-03-15 10:20:30.123456", "2024-03-15 10:20:30.123",
          "Calcite holds TIMESTAMP values as epoch milliseconds"},
  };

  @Test void typeRoundTripAdapterGaps() throws Exception {
    final List<String> failures = new ArrayList<>();
    try (Connection c = calcite(); Connection t = trino()) {
      for (String[] gap : ADAPTER_GAPS) {
        final String got;
        try (Statement s = c.createStatement();
             ResultSet rs =
                 s.executeQuery("select " + gap[0] + " from trino.types_t")) {
          rs.next();
          got = render(rs.getObject(1));
        }
        final String direct;
        try (Statement s = t.createStatement();
             ResultSet rs = s.executeQuery("select " + gap[0]
                 + " from memory.default.types_t")) {
          rs.next();
          final Object o = rs.getObject(1);
          direct = o.getClass().getName() + " " + o;
        }
        System.out.println("ROUNDTRIP-GAP|" + gap[0] + "|trino holds " + gap[1]
            + "|calcite reads " + got + "|driver getObject " + direct + "|"
            + gap[3]);
        if (!gap[2].equals(got)) {
          failures.add(gap[0] + ": Calcite now reads " + got + " (was "
              + gap[2] + "); if it is " + gap[1] + ", move it to"
              + " typeRoundTrips");
        }
      }
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  private static String render(Object o) throws SQLException {
    if (o instanceof java.sql.Time) {
      final java.text.SimpleDateFormat f =
          new java.text.SimpleDateFormat("HH:mm:ss.SSS", java.util.Locale.ROOT);
      return f.format(o);
    }
    if (o instanceof byte[]) {
      final StringBuilder b = new StringBuilder();
      for (byte x : (byte[]) o) {
        b.append(String.format("%02x", x));
      }
      return b.toString();
    }
    if (o instanceof java.sql.Array) {
      return Arrays.toString((Object[]) ((java.sql.Array) o).getArray());
    }
    if (o instanceof java.sql.Struct) {
      return "{" + Arrays.stream(((java.sql.Struct) o).getAttributes())
          .map(String::valueOf).collect(Collectors.joining(", ")) + "}";
    }
    if (o instanceof Object[]) {
      return "{" + Arrays.stream((Object[]) o)
          .map(String::valueOf).collect(Collectors.joining(", ")) + "}";
    }
    return String.valueOf(o);
  }

  private static void assertPlan(String sql, String expectedRows,
      String expectedSqlFragment) throws SQLException {
    final List<String> sqls = new ArrayList<>();
    try (Connection c = calcite()) {
      final List<List<Object>> rows;
      try (Hook.Closeable ignored =
               Hook.QUERY_PLAN.addThread((Consumer<String>) sqls::add)) {
        rows = rows(c, sql);
      }
      final String plan = plan(c, sql);
      System.out.println("PLAN|" + sql);
      System.out.println(plan + "SQL|" + sqls);
      if (expectedRows != null) {
        assertEquals(expectedRows, rows.toString(), sql);
      }
      assertTrue(sqls.stream().anyMatch(q -> q.contains(expectedSqlFragment)),
          () -> "expected pushed SQL containing [" + expectedSqlFragment
              + "] but got " + sqls + "\nplan " + plan);
    }
  }

  /** Asserts that Calcite answers a query correctly with part of it kept
   * out of the pushed SQL: the plan has an Enumerable operator above the
   * JDBC scans. */
  private static void assertKept(String sql, String expectedRows)
      throws SQLException {
    final List<String> sqls = new ArrayList<>();
    try (Connection c = calcite()) {
      final List<List<Object>> rows;
      try (Hook.Closeable ignored =
               Hook.QUERY_PLAN.addThread((Consumer<String>) sqls::add)) {
        rows = rows(c, sql);
      }
      final String plan = plan(c, sql);
      final List<List<Object>> expected =
          rows(c, sql.replace("trino.", "calc."));
      System.out.println("KEPT|" + sql);
      System.out.println(plan + "SQL|" + sqls);
      if (expectedRows != null) {
        assertEquals(expectedRows, rows.toString(), sql);
      }
      assertTrue(same(expected, rows), sql + ": " + expected + " vs " + rows);
      assertTrue(!plan.startsWith("JdbcToEnumerableConverter"),
          () -> "expected part of the plan in Calcite: " + plan);
    }
  }

  private static boolean same(List<List<Object>> a, List<List<Object>> b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      final List<Object> ra = a.get(i);
      final List<Object> rb = b.get(i);
      if (ra.size() != rb.size()) {
        return false;
      }
      for (int j = 0; j < ra.size(); j++) {
        if (!sameValue(ra.get(j), rb.get(j))) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean sameValue(Object a, Object b) {
    if (a == null || b == null) {
      return a == b;
    }
    if (a instanceof Number && b instanceof Number) {
      if (a instanceof Double || a instanceof Float
          || b instanceof Double || b instanceof Float) {
        final double da = ((Number) a).doubleValue();
        final double db = ((Number) b).doubleValue();
        return Math.abs(da - db) <= 1e-9 * Math.max(1d, Math.abs(da));
      }
      return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString()))
          == 0;
    }
    return a.toString().equals(b.toString());
  }

  /** Returns a query's rows, sorted by their string form. */
  private static List<List<Object>> rows(Connection c, String sql)
      throws SQLException {
    final List<List<Object>> rows = unsortedRows(c, sql);
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  private static List<List<Object>> unsortedRows(Connection c, String sql)
      throws SQLException {
    final List<List<Object>> rows = new ArrayList<>();
    try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
      final ResultSetMetaData md = rs.getMetaData();
      while (rs.next()) {
        final List<Object> row = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          row.add(rs.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  private static String plan(Connection c, String sql) throws SQLException {
    final StringBuilder b = new StringBuilder();
    try (Statement s = c.createStatement();
         ResultSet rs = s.executeQuery("explain plan for " + sql)) {
      while (rs.next()) {
        b.append(rs.getString(1));
      }
    }
    return b.toString();
  }

  private static Connection trino() throws SQLException {
    final Properties p = new Properties();
    p.setProperty("user", "calcite");
    return DriverManager.getConnection("jdbc:trino://127.0.0.1:" + port, p);
  }

  private static Connection calcite() throws SQLException {
    final Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("jdbcDriver", "io.trino.jdbc.TrinoDriver");
    operand.put("jdbcUrl", "jdbc:trino://127.0.0.1:" + port + "/memory/default");
    operand.put("jdbcUser", "calcite");
    operand.put("jdbcCatalog", "memory");
    operand.put("jdbcSchema", "default");
    final Map<String, Object> trino = new LinkedHashMap<>();
    trino.put("name", "trino");
    trino.put("type", "custom");
    trino.put("factory", "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory");
    trino.put("operand", operand);
    final Map<String, Object> calc = new LinkedHashMap<>();
    calc.put("name", "calc");
    calc.put("tables",
        Arrays.asList(
            view("t", "select * from (values " + CALCITE_ROWS + ") as v("
                + quoted(COLUMNS) + ")"),
            view("u", "select * from (values (1, cast('ALPHA' as varchar)),"
                + " (2, cast('beta gamma' as varchar)),"
                + " (5, cast('none' as varchar))) as v(\"id\", \"label\")")));
    final Map<String, Object> root = new LinkedHashMap<>();
    root.put("version", "1.0");
    root.put("defaultSchema", "trino");
    root.put("schemas", Arrays.asList(trino, calc));
    final String model;
    try {
      model = "inline:" + new ObjectMapper().writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
    final Properties p = new Properties();
    p.setProperty("model", model);
    p.setProperty("lex", "JAVA");
    p.setProperty("caseSensitive", "false");
    p.setProperty("fun", "postgresql");
    return DriverManager.getConnection("jdbc:calcite:", p);
  }

  private static Map<String, Object> view(String name, String sql) {
    final Map<String, Object> view = new LinkedHashMap<>();
    view.put("name", name);
    view.put("type", "view");
    view.put("sql", sql);
    return view;
  }

  private static String quoted(String columns) {
    return Arrays.stream(columns.split(", "))
        .map(c -> "\"" + c + "\"").collect(Collectors.joining(", "));
  }

  private static void awaitStarted() throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
    while (System.nanoTime() < deadline) {
      try {
        final HttpURLConnection h =
            (HttpURLConnection) new URL("http://127.0.0.1:" + port + "/v1/info")
                .openConnection();
        h.setConnectTimeout(2000);
        h.setReadTimeout(2000);
        if (h.getResponseCode() == 200) {
          final String body = read(h.getInputStream());
          if (body.contains("\"starting\":false")) {
            return;
          }
        }
      } catch (IOException e) {
        // The server is not listening yet; poll again until the deadline.
      }
      Thread.sleep(1000);
    }
    fail("Trino did not start within 3 minutes; logs:\n"
        + docker("logs", "--tail", "50", containerId));
  }

  private static String docker(String... args) throws Exception {
    final List<String> cmd = new ArrayList<>();
    cmd.add("docker");
    cmd.addAll(Arrays.asList(args));
    final Process p =
        new ProcessBuilder(cmd).redirectErrorStream(true).start();
    final String out = read(p.getInputStream());
    if (p.waitFor() != 0) {
      throw new IllegalStateException(cmd + " failed: " + out);
    }
    return out;
  }

  private static String read(InputStream in) throws IOException {
    try (BufferedReader r =
             new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      return r.lines().collect(Collectors.joining("\n"));
    }
  }
}
