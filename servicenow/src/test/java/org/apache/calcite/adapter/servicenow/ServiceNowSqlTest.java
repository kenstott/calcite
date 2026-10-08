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
package org.apache.calcite.adapter.servicenow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SQL through the JDBC driver, the schema factory, the catalog, the Table API client and Calcite,
 * against a local server playing the documented API (fixtures derived from documentation, not
 * captured from an instance).
 */
class ServiceNowSqlTest {

  @TempDir Path cache;

  private Connection connect(FixtureServer server, String extra) throws SQLException {
    return DriverManager.getConnection("jdbc:servicenow:instanceUrl=" + server.url()
        + ";authType=basic;username=" + FixtureServer.USER + ";password=" + FixtureServer.PASSWORD
        + ";catalogCacheDirectory=" + cache + ";pageSize=2" + extra);
  }

  private static List<String> strings(Connection conn, String sql) throws SQLException {
    final List<String> values = new ArrayList<>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        values.add(rs.getString(1));
      }
    }
    return values;
  }

  @Test void tablesAndColumnTypesComeFromTheInstanceMetadata() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      final DatabaseMetaData md = conn.getMetaData();
      final List<String> tables = new ArrayList<>();
      try (ResultSet rs = md.getTables(null, "servicenow", null, null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      assertThat(tables, hasItems("incident", "task", "sys_user"));

      final Map<String, Integer> types = new HashMap<>();
      final Map<String, Integer> nullable = new HashMap<>();
      try (ResultSet rs = md.getColumns(null, "servicenow", "incident", null)) {
        while (rs.next()) {
          types.put(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"));
          nullable.put(rs.getString("COLUMN_NAME"), rs.getInt("NULLABLE"));
        }
      }
      assertThat(types.get("sys_id"), is(Types.VARCHAR));
      assertThat(types.get("priority"), is(Types.INTEGER));
      assertThat(types.get("active"), is(Types.BOOLEAN));
      assertThat(types.get("opened_at"), is(Types.TIMESTAMP));
      assertThat(types.get("start_date"), is(Types.DATE));
      assertThat(types.get("caller_id"), is(Types.VARCHAR));
      assertThat(types.get("caller_id__display"), is(Types.VARCHAR));
      // Every column is nullable without exception: an empty value is NULL for every type
      assertThat(nullable.get("sys_id"), is(DatabaseMetaData.columnNullable));
      assertThat(nullable.get("number"), is(DatabaseMetaData.columnNullable));
    }
  }

  @Test void readsRowsWithTypedValues() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "");
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT number, priority, active, opened_at, closed_at, "
             + "start_date FROM incident ORDER BY number LIMIT 2")) {
      assertThat(rs.next(), is(true));
      assertThat(rs.getString("number"), equalTo("INC0000001"));
      assertThat(rs.getInt("priority"), is(2));
      assertThat(rs.getBoolean("active"), is(true));
      assertThat(rs.getTimestamp("opened_at"),
          equalTo(Timestamp.valueOf("2026-01-01 08:30:00")));
      // An empty value on the wire is NULL, not an empty string or a zero date
      assertThat(rs.getTimestamp("closed_at") == null, is(true));
      assertThat(rs.getDate("start_date").toString(), equalTo("2026-02-01"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString("number"), equalTo("INC0000002"));
    }
  }

  @Test void projectionIsPushedAsSysparmFields() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(strings(conn, "SELECT number FROM incident"),
          contains("INC0000001", "INC0000002", "INC0000003", "INC0000004", "INC0000005"));
      for (FixtureServer.Seen seen : server.requests("incident")) {
        // sys_id rides along for key paging although the query did not select it
        assertThat(seen.params.get("sysparm_fields"), equalTo("sys_id,number"));
        assertThat(seen.params.get("sysparm_display_value"), equalTo("false"));
      }
    }
  }

  @Test void aReferenceFieldGivesTheSysIdAndTheDisplayValue() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "");
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT caller_id, caller_id__display FROM incident ORDER BY number LIMIT 1")) {
        rs.next();
        assertThat(rs.getString(1), equalTo(String.format("%032x", 0xb1)));
        assertThat(rs.getString(2), equalTo("Abel Tuter"));
      }
      // Display values are asked for only when the query selects the display column
      final List<FixtureServer.Seen> requests = server.requests("incident");
      assertThat(requests.get(requests.size() - 1).params.get("sysparm_display_value"),
          equalTo("all"));
    }
  }

  @Test void theSysIdColumnAloneNeedsNoDisplayValues() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      strings(conn, "SELECT caller_id FROM incident");
      for (FixtureServer.Seen seen : server.requests("incident")) {
        assertThat(seen.params.get("sysparm_display_value"), equalTo("false"));
        assertThat(seen.params.get("sysparm_exclude_reference_link"), equalTo("true"));
      }
    }
  }

  @Test void filtersAreEvaluatedByCalciteAndNeverSentToServiceNow() throws Exception {
    // The fixture server rejects any sysparm_query term except the paging key and its order, so
    // this passes only if the filter stays out of the request
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(strings(conn, "SELECT number FROM incident WHERE priority = 2 ORDER BY number"),
          contains("INC0000001", "INC0000004"));
      assertThat(strings(conn,
          "SELECT number FROM incident WHERE short_description LIKE '%3%' OR active = false "
              + "ORDER BY number"),
          contains("INC0000002", "INC0000003", "INC0000004"));
      for (FixtureServer.Seen seen : server.requests("incident")) {
        assertThat(seen.params.get("sysparm_query"), not(containsString("priority")));
        assertThat(seen.params.get("sysparm_query"), not(containsString("active")));
        assertThat(seen.params.get("sysparm_query"), not(containsString("short_description")));
      }
    }
  }

  @Test void aLimitStopsTheScanEarlyWhenNothingFiltersOrSorts() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(strings(conn, "SELECT number FROM incident LIMIT 3").size(), is(3));
      // 5 rows in pages of 2: three requests would be needed to see them all and a fourth to
      // learn the end; three rows need two pages
      assertThat(server.requests("incident").size(), is(2));
    }
  }

  @Test void sortsAreLeftToCalcite() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(strings(conn, "SELECT number FROM incident ORDER BY number DESC LIMIT 2"),
          contains("INC0000005", "INC0000004"));
      for (FixtureServer.Seen seen : server.requests("incident")) {
        assertThat(seen.params.get("sysparm_query"), not(containsString("DESC")));
      }
    }
  }

  @Test void aggregatesAndJoinsRunInCalcite() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(strings(conn, "SELECT COUNT(*) FROM incident"), contains("5"));
      assertThat(strings(conn, "SELECT u.name || ':' || COUNT(*) FROM incident i "
          + "JOIN sys_user u ON i.caller_id = u.sys_id GROUP BY u.name ORDER BY 1"),
          contains("Abel Tuter:3", "Beth Anglin:2"));
    }
  }

  @Test void theTablesOperandNarrowsTheSchema() throws Exception {
    try (FixtureServer server = new FixtureServer();
         Connection conn = connect(server, ";tables=incident,sys_user")) {
      final List<String> tables = new ArrayList<>();
      try (ResultSet rs = conn.getMetaData().getTables(null, "servicenow", null, null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      assertThat(tables, containsInAnyOrder("incident", "sys_user"));
    }
  }

  @Test void aTableNamedInTheOperandButMissingFromTheInstanceIsAnError() throws Exception {
    try (FixtureServer server = new FixtureServer();
         Connection conn = connect(server, ";tables=incident,no_such_table")) {
      final SQLException e = assertThrows(SQLException.class,
          () -> strings(conn, "SELECT number FROM incident"));
      assertThat(rootMessage(e), containsString("no_such_table"));
    }
  }

  @Test void aTableWithAnUnmappableColumnFailsOnQueryNotOnConnect() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(strings(conn, "SELECT number FROM incident LIMIT 1").size(), is(1));
      final SQLException e = assertThrows(SQLException.class,
          () -> strings(conn, "SELECT sys_id FROM bad_table"));
      assertThat(rootMessage(e), containsString("mystery_type"));
    }
  }

  @Test void aServerErrorMidScanSurfacesAsAnError() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      strings(conn, "SELECT number FROM incident LIMIT 1");
      final int[] calls = {0};
      server.override(seen -> seen.table.equals("incident") && calls[0]++ == 1
          ? FixtureServer.error(500, "Transaction cancelled: maximum execution time exceeded", "")
          : null);
      // The runtime exception passes through the JDBC cursor unwrapped
      final ServiceNowException e = assertThrows(ServiceNowException.class,
          () -> strings(conn, "SELECT number FROM incident"));
      assertThat(e.getStatus(), is(500));
      assertThat(e.getMessage(), containsString("maximum execution time exceeded"));
    }
  }

  @Test void theDriverRejectsAnUnknownAuthType() {
    final Exception e = assertThrows(Exception.class, () -> DriverManager.getConnection(
        "jdbc:servicenow:instanceUrl=https://dev1.service-now.com;authType=magic"));
    assertThat(rootMessage(e), containsString("Unknown authType 'magic'"));
  }

  private static String rootMessage(Throwable t) {
    Throwable cause = t;
    final StringBuilder all = new StringBuilder();
    while (cause != null) {
      all.append(cause.getMessage()).append(" | ");
      cause = cause.getCause();
    }
    return all.toString();
  }
}
