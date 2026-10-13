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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * information_schema on the embedded JDBC path (#518): {@code tables} lists every table on the
 * instance, registered or not; {@code columns} filtered by table name reads only that table's
 * dictionary chain; an unfiltered {@code columns} scan is allowed. Only registered tables can be
 * queried. (pgwire-calcite scopes what a role sees to its granted tables; not tested here.)
 */
class InformationSchemaTest {

  @TempDir Path cache;

  /** Connects with only {@code incident} registered. */
  private Connection connect(FixtureServer server) throws SQLException {
    return connect(server, "");
  }

  private Connection connect(FixtureServer server, String extra) throws SQLException {
    return DriverManager.getConnection("jdbc:servicenow:instanceUrl=" + server.url()
        + ";authType=basic;username=" + FixtureServer.USER + ";password=" + FixtureServer.PASSWORD
        + ";catalogCacheDirectory=" + cache + ";tables=incident" + extra);
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

  /** The tables whose dictionary rows were asked for, from each sys_dictionary request. */
  private static List<String> dictionaryReads(FixtureServer server) {
    final List<String> queries = new ArrayList<>();
    for (FixtureServer.Seen seen : server.requests("sys_dictionary")) {
      queries.add(String.valueOf(seen.params.get("sysparm_query")));
    }
    return queries;
  }

  @Test void tablesListsEveryTableOnTheInstance() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server)) {
      final List<String> tables = strings(conn, "SELECT TABLE_NAME FROM information_schema.TABLES "
          + "WHERE TABLE_SCHEMA = 'servicenow'");
      assertThat(tables, hasItems("incident", "task", "sys_user", "bad_table"));
      // listing the tables reads no column metadata
      assertThat(dictionaryReads(server).isEmpty(), is(true));
    }
  }

  @Test void columnsOfOneTableReadOnlyThatTablesDictionaryChain() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server)) {
      final List<String> columns = strings(conn, "SELECT COLUMN_NAME "
          + "FROM information_schema.COLUMNS "
          + "WHERE TABLE_SCHEMA = 'servicenow' AND TABLE_NAME = 'sys_user'");
      assertThat(columns.isEmpty(), is(false));
      assertThat(dictionaryReads(server).isEmpty(), is(false));
      assertThat(dictionaryReads(server), everyItem(containsString("sys_user")));
    }
  }

  @Test void anUnfilteredColumnsScanIsAllowed() throws Exception {
    // bad_table has a column of a type the adapter cannot map; a scan of every table reaches it,
    // so the type is left out as the adapter's error says to
    try (FixtureServer server = new FixtureServer();
         Connection conn = connect(server, ";excludeColumnTypes=mystery_type")) {
      final List<String> tables = strings(conn, "SELECT DISTINCT TABLE_NAME "
          + "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'servicenow'");
      assertThat(tables, hasItems("incident", "task", "sys_user"));
    }
  }

  @Test void aListedTableThatIsNotRegisteredCannotBeQueried() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server)) {
      final SQLException e = assertThrows(SQLException.class,
          () -> strings(conn, "SELECT * FROM sys_user"));
      assertThat(e.getMessage(), containsString("sys_user"));
    }
  }
}
