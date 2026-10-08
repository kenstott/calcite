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

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Opens Calcite connections over the adapter with a pushdown observer attached. */
final class PushdownTestSupport {
  private PushdownTestSupport() {}

  /** What one scan pushed. */
  static final class Scan {
    final String table;
    final String query;
    final Set<String> entries;
    final int remaining;

    Scan(String table, String query, Set<String> entries, int remaining) {
      this.table = table;
      this.query = query;
      this.entries = entries;
      this.remaining = remaining;
    }

    @Override public String toString() {
      return table + " query='" + query + "' entries=" + entries + " remaining=" + remaining;
    }
  }

  /** Records scans. */
  static final class Recorder implements PushdownObserver {
    final List<Scan> scans = new ArrayList<>();

    @Override public synchronized void scan(String table, String query, Set<String> entries,
        int remaining) {
      scans.add(new Scan(table, query, new TreeSet<>(entries), remaining));
    }

    /** The scan of the last planning pass that reached the table. */
    synchronized Scan last() {
      if (scans.isEmpty()) {
        throw new AssertionError("no scan was observed");
      }
      return scans.get(scans.size() - 1);
    }
  }

  /**
   * Opens a connection whose schema "sn" is the adapter, with the given extra operands and
   * pushdown entries trusted.
   */
  static Connection open(String url, Map<String, Object> extra, Collection<String> trusted,
      PushdownObserver observer) throws SQLException {
    final Map<String, Object> operand = new HashMap<>();
    operand.put("instanceUrl", url);
    operand.put("authType", "basic");
    operand.put("username", FixtureServer.USER);
    operand.put("password", FixtureServer.PASSWORD);
    operand.putAll(extra);
    if (!trusted.isEmpty()) {
      operand.put("trustPushdown", new ArrayList<>(trusted));
    }
    operand.put("pushdownObserver", observer);
    final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
    final CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
    calcite.getRootSchema().add("sn",
        ServiceNowSchemaFactory.INSTANCE.create(calcite.getRootSchema(), "sn", operand));
    calcite.setSchema("sn");
    return connection;
  }

  /** Runs a query and returns the first column of every row. */
  static List<String> column(Connection connection, String sql) throws SQLException {
    final List<String> values = new ArrayList<>();
    try (Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(sql)) {
      while (rs.next()) {
        values.add(rs.getString(1));
      }
    }
    return values;
  }
}
