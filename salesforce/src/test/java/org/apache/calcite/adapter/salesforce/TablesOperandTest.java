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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.LikePattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The {@code tables} operand: only the named sObjects are served, so a client that counts or
 * describes every table (pgwire-calcite's catalog) reads those alone. Without it, every queryable
 * sObject is served. Either way the schema serves {@code _salesforce_objects} and
 * {@code _salesforce_columns}, which list what could be named. The instance is a local server
 * answering the sObject list and one describe.
 */
class TablesOperandTest {

  @TempDir Path describeCache;

  private HttpServer server;

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/services/data/v58.0/sobjects/Contact/describe", exchange ->
        answer(exchange, "{\"name\":\"Contact\",\"label\":\"Contact\",\"queryable\":true,"
            + "\"fields\":["
            + "{\"name\":\"Id\",\"label\":\"Contact ID\",\"type\":\"id\",\"length\":18},"
            + "{\"name\":\"LastName\",\"label\":\"Last Name\",\"type\":\"string\","
            + "\"length\":80,\"nillable\":false},"
            + "{\"name\":\"Birthdate\",\"label\":\"Birthdate\",\"type\":\"date\"}]}"));
    server.createContext("/services/data/v58.0/sobjects", exchange ->
        answer(exchange, "{\"sobjects\":["
            + "{\"name\":\"Account\",\"label\":\"Account\",\"queryable\":true},"
            + "{\"name\":\"Contact\",\"label\":\"Contact\",\"queryable\":true},"
            + "{\"name\":\"Lead\",\"label\":\"Lead\",\"queryable\":true},"
            + "{\"name\":\"AccountChangeEvent\",\"label\":\"Account Change Event\","
            + "\"queryable\":false}]}"));
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private static void answer(HttpExchange exchange, String json) throws IOException {
    final byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  private Map<String, Object> operand(Object tables) {
    final Map<String, Object> operand = new HashMap<>();
    operand.put("accessToken", "token");
    operand.put("instanceUrl", "http://127.0.0.1:" + server.getAddress().getPort());
    operand.put("describeCacheDirectory", describeCache.toString());
    operand.put("lowercaseAliases", "false");
    if (tables != null) {
      operand.put("tables", tables);
    }
    return operand;
  }

  /** The sObject tables served: every table but the two listing tables, which are checked. */
  private Set<String> tableNames(Object tables) {
    final Schema schema = new SalesforceSchemaFactory().create(null, "sf", operand(tables));
    final Set<String> names = schema.tables().getNames(LikePattern.any());
    assertThat(names.remove("_salesforce_objects"), is(true));
    assertThat(names.remove("_salesforce_columns"), is(true));
    return names;
  }

  private List<String> rows(String sql) throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:lex=JAVA")) {
      final SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      root.add("sf",
          new SalesforceSchemaFactory().create(root, "sf", operand(Arrays.asList("Account"))));
      final List<String> rows = new ArrayList<>();
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        final int columns = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          final List<String> values = new ArrayList<>();
          for (int i = 1; i <= columns; i++) {
            values.add(rs.getString(i));
          }
          rows.add(String.join("|", values));
        }
      }
      return rows;
    }
  }

  @Test void withoutTablesEveryQueryableSObjectIsServed() {
    assertThat(tableNames(null), containsInAnyOrder("Account", "Contact", "Lead"));
  }

  @Test void aListServesOnlyTheNamedSObjects() {
    assertThat(tableNames(Arrays.asList("Account", "Lead")),
        containsInAnyOrder("Account", "Lead"));
  }

  @Test void commaSeparatedTextServesOnlyTheNamedSObjects() {
    assertThat(tableNames("Account, Contact"), containsInAnyOrder("Account", "Contact"));
  }

  @Test void namesMatchIgnoringCaseAndServeTheSObjectsOwnName() {
    assertThat(tableNames(Arrays.asList("account")), containsInAnyOrder("Account"));
  }

  @Test void anUnknownSObjectIsRefused() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tableNames(Arrays.asList("Account", "Opportunity")));
    assertThat(e.getMessage(),
        is("'tables' names 'Opportunity', which is not a queryable sObject in this org"));
  }

  @Test void aSObjectThatIsNotQueryableIsRefused() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tableNames(Arrays.asList("AccountChangeEvent")));
    assertThat(e.getMessage(),
        is("'tables' names 'AccountChangeEvent', which is not a queryable sObject in this org"));
  }

  @Test void salesforceObjectsListsEveryQueryableSObjectWhateverTablesNames()
      throws SQLException {
    assertThat(rows("SELECT name, label FROM sf._salesforce_objects"),
        contains("Account|Account", "Contact|Contact", "Lead|Lead"));
  }

  @Test void salesforceColumnsListsTheColumnsOfAnSObjectNotServed() throws SQLException {
    assertThat(
        rows("SELECT table_name, column_name, data_type, ordinal_position "
            + "FROM sf._salesforce_columns WHERE table_name = 'Contact'"),
        contains("Contact|Id|character varying|1", "Contact|LastName|character varying|2",
            "Contact|Birthdate|date|3"));
  }

  @Test void salesforceColumnsWithoutATableNameIsRefused() {
    final SQLException e = assertThrows(SQLException.class,
        () -> rows("SELECT column_name FROM sf._salesforce_columns"));
    assertThat(e.getMessage(), containsString("must be queried with table_name = '<sObject>'"));
  }

  @Test void salesforceColumnsOfASObjectThatIsNotQueryableIsRefused() {
    final SQLException e = assertThrows(SQLException.class,
        () -> rows("SELECT column_name FROM sf._salesforce_columns "
            + "WHERE table_name = 'AccountChangeEvent'"));
    assertThat(e.getMessage(),
        containsString("'AccountChangeEvent' is not a queryable sObject in this org"));
  }
}
