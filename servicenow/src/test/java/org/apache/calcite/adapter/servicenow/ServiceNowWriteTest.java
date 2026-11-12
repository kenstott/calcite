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
import org.apache.calcite.schema.Table;

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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * INSERT, UPDATE and DELETE through SQL, against the local stub of the Table API and Import Set
 * API. The Import Set responses are the shape seen on dev453859 on 2026-10-12.
 */
class ServiceNowWriteTest {

  @TempDir Path cache;

  /** The transform map "Notification" from imp_notification to incident, as on dev453859. */
  private static final String NOTIFICATION_MAP = "[{"
      + "\"sys_id\": \"000000000000000000000000000000m1\", \"name\": \"Notification\", \"source_table\": \"imp_notification\","
      + " \"target_table\": \"incident\", \"active\": \"true\"}]";

  private static final String NOTIFICATION_ENTRIES = "["
      + entry("e1", "message", "short_description", false) + ","
      + entry("e2", "uuid", "correlation_id", false) + ","
      + entry("e3", "category", "number", true) + "]";

  private static String entry(String id, String source, String target, boolean scripted) {
    return "{\"sys_id\": \"0000000000000000000000000000000" + id.charAt(1) + id.charAt(0)
        + "\", \"map\": \"000000000000000000000000000000m1\", \"source_field\": \"" + source
        + "\", \"target_field\": \"" + target + "\", \"use_source_script\": \"" + scripted
        + "\"}";
  }

  private Connection connect(FixtureServer server, String extra) throws SQLException {
    return DriverManager.getConnection("jdbc:servicenow:instanceUrl=" + server.url()
        + ";authType=basic;username=" + FixtureServer.USER + ";password=" + FixtureServer.PASSWORD
        + ";catalogCacheDirectory=" + cache + ";pageSize=2;tables=incident,sys_user" + extra);
  }

  private static int update(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      return stmt.executeUpdate(sql);
    }
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

  private static String rootMessage(Throwable e) {
    Throwable t = e;
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t.getMessage();
  }

  /** Every message in the cause chain, outermost first. */
  private static String messages(Throwable e) {
    final StringBuilder text = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      text.append(t.getMessage()).append('\n');
    }
    return text.toString();
  }

  private static ServiceNowTable incident(Connection conn) throws SQLException {
    final Table table = conn.unwrap(CalciteConnection.class).getRootSchema()
        .subSchemas().get("servicenow").tables().get("incident");
    return (ServiceNowTable) table;
  }

  @Test void anInsertIsAPostOfTheColumnsItSets() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      incident(conn).takeInsertedKeys();
      assertThat(update(conn, "INSERT INTO incident (short_description, priority, active, "
          + "opened_at, caller_id) VALUES ('Desk on fire', 1, true, "
          + "TIMESTAMP '2026-10-12 09:30:00', '000000000000000000000000000000b1')"), is(1));
      final List<FixtureServer.Seen> writes = server.writes();
      assertThat(writes.size(), is(1));
      assertThat(writes.get(0).method, is("POST"));
      assertThat(writes.get(0).table, is("incident"));
      // Stored values, in column order: booleans and integers as text, a date-time in UTC, a
      // reference's sys_id
      assertThat(writes.get(0).body.toString(), is("{\"active\":\"true\","
          + "\"caller_id\":\"000000000000000000000000000000b1\","
          + "\"opened_at\":\"2026-10-12 09:30:00\",\"priority\":\"1\","
          + "\"short_description\":\"Desk on fire\"}"));
      // The sys_id the instance assigned is what RETURNING reads the row back by
      final List<String> keys = incident(conn).takeInsertedKeys();
      assertThat(keys.size(), is(1));
      assertThat(strings(conn, "SELECT short_description FROM incident WHERE sys_id = '"
          + keys.get(0) + "'"), contains("Desk on fire"));
    }
  }

  @Test void systemAndDisplayColumnsAreNotWritten() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      SQLException e = assertThrows(SQLException.class, () -> update(conn,
          "INSERT INTO incident (sys_id, short_description) VALUES ('x', 'y')"));
      assertThat(rootMessage(e), containsString("incident.sys_id is a system field"));
      e = assertThrows(SQLException.class, () -> update(conn,
          "INSERT INTO incident (caller_id__display) VALUES ('Abel Tuter')"));
      assertThat(rootMessage(e), containsString("write the referenced record's sys_id to "
          + "caller_id"));
      assertThat(server.writes().isEmpty(), is(true));
    }
  }

  @Test void anUpdateIsAPatchOfEachMatchingRecordBySysId() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(update(conn, "UPDATE incident SET priority = 4, closed_at = NULL "
          + "WHERE number IN ('INC0000001', 'INC0000002')"), is(2));
      final List<String> patched = new ArrayList<>();
      for (FixtureServer.Seen seen : server.writes()) {
        assertThat(seen.method, is("PATCH"));
        // NULL is the empty value
        assertThat(seen.body.toString(), is("{\"priority\":\"4\",\"closed_at\":\"\"}"));
        patched.add(seen.sysId);
      }
      assertThat(patched, contains("000000000000000000000000000000c1",
          "000000000000000000000000000000c2"));
    }
  }

  @Test void aDeleteRemovesEachMatchingRecordBySysId() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      assertThat(update(conn, "DELETE FROM incident WHERE number = 'INC0000003'"), is(1));
      assertThat(server.writes().size(), is(1));
      assertThat(server.writes().get(0).method, is("DELETE"));
      assertThat(strings(conn, "SELECT number FROM incident WHERE number = 'INC0000003'")
          .isEmpty(), is(true));
    }
  }

  /**
   * An UPDATE or DELETE reads only the sys_id of each row it writes and the fields its filter
   * reads: a field the instance does not return to this user (sys_user's accumulated_roles on
   * dev453859) is never asked for, so it cannot fail the write.
   */
  @Test void anUpdateOrDeleteReadsOnlyTheSysIdAndItsFilter() throws Exception {
    for (String sql : new String[] {
        "UPDATE incident SET priority = 4 WHERE number = 'INC0000001'",
        "DELETE FROM incident WHERE number = 'INC0000003'"}) {
      try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
        assertThat(update(conn, sql), is(1));
        final List<String> read = new ArrayList<>();
        for (FixtureServer.Seen seen : server.requests("incident")) {
          if (seen.method.equals("GET")) {
            read.add(seen.params.get("sysparm_fields"));
          }
        }
        // Every page of the scan
        assertThat(sql, read.isEmpty(), is(false));
        assertThat(sql, read, everyItem(is("sys_id,number")));
      }
    }
  }

  @Test void aFailureNamesTheRowAndLeavesTheRowsBeforeItWritten() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      final int[] patches = {0};
      server.override(seen -> seen.method.equals("PATCH") && patches[0]++ == 1
          ? FixtureServer.error(403, "Operation Failed", "ACL Exception Update Failed")
          : null);
      final SQLException e = assertThrows(SQLException.class, () -> update(conn,
          "UPDATE incident SET priority = 4 WHERE number IN ('INC0000001', 'INC0000002')"));
      final String message = messages(e);
      assertThat(message, containsString("UPDATE on incident failed at row 2 (sys_id "
          + "000000000000000000000000000000c2); 1 row(s) before it were written"));
      assertThat(message, containsString("HTTP 403 (the instance refused it"));
      assertThat(message, containsString("ACL Exception Update Failed"));
    }
  }

  @Test void anInsertOnABoundTableGoesThroughTheImportSetUnderTheStagingFields()
      throws Exception {
    try (FixtureServer server = new FixtureServer();
         Connection conn = connect(server, ";importSet=incident:imp_notification")) {
      server.addRows("sys_transform_map", NOTIFICATION_MAP);
      server.addRows("sys_transform_entry", NOTIFICATION_ENTRIES);
      server.override(seen -> seen.api.equals("import")
          ? FixtureServer.Reply.json("{\"import_set\":\"ISET0010001\","
              + "\"staging_table\":\"imp_notification\",\"result\":[{\"transform_map\":"
              + "\"Notification\",\"table\":\"incident\",\"display_name\":\"number\","
              + "\"display_value\":\"INC0010003\",\"status\":\"updated\","
              + "\"sys_id\":\"000000000000000000000000000000c1\"}]}").withStatus(201)
          : null);
      incident(conn).takeInsertedKeys();
      assertThat(update(conn, "INSERT INTO incident (short_description) "
          + "VALUES ('Printer 1 is down again')"), is(1));
      final List<FixtureServer.Seen> writes = server.writes();
      assertThat(writes.size(), is(1));
      assertThat(writes.get(0).api, is("import"));
      assertThat(writes.get(0).table, is("imp_notification"));
      assertThat(writes.get(0).body.toString(), is("{\"message\":\"Printer 1 is down again\"}"));
      // A coalesce that updated a record is a written row; RETURNING reads that record
      assertThat(incident(conn).takeInsertedKeys(),
          contains("000000000000000000000000000000c1"));
    }
  }

  @Test void aColumnTheMapCannotCarryIsRefusedBeforeAnythingIsSent() throws Exception {
    try (FixtureServer server = new FixtureServer();
         Connection conn = connect(server, ";importSet=incident:imp_notification")) {
      server.addRows("sys_transform_map", NOTIFICATION_MAP);
      server.addRows("sys_transform_entry", NOTIFICATION_ENTRIES);
      SQLException e = assertThrows(SQLException.class, () -> update(conn,
          "INSERT INTO incident (short_description, priority) VALUES ('a', 2)"));
      assertThat(rootMessage(e), containsString("Cannot insert incident.priority through the "
          + "import set imp_notification: transform map 'Notification' has no field entry"));
      e = assertThrows(SQLException.class, () -> update(conn,
          "INSERT INTO incident (number) VALUES ('INC9')"));
      assertThat(rootMessage(e), containsString("computes it with a script"));
      assertThat(server.writes().isEmpty(), is(true));
    }
  }

  @Test void anImportErrorIsARefusalNamingTheRow() throws Exception {
    try (FixtureServer server = new FixtureServer();
         Connection conn = connect(server, ";importSet=incident:imp_notification")) {
      server.addRows("sys_transform_map", NOTIFICATION_MAP);
      server.addRows("sys_transform_entry", NOTIFICATION_ENTRIES);
      server.override(seen -> seen.api.equals("import")
          ? FixtureServer.Reply.json("{\"import_set\":\"ISET0010002\","
              + "\"staging_table\":\"imp_notification\",\"result\":[{\"transform_map\":"
              + "\"Notification\",\"table\":\"incident\",\"status\":\"error\","
              + "\"error_message\":\"Unable to resolve target record\"}]}").withStatus(201)
          : null);
      final SQLException e = assertThrows(SQLException.class, () -> update(conn,
          "INSERT INTO incident (short_description) VALUES ('a')"));
      assertThat(rootMessage(e), containsString("Transform map 'Notification' refused row 1"));
      assertThat(rootMessage(e), containsString("Unable to resolve target record"));
    }
  }

  @Test void aBindingOfAnUnregisteredTableIsRefused() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
        ServiceNowSchemaFactory.INSTANCE.create(null, "sn",
            new java.util.HashMap<String, Object>() {{
              put("instanceUrl", "https://dev1.service-now.com");
              put("authType", "basic");
              put("username", "u");
              put("password", "p");
              put("tables", "sys_user");
              put("importSet", java.util.Collections.singletonMap("incident",
                  "imp_notification"));
            }}));
    assertThat(e.getMessage(), containsString("binds table 'incident', which is not among the "
        + "registered tables"));
  }

  @Test void theTransformMapsOfOneTargetAreListedWithTheConditionSent() throws Exception {
    try (FixtureServer server = new FixtureServer(); Connection conn = connect(server, "")) {
      server.addRows("sys_transform_map", NOTIFICATION_MAP);
      server.lenientTerms();
      assertThat(strings(conn, "SELECT source_table FROM _servicenow_transform_maps "
          + "WHERE target_table = 'incident'"), contains("imp_notification"));
      assertThat(server.requests("sys_transform_map").get(0).params.get("sysparm_query"),
          containsString("target_table=incident"));
    }
  }
}
