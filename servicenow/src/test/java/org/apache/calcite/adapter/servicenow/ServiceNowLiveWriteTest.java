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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Writes against a live ServiceNow instance: INSERT, UPDATE and DELETE of incidents the test
 * creates itself, directly and through the import set bound to incident. Every record it writes
 * is marked ZZ_LIVE_ and deleted in a finally block, with the staging rows and import sets of the
 * import route. Use a developer instance, never a production one.
 *
 * <p>Isolated: it changes incident while it runs, and the read tests count incident's rows.
 * Skipped unless {@code govdata/.env.prod} holds SN_INSTANCE_URL, SN_USERNAME and SN_PASSWORD;
 * runs only with {@code -PincludeTags=integration}.
 */
@Tag("integration")
@Isolated
class ServiceNowLiveWriteTest {

  private static ServiceNowTestCredentials credentials;

  @TempDir static Path catalogCache;

  @BeforeAll static void loadCredentials() throws IOException {
    credentials = ServiceNowTestCredentials.load();
    assumeTrue(credentials != null,
        "no ServiceNow instance configured: SN_INSTANCE_URL, SN_USERNAME, SN_PASSWORD");
  }

  private static Connection connect(String tables, String importSet) throws SQLException {
    final Properties info = new Properties();
    info.setProperty("instanceUrl", credentials.instanceUrl);
    info.setProperty("authType", "basic");
    info.setProperty("username", credentials.username);
    info.setProperty("password", credentials.password);
    info.setProperty("catalogCacheDirectory", catalogCache.toString());
    info.setProperty("tables", tables);
    if (!importSet.isEmpty()) {
      info.setProperty("importSet", importSet);
    }
    final StringBuilder url = new StringBuilder("jdbc:servicenow:");
    for (String key : info.stringPropertyNames()) {
      if (url.length() > "jdbc:servicenow:".length()) {
        url.append(';');
      }
      url.append(key).append('=')
          .append(java.net.URLEncoder.encode(info.getProperty(key),
              java.nio.charset.StandardCharsets.UTF_8));
    }
    return DriverManager.getConnection(url.toString());
  }

  /** A direct REST call, for checking and cleaning up what a write test did. */
  private static com.fasterxml.jackson.databind.JsonNode rest(String method, String path)
      throws Exception {
    final java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder(
            java.net.URI.create(credentials.instanceUrl.replaceAll("/$", "") + path))
        .header("Accept", "application/json")
        .header("Authorization", "Basic " + java.util.Base64.getEncoder().encodeToString(
            (credentials.username + ":" + credentials.password)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        .method(method, java.net.http.HttpRequest.BodyPublishers.noBody())
        .build();
    final java.net.http.HttpResponse<String> response = java.net.http.HttpClient.newHttpClient()
        .send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() >= 300 && response.statusCode() != 404) {
      throw new AssertionError(method + " " + path + ": HTTP " + response.statusCode() + " "
          + response.body());
    }
    return response.body().isEmpty() ? null
        : new com.fasterxml.jackson.databind.ObjectMapper().readTree(response.body());
  }

  private static List<String> column(Connection conn, String sql) throws SQLException {
    final List<String> values = new ArrayList<>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        values.add(rs.getString(1));
      }
    }
    return values;
  }

  private static ServiceNowTable incidentTable(Connection conn) throws SQLException {
    return (ServiceNowTable) conn.unwrap(org.apache.calcite.jdbc.CalciteConnection.class)
        .getRootSchema().subSchemas().get("servicenow").tables().get("incident");
  }

  @Test void insertsUpdatesAndDeletesAnIncidentDirectly() throws Exception {
    final String mark = "ZZ_LIVE_" + java.util.UUID.randomUUID();
    String sysId = null;
    try (Connection conn = connect("incident", "")) {
      incidentTable(conn).takeInsertedKeys();
      try (Statement stmt = conn.createStatement()) {
        assertThat(stmt.executeUpdate("INSERT INTO incident (short_description) VALUES ('"
            + mark + "')"), is(1));
      }
      final List<String> keys = incidentTable(conn).takeInsertedKeys();
      assertThat(keys.size(), is(1));
      sysId = keys.get(0);
      assertThat(column(conn, "SELECT short_description FROM incident WHERE sys_id = '" + sysId
          + "'"), equalTo(java.util.Collections.singletonList(mark)));

      try (Statement stmt = conn.createStatement()) {
        assertThat(stmt.executeUpdate("UPDATE incident SET short_description = '" + mark
            + " updated' WHERE sys_id = '" + sysId + "'"), is(1));
      }
      assertThat(column(conn, "SELECT short_description FROM incident WHERE sys_id = '" + sysId
          + "'"), equalTo(java.util.Collections.singletonList(mark + " updated")));

      try (Statement stmt = conn.createStatement()) {
        assertThat(stmt.executeUpdate("DELETE FROM incident WHERE sys_id = '" + sysId + "'"),
            is(1));
      }
      assertThat(column(conn, "SELECT sys_id FROM incident WHERE sys_id = '" + sysId + "'")
          .isEmpty(), is(true));
      sysId = null;
    } finally {
      if (sysId != null) {
        rest("DELETE", "/api/now/table/incident/" + sysId);
      }
    }
  }

  /**
   * An INSERT through the import set bound to incident on dev453859 (staging table
   * imp_notification, map "Notification": message -> short_description, uuid -> correlation_id,
   * which it coalesces on). A second row with the same correlation_id updates the same record.
   */
  @Test void insertsAnIncidentThroughItsImportSetAndCoalesces() throws Exception {
    final String mark = "ZZ_LIVE_" + java.util.UUID.randomUUID();
    final List<String> created = new ArrayList<>();
    try (Connection conn = connect("incident", "incident:imp_notification")) {
      incidentTable(conn).takeInsertedKeys();
      try (Statement stmt = conn.createStatement()) {
        assertThat(stmt.executeUpdate("INSERT INTO incident (short_description, correlation_id) "
            + "VALUES ('" + mark + "', '" + mark + "')"), is(1));
        created.addAll(incidentTable(conn).takeInsertedKeys());
        assertThat(created.size(), is(1));
        assertThat(column(conn, "SELECT short_description FROM incident WHERE sys_id = '"
            + created.get(0) + "'"), equalTo(java.util.Collections.singletonList(mark)));

        // The map coalesces on correlation_id: the same record is updated, and is the key
        assertThat(stmt.executeUpdate("INSERT INTO incident (short_description, correlation_id) "
            + "VALUES ('" + mark + " again', '" + mark + "')"), is(1));
        assertThat(incidentTable(conn).takeInsertedKeys(), equalTo(created));
        assertThat(column(conn, "SELECT short_description FROM incident WHERE sys_id = '"
            + created.get(0) + "'"),
            equalTo(java.util.Collections.singletonList(mark + " again")));

        // A column the map does not carry is refused before anything is sent
        final SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
            () -> stmt.executeUpdate("INSERT INTO incident (short_description, priority) "
                + "VALUES ('" + mark + "', 2)"));
        assertThat(e.toString() + (e.getCause() == null ? "" : e.getCause().toString()),
            org.hamcrest.Matchers.containsString("incident.priority"));
      }
    } finally {
      for (String sysId : created) {
        rest("DELETE", "/api/now/table/incident/" + sysId);
      }
      // The staging rows and their import set
      final com.fasterxml.jackson.databind.JsonNode staged = rest("GET",
          "/api/now/table/imp_notification?sysparm_fields=sys_id,sys_import_set"
              + "&sysparm_exclude_reference_link=true&sysparm_query=uuid%3D" + mark);
      final Set<String> sets = new HashSet<>();
      for (com.fasterxml.jackson.databind.JsonNode row : staged.path("result")) {
        sets.add(row.path("sys_import_set").asText());
        rest("DELETE", "/api/now/table/imp_notification/" + row.path("sys_id").asText());
      }
      for (String set : sets) {
        rest("DELETE", "/api/now/table/sys_import_set/" + set);
      }
    }
  }
}
