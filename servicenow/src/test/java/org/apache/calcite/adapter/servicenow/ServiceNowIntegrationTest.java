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

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test against a live ServiceNow instance.
 *
 * <p>First run against a live instance on 2026-10-12 (dev453859, a developer instance): all
 * tests pass. It is skipped unless {@code govdata/.env.prod} holds SN_INSTANCE_URL, SN_USERNAME
 * and SN_PASSWORD, and it runs only with {@code -PincludeTags=integration}. What the adapter
 * assumes about responses is in {@code src/test/resources/servicenow/doc-derived/README.md}.
 *
 * <p>It needs the instance's usual demo data: more than 7 rows in {@code incident}, and
 * incidents with a caller.
 */
@Tag("integration")
class ServiceNowIntegrationTest {

  private static ServiceNowTestCredentials credentials;

  /** Catalog cache shared by this class's tests, so the metadata is read once. */
  @TempDir static Path catalogCache;

  @BeforeAll static void loadCredentials() throws IOException {
    credentials = ServiceNowTestCredentials.load();
    assumeTrue(credentials != null,
        "no ServiceNow instance configured: SN_INSTANCE_URL, SN_USERNAME, SN_PASSWORD");
  }

  /** Connects with incident and sys_user registered. */
  private static Connection connect(int pageSize) throws SQLException {
    return connect(pageSize, "incident,sys_user");
  }

  private static Connection connect(int pageSize, String tables) throws SQLException {
    final Properties info = new Properties();
    if (!tables.isEmpty()) {
      info.setProperty("tables", tables);
    }
    info.setProperty("instanceUrl", credentials.instanceUrl);
    info.setProperty("authType", "basic");
    info.setProperty("username", credentials.username);
    info.setProperty("password", credentials.password);
    info.setProperty("catalogCacheDirectory", catalogCache.toString());
    info.setProperty("pageSize", Integer.toString(pageSize));
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

  /**
   * The number of sys_db_object rows the user can read, from one unpaged Table API request rather
   * than the adapter's paging. Not the Aggregate API's count: on dev453859 that counts 37 rows
   * (6,591) that no Table API request returns to this user (6,554).
   */
  private static long tableCount() throws Exception {
    final java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder(
            java.net.URI.create(credentials.instanceUrl.replaceAll("/$", "")
                + "/api/now/table/sys_db_object?sysparm_limit=100000&sysparm_fields=sys_id"))
        .header("Accept", "application/json")
        .header("Authorization", "Basic " + java.util.Base64.getEncoder().encodeToString(
            (credentials.username + ":" + credentials.password)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        .build();
    final String body = java.net.http.HttpClient.newHttpClient()
        .send(request, java.net.http.HttpResponse.BodyHandlers.ofString()).body();
    return new com.fasterxml.jackson.databind.ObjectMapper().readTree(body).path("result").size();
  }

  private static long count(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  @Test void connectsAndListsTables() throws SQLException {
    try (Connection conn = connect(1000)) {
      final Set<String> tables = new HashSet<>();
      final DatabaseMetaData md = conn.getMetaData();
      try (ResultSet rs = md.getTables(null, "servicenow", null, null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      assertThat(tables, equalTo(new HashSet<>(java.util.Arrays.asList("incident", "sys_user",
          ListingTables.TABLES, ListingTables.COLUMNS, ListingTables.TRANSFORM_MAPS))));
    }
  }

  @Test void everyTableIsListedWithItsLabelAndTheme() throws Exception {
    try (Connection conn = connect(1000, "")) {
      final java.util.Map<String, String> themes = new java.util.HashMap<>();
      final java.util.Map<String, String> labels = new java.util.HashMap<>();
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT name, label, theme FROM _servicenow_tables")) {
        while (rs.next()) {
          themes.put(rs.getString(1), rs.getString(3));
          labels.put(rs.getString(1), rs.getString(2));
        }
      }
      // A developer instance has thousands of tables; the listing must not lose any to paging
      assertThat((long) themes.size(), is(tableCount()));
      assertThat(themes.get("incident"), equalTo("Incident"));
      assertThat(labels.get("incident"), equalTo("Incident"));
      assertThat(themes.get("change_request"), equalTo("Change Request"));
      assertThat(themes.get("cmdb_ci"), equalTo("Configuration Management (CMDB)"));
    }
  }

  @Test void theColumnsOfOneTableComeFromItsDictionaryChain() throws SQLException {
    try (Connection conn = connect(1000, "");
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT column_name, data_type FROM _servicenow_columns "
             + "WHERE table_name = 'incident'")) {
      final java.util.Map<String, String> types = new java.util.HashMap<>();
      while (rs.next()) {
        types.put(rs.getString(1), rs.getString(2));
      }
      // number is incident's own; short_description and caller_id's sibling assigned_to are task's
      assertThat(types.get("sys_id"), equalTo("character varying"));
      assertThat(types.get("number"), equalTo("character varying"));
      assertThat(types.get("short_description"), equalTo("character varying"));
      assertThat(types.get("opened_at"), equalTo("timestamp"));
      assertThat(types.get("caller_id__display"), equalTo("character varying"));
    }
  }

  @Test void readsRowsFromIncident() throws SQLException {
    try (Connection conn = connect(1000); Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT sys_id, number, short_description, opened_at FROM incident LIMIT 5")) {
      int rows = 0;
      while (rs.next()) {
        rows++;
        assertThat(rs.getString("sys_id").length(), is(32));
        assertThat(rs.getString("number").startsWith("INC"), is(true));
      }
      assertThat(rows, greaterThan(0));
    }
  }

  @Test void projectionReturnsOnlyTheSelectedColumns() throws SQLException {
    try (Connection conn = connect(1000); Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT number, priority FROM incident LIMIT 3")) {
      final ResultSetMetaData md = rs.getMetaData();
      assertThat(md.getColumnCount(), is(2));
      assertThat(md.getColumnName(1), equalTo("number"));
      assertThat(md.getColumnName(2), equalTo("priority"));
      while (rs.next()) {
        assertThat(rs.getInt("priority"), greaterThan(0));
      }
    }
  }

  @Test void aReferenceFieldHasItsSysIdAndItsDisplayValue() throws SQLException {
    try (Connection conn = connect(1000); Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT caller_id, caller_id__display FROM incident "
             + "WHERE caller_id IS NOT NULL LIMIT 3")) {
      int rows = 0;
      while (rs.next()) {
        rows++;
        final String sysId = rs.getString(1);
        assertThat(sysId.length(), is(32));
        final String display = rs.getString(2);
        assertThat(display == null || display.isEmpty(), is(false));
        // The display value of a user reference is the user's name
        try (Statement lookup = conn.createStatement();
             ResultSet user = lookup.executeQuery(
                 "SELECT name FROM sys_user WHERE sys_id = '" + sysId + "'")) {
          assertThat(user.next(), is(true));
          assertThat(user.getString(1), equalTo(display));
        }
      }
      assertThat(rows, greaterThan(0));
    }
  }

  @Test void pagingPastOnePageReturnsEveryRowOnce() throws SQLException {
    final long total;
    try (Connection conn = connect(1000)) {
      total = count(conn, "SELECT COUNT(*) FROM incident");
    }
    assumeTrue(total > 7, "needs more than 7 incidents, found " + total);
    try (Connection conn = connect(7); Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT sys_id FROM incident")) {
      final List<String> ids = new ArrayList<>();
      while (rs.next()) {
        ids.add(rs.getString(1));
      }
      assertThat((long) ids.size(), is(total));
      assertThat(new HashSet<>(ids).size(), is(ids.size()));
    }
  }

  @Test void filtersAreEvaluatedByCalcite() throws SQLException {
    try (Connection conn = connect(1000)) {
      final long all = count(conn, "SELECT COUNT(*) FROM incident");
      final long active = count(conn, "SELECT COUNT(*) FROM incident WHERE active = true");
      final long inactive = count(conn, "SELECT COUNT(*) FROM incident WHERE active = false");
      assertThat(active + inactive, is(all));
    }
  }

  @Test void operandMapIsAcceptedByTheFactoryDirectly() {
    final Map<String, Object> operand = credentials.operand(catalogCache);
    ServiceNowSchemaFactory.INSTANCE.create(null, "servicenow", operand);
  }
}
