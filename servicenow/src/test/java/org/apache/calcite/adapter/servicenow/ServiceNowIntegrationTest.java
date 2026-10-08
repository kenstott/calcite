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
 * <p>NEVER RUN so far: no instance was available when it was written. It is skipped unless
 * {@code govdata/.env.prod} holds SN_INSTANCE_URL, SN_USERNAME and SN_PASSWORD, and it runs only
 * with {@code -PincludeTags=integration}. Expect to fix assumptions in it the first time it runs
 * against a real instance; the list of what the adapter assumes is in
 * {@code src/test/resources/servicenow/doc-derived/README.md}.
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

  private static Connection connect(int pageSize) throws SQLException {
    final Properties info = new Properties();
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
      assertThat(tables, hasItems("incident", "task", "sys_user", "sys_db_object"));
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
