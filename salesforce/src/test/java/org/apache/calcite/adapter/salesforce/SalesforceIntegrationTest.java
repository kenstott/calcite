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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration test against a live Salesforce org: model JSON to
 * {@link SalesforceSchemaFactory}, schema to sObject tables, describe to
 * column types, and SQL through the adapter.
 *
 * <p>Credentials come from {@code govdata/.env.prod}: SF_LOGIN_URL (the org's
 * My Domain host), SF_CONSUMER_KEY and SF_CONSUMER_SECRET, using the OAuth
 * client credentials flow.
 */
@Tag("integration")
class SalesforceIntegrationTest {

  private static final String SCHEMA = "sf";

  private static Map<String, String> env;

  /** Describe cache of this run, so the tests never read one a previous run left behind. */
  @TempDir static Path describeCache;

  @BeforeAll static void loadEnv() throws IOException {
    String rootDir = System.getProperty("gradle.rootDir");
    if (rootDir == null) {
      throw new IllegalStateException("gradle.rootDir system property is not set");
    }
    Path envFile = Paths.get(rootDir, "govdata", ".env.prod");
    if (!Files.exists(envFile)) {
      throw new IllegalStateException("Salesforce credentials file not found: " + envFile);
    }
    env = parseEnvFile(envFile);
    for (String key : new String[] {"SF_LOGIN_URL", "SF_CONSUMER_KEY", "SF_CONSUMER_SECRET"}) {
      if (!env.containsKey(key)) {
        throw new IllegalStateException(key + " is missing from " + envFile);
      }
    }
  }

  private static Map<String, String> parseEnvFile(Path file) throws IOException {
    Map<String, String> result = new HashMap<>();
    for (String raw : Files.readAllLines(file, StandardCharsets.UTF_8)) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      if (line.startsWith("export ")) {
        line = line.substring("export ".length()).trim();
      }
      int eq = line.indexOf('=');
      if (eq <= 0) {
        continue;
      }
      String key = line.substring(0, eq).trim();
      String value = line.substring(eq + 1).trim();
      if (value.length() >= 2
          && (value.startsWith("\"") && value.endsWith("\"")
              || value.startsWith("'") && value.endsWith("'"))) {
        value = value.substring(1, value.length() - 1);
      }
      result.put(key, value);
    }
    return result;
  }

  private static String loginUrl() {
    String host = env.get("SF_LOGIN_URL");
    return host.startsWith("https://") ? host : "https://" + host;
  }

  private static String jsonString(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static Connection connect() throws SQLException {
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"" + SCHEMA + "\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"" + SCHEMA + "\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"" + SalesforceSchemaFactory.class.getName() + "\",\n"
        + "    \"operand\": {\n"
        + "      \"loginUrl\": " + jsonString(loginUrl()) + ",\n"
        + "      \"clientId\": " + jsonString(env.get("SF_CONSUMER_KEY")) + ",\n"
        + "      \"clientSecret\": " + jsonString(env.get("SF_CONSUMER_SECRET")) + ",\n"
        + "      \"apiVersion\": \"v61.0\",\n"
        + "      \"describeCacheDirectory\": " + jsonString(describeCache.toString()) + "\n"
        + "    }\n"
        + "  }]\n"
        + "}";
    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "JAVA");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  @Test void schemaExposesStandardSObjectsAsTables() throws SQLException {
    try (Connection conn = connect()) {
      DatabaseMetaData md = conn.getMetaData();
      Set<String> tables = new HashSet<>();
      try (ResultSet rs = md.getTables(null, SCHEMA, null, null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      assertThat(tables, hasItems("Account", "Contact", "Opportunity", "Lead", "User"));
    }
  }

  @Test void describeMapsFieldTypesToColumns() throws SQLException {
    try (Connection conn = connect()) {
      DatabaseMetaData md = conn.getMetaData();
      Map<String, Integer> columns = new HashMap<>();
      try (ResultSet rs = md.getColumns(null, SCHEMA, "Account", null)) {
        while (rs.next()) {
          columns.put(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"));
        }
      }
      assertThat(columns.get("Id"), equalTo(Types.VARCHAR));
      assertThat(columns.get("Name"), equalTo(Types.VARCHAR));
      assertThat(columns.get("IsDeleted"), equalTo(Types.BOOLEAN));
      assertThat(columns.get("AnnualRevenue"), equalTo(Types.DECIMAL));
      assertThat(columns.get("NumberOfEmployees"), equalTo(Types.INTEGER));
      assertThat(columns.get("CreatedDate"), equalTo(Types.TIMESTAMP));
    }
  }

  @Test void countRows() throws SQLException {
    try (Connection conn = connect();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Account")) {
      rs.next();
      assertThat(rs.getLong(1), greaterThan(0L));
    }
  }

  @Test void filterProjectSortLimit() throws SQLException {
    String sql = "SELECT Name, Industry FROM Account "
        + "WHERE Name IS NOT NULL ORDER BY Name LIMIT 3";
    try (Connection conn = connect();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      List<String> names = new ArrayList<>();
      while (rs.next()) {
        names.add(rs.getString("Name"));
      }
      assertThat(names.size(), greaterThan(0));
      assertThat(names.size(), lessThanOrEqualTo(3));
      List<String> sorted = new ArrayList<>(names);
      sorted.sort(String::compareTo);
      assertThat(names, equalTo(sorted));
    }
  }

  @Test void singleTimestampColumn() throws SQLException {
    String sql = "SELECT CreatedDate FROM Account LIMIT 1";
    try (Connection conn = connect();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      assertThat(rs.next(), equalTo(true));
      Timestamp created = rs.getTimestamp(1);
      assertThat(created.getTime(), greaterThan(0L));
    }
  }

  @Test void joinAcrossSObjects() throws SQLException {
    String sql = "SELECT a.Name, c.LastName FROM Contact c "
        + "JOIN Account a ON c.AccountId = a.Id";
    try (Connection conn = connect();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      int rows = 0;
      while (rs.next()) {
        rows++;
      }
      assertThat(rows, greaterThan(0));
    }
  }

  @Test void computedProjectionAndNonPushableFilterRunInCalcite() throws SQLException {
    String sql = "SELECT UPPER(Name) FROM Account "
        + "WHERE CHAR_LENGTH(Name) > 0 AND Name LIKE '%a%' LIMIT 2";
    try (Connection conn = connect();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      assertThat(rs.next(), equalTo(true));
      String upper = rs.getString(1);
      assertThat(upper, equalTo(upper.toUpperCase(java.util.Locale.ROOT)));
    }
  }

  @Test void preparedStatementParameterFiltersRows() throws SQLException {
    try (Connection conn = connect()) {
      String accountId;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT AccountId FROM Contact WHERE AccountId IS NOT NULL LIMIT 1")) {
        assertThat(rs.next(), equalTo(true));
        accountId = rs.getString(1);
      }
      String sql = "SELECT AccountId FROM Contact WHERE AccountId = ? OR AccountId = ?";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN PLAN FOR " + sql)) {
        ps.setString(1, accountId);
        ps.setString(2, accountId);
        try (ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next(), equalTo(true));
          String plan = rs.getString(1);
          assertThat(plan, containsString("SalesforceFilter"));
          assertThat(plan, not(containsString("EnumerableCalc")));
        }
      }
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, accountId);
        ps.setString(2, "000000000000000AAA");
        try (ResultSet rs = ps.executeQuery()) {
          int rows = 0;
          while (rs.next()) {
            assertThat(rs.getString(1), equalTo(accountId));
            rows++;
          }
          assertThat(rows, greaterThan(0));
        }
        // A null parameter matches no row; "AccountId = null" in SOQL would match orphans
        ps.setString(1, null);
        try (ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next(), equalTo(false));
        }
      }
    }
  }

  @Test void preparedStatementTypedParameters() throws SQLException {
    String sql = "SELECT Name FROM Account "
        + "WHERE CreatedDate > ? AND IsDeleted = ? AND (NumberOfEmployees >= ? OR Name LIKE ?)";
    try (Connection conn = connect();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setTimestamp(1, Timestamp.valueOf("2000-01-01 00:00:00"));
      ps.setBoolean(2, false);
      ps.setInt(3, 0);
      ps.setString(4, "%");
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next(), equalTo(true));
      }
      ps.setTimestamp(1, Timestamp.valueOf("2999-01-01 00:00:00"));
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next(), equalTo(false));
      }
    }
  }

  @Test void describeIsCachedOnDiskBetweenConnections() throws SQLException, IOException {
    String sql = "SELECT Id FROM Campaign LIMIT 1";
    try (Connection conn = connect();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      rs.next();
    }
    Path cached;
    try (Stream<Path> paths = Files.walk(describeCache)) {
      cached = paths.filter(path -> path.getFileName().toString().equals("Campaign.json"))
          .findFirst().orElseThrow(
              () -> new AssertionError("no Campaign.json under " + describeCache));
    }
    // A describe that only the cache can supply: the next connection must see this column
    String json = new String(Files.readAllBytes(cached), StandardCharsets.UTF_8);
    String marked = json.replaceFirst("\\{\"aggregatable\"",
        "{\"name\":\"CachedOnly__c\",\"type\":\"string\",\"length\":10,\"nillable\":true},"
            + "{\"aggregatable\"");
    assertThat(marked.equals(json), equalTo(false));
    Files.write(cached, marked.getBytes(StandardCharsets.UTF_8));
    try (Connection conn = connect();
         ResultSet rs = conn.getMetaData().getColumns(null, SCHEMA, "Campaign", "CachedOnly__c")) {
      assertThat(rs.next(), equalTo(true));
    } finally {
      Files.write(cached, json.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test void insertUpdateDelete() throws SQLException {
    String marker = "CalciteIT-" + UUID.randomUUID();
    try (Connection conn = connect();
         Statement stmt = conn.createStatement()) {
      try {
        int inserted = stmt.executeUpdate(
            "INSERT INTO Account (Name, Industry, NumberOfEmployees) VALUES "
                + "('" + marker + "-1', 'Energy', 10), "
                + "('" + marker + "-2', 'Energy', 20)");
        assertThat(inserted, equalTo(2));

        try (ResultSet rs = stmt.executeQuery(
            "SELECT Id, Industry, NumberOfEmployees, CreatedDate FROM Account "
                + "WHERE Name LIKE '" + marker + "%' ORDER BY Name")) {
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getString("Id").length(), equalTo(18));
          assertThat(rs.getString("Industry"), equalTo("Energy"));
          assertThat(rs.getInt("NumberOfEmployees"), equalTo(10));
          assertThat(rs.getTimestamp("CreatedDate").getTime(), greaterThan(0L));
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getInt("NumberOfEmployees"), equalTo(20));
          assertThat(rs.next(), equalTo(false));
        }

        int updated = stmt.executeUpdate(
            "UPDATE Account SET Industry = 'Banking', "
                + "NumberOfEmployees = NumberOfEmployees + 1 "
                + "WHERE Name = '" + marker + "-2'");
        assertThat(updated, equalTo(1));

        try (ResultSet rs = stmt.executeQuery(
            "SELECT Name, Industry, NumberOfEmployees FROM Account "
                + "WHERE Name LIKE '" + marker + "%' ORDER BY Name")) {
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getString("Industry"), equalTo("Energy"));
          assertThat(rs.getInt("NumberOfEmployees"), equalTo(10));
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getString("Industry"), equalTo("Banking"));
          assertThat(rs.getInt("NumberOfEmployees"), equalTo(21));
        }

        int cleared = stmt.executeUpdate(
            "UPDATE Account SET Industry = NULL WHERE Name = '" + marker + "-1'");
        assertThat(cleared, equalTo(1));
        try (ResultSet rs = stmt.executeQuery(
            "SELECT Industry FROM Account WHERE Name = '" + marker + "-1'")) {
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getString(1), equalTo(null));
        }

        int deleted = stmt.executeUpdate(
            "DELETE FROM Account WHERE Name LIKE '" + marker + "%'");
        assertThat(deleted, equalTo(2));

        try (ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM Account WHERE Name LIKE '" + marker + "%'")) {
          rs.next();
          assertThat(rs.getLong(1), equalTo(0L));
        }
      } finally {
        // Remove anything a failed step left behind in the shared org
        stmt.executeUpdate("DELETE FROM Account WHERE Name LIKE '" + marker + "%'");
      }
    }
  }

  @Test void insertIntoSystemFieldIsRejected() throws SQLException {
    try (Connection conn = connect();
         Statement stmt = conn.createStatement()) {
      SQLException e = assertThrows(SQLException.class, () ->
          stmt.executeUpdate("INSERT INTO Account (Id, Name) "
              + "VALUES ('001000000000000AAA', 'never-written')"));
      assertThat(e.getMessage(), containsString("Id"));
    }
  }

  @Test void insertWithoutRequiredFieldIsRejected() throws SQLException {
    try (Connection conn = connect();
         Statement stmt = conn.createStatement()) {
      SQLException e = assertThrows(SQLException.class, () ->
          stmt.executeUpdate("INSERT INTO Account (Industry) VALUES ('Energy')"));
      assertThat(e.getMessage(), containsString("Name"));
    }
  }

  @Test void jdbcDriverUrl() throws SQLException {
    String url = "jdbc:salesforce:loginUrl=" + urlEncode(loginUrl())
        + ";clientId=" + urlEncode(env.get("SF_CONSUMER_KEY"))
        + ";clientSecret=" + urlEncode(env.get("SF_CONSUMER_SECRET"))
        + ";cacheMaxSize=50";
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    try (Connection conn = DriverManager.getConnection(url, info);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM salesforce.Account")) {
      rs.next();
      assertThat(rs.getLong(1), greaterThan(0L));
    }
  }

  private static String urlEncode(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
