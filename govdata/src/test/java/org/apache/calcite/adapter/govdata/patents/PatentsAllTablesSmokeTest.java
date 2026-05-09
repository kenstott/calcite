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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end smoke test for the patents schema (all 7 tables).
 *
 * <p>Tables covered:
 * <ul>
 *   <li>patent_grants — core patent dimension (~350K/year)</li>
 *   <li>patent_assignees — patent ownership at grant time (~250K/year)</li>
 *   <li>patent_inventors — inventor listing per patent (~600K rows/year)</li>
 *   <li>patent_cpc_classes — CPC technology classifications (~1.5M rows/year)</li>
 *   <li>patent_claims — individual patent claims (~5M rows/year)</li>
 *   <li>patent_summaries — brief summary text (~300K/year)</li>
 *   <li>trademark_applications — USPTO trademark filings (~400K/year)</li>
 * </ul>
 *
 * <p>Scoped to {@code SMOKE_YEAR} (2023) via {@code startYear}/{@code endYear} to
 * minimize download time. First run will download large files from PatentsView S3;
 * subsequent runs re-use the 90-day local cache.
 *
 * <p>WARNING: {@code patent_inventors} requires ~8 GB download on first run.
 * If the inventor test times out, re-run with an extended Gradle timeout:
 * {@code gtimeout 3600 ./gradlew ...}
 *
 * <p>Run with:
 * <pre>
 * PATENTS_INTEGRATION_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*PatentsAllTablesSmokeTest*" \
 *     --console=plain
 * </pre>
 *
 * <p>With explicit cache directory (recommended to preserve between runs):
 * <pre>
 * PATENTS_INTEGRATION_TESTS=true \
 * GOVDATA_CACHE_DIR=/path/to/cache \
 * GOVDATA_PARQUET_DIR=/path/to/parquet \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*PatentsAllTablesSmokeTest*" \
 *     --console=plain
 * </pre>
 */
@Tag("integration")
class PatentsAllTablesSmokeTest {

  private static final Logger LOG = LoggerFactory.getLogger(PatentsAllTablesSmokeTest.class);

  private static final String SCHEMA = "patents";
  private static final int SMOKE_YEAR = 2023;

  private static String warehouse;
  private static String operatingDir;
  private static String cacheDir;

  @BeforeAll static void setup() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("PATENTS_INTEGRATION_TESTS")),
        "Set PATENTS_INTEGRATION_TESTS=true to run patents smoke tests");

    TestEnvironmentLoader.ensureLoaded();

    File tmpBase = Files.createTempDirectory("patents_smoke_").toFile();
    warehouse = new File(tmpBase, "parquet").getAbsolutePath();
    operatingDir = new File(tmpBase, "op").getAbsolutePath();
    new File(operatingDir).mkdirs();
    new File(warehouse).mkdirs();

    String configured = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    cacheDir = (configured != null && !configured.isEmpty() && !configured.startsWith("s3://"))
        ? configured + "/patents"
        : new File(tmpBase, "cache").getAbsolutePath();

    LOG.info("Smoke year:    {}", SMOKE_YEAR);
    LOG.info("Warehouse:     {}", warehouse);
    LOG.info("Cache dir:     {}", cacheDir);
  }

  // ── patent_grants ────────────────────────────────────────────────────────────

  @Test void patentGrantsHaveData() throws Exception {
    LOG.info("=== patents: patent_grants ===");
    try (Connection conn = patentsConn("patent_grants")) {
      assertRowCount(conn, SCHEMA, "patent_grants", 100);
      assertPkNonNull(conn, SCHEMA, "patent_grants", "patent_id");
      assertNoDuplicatePk(conn, SCHEMA, "patent_grants", "patent_id");
    }
  }

  @Test void patentGrantsSampleRow() throws Exception {
    LOG.info("=== patents: patent_grants sample row ===");
    try (Connection conn = patentsConn("patent_grants")) {
      sampleRow(conn, SCHEMA, "patent_grants");
    }
  }

  // ── patent_assignees ─────────────────────────────────────────────────────────

  @Test void patentAssigneesHaveData() throws Exception {
    LOG.info("=== patents: patent_assignees ===");
    try (Connection conn = patentsConn("patent_assignees")) {
      assertRowCount(conn, SCHEMA, "patent_assignees", 50);
      assertPkNonNull(conn, SCHEMA, "patent_assignees", "patent_id");
      // composite PK: (patent_id, assignee_sequence) — multiple assignees per patent
      warnDuplicatePk(conn, SCHEMA, "patent_assignees", "patent_id");
    }
  }

  @Test void patentAssigneesSampleRow() throws Exception {
    LOG.info("=== patents: patent_assignees sample row ===");
    try (Connection conn = patentsConn("patent_assignees")) {
      sampleRow(conn, SCHEMA, "patent_assignees");
    }
  }

  // ── patent_inventors ─────────────────────────────────────────────────────────

  @Test void patentInventorsHaveData() throws Exception {
    LOG.info("=== patents: patent_inventors ===");
    try (Connection conn = patentsConn("patent_inventors")) {
      assertRowCount(conn, SCHEMA, "patent_inventors", 100);
      assertPkNonNull(conn, SCHEMA, "patent_inventors", "patent_id");
      // composite PK: (patent_id, inventor_sequence) — multiple inventors per patent
      warnDuplicatePk(conn, SCHEMA, "patent_inventors", "patent_id");
    }
  }

  @Test void patentInventorsSampleRow() throws Exception {
    LOG.info("=== patents: patent_inventors sample row ===");
    try (Connection conn = patentsConn("patent_inventors")) {
      sampleRow(conn, SCHEMA, "patent_inventors");
    }
  }

  // ── patent_cpc_classes ───────────────────────────────────────────────────────

  @Test void patentCpcClassesHaveData() throws Exception {
    LOG.info("=== patents: patent_cpc_classes ===");
    try (Connection conn = patentsConn("patent_cpc_classes")) {
      assertRowCount(conn, SCHEMA, "patent_cpc_classes", 200);
      assertPkNonNull(conn, SCHEMA, "patent_cpc_classes", "patent_id");
      // composite PK: (patent_id, cpc_sequence) — multiple CPC codes per patent
      warnDuplicatePk(conn, SCHEMA, "patent_cpc_classes", "patent_id");
    }
  }

  @Test void patentCpcClassesSampleRow() throws Exception {
    LOG.info("=== patents: patent_cpc_classes sample row ===");
    try (Connection conn = patentsConn("patent_cpc_classes")) {
      sampleRow(conn, SCHEMA, "patent_cpc_classes");
    }
  }

  // ── patent_claims ────────────────────────────────────────────────────────────

  @Test void patentClaimsHaveData() throws Exception {
    LOG.info("=== patents: patent_claims ===");
    try (Connection conn = patentsConn("patent_claims")) {
      assertRowCount(conn, SCHEMA, "patent_claims", 500);
      assertPkNonNull(conn, SCHEMA, "patent_claims", "patent_id");
      // composite PK: (patent_id, claim_sequence) — multiple claims per patent
      warnDuplicatePk(conn, SCHEMA, "patent_claims", "patent_id");
    }
  }

  @Test void patentClaimsSampleRow() throws Exception {
    LOG.info("=== patents: patent_claims sample row ===");
    try (Connection conn = patentsConn("patent_claims")) {
      sampleRow(conn, SCHEMA, "patent_claims");
    }
  }

  // ── patent_summaries ─────────────────────────────────────────────────────────

  @Test void patentSummariesHaveData() throws Exception {
    LOG.info("=== patents: patent_summaries ===");
    try (Connection conn = patentsConn("patent_summaries")) {
      assertRowCount(conn, SCHEMA, "patent_summaries", 100);
      assertPkNonNull(conn, SCHEMA, "patent_summaries", "patent_id");
      // one summary per patent — duplicates indicate ETL issue
      assertNoDuplicatePk(conn, SCHEMA, "patent_summaries", "patent_id");
    }
  }

  @Test void patentSummariesSampleRow() throws Exception {
    LOG.info("=== patents: patent_summaries sample row ===");
    try (Connection conn = patentsConn("patent_summaries")) {
      sampleRow(conn, SCHEMA, "patent_summaries");
    }
  }

  // ── trademark_applications ───────────────────────────────────────────────────

  @Test void trademarkApplicationsHaveData() throws Exception {
    LOG.info("=== patents: trademark_applications ===");
    try (Connection conn = patentsConn("trademark_applications")) {
      assertRowCount(conn, SCHEMA, "trademark_applications", 100);
      assertPkNonNull(conn, SCHEMA, "trademark_applications", "serial_no");
      assertNoDuplicatePk(conn, SCHEMA, "trademark_applications", "serial_no");
    }
  }

  @Test void trademarkApplicationsSampleRow() throws Exception {
    LOG.info("=== patents: trademark_applications sample row ===");
    try (Connection conn = patentsConn("trademark_applications")) {
      sampleRow(conn, SCHEMA, "trademark_applications");
    }
  }

  // ── FK joins: child tables → patent_grants ───────────────────────────────────

  @Test void assigneesJoinToGrants() throws Exception {
    LOG.info("=== FK: patent_assignees.patent_id → patent_grants.patent_id ===");
    try (Connection conn = patentsConn("patent_grants", "patent_assignees")) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_grants\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_assignees\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_assignees\""
          + " WHERE patent_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_assignees\" a"
          + " INNER JOIN \"" + SCHEMA + "\".\"patent_grants\" g"
          + " ON a.patent_id = g.patent_id");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("assignees→grants: {}/{} assignee rows matched ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.80,
          String.format("assignees→grants match rate %.1f%% below 80%% threshold", matchRate * 100));
    }
  }

  @Test void inventorsJoinToGrants() throws Exception {
    LOG.info("=== FK: patent_inventors.patent_id → patent_grants.patent_id ===");
    try (Connection conn = patentsConn("patent_grants", "patent_inventors")) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_grants\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_inventors\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_inventors\""
          + " WHERE patent_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_inventors\" i"
          + " INNER JOIN \"" + SCHEMA + "\".\"patent_grants\" g"
          + " ON i.patent_id = g.patent_id");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("inventors→grants: {}/{} inventor rows matched ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.80,
          String.format("inventors→grants match rate %.1f%% below 80%% threshold", matchRate * 100));
    }
  }

  @Test void cpcClassesJoinToGrants() throws Exception {
    LOG.info("=== FK: patent_cpc_classes.patent_id → patent_grants.patent_id ===");
    try (Connection conn = patentsConn("patent_grants", "patent_cpc_classes")) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_grants\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_cpc_classes\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_cpc_classes\""
          + " WHERE patent_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_cpc_classes\" c"
          + " INNER JOIN \"" + SCHEMA + "\".\"patent_grants\" g"
          + " ON c.patent_id = g.patent_id");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("cpc_classes→grants: {}/{} CPC rows matched ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.80,
          String.format("cpc_classes→grants match rate %.1f%% below 80%% threshold", matchRate * 100));
    }
  }

  @Test void claimsJoinToGrants() throws Exception {
    LOG.info("=== FK: patent_claims.patent_id → patent_grants.patent_id ===");
    try (Connection conn = patentsConn("patent_grants", "patent_claims")) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_grants\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_claims\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_claims\""
          + " WHERE patent_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_claims\" cl"
          + " INNER JOIN \"" + SCHEMA + "\".\"patent_grants\" g"
          + " ON cl.patent_id = g.patent_id");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("claims→grants: {}/{} claim rows matched ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.70,
          String.format("claims→grants match rate %.1f%% below 70%% threshold", matchRate * 100));
    }
  }

  @Test void summariesJoinToGrants() throws Exception {
    LOG.info("=== FK: patent_summaries.patent_id → patent_grants.patent_id ===");
    try (Connection conn = patentsConn("patent_grants", "patent_summaries")) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_grants\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_summaries\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_summaries\""
          + " WHERE patent_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"patent_summaries\" s"
          + " INNER JOIN \"" + SCHEMA + "\".\"patent_grants\" g"
          + " ON s.patent_id = g.patent_id");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("summaries→grants: {}/{} summary rows matched ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.70,
          String.format("summaries→grants match rate %.1f%% below 70%% threshold", matchRate * 100));
    }
  }

  // ── helpers ──────────────────────────────────────────────────────────────────

  private Connection patentsConn(String... enabledTables) throws Exception {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    StringBuilder tables = new StringBuilder();
    for (int i = 0; i < enabledTables.length; i++) {
      if (i > 0) {
        tables.append(",");
      }
      tables.append("\"").append(enabledTables[i]).append("\"");
    }
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"" + SCHEMA + "\","
        + "\"schemas\":[{"
        + "  \"name\":\"" + SCHEMA + "\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\":{"
        + "    \"dataSource\":\"patents\","
        + "    \"executionEngine\":\"" + engine + "\","
        + "    \"directory\":\"" + warehouse + "\","
        + "    \"operatingDirectory\":\"" + operatingDir + "\","
        + "    \"cacheDirectory\":\"" + cacheDir + "\","
        + "    \"ephemeralCache\":false,"
        + "    \"autoDownload\":true,"
        + "    \"startYear\":" + SMOKE_YEAR + ","
        + "    \"endYear\":" + SMOKE_YEAR + ","
        + "    \"enabledTables\":[" + tables + "],"
        + "    \"database_filename\":\"" + operatingDir + "/patents_db.duckdb\""
        + "  }"
        + "}]}";
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + model);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  private long scalar(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getLong(1) : 0L;
    }
  }

  private void assertRowCount(Connection conn, String schema, String table,
      long minExpected) throws Exception {
    long count = scalar(conn, "SELECT COUNT(*) FROM \"" + schema + "\".\"" + table + "\"");
    LOG.info("{}.{}: {} rows", schema, table, count);
    assertTrue(count >= minExpected,
        schema + "." + table + ": expected >=" + minExpected + " rows, got " + count);
  }

  private void assertPkNonNull(Connection conn, String schema, String table,
      String pkColumn) throws Exception {
    long nulls = scalar(conn,
        "SELECT COUNT(*) FROM \"" + schema + "\".\"" + table + "\""
        + " WHERE \"" + pkColumn + "\" IS NULL");
    assertEquals(0L, nulls,
        schema + "." + table + "." + pkColumn + ": found " + nulls + " NULL PK values");
    LOG.info("{}.{}.{}: no NULL values", schema, table, pkColumn);
  }

  private void warnNullPk(Connection conn, String schema, String table,
      String pkColumn) throws Exception {
    long nulls = scalar(conn,
        "SELECT COUNT(*) FROM \"" + schema + "\".\"" + table + "\""
        + " WHERE \"" + pkColumn + "\" IS NULL");
    if (nulls > 0) {
      LOG.warn("{}.{}.{}: {} NULL PK values (kept for partial-join analytics)",
          schema, table, pkColumn, nulls);
    } else {
      LOG.info("{}.{}.{}: no NULL values", schema, table, pkColumn);
    }
  }

  private void assertNoDuplicatePk(Connection conn, String schema, String table,
      String pkColumn) throws Exception {
    long dups = scalar(conn,
        "SELECT COUNT(*) FROM ("
        + "SELECT \"" + pkColumn + "\", COUNT(*) AS cnt"
        + " FROM \"" + schema + "\".\"" + table + "\""
        + " WHERE \"" + pkColumn + "\" IS NOT NULL"
        + " GROUP BY \"" + pkColumn + "\""
        + " HAVING COUNT(*) > 1) t");
    if (dups > 0) {
      logExamples(conn,
          "SELECT \"" + pkColumn + "\", COUNT(*) AS cnt"
          + " FROM \"" + schema + "\".\"" + table + "\""
          + " WHERE \"" + pkColumn + "\" IS NOT NULL"
          + " GROUP BY \"" + pkColumn + "\""
          + " HAVING COUNT(*) > 1"
          + " ORDER BY cnt DESC LIMIT 5",
          "duplicate PKs in " + schema + "." + table);
    }
    assertEquals(0L, dups,
        schema + "." + table + "." + pkColumn + ": found " + dups + " duplicate PK values");
    LOG.info("{}.{}.{}: no duplicates", schema, table, pkColumn);
  }

  private void warnDuplicatePk(Connection conn, String schema, String table,
      String pkColumn) throws Exception {
    long dups = scalar(conn,
        "SELECT COUNT(*) FROM ("
        + "SELECT \"" + pkColumn + "\", COUNT(*) AS cnt"
        + " FROM \"" + schema + "\".\"" + table + "\""
        + " WHERE \"" + pkColumn + "\" IS NOT NULL"
        + " GROUP BY \"" + pkColumn + "\""
        + " HAVING COUNT(*) > 1) t");
    if (dups > 0) {
      LOG.warn("{}.{}.{}: {} duplicate values (expected — this is a child table with multiple rows per patent)",
          schema, table, pkColumn, dups);
    } else {
      LOG.info("{}.{}.{}: no duplicates", schema, table, pkColumn);
    }
  }

  private void sampleRow(Connection conn, String schema, String table) {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT * FROM \"" + schema + "\".\"" + table + "\" LIMIT 1")) {
      ResultSetMetaData meta = rs.getMetaData();
      int cols = meta.getColumnCount();
      if (rs.next()) {
        StringBuilder sb = new StringBuilder("\n=== ")
            .append(schema).append(".").append(table).append(" ===\n");
        for (int i = 1; i <= cols; i++) {
          String val = rs.getString(i);
          if (val != null && val.length() > 120) {
            val = val.substring(0, 120) + "...";
          }
          sb.append(String.format("  %-32s = %s%n", meta.getColumnName(i), val));
        }
        LOG.info(sb.toString());
      } else {
        LOG.warn("=== {}.{} === (0 rows)", schema, table);
      }
    } catch (Exception e) {
      LOG.error("=== {}.{} === FAILED: {}", schema, table, e.getMessage());
    }
  }

  private void logExamples(Connection conn, String sql, String label) {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData meta = rs.getMetaData();
      int cols = meta.getColumnCount();
      StringBuilder sb = new StringBuilder("\nExamples — ").append(label).append(":\n");
      while (rs.next()) {
        for (int i = 1; i <= cols; i++) {
          if (i > 1) {
            sb.append(" | ");
          }
          sb.append(meta.getColumnName(i)).append("=").append(rs.getString(i));
        }
        sb.append("\n");
      }
      LOG.info(sb.toString());
    } catch (Exception e) {
      LOG.warn("logExamples failed for '{}': {}", label, e.getMessage());
    }
  }

  private static String coalesce(String... values) {
    for (String v : values) {
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return "";
  }
}
