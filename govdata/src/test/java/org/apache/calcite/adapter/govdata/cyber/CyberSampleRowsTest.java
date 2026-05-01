/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.govdata.cyber;

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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Shows one sample row from each cyber table by going through the full
 * file-adapter / Iceberg materialization stack via a Calcite JDBC connection.
 *
 * <p>Does NOT call transformers directly — the file adapter owns fetch, transform,
 * and Iceberg write; this test only issues SQL queries.
 *
 * <p>Run with:
 * <pre>
 * CYBER_INTEGRATION_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*CyberSampleRows*" --console=plain
 * </pre>
 */
@Tag("integration")
class CyberSampleRowsTest {

  private static final Logger LOG = LoggerFactory.getLogger(CyberSampleRowsTest.class);

  private static final List<String> VULN_TABLES = Arrays.asList(
      "cwe_catalog",
      "kev_catalog",
      "vulnerabilities"
  );

  private static final List<String> THREAT_TABLES = Arrays.asList(
      "attack_techniques",
      "ioc_urls",
      "ioc_hashes",
      "ioc_ips",
      "ioc_mixed"
  );

  private static String vulnWarehouse;
  private static String threatWarehouse;
  private static String vulnOperatingDir;
  private static String threatOperatingDir;
  private static String cacheDir;

  @BeforeAll static void setup() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("CYBER_INTEGRATION_TESTS")),
        "CYBER_INTEGRATION_TESTS=true required");

    TestEnvironmentLoader.ensureLoaded();

    // Use dedicated temp base so Iceberg warehouse AND DuckDB catalog are isolated per run.
    // Co-locating the operatingDirectory with tmpBase prevents stale DuckDB views from
    // a previous run from serving old paths after the temp warehouse is recreated.
    File tmpBase = Files.createTempDirectory("cyber_sample_iceberg_").toFile();
    vulnWarehouse = new File(tmpBase, "vuln").getAbsolutePath();
    threatWarehouse = new File(tmpBase, "threat").getAbsolutePath();
    vulnOperatingDir = new File(tmpBase, "aperio_vuln").getAbsolutePath();
    threatOperatingDir = new File(tmpBase, "aperio_threat").getAbsolutePath();

    // Raw cache: use configured cache dir or a temp fallback
    String configured = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    cacheDir = (configured != null && !configured.startsWith("s3://"))
        ? configured + "/cyber"
        : new File(tmpBase, "cache").getAbsolutePath();

    LOG.info("Iceberg warehouse (vuln):   {}", vulnWarehouse);
    LOG.info("Iceberg warehouse (threat): {}", threatWarehouse);
    LOG.info("Operating dir (vuln):       {}", vulnOperatingDir);
    LOG.info("Operating dir (threat):     {}", threatOperatingDir);
    LOG.info("Raw cache dir:              {}", cacheDir);
  }

  // ── cyber_vuln (smoke: cwe_catalog, kev_catalog, vulnerabilities) ──────────

  @Test void sampleVulnTables() throws Exception {
    String nvdApiKey = coalesce(
        TestEnvironmentLoader.getEnv("CYBER_NVD_API_KEY"),
        System.getenv("CYBER_NVD_API_KEY"), "");

    String modelJson = buildModel("cyber_vuln", "cyber_vuln_smoke", vulnWarehouse,
        vulnOperatingDir, nvdApiKey);

    LOG.info("=== cyber_vuln (smoke schema) — materializing to Iceberg then sampling ===");
    try (Connection conn = connect(modelJson)) {
      for (String table : VULN_TABLES) {
        sampleTable(conn, "cyber_vuln", table);
      }
    }
  }

  // ── cyber_threat ───────────────────────────────────────────────────────────

  @Test void sampleThreatTables() throws Exception {
    String threatfoxKey = coalesce(
        TestEnvironmentLoader.getEnv("CYBER_THREATFOX_API_KEY"),
        System.getenv("CYBER_THREATFOX_API_KEY"), "");
    boolean hasThreatfox = !threatfoxKey.isEmpty();

    String modelJson = buildModel("cyber_threat", "cyber_threat", threatWarehouse,
        threatOperatingDir, null);

    LOG.info("=== cyber_threat — materializing to Iceberg then sampling ===");
    try (Connection conn = connect(modelJson)) {
      for (String table : THREAT_TABLES) {
        if ("ioc_mixed".equals(table) && !hasThreatfox) {
          LOG.info("=== cyber_threat.ioc_mixed === (skipped — CYBER_THREATFOX_API_KEY not set)");
          continue;
        }
        sampleTable(conn, "cyber_threat", table);
      }
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private void sampleTable(Connection conn, String schema, String table) {
    String sql = "SELECT * FROM \"" + schema + "\".\"" + table + "\" LIMIT 1";
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData meta = rs.getMetaData();
      int cols = meta.getColumnCount();
      if (rs.next()) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== ").append(schema).append(".").append(table).append(" ===\n");
        for (int i = 1; i <= cols; i++) {
          sb.append(String.format("  %-30s = %s%n",
              meta.getColumnName(i),
              rs.getString(i)));
        }
        LOG.info(sb.toString());
      } else {
        LOG.warn("=== {}.{} === (0 rows returned)", schema, table);
      }
    } catch (SQLException e) {
      LOG.error("=== {}.{} === FAILED: {}", schema, table, e.getMessage());
    }
  }

  private Connection connect(String modelJson) throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  /** Builds a Calcite model JSON for a cyber schema that writes Iceberg to a local warehouse. */
  private String buildModel(String schemaName, String dataSource,
      String warehouse, String operatingDir, String nvdApiKey) {
    String executionEngine = coalesce(
        TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");

    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("  \"version\": \"1.0\",");
    sb.append("  \"defaultSchema\": \"").append(schemaName).append("\",");
    sb.append("  \"schemas\": [{");
    sb.append("    \"name\": \"").append(schemaName).append("\",");
    sb.append("    \"type\": \"custom\",");
    sb.append("    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",");
    sb.append("    \"operand\": {");
    sb.append("      \"dataSource\": \"").append(dataSource).append("\",");
    sb.append("      \"executionEngine\": \"").append(executionEngine).append("\",");
    sb.append("      \"directory\": \"").append(warehouse).append("\",");
    sb.append("      \"operatingDirectory\": \"").append(operatingDir).append("\",");
    sb.append("      \"cacheDirectory\": \"").append(cacheDir).append("\",");
    sb.append("      \"ephemeralCache\": false,");
    sb.append("      \"autoDownload\": true,");
    sb.append("      \"database_filename\": \"").append(operatingDir).append("/").append(schemaName).append("_db.duckdb\"");
    if (nvdApiKey != null && !nvdApiKey.isEmpty()) {
      sb.append(",");
      sb.append("      \"nvdApiKey\": \"").append(nvdApiKey).append("\"");
    }
    sb.append("    }");
    sb.append("  }]");
    sb.append("}");
    return sb.toString();
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
