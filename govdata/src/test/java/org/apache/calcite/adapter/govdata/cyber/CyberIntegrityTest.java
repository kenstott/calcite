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
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Validates referential integrity across the three key FK relationships in cyber_vuln:
 * <ol>
 *   <li>kev_catalog.cve_id → vulnerabilities.cve_id</li>
 *   <li>vulnerability_cwes.cwe_id → cwe_catalog.cwe_id  (via junction view)</li>
 *   <li>kev_catalog.cwes (unnested) → cwe_catalog.cwe_id</li>
 * </ol>
 *
 * <p>Results are logged as counts + examples; no hard assertion thresholds since
 * feed timing means a small number of orphans is expected (e.g., new CVEs not yet
 * in NVD, or CWEs referenced before the catalog is refreshed).
 *
 * <p>Run with:
 * <pre>
 * CYBER_INTEGRATION_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*CyberIntegrity*" --console=plain
 * </pre>
 */
@Tag("integration")
class CyberIntegrityTest {

  private static final Logger LOG = LoggerFactory.getLogger(CyberIntegrityTest.class);

  private static String warehouse;
  private static String operatingDir;
  private static String cacheDir;

  @BeforeAll static void setup() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("CYBER_INTEGRATION_TESTS")),
        "CYBER_INTEGRATION_TESTS=true required");

    TestEnvironmentLoader.ensureLoaded();

    String govDataCacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    if (govDataCacheDir != null && !govDataCacheDir.isEmpty()
        && !govDataCacheDir.startsWith("s3://")) {
      cacheDir = govDataCacheDir + "/cyber";
      warehouse = cacheDir + "/iceberg";
      operatingDir = cacheDir + "/aperio_vuln";
    } else {
      File tmpBase = Files.createTempDirectory("cyber_integrity_").toFile();
      cacheDir = new File(tmpBase, "cache").getAbsolutePath();
      warehouse = new File(tmpBase, "iceberg").getAbsolutePath();
      operatingDir = new File(tmpBase, "aperio_vuln").getAbsolutePath();
    }

    LOG.info("Iceberg warehouse: {}", warehouse);
    LOG.info("Operating dir:     {}", operatingDir);
    LOG.info("Raw cache dir:     {}", cacheDir);
  }

  /** FK 1: every KEV entry should have a matching NVD CVE record. */
  @Test void kevReferencesVulnerabilities() throws Exception {
    String nvdApiKey = coalesce(
        TestEnvironmentLoader.getEnv("CYBER_NVD_API_KEY"),
        System.getenv("CYBER_NVD_API_KEY"), "");

    try (Connection conn = connect(buildModel(warehouse, operatingDir, nvdApiKey))) {
      long total = scalar(conn, "SELECT COUNT(*) FROM cyber_vuln.kev_catalog");
      long orphans = scalar(conn,
          "SELECT COUNT(*) FROM cyber_vuln.kev_catalog k"
          + " LEFT JOIN cyber_vuln.vulnerabilities v ON k.cve_id = v.cve_id"
          + " WHERE v.cve_id IS NULL");

      LOG.info("KEV → vulnerabilities: {} / {} entries have no NVD record", orphans, total);
      if (orphans > 0) {
        logExamples(conn,
            "SELECT k.cve_id, k.date_added FROM cyber_vuln.kev_catalog k"
            + " LEFT JOIN cyber_vuln.vulnerabilities v ON k.cve_id = v.cve_id"
            + " WHERE v.cve_id IS NULL ORDER BY k.date_added DESC LIMIT 5",
            "orphaned KEV entries");
      }
    }
  }

  /** FK 2: every CWE ID in the vulnerability_cwes junction view should exist in cwe_catalog. */
  @Test void vulnerabilityCwesReferenceCweCatalog() throws Exception {
    String nvdApiKey = coalesce(
        TestEnvironmentLoader.getEnv("CYBER_NVD_API_KEY"),
        System.getenv("CYBER_NVD_API_KEY"), "");

    try (Connection conn = connect(buildModel(warehouse, operatingDir, nvdApiKey))) {
      long total = scalar(conn, "SELECT COUNT(*) FROM cyber_vuln.vulnerability_cwes");
      long orphans = scalar(conn,
          "SELECT COUNT(*) FROM cyber_vuln.vulnerability_cwes vc"
          + " LEFT JOIN cyber_vuln.cwe_catalog c ON vc.cwe_id = c.cwe_id"
          + " WHERE c.cwe_id IS NULL");

      LOG.info("vulnerability_cwes → cwe_catalog: {} / {} rows have no catalog entry",
          orphans, total);
      if (orphans > 0) {
        logExamples(conn,
            "SELECT vc.cve_id, vc.cwe_id FROM cyber_vuln.vulnerability_cwes vc"
            + " LEFT JOIN cyber_vuln.cwe_catalog c ON vc.cwe_id = c.cwe_id"
            + " WHERE c.cwe_id IS NULL ORDER BY vc.cwe_id LIMIT 5",
            "vulnerability_cwes with unknown CWE");
      }
    }
  }

  /** FK 3: every CWE ID in kev_catalog.cwes (pipe-delimited) should exist in cwe_catalog. */
  @Test void kevCwesReferenceCweCatalog() throws Exception {
    String nvdApiKey = coalesce(
        TestEnvironmentLoader.getEnv("CYBER_NVD_API_KEY"),
        System.getenv("CYBER_NVD_API_KEY"), "");

    try (Connection conn = connect(buildModel(warehouse, operatingDir, nvdApiKey))) {
      long withCwes = scalar(conn,
          "SELECT COUNT(*) FROM cyber_vuln.kev_cwes");
      long orphans = scalar(conn,
          "SELECT COUNT(*) FROM cyber_vuln.kev_cwes kc"
          + " LEFT JOIN cyber_vuln.cwe_catalog c ON kc.cwe_id = c.cwe_id"
          + " WHERE c.cwe_id IS NULL");

      LOG.info("kev_catalog.cwes -> cwe_catalog: {} distinct CWE IDs unresolved"
          + " ({} KEV CWE rows total)", orphans, withCwes);
      if (orphans > 0) {
        logExamples(conn,
            "SELECT DISTINCT kc.cwe_id FROM cyber_vuln.kev_cwes kc"
            + " LEFT JOIN cyber_vuln.cwe_catalog c ON kc.cwe_id = c.cwe_id"
            + " WHERE c.cwe_id IS NULL ORDER BY kc.cwe_id LIMIT 5",
            "KEV CWE IDs not in catalog");
      }
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private long scalar(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getLong(1) : 0L;
    }
  }

  private void logExamples(Connection conn, String sql, String label) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      StringBuilder sb = new StringBuilder("\nExamples — ").append(label).append(":\n");
      int cols = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        sb.append("  ");
        for (int i = 1; i <= cols; i++) {
          if (i > 1) sb.append(" | ");
          sb.append(rs.getMetaData().getColumnName(i)).append("=").append(rs.getString(i));
        }
        sb.append("\n");
      }
      LOG.info(sb.toString());
    }
  }

  private Connection connect(String modelJson) throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  private String buildModel(String warehousePath, String operatingDirectory, String nvdApiKey) {
    String executionEngine = coalesce(
        TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");

    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("  \"version\": \"1.0\",");
    sb.append("  \"defaultSchema\": \"cyber_vuln\",");
    sb.append("  \"schemas\": [{");
    sb.append("    \"name\": \"cyber_vuln\",");
    sb.append("    \"type\": \"custom\",");
    sb.append("    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",");
    sb.append("    \"operand\": {");
    sb.append("      \"dataSource\": \"cyber_vuln_smoke\",");
    sb.append("      \"executionEngine\": \"").append(executionEngine).append("\",");
    sb.append("      \"directory\": \"").append(warehousePath).append("\",");
    sb.append("      \"operatingDirectory\": \"").append(operatingDirectory).append("\",");
    sb.append("      \"cacheDirectory\": \"").append(cacheDir).append("\",");
    sb.append("      \"ephemeralCache\": false,");
    sb.append("      \"autoDownload\": true,");
    sb.append("      \"materializeDirectory\": \"").append(warehousePath).append("\",");
    sb.append("      \"database_filename\": \"").append(operatingDirectory)
        .append("/cyber_vuln_db.duckdb\"");
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
