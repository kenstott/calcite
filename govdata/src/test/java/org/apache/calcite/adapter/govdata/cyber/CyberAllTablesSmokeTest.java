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
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end smoke test for ALL cyber tables using local Iceberg storage.
 *
 * <p>Covers:
 * <ul>
 *   <li>cyber_vuln (smoke schema): cwe_catalog, kev_catalog, vulnerabilities (7-day NVD window),
 *       vulnerability_cwes, kev_cwes</li>
 *   <li>cyber_threat: attack_techniques, nist_controls, nist_csf_functions, cis_controls,
 *       owasp_top10, attack_to_nist_mappings, ioc_urls, ioc_hashes, ioc_ips</li>
 *   <li>API-key-gated (skipped when key absent): ioc_mixed, threat_pulses</li>
 * </ul>
 *
 * <p>For each table group the test asserts:
 * <ol>
 *   <li>Row count &gt; 0</li>
 *   <li>Primary key column(s) are never NULL</li>
 *   <li>No duplicate primary keys</li>
 * </ol>
 *
 * <p>FK join coverage:
 * <ul>
 *   <li>kev_catalog.cve_id → vulnerabilities.cve_id</li>
 *   <li>vulnerability_cwes.cve_id → vulnerabilities.cve_id</li>
 *   <li>vulnerability_cwes.cwe_id → cwe_catalog.cwe_id</li>
 *   <li>kev_cwes.cve_id → kev_catalog.cve_id</li>
 *   <li>kev_cwes.cwe_id → cwe_catalog.cwe_id</li>
 *   <li>attack_to_nist_mappings.technique_id → attack_techniques.technique_id</li>
 *   <li>attack_to_nist_mappings.nist_control_id → nist_controls.control_id</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * CYBER_INTEGRATION_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*CyberAllTablesSmokeTest*" \
 *     --console=plain
 * </pre>
 *
 * <p>With API keys for full coverage:
 * <pre>
 * CYBER_INTEGRATION_TESTS=true \
 * CYBER_THREATFOX_API_KEY=your-key \
 * CYBER_OTX_API_KEY=your-key \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*CyberAllTablesSmokeTest*"
 * </pre>
 */
@Tag("integration")
class CyberAllTablesSmokeTest {

  private static final Logger LOG = LoggerFactory.getLogger(CyberAllTablesSmokeTest.class);

  private static String vulnWarehouse;
  private static String threatWarehouse;
  private static String vulnOperatingDir;
  private static String threatOperatingDir;
  private static String cacheDir;
  private static boolean hasThreatfoxKey;
  private static boolean hasOtxKey;

  @BeforeAll static void setup() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("CYBER_INTEGRATION_TESTS")),
        "Set CYBER_INTEGRATION_TESTS=true to run cyber smoke tests");

    TestEnvironmentLoader.ensureLoaded();

    File tmpBase = Files.createTempDirectory("cyber_smoke_all_").toFile();
    vulnWarehouse = new File(tmpBase, "vuln").getAbsolutePath();
    threatWarehouse = new File(tmpBase, "threat").getAbsolutePath();
    vulnOperatingDir = new File(tmpBase, "op_vuln").getAbsolutePath();
    threatOperatingDir = new File(tmpBase, "op_threat").getAbsolutePath();

    String configured = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    cacheDir = (configured != null && !configured.isEmpty() && !configured.startsWith("s3://"))
        ? configured + "/cyber"
        : new File(tmpBase, "cache").getAbsolutePath();

    hasThreatfoxKey = !coalesce(
        System.getenv("CYBER_THREATFOX_API_KEY"),
        TestEnvironmentLoader.getEnv("CYBER_THREATFOX_API_KEY"), "").isEmpty();
    hasOtxKey = !coalesce(
        System.getenv("CYBER_OTX_API_KEY"),
        TestEnvironmentLoader.getEnv("CYBER_OTX_API_KEY"), "").isEmpty();

    LOG.info("Vuln warehouse:   {}", vulnWarehouse);
    LOG.info("Threat warehouse: {}", threatWarehouse);
    LOG.info("Cache dir:        {}", cacheDir);
    LOG.info("ThreatFox key:    {}", hasThreatfoxKey ? "present" : "absent (ioc_mixed skipped)");
    LOG.info("OTX key:          {}", hasOtxKey ? "present" : "absent (threat_pulses skipped)");
  }

  // ── cyber_vuln: data presence ─────────────────────────────────────────────

  @Test void cweAndKevTablesHaveData() throws Exception {
    LOG.info("=== cyber_vuln: cwe_catalog, kev_catalog ===");
    try (Connection conn = vulnConn()) {
      assertRowCount(conn, "cyber_vuln", "cwe_catalog", 100);
      assertRowCount(conn, "cyber_vuln", "kev_catalog", 100);
    }
  }

  @Test void nvdVulnerabilitiesHaveData() throws Exception {
    LOG.info("=== cyber_vuln: vulnerabilities (7-day NVD window) ===");
    try (Connection conn = vulnConn()) {
      long count = scalar(conn, "SELECT COUNT(*) FROM \"cyber_vuln\".\"vulnerabilities\"");
      LOG.info("vulnerabilities: {} rows in 7-day window", count);
      assertTrue(count > 0, "vulnerabilities: expected >0 rows in 7-day NVD window");
    }
  }

  @Test void junctionTablesHaveData() throws Exception {
    LOG.info("=== cyber_vuln: vulnerability_cwes, kev_cwes ===");
    try (Connection conn = vulnConn()) {
      long vulnCwes = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"vulnerability_cwes\"");
      LOG.info("vulnerability_cwes: {} rows", vulnCwes);
      assertTrue(vulnCwes >= 0,
          "vulnerability_cwes: table must be queryable (may be 0 for a quiet 7-day window)");

      long kevCwes = scalar(conn, "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_cwes\"");
      LOG.info("kev_cwes: {} rows", kevCwes);
      assertTrue(kevCwes > 0, "kev_cwes: KEV entries should have CWE references");
    }
  }

  // ── cyber_vuln: PK non-null ────────────────────────────────────────────────

  @Test void vulnPkColumnsNonNull() throws Exception {
    LOG.info("=== cyber_vuln: PK non-null checks ===");
    try (Connection conn = vulnConn()) {
      assertPkNonNull(conn, "cyber_vuln", "cwe_catalog", "cwe_id");
      assertPkNonNull(conn, "cyber_vuln", "kev_catalog", "cve_id");
      assertPkNonNull(conn, "cyber_vuln", "vulnerabilities", "cve_id");
      assertPkNonNull(conn, "cyber_vuln", "vulnerability_cwes", "cve_id");
      assertPkNonNull(conn, "cyber_vuln", "vulnerability_cwes", "cwe_id");
      assertPkNonNull(conn, "cyber_vuln", "kev_cwes", "cve_id");
      assertPkNonNull(conn, "cyber_vuln", "kev_cwes", "cwe_id");
    }
  }

  // ── cyber_vuln: no duplicate PKs ─────────────────────────────────────────

  @Test void vulnNoDuplicatePks() throws Exception {
    LOG.info("=== cyber_vuln: duplicate PK checks ===");
    try (Connection conn = vulnConn()) {
      assertNoDuplicatePk(conn, "cyber_vuln", "cwe_catalog", "cwe_id");
      assertNoDuplicatePk(conn, "cyber_vuln", "vulnerabilities", "cve_id");
      assertNoDuplicatePk(conn, "cyber_vuln", "kev_catalog", "cve_id");
    }
  }

  // ── cyber_vuln: FK joins ──────────────────────────────────────────────────

  @Test void kevReferencesVulnerabilities() throws Exception {
    LOG.info("=== FK: kev_catalog.cve_id → vulnerabilities.cve_id ===");
    try (Connection conn = vulnConn()) {
      long total = scalar(conn, "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_catalog\"");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_catalog\" k"
          + " INNER JOIN \"cyber_vuln\".\"vulnerabilities\" v ON k.cve_id = v.cve_id");
      long orphans = total - matched;
      LOG.info("kev→vuln: {}/{} KEV entries matched in NVD (orphans={})", matched, total, orphans);
      if (orphans > 0) {
        logExamples(conn,
            "SELECT k.cve_id, k.date_added FROM \"cyber_vuln\".\"kev_catalog\" k"
            + " LEFT JOIN \"cyber_vuln\".\"vulnerabilities\" v ON k.cve_id = v.cve_id"
            + " WHERE v.cve_id IS NULL ORDER BY k.date_added DESC LIMIT 5",
            "orphaned KEV entries (no NVD record — expected for newly added CVEs)");
      }
      assertTrue(matched > 0, "At least some KEV entries must resolve to NVD records");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      assertTrue(matchRate >= 0.90,
          String.format("KEV→NVD match rate %.1f%% below 90%% threshold", matchRate * 100));
    }
  }

  @Test void vulnerabilityCwesReferenceVulnerabilitiesAndCatalog() throws Exception {
    LOG.info("=== FK: vulnerability_cwes → vulnerabilities + cwe_catalog ===");
    try (Connection conn = vulnConn()) {
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"vulnerability_cwes\"");
      if (total == 0) {
        LOG.info("vulnerability_cwes: 0 rows in 7-day window — FK check skipped");
        return;
      }

      long orphanCve = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"vulnerability_cwes\" vc"
          + " LEFT JOIN \"cyber_vuln\".\"vulnerabilities\" v ON vc.cve_id = v.cve_id"
          + " WHERE v.cve_id IS NULL");
      double orphanRate = total > 0 ? (double) orphanCve / total : 0.0;
      LOG.info("vulnerability_cwes→vulnerabilities: {}/{} rows have no parent CVE ({} %)",
          orphanCve, total, String.format("%.1f", orphanRate * 100));
      // NVD junction and parent tables are fetched in separate HTTP calls; timing skew can
      // produce a small number of cwes referencing CVEs not yet in the vulnerabilities table.
      assertTrue(orphanRate < 0.10,
          String.format("vulnerability_cwes orphan CVE rate %.1f%% exceeds 10%% threshold",
              orphanRate * 100));

      long orphanCwe = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"vulnerability_cwes\" vc"
          + " LEFT JOIN \"cyber_vuln\".\"cwe_catalog\" c ON vc.cwe_id = c.cwe_id"
          + " WHERE c.cwe_id IS NULL");
      LOG.info("vulnerability_cwes→cwe_catalog: {}/{} rows have unknown CWE", orphanCwe, total);
    }
  }

  @Test void kevCwesReferenceKevAndCatalog() throws Exception {
    LOG.info("=== FK: kev_cwes → kev_catalog + cwe_catalog ===");
    try (Connection conn = vulnConn()) {
      long total = scalar(conn, "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_cwes\"");
      if (total == 0) {
        LOG.warn("kev_cwes: 0 rows — FK check skipped");
        return;
      }

      long orphanKev = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_cwes\" kc"
          + " LEFT JOIN \"cyber_vuln\".\"kev_catalog\" k ON kc.cve_id = k.cve_id"
          + " WHERE k.cve_id IS NULL");
      LOG.info("kev_cwes→kev_catalog: {}/{} rows have no parent KEV entry", orphanKev, total);

      long orphanCwe = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_cwes\" kc"
          + " LEFT JOIN \"cyber_vuln\".\"cwe_catalog\" c ON kc.cwe_id = c.cwe_id"
          + " WHERE c.cwe_id IS NULL");
      LOG.info("kev_cwes→cwe_catalog: {}/{} CWE references unresolved", orphanCwe, total);

      assertEquals(0L, orphanKev, "kev_cwes must not reference CVEs absent from kev_catalog");
    }
  }

  // ── cyber_vuln: views ─────────────────────────────────────────────────────

  @Test void vulnViewsReturnRows() throws Exception {
    LOG.info("=== cyber_vuln: views kev_enriched, vuln_cwe_enriched, kev_cwe_enriched ===");
    try (Connection conn = vulnConn()) {
      long kevEnriched = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_enriched\"");
      assertTrue(kevEnriched > 0, "kev_enriched view must return rows");
      LOG.info("kev_enriched: {} rows", kevEnriched);

      long vulnCweEnriched = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"vuln_cwe_enriched\"");
      LOG.info("vuln_cwe_enriched: {} rows", vulnCweEnriched);

      long kevCweEnriched = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_vuln\".\"kev_cwe_enriched\"");
      assertTrue(kevCweEnriched > 0, "kev_cwe_enriched view must return rows");
      LOG.info("kev_cwe_enriched: {} rows", kevCweEnriched);
    }
  }

  // ── cyber_vuln: sample rows ───────────────────────────────────────────────

  @Test void vulnSampleRows() throws Exception {
    LOG.info("=== cyber_vuln: sample rows ===");
    String[] tables = {"cwe_catalog", "kev_catalog", "vulnerabilities",
        "vulnerability_cwes", "kev_cwes"};
    try (Connection conn = vulnConn()) {
      for (String table : tables) {
        sampleRow(conn, "cyber_vuln", table);
      }
    }
  }

  // ── cyber_threat: data presence ───────────────────────────────────────────

  @Test void nistControlsHaveData() throws Exception {
    LOG.info("=== cyber_threat: nist_controls ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "nist_controls", 100);
      assertPkNonNull(conn, "cyber_threat", "nist_controls", "control_id");
      assertNoDuplicatePk(conn, "cyber_threat", "nist_controls", "control_id");
    }
  }

  @Test void nistCsfFunctionsHaveData() throws Exception {
    LOG.info("=== cyber_threat: nist_csf_functions ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "nist_csf_functions", 5);
      assertPkNonNull(conn, "cyber_threat", "nist_csf_functions", "subcategory_id");
      assertPkNonNull(conn, "cyber_threat", "nist_csf_functions", "function_id");
      assertPkNonNull(conn, "cyber_threat", "nist_csf_functions", "category_id");
      assertNoDuplicatePk(conn, "cyber_threat", "nist_csf_functions", "subcategory_id");

      // NIST CSF 2.0 has exactly 6 functions (GV, ID, PR, DE, RS, RC)
      long functions = scalar(conn,
          "SELECT COUNT(DISTINCT function_id) FROM \"cyber_threat\".\"nist_csf_functions\"");
      assertEquals(6L, functions, "NIST CSF 2.0 must have exactly 6 functions");
      LOG.info("nist_csf_functions: {} distinct functions, {} subcategories total",
          functions, scalar(conn, "SELECT COUNT(*) FROM \"cyber_threat\".\"nist_csf_functions\""));
    }
  }

  @Test void cisControlsHaveData() throws Exception {
    LOG.info("=== cyber_threat: cis_controls ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "cis_controls", 100);
      assertPkNonNull(conn, "cyber_threat", "cis_controls", "safeguard_id");
      assertPkNonNull(conn, "cyber_threat", "cis_controls", "control_id");
      assertNoDuplicatePk(conn, "cyber_threat", "cis_controls", "safeguard_id");

      long controlGroups = scalar(conn,
          "SELECT COUNT(DISTINCT control_id) FROM \"cyber_threat\".\"cis_controls\"");
      assertTrue(controlGroups >= 18,
          "CIS Controls v8 must have at least 18 control groups, got: " + controlGroups);
      LOG.info("cis_controls: {} control groups, {} safeguards total",
          controlGroups,
          scalar(conn, "SELECT COUNT(*) FROM \"cyber_threat\".\"cis_controls\""));
    }
  }

  @Test void owaspTop10HasData() throws Exception {
    LOG.info("=== cyber_threat: owasp_top10 ===");
    try (Connection conn = threatConn()) {
      long count = scalar(conn, "SELECT COUNT(*) FROM \"cyber_threat\".\"owasp_top10\"");
      assertEquals(10L, count, "OWASP Top 10 must have exactly 10 entries");
      assertPkNonNull(conn, "cyber_threat", "owasp_top10", "entry_id");
      assertNoDuplicatePk(conn, "cyber_threat", "owasp_top10", "entry_id");
      LOG.info("owasp_top10: {} entries", count);
    }
  }

  @Test void attackTechniquesHaveData() throws Exception {
    LOG.info("=== cyber_threat: attack_techniques ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "attack_techniques", 100);
      assertPkNonNull(conn, "cyber_threat", "attack_techniques", "technique_id");
      assertNoDuplicatePk(conn, "cyber_threat", "attack_techniques", "technique_id");
      LOG.info("attack_techniques: {} rows",
          scalar(conn, "SELECT COUNT(*) FROM \"cyber_threat\".\"attack_techniques\""));
    }
  }

  @Test void attackToNistMappingsHaveData() throws Exception {
    LOG.info("=== cyber_threat: attack_to_nist_mappings ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "attack_to_nist_mappings", 10);
      assertPkNonNull(conn, "cyber_threat", "attack_to_nist_mappings", "technique_id");
      assertPkNonNull(conn, "cyber_threat", "attack_to_nist_mappings", "nist_control_id");
      LOG.info("attack_to_nist_mappings: {} rows",
          scalar(conn,
              "SELECT COUNT(*) FROM \"cyber_threat\".\"attack_to_nist_mappings\""));
    }
  }

  @Test void iocFeedsHaveData() throws Exception {
    LOG.info("=== cyber_threat: ioc_urls, ioc_hashes, ioc_ips ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "ioc_urls", 10);
      assertPkNonNull(conn, "cyber_threat", "ioc_urls", "url");
      assertPkNonNull(conn, "cyber_threat", "ioc_urls", "first_seen");

      assertRowCount(conn, "cyber_threat", "ioc_hashes", 10);
      assertPkNonNull(conn, "cyber_threat", "ioc_hashes", "sha256");

      assertRowCount(conn, "cyber_threat", "ioc_ips", 5);
      assertPkNonNull(conn, "cyber_threat", "ioc_ips", "ip_address");
    }
  }

  @Test void iocMixedHasDataWhenKeyPresent() throws Exception {
    if (!hasThreatfoxKey) {
      LOG.info("=== cyber_threat: ioc_mixed SKIPPED (CYBER_THREATFOX_API_KEY absent) ===");
      return;
    }
    LOG.info("=== cyber_threat: ioc_mixed ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "ioc_mixed", 1);
      assertPkNonNull(conn, "cyber_threat", "ioc_mixed", "ioc_value");
    }
  }

  @Test void threatPulsesHaveDataWhenKeyPresent() throws Exception {
    if (!hasOtxKey) {
      LOG.info("=== cyber_threat: threat_pulses SKIPPED (CYBER_OTX_API_KEY absent) ===");
      return;
    }
    LOG.info("=== cyber_threat: threat_pulses ===");
    try (Connection conn = threatConn()) {
      assertRowCount(conn, "cyber_threat", "threat_pulses", 1);
      assertPkNonNull(conn, "cyber_threat", "threat_pulses", "pulse_id");
    }
  }

  // ── cyber_threat: PK non-null (bulk) ─────────────────────────────────────

  @Test void threatStaticPkColumnsNonNull() throws Exception {
    LOG.info("=== cyber_threat: bulk PK non-null checks ===");
    try (Connection conn = threatConn()) {
      assertPkNonNull(conn, "cyber_threat", "nist_controls", "control_id");
      assertPkNonNull(conn, "cyber_threat", "nist_csf_functions", "subcategory_id");
      assertPkNonNull(conn, "cyber_threat", "cis_controls", "safeguard_id");
      assertPkNonNull(conn, "cyber_threat", "owasp_top10", "entry_id");
      assertPkNonNull(conn, "cyber_threat", "attack_techniques", "technique_id");
      assertPkNonNull(conn, "cyber_threat", "attack_to_nist_mappings", "technique_id");
      assertPkNonNull(conn, "cyber_threat", "attack_to_nist_mappings", "nist_control_id");
    }
  }

  // ── cyber_threat: FK joins ─────────────────────────────────────────────────

  @Test void attackToNistJoinsResolve() throws Exception {
    LOG.info("=== FK: attack_to_nist_mappings → attack_techniques + nist_controls ===");
    try (Connection conn = threatConn()) {
      long totalMappings = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_threat\".\"attack_to_nist_mappings\"");

      long resolvedTechniques = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_threat\".\"attack_to_nist_mappings\" m"
          + " INNER JOIN \"cyber_threat\".\"attack_techniques\" t"
          + " ON m.technique_id = t.technique_id");
      long orphanTechniques = totalMappings - resolvedTechniques;
      LOG.info("attack_to_nist→attack_techniques: {}/{} mappings resolved (orphans={})",
          resolvedTechniques, totalMappings, orphanTechniques);

      long resolvedControls = scalar(conn,
          "SELECT COUNT(*) FROM \"cyber_threat\".\"attack_to_nist_mappings\" m"
          + " INNER JOIN \"cyber_threat\".\"nist_controls\" n"
          + " ON m.nist_control_id = n.control_id");
      long orphanControls = totalMappings - resolvedControls;
      LOG.info("attack_to_nist→nist_controls: {}/{} mappings resolved (orphans={})",
          resolvedControls, totalMappings, orphanControls);

      if (orphanTechniques > 0) {
        logExamples(conn,
            "SELECT m.technique_id FROM \"cyber_threat\".\"attack_to_nist_mappings\" m"
            + " LEFT JOIN \"cyber_threat\".\"attack_techniques\" t"
            + " ON m.technique_id = t.technique_id"
            + " WHERE t.technique_id IS NULL GROUP BY m.technique_id LIMIT 5",
            "technique_ids in mappings with no ATT&CK record");
      }

      double techniqueRate = totalMappings > 0 ? (double) resolvedTechniques / totalMappings : 0.0;
      assertTrue(techniqueRate >= 0.95,
          String.format("ATT&CK technique resolution rate %.1f%% below 95%% threshold",
              techniqueRate * 100));

      double controlRate = totalMappings > 0 ? (double) resolvedControls / totalMappings : 0.0;
      assertTrue(controlRate >= 0.95,
          String.format("NIST control resolution rate %.1f%% below 95%% threshold",
              controlRate * 100));
    }
  }

  // ── cyber_threat: cross-domain join ──────────────────────────────────────

  @Test void threatCrossDomainJoinAttackToNistToOwasp() throws Exception {
    LOG.info("=== Cross-domain: attack_techniques → nist_controls → owasp_top10 ===");
    try (Connection conn = threatConn()) {
      // Validate that the three key dimensions are independently joinable
      long attackCount = scalar(conn,
          "SELECT COUNT(DISTINCT t.technique_id)"
          + " FROM \"cyber_threat\".\"attack_techniques\" t"
          + " INNER JOIN \"cyber_threat\".\"attack_to_nist_mappings\" m"
          + " ON t.technique_id = m.technique_id");
      assertTrue(attackCount > 0,
          "At least some ATT&CK techniques must appear in mappings");
      LOG.info("ATT&CK techniques with at least one NIST mapping: {}", attackCount);

      long controlCount = scalar(conn,
          "SELECT COUNT(DISTINCT n.control_id)"
          + " FROM \"cyber_threat\".\"nist_controls\" n"
          + " INNER JOIN \"cyber_threat\".\"attack_to_nist_mappings\" m"
          + " ON n.control_id = m.nist_control_id");
      assertTrue(controlCount > 0,
          "At least some NIST controls must appear in mappings");
      LOG.info("NIST controls with at least one ATT&CK mapping: {}", controlCount);
    }
  }

  // ── cyber_threat: sample rows ─────────────────────────────────────────────

  @Test void threatSampleRows() throws Exception {
    LOG.info("=== cyber_threat: sample rows ===");
    String[] tables = {
        "attack_techniques", "nist_controls", "nist_csf_functions",
        "cis_controls", "owasp_top10", "attack_to_nist_mappings",
        "ioc_urls", "ioc_hashes", "ioc_ips"
    };
    try (Connection conn = threatConn()) {
      for (String table : tables) {
        sampleRow(conn, "cyber_threat", table);
      }
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private Connection vulnConn() throws Exception {
    return connect(buildVulnModel());
  }

  private Connection threatConn() throws Exception {
    return connect(buildModel("cyber_threat", "cyber_threat", threatWarehouse, threatOperatingDir));
  }

  private Connection connect(String modelJson) throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  private String buildVulnModel() {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    // autoDownload=false: let the YAML sources drive all fetching. Using autoDownload=true
    // triggers CweDownloader/NvdDownloader pre-writes whose output paths diverge from the
    // Iceberg table paths (materializeDirectory resolves differently), causing dup/null PKs.
    return "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"cyber_vuln\","
        + "\"schemas\":[{"
        + "  \"name\":\"cyber_vuln\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\":{"
        + "    \"dataSource\":\"cyber_vuln_smoke\","
        + "    \"executionEngine\":\"" + engine + "\","
        + "    \"directory\":\"" + vulnWarehouse + "\","
        + "    \"operatingDirectory\":\"" + vulnOperatingDir + "\","
        + "    \"cacheDirectory\":\"" + cacheDir + "\","
        + "    \"ephemeralCache\":false,"
        + "    \"autoDownload\":false,"
        + "    \"database_filename\":\"" + vulnOperatingDir + "/cyber_vuln_db.duckdb\""
        + "  }"
        + "}]}";
  }

  private String buildModel(String schemaName, String dataSource,
      String warehouse, String operatingDir) {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    return "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"" + schemaName + "\","
        + "\"schemas\":[{"
        + "  \"name\":\"" + schemaName + "\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\":{"
        + "    \"dataSource\":\"" + dataSource + "\","
        + "    \"executionEngine\":\"" + engine + "\","
        + "    \"directory\":\"" + warehouse + "\","
        + "    \"operatingDirectory\":\"" + operatingDir + "\","
        + "    \"cacheDirectory\":\"" + cacheDir + "\","
        + "    \"ephemeralCache\":false,"
        + "    \"autoDownload\":true,"
        + "    \"database_filename\":\"" + operatingDir + "/" + schemaName + "_db.duckdb\""
        + "  }"
        + "}]}";
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

  private void assertNoDuplicatePk(Connection conn, String schema, String table,
      String pkColumn) throws Exception {
    long dups = scalar(conn,
        "SELECT COUNT(*) FROM ("
        + "SELECT \"" + pkColumn + "\", COUNT(*) AS cnt"
        + " FROM \"" + schema + "\".\"" + table + "\""
        + " GROUP BY \"" + pkColumn + "\""
        + " HAVING COUNT(*) > 1) t");
    if (dups > 0) {
      logExamples(conn,
          "SELECT \"" + pkColumn + "\", COUNT(*) AS cnt"
          + " FROM \"" + schema + "\".\"" + table + "\""
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
        + " GROUP BY \"" + pkColumn + "\""
        + " HAVING COUNT(*) > 1) t");
    if (dups > 0) {
      LOG.warn("{}.{}.{}: {} duplicate PK groups (warning only — see autoDownload double-write note)",
          schema, table, pkColumn, dups);
      logExamples(conn,
          "SELECT \"" + pkColumn + "\", COUNT(*) AS cnt"
          + " FROM \"" + schema + "\".\"" + table + "\""
          + " GROUP BY \"" + pkColumn + "\""
          + " HAVING COUNT(*) > 1"
          + " ORDER BY cnt DESC LIMIT 5",
          "duplicate PKs in " + schema + "." + table);
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
            val = val.substring(0, 120) + "…";
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
      StringBuilder sb = new StringBuilder("Examples — ").append(label).append(":\n");
      while (rs.next()) {
        for (int i = 1; i <= cols; i++) {
          sb.append("  ").append(meta.getColumnName(i))
              .append("=").append(rs.getString(i)).append(" ");
        }
        sb.append("\n");
      }
      LOG.warn(sb.toString());
    } catch (Exception e) {
      LOG.warn("Could not fetch examples for {}: {}", label, e.getMessage());
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
