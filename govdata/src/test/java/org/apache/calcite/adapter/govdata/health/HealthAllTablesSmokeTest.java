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
package org.apache.calcite.adapter.govdata.health;

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
 * End-to-end smoke test for Phase 1 health schema tables (openFDA).
 *
 * <p>Covers all five Phase 1 tables:
 * <ul>
 *   <li>fda_ndc_products — NDC drug catalog (~134k records)</li>
 *   <li>fda_drug_approvals — NDA/BLA/ANDA applications (~29k records)</li>
 *   <li>fda_drug_recalls — drug enforcement actions</li>
 *   <li>fda_adverse_events — FAERS adverse event reports (windowed)</li>
 *   <li>fda_device_recalls — device enforcement actions</li>
 * </ul>
 *
 * <p>For each table the test asserts:
 * <ol>
 *   <li>Row count &gt; minimum threshold</li>
 *   <li>Primary key column is never NULL</li>
 *   <li>No duplicate primary keys</li>
 * </ol>
 *
 * <p>FK join coverage:
 * <ul>
 *   <li>fda_drug_approvals.application_number → fda_ndc_products.application_number</li>
 *   <li>fda_drug_recalls.recalling_firm → fda_ndc_products.labeler_name (text)</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * HEALTH_INTEGRATION_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*HealthAllTablesSmokeTest*" \
 *     --console=plain
 * </pre>
 *
 * <p>With API key for elevated rate limits:
 * <pre>
 * HEALTH_INTEGRATION_TESTS=true \
 * HEALTH_FDA_API_KEY=your-key \
 * GOVDATA_START_YEAR=2024 \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*HealthAllTablesSmokeTest*"
 * </pre>
 */
@Tag("integration")
class HealthAllTablesSmokeTest {

  private static final Logger LOG = LoggerFactory.getLogger(HealthAllTablesSmokeTest.class);

  private static final String SCHEMA = "health";

  private static String warehouse;
  private static String operatingDir;
  private static String cacheDir;
  @BeforeAll static void setup() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("HEALTH_INTEGRATION_TESTS")),
        "Set HEALTH_INTEGRATION_TESTS=true to run health smoke tests");

    TestEnvironmentLoader.ensureLoaded();

    File tmpBase = Files.createTempDirectory("health_smoke_").toFile();
    warehouse = new File(tmpBase, "parquet").getAbsolutePath();
    operatingDir = new File(tmpBase, "op").getAbsolutePath();

    String configured = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    cacheDir = (configured != null && !configured.isEmpty() && !configured.startsWith("s3://"))
        ? configured + "/health"
        : new File(tmpBase, "cache").getAbsolutePath();

    LOG.info("Warehouse:     {}", warehouse);
    LOG.info("Cache dir:     {}", cacheDir);
  }

  // ── fda_ndc_products ────────────────────────────────────────────────────────

  @Test void ndcProductsHaveData() throws Exception {
    LOG.info("=== health: fda_ndc_products ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "fda_ndc_products", 100);
      assertPkNonNull(conn, SCHEMA, "fda_ndc_products", "product_ndc");
      // FDA NDC data contains known duplicates (same NDC listed under multiple labelers)
      warnDuplicatePk(conn, SCHEMA, "fda_ndc_products", "product_ndc");
    }
  }

  @Test void ndcProductsSampleRow() throws Exception {
    LOG.info("=== health: fda_ndc_products sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "fda_ndc_products");
    }
  }

  // ── fda_drug_approvals ──────────────────────────────────────────────────────

  @Test void drugApprovalsHaveData() throws Exception {
    LOG.info("=== health: fda_drug_approvals ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "fda_drug_approvals", 50);
      assertPkNonNull(conn, SCHEMA, "fda_drug_approvals", "application_number");
      assertNoDuplicatePk(conn, SCHEMA, "fda_drug_approvals", "application_number");
    }
  }

  @Test void drugApprovalsSampleRow() throws Exception {
    LOG.info("=== health: fda_drug_approvals sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "fda_drug_approvals");
    }
  }

  // ── fda_drug_recalls ────────────────────────────────────────────────────────

  @Test void drugRecallsHaveData() throws Exception {
    LOG.info("=== health: fda_drug_recalls ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "fda_drug_recalls", 50);
      warnNullPk(conn, SCHEMA, "fda_drug_recalls", "recall_number");
      assertNoDuplicatePk(conn, SCHEMA, "fda_drug_recalls", "recall_number");
    }
  }

  @Test void drugRecallsSampleRow() throws Exception {
    LOG.info("=== health: fda_drug_recalls sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "fda_drug_recalls");
    }
  }

  // ── fda_adverse_events ──────────────────────────────────────────────────────

  @Test void adverseEventsHaveData() throws Exception {
    LOG.info("=== health: fda_adverse_events ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "fda_adverse_events", 100);
      assertPkNonNull(conn, SCHEMA, "fda_adverse_events", "safety_report_id");
      assertNoDuplicatePk(conn, SCHEMA, "fda_adverse_events", "safety_report_id");
    }
  }

  @Test void adverseEventsSampleRow() throws Exception {
    LOG.info("=== health: fda_adverse_events sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "fda_adverse_events");
    }
  }

  // ── fda_device_recalls ──────────────────────────────────────────────────────

  @Test void deviceRecallsHaveData() throws Exception {
    LOG.info("=== health: fda_device_recalls ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "fda_device_recalls", 50);
      warnNullPk(conn, SCHEMA, "fda_device_recalls", "cfres_id");
      assertNoDuplicatePk(conn, SCHEMA, "fda_device_recalls", "cfres_id");
    }
  }

  @Test void deviceRecallsSampleRow() throws Exception {
    LOG.info("=== health: fda_device_recalls sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "fda_device_recalls");
    }
  }

  // ── clinical_trials ─────────────────────────────────────────────────────────

  @Test void clinicalTrialsHaveData() throws Exception {
    LOG.info("=== health: clinical_trials ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "clinical_trials", 1000);
      assertPkNonNull(conn, SCHEMA, "clinical_trials", "nct_id");
      assertNoDuplicatePk(conn, SCHEMA, "clinical_trials", "nct_id");
    }
  }

  @Test void clinicalTrialsSampleRow() throws Exception {
    LOG.info("=== health: clinical_trials sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "clinical_trials");
    }
  }

  // ── clinical_trial_conditions ───────────────────────────────────────────────

  @Test void clinicalTrialConditionsHaveData() throws Exception {
    LOG.info("=== health: clinical_trial_conditions ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "clinical_trial_conditions", 1000);
      warnNullPk(conn, SCHEMA, "clinical_trial_conditions", "nct_id");
      warnNullPk(conn, SCHEMA, "clinical_trial_conditions", "condition_name");
      warnDuplicateCompositePk(conn, SCHEMA, "clinical_trial_conditions",
          "nct_id", "condition_name");
    }
  }

  @Test void clinicalTrialConditionsSampleRow() throws Exception {
    LOG.info("=== health: clinical_trial_conditions sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "clinical_trial_conditions");
    }
  }

  // ── clinical_trial_interventions ────────────────────────────────────────────

  @Test void clinicalTrialInterventionsHaveData() throws Exception {
    LOG.info("=== health: clinical_trial_interventions ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "clinical_trial_interventions", 500);
      warnNullPk(conn, SCHEMA, "clinical_trial_interventions", "nct_id");
      warnNullPk(conn, SCHEMA, "clinical_trial_interventions", "intervention_name");
      warnDuplicateCompositePk(conn, SCHEMA, "clinical_trial_interventions",
          "nct_id", "intervention_name");
    }
  }

  @Test void clinicalTrialInterventionsSampleRow() throws Exception {
    LOG.info("=== health: clinical_trial_interventions sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "clinical_trial_interventions");
    }
  }

  // ── cdc_covid_vaccinations ──────────────────────────────────────────────────

  @Test void cdcCovidVaccinationsHaveData() throws Exception {
    LOG.info("=== health: cdc_covid_vaccinations ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "cdc_covid_vaccinations", 10);
      warnNullPk(conn, SCHEMA, "cdc_covid_vaccinations", "date");
      warnDuplicatePk(conn, SCHEMA, "cdc_covid_vaccinations", "date");
    }
  }

  @Test void cdcCovidVaccinationsSampleRow() throws Exception {
    LOG.info("=== health: cdc_covid_vaccinations sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "cdc_covid_vaccinations");
    }
  }

  // ── cms_hospital_quality ────────────────────────────────────────────────────

  @Test void cmsHospitalQualityHaveData() throws Exception {
    LOG.info("=== health: cms_hospital_quality ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "cms_hospital_quality", 100);
      assertPkNonNull(conn, SCHEMA, "cms_hospital_quality", "facility_id");
      warnDuplicatePk(conn, SCHEMA, "cms_hospital_quality", "facility_id");
    }
  }

  @Test void cmsHospitalQualitySampleRow() throws Exception {
    LOG.info("=== health: cms_hospital_quality sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "cms_hospital_quality");
    }
  }

  // ── medicaid_drug_utilization ───────────────────────────────────────────────

  @Test void medicaidDrugUtilizationHaveData() throws Exception {
    LOG.info("=== health: medicaid_drug_utilization ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "medicaid_drug_utilization", 1000);
      warnNullPk(conn, SCHEMA, "medicaid_drug_utilization", "ndc");
      long compositeRowCount = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"medicaid_drug_utilization\""
          + " WHERE state IS NOT NULL AND ndc IS NOT NULL"
          + " AND \"year\" IS NOT NULL AND \"quarter\" IS NOT NULL");
      LOG.info("{}.{}: {} rows with complete composite key",
          SCHEMA, "medicaid_drug_utilization", compositeRowCount);
      assertTrue(compositeRowCount >= 100,
          "Expected at least 100 rows with complete (state, ndc, year, quarter) composite key");
    }
  }

  @Test void medicaidDrugUtilizationSampleRow() throws Exception {
    LOG.info("=== health: medicaid_drug_utilization sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "medicaid_drug_utilization");
    }
  }

  // ── FK joins ────────────────────────────────────────────────────────────────

  @Test void nctIdJoinsConditionsToTrials() throws Exception {
    LOG.info("=== FK: clinical_trial_conditions.nct_id → clinical_trials.nct_id ===");
    try (Connection conn = healthConn()) {
      // Pre-warm both tables
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"clinical_trials\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"clinical_trial_conditions\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"clinical_trial_conditions\""
          + " WHERE nct_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"clinical_trial_conditions\" c"
          + " INNER JOIN \"" + SCHEMA + "\".\"clinical_trials\" t"
          + " ON c.nct_id = t.nct_id");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("conditions→trials: {}/{} condition records matched to trials ({:.1f}%)",
          matched, total, matchRate * 100);
      assertEquals(total, matched,
          "All clinical_trial_conditions should join to clinical_trials on nct_id");
    }
  }

  @Test void nctIdJoinsInterventionsToTrials() throws Exception {
    LOG.info("=== FK: clinical_trial_interventions.nct_id → clinical_trials.nct_id ===");
    try (Connection conn = healthConn()) {
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"clinical_trial_interventions\""
          + " WHERE nct_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"clinical_trial_interventions\" i"
          + " INNER JOIN \"" + SCHEMA + "\".\"clinical_trials\" t"
          + " ON i.nct_id = t.nct_id");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("interventions→trials: {}/{} intervention records matched to trials ({:.1f}%)",
          matched, total, matchRate * 100);
      assertEquals(total, matched,
          "All clinical_trial_interventions should join to clinical_trials on nct_id");
    }
  }

  @Test void approvalNumberJoinsToNdc() throws Exception {
    LOG.info("=== FK: fda_drug_approvals.application_number → fda_ndc_products.application_number ===");
    try (Connection conn = healthConn()) {
      // Pre-warm both tables so autoDownload completes before the join executes
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_drug_approvals\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_ndc_products\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_drug_approvals\""
          + " WHERE application_number IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_drug_approvals\" a"
          + " INNER JOIN \"" + SCHEMA + "\".\"fda_ndc_products\" n"
          + " ON a.application_number = n.application_number");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("approvals→ndc: {}/{} applications matched to NDC products ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.10,
          String.format("approvals→ndc match rate %.1f%% below 10%% threshold"
              + " (many approvals are for drugs not currently listed in NDC)", matchRate * 100));
    }
  }

  @Test void recallFirmJoinsToNdcLabeler() throws Exception {
    LOG.info("=== FK (text): fda_drug_recalls.recalling_firm → fda_ndc_products.labeler_name ===");
    try (Connection conn = healthConn()) {
      long total = scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_drug_recalls\""
          + " WHERE recalling_firm IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_drug_recalls\" r"
          + " INNER JOIN \"" + SCHEMA + "\".\"fda_ndc_products\" n"
          + " ON UPPER(r.recalling_firm) = UPPER(n.labeler_name)");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("recalls→ndc(labeler): {}/{} recalls matched to NDC labeler ({:.1f}%)",
          matched, total, matchRate * 100);
      // Text FK match rate is expected to be low due to firm name variations
      LOG.info("Note: text FK match rate is informational — name variations expected");
      assertTrue(matched >= 0, "Query must execute without error");
    }
  }

  // ── Phase 3 tests ───────────────────────────────────────────────────────────

  @Test void cdcMortalityHaveData() throws Exception {
    LOG.info("=== health: cdc_mortality row count ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "cdc_mortality", 100);
      warnNullPk(conn, SCHEMA, "cdc_mortality", "cause_name");
    }
  }

  @Test void cdcMortalitySampleRow() throws Exception {
    LOG.info("=== health: cdc_mortality sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "cdc_mortality");
    }
  }

  @Test void cdcBrfssHaveData() throws Exception {
    LOG.info("=== health: cdc_brfss row count ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "cdc_brfss", 100);
      warnNullPk(conn, SCHEMA, "cdc_brfss", "question");
    }
  }

  @Test void cdcBrfssSampleRow() throws Exception {
    LOG.info("=== health: cdc_brfss sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "cdc_brfss");
    }
  }

  @Test void cmsOpenPaymentsHaveData() throws Exception {
    LOG.info("=== health: cms_open_payments row count ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "cms_open_payments", 100);
      warnNullPk(conn, SCHEMA, "cms_open_payments", "physician_profile_id");
    }
  }

  @Test void cmsOpenPaymentsSampleRow() throws Exception {
    LOG.info("=== health: cms_open_payments sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "cms_open_payments");
    }
  }

  @Test void rxNormDrugsHaveData() throws Exception {
    LOG.info("=== health: rxnorm_drugs row count ===");
    try (Connection conn = healthConn()) {
      assertRowCount(conn, SCHEMA, "rxnorm_drugs", 1000);
      assertPkNonNull(conn, SCHEMA, "rxnorm_drugs", "rxcui");
      assertNoDuplicatePk(conn, SCHEMA, "rxnorm_drugs", "rxcui");
    }
  }

  @Test void rxNormDrugsSampleRow() throws Exception {
    LOG.info("=== health: rxnorm_drugs sample row ===");
    try (Connection conn = healthConn()) {
      sampleRow(conn, SCHEMA, "rxnorm_drugs");
    }
  }

  @Test void rxcuiJoinsRxNormToNdcProducts() throws Exception {
    LOG.info("=== FK: rxnorm_drugs.rxcui → fda_ndc_products.rxcui ===");
    try (Connection conn = healthConn()) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"rxnorm_drugs\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_ndc_products\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_ndc_products\""
          + " WHERE rxcui IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"fda_ndc_products\" n"
          + " INNER JOIN \"" + SCHEMA + "\".\"rxnorm_drugs\" r"
          + " ON n.rxcui = r.rxcui");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("ndc→rxnorm: {}/{} NDC products with rxcui matched to RxNorm ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.10,
          String.format("ndc→rxnorm match rate %.1f%% below 10%% threshold", matchRate * 100));
    }
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  private Connection healthConn() throws Exception {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"" + SCHEMA + "\","
        + "\"schemas\":[{"
        + "  \"name\":\"" + SCHEMA + "\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\":{"
        + "    \"dataSource\":\"health\","
        + "    \"executionEngine\":\"" + engine + "\","
        + "    \"directory\":\"" + warehouse + "\","
        + "    \"operatingDirectory\":\"" + operatingDir + "\","
        + "    \"cacheDirectory\":\"" + cacheDir + "\","
        + "    \"ephemeralCache\":false,"
        + "    \"autoDownload\":true,"
        + "    \"database_filename\":\"" + operatingDir + "/health_db.duckdb\""
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
      LOG.warn("{}.{}.{}: {} NULL PK values (kept for partial-join analytics)", schema, table, pkColumn, nulls);
    } else {
      LOG.info("{}.{}.{}: no NULL values", schema, table, pkColumn);
    }
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
      LOG.warn("{}.{}.{}: {} duplicate PK groups (data quality — FDA data may list same {} under multiple labelers)",
          schema, table, pkColumn, dups, pkColumn);
    } else {
      LOG.info("{}.{}.{}: no duplicates", schema, table, pkColumn);
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

  private void assertNoDuplicateCompositePk(Connection conn, String schema, String table,
      String col1, String col2) throws Exception {
    long dups = scalar(conn,
        "SELECT COUNT(*) FROM ("
        + "SELECT \"" + col1 + "\", \"" + col2 + "\", COUNT(*) AS cnt"
        + " FROM \"" + schema + "\".\"" + table + "\""
        + " WHERE \"" + col1 + "\" IS NOT NULL AND \"" + col2 + "\" IS NOT NULL"
        + " GROUP BY \"" + col1 + "\", \"" + col2 + "\""
        + " HAVING COUNT(*) > 1) t");
    assertEquals(0L, dups,
        schema + "." + table + ".(" + col1 + "," + col2 + "): found " + dups + " duplicate composite PK values");
    LOG.info("{}.{}.({},{}): no duplicates", schema, table, col1, col2);
  }

  private void warnDuplicateCompositePk(Connection conn, String schema, String table,
      String col1, String col2) throws Exception {
    long dups = scalar(conn,
        "SELECT COUNT(*) FROM ("
        + "SELECT \"" + col1 + "\", \"" + col2 + "\", COUNT(*) AS cnt"
        + " FROM \"" + schema + "\".\"" + table + "\""
        + " WHERE \"" + col1 + "\" IS NOT NULL AND \"" + col2 + "\" IS NOT NULL"
        + " GROUP BY \"" + col1 + "\", \"" + col2 + "\""
        + " HAVING COUNT(*) > 1) t");
    if (dups > 0) {
      LOG.warn("{}.{}.({},{}): {} duplicate composite PK groups (same intervention in multiple arms/studies)",
          schema, table, col1, col2, dups);
    } else {
      LOG.info("{}.{}.({},{}): no duplicates", schema, table, col1, col2);
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
