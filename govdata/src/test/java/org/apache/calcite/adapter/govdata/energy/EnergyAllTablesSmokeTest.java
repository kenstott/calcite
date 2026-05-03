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
package org.apache.calcite.adapter.govdata.energy;

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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end smoke test for all energy schema tables.
 *
 * <p>API-based tables (always run when ENERGY_INTEGRATION_TESTS=true):
 * <ul>
 *   <li>eia_electricity_generation — monthly generation by state/fuel/sector</li>
 *   <li>eia_electricity_prices — annual retail electricity prices by state/sector</li>
 *   <li>eia_fossil_fuel_production — monthly crude oil and natural gas production</li>
 *   <li>eia_state_energy_consumption — annual SEDS consumption by state/MSN</li>
 *   <li>eia_natural_gas_storage — weekly underground storage by region</li>
 *   <li>eia_petroleum_stocks — weekly petroleum product stocks by PADD</li>
 *   <li>eia_refinery_operations — monthly refinery input and capacity (tall format)</li>
 * </ul>
 *
 * <p>Bulk XLSX/ZIP tables (gated by ENERGY_BULK_TESTS=true):
 * <ul>
 *   <li>eia_utility_annual — EIA-861 annual utility survey</li>
 *   <li>eia_power_plants — EIA-860 annual generator inventory</li>
 *   <li>eia_capacity_changes — EIA-860M monthly capacity change snapshots</li>
 *   <li>eia_crude_oil_imports — EIA-814 monthly crude oil import survey</li>
 *   <li>eia_coal_mines — MSHA MinesProdYearly coal production</li>
 * </ul>
 *
 * <p>View smoke tests (views referencing bulk tables are also bulk-gated):
 * <ul>
 *   <li>state_energy_mix — API-based, joins geo.states</li>
 *   <li>weekly_gas_storage_signal — API-based, LAG window over storage</li>
 *   <li>refinery_utilization_summary — API-based, pivots refinery operations</li>
 *   <li>utility_scorecard — bulk-gated, joins eia_utility_annual + eia_power_plants</li>
 *   <li>capacity_pipeline — bulk-gated, aggregates eia_capacity_changes</li>
 * </ul>
 *
 * <p>FK join test (bulk-gated):
 * <ul>
 *   <li>eia_power_plants.utility_id → eia_utility_annual.utility_id</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * ENERGY_INTEGRATION_TESTS=true \
 * ENERGY_EIA_API_KEY=your-key \
 * ENERGY_SINCE_YEAR=2023 \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*EnergyAllTablesSmokeTest*" \
 *     --console=plain
 * </pre>
 *
 * <p>With bulk table tests:
 * <pre>
 * ENERGY_INTEGRATION_TESTS=true \
 * ENERGY_EIA_API_KEY=your-key \
 * ENERGY_SINCE_YEAR=2023 \
 * ENERGY_BULK_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*EnergyAllTablesSmokeTest*"
 * </pre>
 */
@Tag("integration")
class EnergyAllTablesSmokeTest {

  private static final Logger LOG = LoggerFactory.getLogger(EnergyAllTablesSmokeTest.class);

  private static final String SCHEMA = "energy";

  private static String warehouse;
  private static String operatingDir;
  private static String cacheDir;

  @BeforeAll static void setup() throws Exception {
    TestEnvironmentLoader.ensureLoaded();

    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_INTEGRATION_TESTS")),
        "Set ENERGY_INTEGRATION_TESTS=true to run energy smoke tests");

    // Limit data volume to a single year by default
    String envYear = TestEnvironmentLoader.getEnv("ENERGY_SINCE_YEAR");
    if (envYear == null || envYear.isEmpty()) {
      envYear = "2023";
    }
    System.setProperty("ENERGY_SINCE_YEAR", envYear);

    File tmpBase = Files.createTempDirectory("energy_smoke_").toFile();
    warehouse = new File(tmpBase, "parquet").getAbsolutePath();
    operatingDir = new File(tmpBase, "op").getAbsolutePath();

    String configured = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    cacheDir = (configured != null && !configured.isEmpty() && !configured.startsWith("s3://"))
        ? configured + "/energy"
        : new File(tmpBase, "cache").getAbsolutePath();

    LOG.info("Warehouse:     {}", warehouse);
    LOG.info("Cache dir:     {}", cacheDir);
    LOG.info("ENERGY_SINCE_YEAR: {}",
        coalesce(System.getProperty("ENERGY_SINCE_YEAR"), System.getenv("ENERGY_SINCE_YEAR"), "2023"));
  }

  // ── eia_electricity_generation ──────────────────────────────────────────────

  @Test void electricityGenerationHaveData() throws Exception {
    LOG.info("=== energy: eia_electricity_generation ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_electricity_generation", 100);
      warnNullCompositePk(conn, SCHEMA, "eia_electricity_generation",
          "state_abbr", "energy_source_code", "sector_code", "generation_year", "generation_month");
    }
  }

  @Test void electricityGenerationSampleRow() throws Exception {
    LOG.info("=== energy: eia_electricity_generation sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_electricity_generation");
    }
  }

  // ── eia_electricity_prices ──────────────────────────────────────────────────

  @Test void electricityPricesHaveData() throws Exception {
    LOG.info("=== energy: eia_electricity_prices ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_electricity_prices", 50);
      warnNullCompositePk(conn, SCHEMA, "eia_electricity_prices",
          "state_abbr", "sector_code", "price_year", "price_month");
    }
  }

  @Test void electricityPricesSampleRow() throws Exception {
    LOG.info("=== energy: eia_electricity_prices sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_electricity_prices");
    }
  }

  // ── eia_fossil_fuel_production ──────────────────────────────────────────────

  @Test void fossilFuelProductionHaveData() throws Exception {
    LOG.info("=== energy: eia_fossil_fuel_production ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_fossil_fuel_production", 10);
      warnNullCompositePk(conn, SCHEMA, "eia_fossil_fuel_production",
          "eia_area_code", "fuel_type", "process_code", "production_year", "production_month");
    }
  }

  @Test void fossilFuelProductionSampleRow() throws Exception {
    LOG.info("=== energy: eia_fossil_fuel_production sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_fossil_fuel_production");
    }
  }

  // ── eia_state_energy_consumption ────────────────────────────────────────────

  @Test void stateEnergyConsumptionHaveData() throws Exception {
    LOG.info("=== energy: eia_state_energy_consumption ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_state_energy_consumption", 100);
      warnNullCompositePk(conn, SCHEMA, "eia_state_energy_consumption",
          "state_abbr", "msn", "consumption_year");
    }
  }

  @Test void stateEnergyConsumptionSampleRow() throws Exception {
    LOG.info("=== energy: eia_state_energy_consumption sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_state_energy_consumption");
    }
  }

  // ── eia_natural_gas_storage ─────────────────────────────────────────────────

  @Test void naturalGasStorageHaveData() throws Exception {
    LOG.info("=== energy: eia_natural_gas_storage ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_natural_gas_storage", 10);
      warnNullCompositePk(conn, SCHEMA, "eia_natural_gas_storage",
          "eia_region_code", "storage_type_code", "report_date");
    }
  }

  @Test void naturalGasStorageSampleRow() throws Exception {
    LOG.info("=== energy: eia_natural_gas_storage sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_natural_gas_storage");
    }
  }

  // ── eia_petroleum_stocks ────────────────────────────────────────────────────

  @Test void petroleumStocksHaveData() throws Exception {
    LOG.info("=== energy: eia_petroleum_stocks ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_petroleum_stocks", 10);
      warnNullCompositePk(conn, SCHEMA, "eia_petroleum_stocks",
          "eia_area_code", "product_code", "process_code", "report_date");
    }
  }

  @Test void petroleumStocksSampleRow() throws Exception {
    LOG.info("=== energy: eia_petroleum_stocks sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_petroleum_stocks");
    }
  }

  // ── eia_refinery_operations ─────────────────────────────────────────────────

  @Test void refineryOperationsHaveData() throws Exception {
    LOG.info("=== energy: eia_refinery_operations ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_refinery_operations", 10);
      warnNullCompositePk(conn, SCHEMA, "eia_refinery_operations",
          "eia_area_code", "series_id", "report_year", "report_month");
    }
  }

  @Test void refineryOperationsSampleRow() throws Exception {
    LOG.info("=== energy: eia_refinery_operations sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_refinery_operations");
    }
  }

  // ── eia_utility_annual (bulk) ───────────────────────────────────────────────

  @Test void utilityAnnualHaveData() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_utility_annual ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_utility_annual", 100);
      warnNullCompositePk(conn, SCHEMA, "eia_utility_annual", "utility_id", "report_year");
    }
  }

  @Test void utilityAnnualSampleRow() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_utility_annual sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_utility_annual");
    }
  }

  // ── eia_power_plants (bulk) ─────────────────────────────────────────────────

  @Test void powerPlantsHaveData() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_power_plants ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_power_plants", 100);
      warnNullCompositePk(conn, SCHEMA, "eia_power_plants",
          "plant_id", "generator_id", "report_year");
    }
  }

  @Test void powerPlantsSampleRow() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_power_plants sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_power_plants");
    }
  }

  // ── eia_capacity_changes (bulk) ─────────────────────────────────────────────

  @Test void capacityChangesHaveData() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_capacity_changes ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_capacity_changes", 10);
      warnNullCompositePk(conn, SCHEMA, "eia_capacity_changes",
          "plant_id", "generator_id", "snapshot_year", "snapshot_month", "change_type");
    }
  }

  @Test void capacityChangesSampleRow() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_capacity_changes sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_capacity_changes");
    }
  }

  // ── eia_crude_oil_imports (bulk) ────────────────────────────────────────────

  @Test void crudeOilImportsHaveData() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_crude_oil_imports ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_crude_oil_imports", 10);
      warnNullCompositePk(conn, SCHEMA, "eia_crude_oil_imports",
          "importer_name", "origin_country_code", "refinery_site_id", "rpt_period");
    }
  }

  @Test void crudeOilImportsSampleRow() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_crude_oil_imports sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_crude_oil_imports");
    }
  }

  // ── eia_coal_mines (bulk) ───────────────────────────────────────────────────

  @Test void coalMinesHaveData() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_coal_mines ===");
    try (Connection conn = energyConn()) {
      assertRowCount(conn, SCHEMA, "eia_coal_mines", 10);
      warnNullCompositePk(conn, SCHEMA, "eia_coal_mines",
          "mine_id", "subunit_code", "report_year");
    }
  }

  @Test void coalMinesSampleRow() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk XLSX table tests");
    LOG.info("=== energy: eia_coal_mines sample row ===");
    try (Connection conn = energyConn()) {
      sampleRow(conn, SCHEMA, "eia_coal_mines");
    }
  }

  // ── FK joins (bulk-gated) ───────────────────────────────────────────────────

  @Test void powerPlantsUtilityIdJoinsToUtilityAnnual() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk FK join tests");
    LOG.info("=== FK: eia_power_plants.utility_id → eia_utility_annual.utility_id ===");
    try (Connection conn = energyConn()) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_power_plants\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_utility_annual\"");
      long total = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_power_plants\""
          + " WHERE utility_id IS NOT NULL");
      long matched = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_power_plants\" p"
          + " INNER JOIN \"" + SCHEMA + "\".\"eia_utility_annual\" u"
          + " ON p.utility_id = u.utility_id AND p.report_year = u.report_year");
      double matchRate = total > 0 ? (double) matched / total : 0.0;
      LOG.info("power_plants→utility_annual: {}/{} generators matched to utility survey ({:.1f}%)",
          matched, total, matchRate * 100);
      assertTrue(matchRate >= 0.50,
          String.format("power_plants→utility_annual match rate %.1f%% below 50%% threshold",
              matchRate * 100));
    }
  }

  // ── Views (API-based) ───────────────────────────────────────────────────────

  @Test void stateEnergyMixViewReturnsRows() throws Exception {
    LOG.info("=== view: state_energy_mix ===");
    try (Connection conn = energyConn()) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_electricity_generation\"");
      long count = scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"state_energy_mix\"");
      LOG.info("state_energy_mix: {} rows", count);
      assertTrue(count >= 1, "state_energy_mix view should return at least 1 row");
    }
  }

  @Test void weeklyGasStorageSignalViewReturnsRows() throws Exception {
    LOG.info("=== view: weekly_gas_storage_signal ===");
    try (Connection conn = energyConn()) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_natural_gas_storage\"");
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"weekly_gas_storage_signal\"");
      LOG.info("weekly_gas_storage_signal: {} rows", count);
      assertTrue(count >= 1, "weekly_gas_storage_signal view should return at least 1 row");
    }
  }

  @Test void refineryUtilizationSummaryViewReturnsRows() throws Exception {
    LOG.info("=== view: refinery_utilization_summary ===");
    try (Connection conn = energyConn()) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_refinery_operations\"");
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"refinery_utilization_summary\"");
      LOG.info("refinery_utilization_summary: {} rows", count);
      assertTrue(count >= 1, "refinery_utilization_summary view should return at least 1 row");
    }
  }

  // ── Views (bulk-gated) ──────────────────────────────────────────────────────

  @Test void utilityScorecardViewReturnsRows() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk view tests");
    LOG.info("=== view: utility_scorecard ===");
    try (Connection conn = energyConn()) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_utility_annual\"");
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_power_plants\"");
      long count = scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"utility_scorecard\"");
      LOG.info("utility_scorecard: {} rows", count);
      assertTrue(count >= 1, "utility_scorecard view should return at least 1 row");
    }
  }

  @Test void capacityPipelineViewReturnsRows() throws Exception {
    assumeTrue("true".equalsIgnoreCase(TestEnvironmentLoader.getEnv("ENERGY_BULK_TESTS")),
        "Set ENERGY_BULK_TESTS=true to run bulk view tests");
    LOG.info("=== view: capacity_pipeline ===");
    try (Connection conn = energyConn()) {
      scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"eia_capacity_changes\"");
      long count = scalar(conn, "SELECT COUNT(*) FROM \"" + SCHEMA + "\".\"capacity_pipeline\"");
      LOG.info("capacity_pipeline: {} rows", count);
      assertTrue(count >= 1, "capacity_pipeline view should return at least 1 row");
    }
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  private Connection energyConn() throws Exception {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"" + SCHEMA + "\","
        + "\"schemas\":[{"
        + "  \"name\":\"" + SCHEMA + "\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\":{"
        + "    \"dataSource\":\"energy\","
        + "    \"executionEngine\":\"" + engine + "\","
        + "    \"directory\":\"" + warehouse + "\","
        + "    \"operatingDirectory\":\"" + operatingDir + "\","
        + "    \"cacheDirectory\":\"" + cacheDir + "\","
        + "    \"ephemeralCache\":false,"
        + "    \"autoDownload\":true,"
        + "    \"database_filename\":\"" + operatingDir + "/energy_db.duckdb\""
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

  private void warnNullCompositePk(Connection conn, String schema, String table,
      String... pkColumns) throws Exception {
    StringBuilder sb = new StringBuilder(
        "SELECT COUNT(*) FROM \"" + schema + "\".\"" + table + "\" WHERE ");
    for (int i = 0; i < pkColumns.length; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append("\"").append(pkColumns[i]).append("\" IS NULL");
    }
    long nulls = scalar(conn, sb.toString());
    if (nulls > 0) {
      LOG.warn("{}.{}: {} rows with at least one NULL PK column (composite PK: {})",
          schema, table, nulls, String.join(", ", pkColumns));
    } else {
      LOG.info("{}.{}: all composite PK columns non-null ({})",
          schema, table, String.join(", ", pkColumns));
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
          sb.append(String.format("  %-36s = %s%n", meta.getColumnName(i), val));
        }
        LOG.info(sb.toString());
      } else {
        LOG.warn("=== {}.{} === (0 rows)", schema, table);
      }
    } catch (Exception e) {
      LOG.error("=== {}.{} === FAILED: {}", schema, table, e.getMessage());
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
