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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for health schema convenience views.
 *
 * <p>Intra-schema views (single health connection):
 * <ul>
 *   <li>drug_manufacturer_span — labeler → approval → Medicaid spend</li>
 *   <li>rxnorm_medicaid_bridge — rxcui → NDC → Medicaid spend</li>
 *   <li>drug_spend_trends — annual Medicaid spend by state+ndc+year</li>
 * </ul>
 *
 * <p>Cross-schema views (health + geo connection):
 * <ul>
 *   <li>health_social_equity — mortality + BRFSS + state FIPS</li>
 *   <li>hospital_county_summary — hospital quality aggregated to county</li>
 * </ul>
 *
 * <p>Cross-schema views (health + geo + weather connection):
 * <ul>
 *   <li>weather_health_correlation — mortality + CDO climate by state+year</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * HEALTH_INTEGRATION_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*HealthViewsTest*" \
 *     --console=plain
 * </pre>
 */
@Tag("integration")
class HealthViewsTest {

  private static final Logger LOG = LoggerFactory.getLogger(HealthViewsTest.class);

  private static String healthWarehouse;
  private static String geoWarehouse;
  private static String weatherWarehouse;
  private static String operatingDir;
  private static String cacheDir;
  private static String dbFile;
  private static String parquetDir;
  private static String s3Config;

  @BeforeAll static void setup() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("HEALTH_INTEGRATION_TESTS")),
        "Set HEALTH_INTEGRATION_TESTS=true to run health view tests");
    TestEnvironmentLoader.ensureLoaded();

    File tmpBase = Files.createTempDirectory("health_views_").toFile();
    operatingDir = new File(tmpBase, "op").getAbsolutePath();
    dbFile       = new File(operatingDir, "shared.duckdb").getAbsolutePath();

    String rawParquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    parquetDir = (rawParquetDir != null && !rawParquetDir.isEmpty()) ? rawParquetDir : null;

    boolean useS3 = parquetDir != null && parquetDir.startsWith("s3://");
    healthWarehouse  = useS3 ? parquetDir : new File(tmpBase, "health_parquet").getAbsolutePath();
    geoWarehouse     = useS3 ? parquetDir : new File(tmpBase, "geo_parquet").getAbsolutePath();
    weatherWarehouse = useS3 ? parquetDir : new File(tmpBase, "weather_parquet").getAbsolutePath();

    String configured = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    cacheDir = (configured != null && !configured.isEmpty() && !configured.startsWith("s3://"))
        ? configured
        : new File(tmpBase, "cache").getAbsolutePath();

    String accessKey = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String secretKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String endpoint  = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String region    = coalesce(TestEnvironmentLoader.getEnv("AWS_REGION"), "auto");
    if (accessKey != null && !accessKey.isEmpty()) {
      s3Config = ",\"s3Config\":{"
          + "\"accessKeyId\":\"" + accessKey + "\","
          + "\"secretAccessKey\":\"" + secretKey + "\","
          + (endpoint != null && !endpoint.isEmpty() ? "\"endpoint\":\"" + endpoint + "\"," : "")
          + "\"region\":\"" + region + "\"}";
    } else {
      s3Config = "";
    }
  }

  // ── intra-schema views ──────────────────────────────────────────────────────

  @Test void drugManufacturerSpanReturnsRows() throws Exception {
    LOG.info("=== health view: drug_manufacturer_span ===");
    try (Connection conn = healthConn()) {
      long count = scalar(conn, "SELECT COUNT(*) FROM \"health\".\"drug_manufacturer_span\"");
      LOG.info("drug_manufacturer_span: {} rows", count);
      assertTrue(count > 0, "drug_manufacturer_span must return rows");
      sampleRow(conn, "health", "drug_manufacturer_span");
    }
  }

  @Test void rxnormMedicaidBridgeReturnsRows() throws Exception {
    LOG.info("=== health view: rxnorm_medicaid_bridge ===");
    try (Connection conn = healthConn()) {
      long count = scalar(conn, "SELECT COUNT(*) FROM \"health\".\"rxnorm_medicaid_bridge\"");
      LOG.info("rxnorm_medicaid_bridge: {} rows", count);
      assertTrue(count > 0, "rxnorm_medicaid_bridge must return rows");
      sampleRow(conn, "health", "rxnorm_medicaid_bridge");
    }
  }

  @Test void drugSpendTrendsReturnsRows() throws Exception {
    LOG.info("=== health view: drug_spend_trends ===");
    try (Connection conn = healthConn()) {
      long count = scalar(conn, "SELECT COUNT(*) FROM \"health\".\"drug_spend_trends\"");
      LOG.info("drug_spend_trends: {} rows", count);
      assertTrue(count > 0, "drug_spend_trends must return rows");
      sampleRow(conn, "health", "drug_spend_trends");
    }
  }

  // ── cross-schema views (health + geo) ──────────────────────────────────────

  @Test void healthSocialEquityReturnsRows() throws Exception {
    LOG.info("=== health view: health_social_equity (health+geo) ===");
    try (Connection conn = healthGeoConn()) {
      long count = scalar(conn, "SELECT COUNT(*) FROM \"health\".\"health_social_equity\"");
      LOG.info("health_social_equity: {} state-year rows", count);
      assertTrue(count > 0, "health_social_equity must return rows");
      sampleRow(conn, "health", "health_social_equity");
    }
  }

  @Test void hospitalCountySummaryReturnsRows() throws Exception {
    LOG.info("=== health view: hospital_county_summary (health+geo) ===");
    try (Connection conn = healthGeoConn()) {
      long count = scalar(conn, "SELECT COUNT(*) FROM \"health\".\"hospital_county_summary\"");
      LOG.info("hospital_county_summary: {} county rows", count);
      assertTrue(count > 0, "hospital_county_summary must return rows");
      sampleRow(conn, "health", "hospital_county_summary");
    }
  }

  // ── cross-schema views (health + geo + weather) ─────────────────────────────

  @Test void weatherHealthCorrelationReturnsRows() throws Exception {
    LOG.info("=== health view: weather_health_correlation (health+geo+weather) ===");
    try (Connection conn = healthGeoWeatherConn()) {
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"health\".\"weather_health_correlation\"");
      LOG.info("weather_health_correlation: {} state-year rows", count);
      assertTrue(count > 0, "weather_health_correlation must return rows");
      sampleRow(conn, "health", "weather_health_correlation");
    }
  }

  // ── connection factories ────────────────────────────────────────────────────

  private Connection healthConn() throws Exception {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"health\",\"schemas\":[{"
        + "\"name\":\"health\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{\"dataSource\":\"health\",\"executionEngine\":\"" + engine + "\","
        + "\"directory\":\"" + healthWarehouse + "\","
        + "\"operatingDirectory\":\"" + operatingDir + "\","
        + "\"cacheDirectory\":\"" + cacheDir + "/health\","
        + "\"ephemeralCache\":false,\"autoDownload\":" + (s3Config.isEmpty()) + ","
        + "\"database_filename\":\"" + dbFile + "\""
        + s3Config + "}}]}";
    return connect(model);
  }

  private Connection healthGeoConn() throws Exception {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"health\",\"schemas\":["
        + "{\"name\":\"health\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{\"dataSource\":\"health\",\"executionEngine\":\"" + engine + "\","
        + "\"directory\":\"" + healthWarehouse + "\","
        + "\"operatingDirectory\":\"" + operatingDir + "\","
        + "\"cacheDirectory\":\"" + cacheDir + "/health\","
        + "\"ephemeralCache\":false,\"autoDownload\":" + (s3Config.isEmpty()) + ","
        + "\"database_filename\":\"" + dbFile + "\""
        + s3Config + "}},"
        + "{\"name\":\"geo\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{\"dataSource\":\"geo\",\"executionEngine\":\"" + engine + "\","
        + "\"directory\":\"" + geoWarehouse + "\","
        + "\"operatingDirectory\":\"" + operatingDir + "\","
        + "\"cacheDirectory\":\"" + cacheDir + "/geo\","
        + "\"ephemeralCache\":false,\"autoDownload\":" + (s3Config.isEmpty()) + ","
        + "\"database_filename\":\"" + dbFile + "\""
        + s3Config + "}}]}";
    return connect(model);
  }

  private Connection healthGeoWeatherConn() throws Exception {
    String engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"health\",\"schemas\":["
        + "{\"name\":\"health\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{\"dataSource\":\"health\",\"executionEngine\":\"" + engine + "\","
        + "\"directory\":\"" + healthWarehouse + "\","
        + "\"operatingDirectory\":\"" + operatingDir + "\","
        + "\"cacheDirectory\":\"" + cacheDir + "/health\","
        + "\"ephemeralCache\":false,\"autoDownload\":" + (s3Config.isEmpty()) + ","
        + "\"database_filename\":\"" + dbFile + "\""
        + s3Config + "}},"
        + "{\"name\":\"geo\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{\"dataSource\":\"geo\",\"executionEngine\":\"" + engine + "\","
        + "\"directory\":\"" + geoWarehouse + "\","
        + "\"operatingDirectory\":\"" + operatingDir + "\","
        + "\"cacheDirectory\":\"" + cacheDir + "/geo\","
        + "\"ephemeralCache\":false,\"autoDownload\":" + (s3Config.isEmpty()) + ","
        + "\"database_filename\":\"" + dbFile + "\""
        + s3Config + "}},"
        + "{\"name\":\"weather\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{\"dataSource\":\"weather\",\"executionEngine\":\"" + engine + "\","
        + "\"directory\":\"" + weatherWarehouse + "\","
        + "\"operatingDirectory\":\"" + operatingDir + "\","
        + "\"cacheDirectory\":\"" + cacheDir + "/weather\","
        + "\"ephemeralCache\":false,\"autoDownload\":" + (s3Config.isEmpty()) + ","
        + "\"database_filename\":\"" + dbFile + "\""
        + s3Config + "}}]}";
    return connect(model);
  }

  private Connection connect(String modelJson) throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  private long scalar(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getLong(1) : 0L;
    }
  }

  private void sampleRow(Connection conn, String schema, String view) {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT * FROM \"" + schema + "\".\"" + view + "\" LIMIT 1")) {
      ResultSetMetaData meta = rs.getMetaData();
      int cols = meta.getColumnCount();
      if (rs.next()) {
        StringBuilder sb = new StringBuilder("\n=== ")
            .append(schema).append(".").append(view).append(" (view) ===\n");
        for (int i = 1; i <= cols; i++) {
          String val = rs.getString(i);
          if (val != null && val.length() > 120) {
            val = val.substring(0, 120) + "...";
          }
          sb.append(String.format("  %-32s = %s%n", meta.getColumnName(i), val));
        }
        LOG.info(sb.toString());
      } else {
        LOG.warn("=== {}.{} === (0 rows)", schema, view);
      }
    } catch (Exception e) {
      LOG.error("=== {}.{} (view) === FAILED: {}", schema, view, e.getMessage());
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
