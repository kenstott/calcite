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
package org.apache.calcite.adapter.govdata;

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
 * Integration tests for cross-schema convenience views.
 *
 * <p>Views tested:
 * <ul>
 *   <li>sec.company_financial_profile — filing_metadata → gleif_cik_mapping → gleif_entities</li>
 *   <li>ref.ticker_instrument_map — figi_instruments → sec.filing_metadata</li>
 *   <li>fec.candidate_district_profile — candidates → geo.states → geo.congressional_districts</li>
 *   <li>econ.state_economic_snapshot — state_wages + jolts_state → geo.states</li>
 *   <li>crime.crime_enforcement_equity — cde_offenses + cde_police_employment → geo.states</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * GOVDATA_INTEGRATION_TESTS=true \
 * GOVDATA_CACHE_DIR=/path/to/cache \
 * ./gradlew :govdata:test -PincludeTags=integration \
 *     --tests "*ConvenienceViewsIntegrationTest*" --console=plain
 * </pre>
 */
@Tag("integration")
public class ConvenienceViewsIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConvenienceViewsIntegrationTest.class);

  private static String baseDir;
  private static String cacheDir;
  private static String engine;
  private static String parquetDir;
  private static String s3Config;

  @BeforeAll static void setup() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("GOVDATA_INTEGRATION_TESTS")),
        "Set GOVDATA_INTEGRATION_TESTS=true to run cross-schema view tests");
    TestEnvironmentLoader.ensureLoaded();

    File tmpBase = Files.createTempDirectory("conv_views_").toFile();
    baseDir = tmpBase.getAbsolutePath();

    String configured = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    cacheDir = (configured != null && !configured.isEmpty() && !configured.startsWith("s3://"))
        ? configured
        : new File(tmpBase, "cache").getAbsolutePath();

    engine = coalesce(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"), "DUCKDB");

    String rawParquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    parquetDir = (rawParquetDir != null && !rawParquetDir.isEmpty()) ? rawParquetDir : null;

    String accessKey    = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String secretKey    = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String endpoint     = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String region       = coalesce(TestEnvironmentLoader.getEnv("AWS_REGION"), "auto");
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

  // ── sec: company_financial_profile (sec + ref) ──────────────────────────────

  @Test void companyFinancialProfileReturnsRows() throws Exception {
    LOG.info("=== sec view: company_financial_profile (sec+ref) ===");
    try (Connection conn = secRefConn()) {
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"sec\".\"company_financial_profile\"");
      LOG.info("company_financial_profile: {} company rows", count);
      assertTrue(count > 0, "company_financial_profile must return rows");
      sampleRow(conn, "sec", "company_financial_profile");
    }
  }

  // ── ref: ticker_instrument_map (ref + sec) ──────────────────────────────────

  @Test void tickerInstrumentMapReturnsRows() throws Exception {
    LOG.info("=== ref view: ticker_instrument_map (ref+sec) ===");
    try (Connection conn = refSecConn()) {
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"ref\".\"ticker_instrument_map\"");
      LOG.info("ticker_instrument_map: {} ticker rows", count);
      assertTrue(count > 0, "ticker_instrument_map must return rows");
      sampleRow(conn, "ref", "ticker_instrument_map");
    }
  }

  // ── fec: candidate_district_profile (fec + geo) ─────────────────────────────

  @Test void candidateDistrictProfileReturnsRows() throws Exception {
    LOG.info("=== fec view: candidate_district_profile (fec+geo) ===");
    try (Connection conn = fecGeoConn()) {
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"fec\".\"candidate_district_profile\"");
      LOG.info("candidate_district_profile: {} candidate rows", count);
      assertTrue(count > 0, "candidate_district_profile must return rows");
      sampleRow(conn, "fec", "candidate_district_profile");
    }
  }

  // ── econ: state_economic_snapshot (econ + geo) ──────────────────────────────

  @Test void stateEconomicSnapshotReturnsRows() throws Exception {
    LOG.info("=== econ view: state_economic_snapshot (econ+geo) ===");
    try (Connection conn = econGeoConn()) {
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"econ\".\"state_economic_snapshot\"");
      LOG.info("state_economic_snapshot: {} rows", count);
      assertTrue(count > 0, "state_economic_snapshot must return rows");
      sampleRow(conn, "econ", "state_economic_snapshot");
    }
  }

  // ── crime: crime_enforcement_equity (crime + geo) ───────────────────────────

  @Test void crimeEnforcementEquityReturnsRows() throws Exception {
    LOG.info("=== crime view: crime_enforcement_equity (crime+geo) ===");
    try (Connection conn = crimeGeoConn()) {
      long count = scalar(conn,
          "SELECT COUNT(*) FROM \"crime\".\"crime_enforcement_equity\"");
      LOG.info("crime_enforcement_equity: {} state+year+offense rows", count);
      assertTrue(count > 0, "crime_enforcement_equity must return rows");
      sampleRow(conn, "crime", "crime_enforcement_equity");
    }
  }

  // ── connection factories ────────────────────────────────────────────────────

  private Connection secRefConn() throws Exception {
    String dbFile = new File(baseDir, "sec_ref.duckdb").getAbsolutePath();
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"sec\",\"schemas\":["
        + schema("sec",  "sec",  dbFile)  + ","
        + schema("ref",  "ref",  dbFile)
        + "]}";
    return connect(model);
  }

  private Connection refSecConn() throws Exception {
    String dbFile = new File(baseDir, "ref_sec.duckdb").getAbsolutePath();
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"ref\",\"schemas\":["
        + schema("ref",  "ref",  dbFile) + ","
        + schema("sec",  "sec",  dbFile)
        + "]}";
    return connect(model);
  }

  private Connection fecGeoConn() throws Exception {
    String dbFile = new File(baseDir, "fec_geo.duckdb").getAbsolutePath();
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"fec\",\"schemas\":["
        + schema("fec", "fec", dbFile) + ","
        + schema("geo", "geo", dbFile)
        + "]}";
    return connect(model);
  }

  private Connection econGeoConn() throws Exception {
    String dbFile = new File(baseDir, "econ_geo.duckdb").getAbsolutePath();
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"econ\",\"schemas\":["
        + schema("econ", "econ", dbFile) + ","
        + schema("geo",  "geo",  dbFile)
        + "]}";
    return connect(model);
  }

  private Connection crimeGeoConn() throws Exception {
    String dbFile = new File(baseDir, "crime_geo.duckdb").getAbsolutePath();
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"crime\",\"schemas\":["
        + schema("crime", "crime", dbFile) + ","
        + schema("geo",   "geo",   dbFile)
        + "]}";
    return connect(model);
  }

  private String schema(String name, String dataSource, String dbFile) {
    boolean useS3 = parquetDir != null && parquetDir.startsWith("s3://");
    String dir = useS3 ? parquetDir : new File(baseDir, name + "_parquet").getAbsolutePath();
    return "{\"name\":\"" + name + "\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{"
        + "\"dataSource\":\"" + dataSource + "\","
        + "\"executionEngine\":\"" + engine + "\","
        + "\"directory\":\"" + dir + "\","
        + "\"operatingDirectory\":\"" + new File(baseDir, name + "_op").getAbsolutePath() + "\","
        + "\"cacheDirectory\":\"" + cacheDir + "/" + name + "\","
        + "\"ephemeralCache\":false,"
        + "\"autoDownload\":" + !useS3 + ","
        + "\"database_filename\":\"" + dbFile + "\""
        + s3Config + "}}";
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
