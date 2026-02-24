/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live integration tests for all weather schema datasets.
 *
 * <p>Downloads minimal data from each endpoint (1 state, 1 year),
 * transforms via the appropriate ResponseTransformer, writes to Parquet,
 * and verifies readability via DuckDB.
 *
 * <p>Covers 100% of weather tables:
 * <ul>
 *   <li>NWS: stations, alerts (no auth)</li>
 *   <li>CDO: stations, monthly_summaries, annual_summaries (NOAA_CDO_TOKEN)</li>
 *   <li>EPA: annual_aqi (EPA_AQS_EMAIL + EPA_AQS_KEY)</li>
 * </ul>
 */
@Tag("integration")
class WeatherDataLiveTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WeatherDataLiveTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TEST_STATE = "AL";
  private static final String TEST_FIPS = "01"; // Alabama
  private static final String TEST_YEAR = "2022";

  @TempDir
  Path tempDir;

  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }

  // =========================================================================
  // NWS ENDPOINTS (weather.gov - no auth)
  // =========================================================================

  @Test void testNwsStations() throws Exception {
    String url = "https://api.weather.gov/stations?state=" + TEST_STATE + "&limit=50";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);

    verifyEndpoint("nws_stations", url, new NwsStationTransformer(), dims,
        new String[]{"station_id", "station_name", "state_abbr", "latitude", "longitude"},
        nwsHeaders());
  }

  @Test void testNwsAlerts() throws Exception {
    String url = "https://api.weather.gov/alerts?area=" + TEST_STATE
        + "&status=actual&limit=50";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);

    // Alerts may be empty if no active alerts - that's OK
    verifyEndpointAllowEmpty("nws_alerts", url, new NwsAlertTransformer(), dims,
        nwsHeaders());
  }

  // =========================================================================
  // CDO ENDPOINTS (ncdc.noaa.gov - requires NOAA_CDO_TOKEN)
  // =========================================================================

  @Test void testCdoStations() throws Exception {
    String token = TestEnvironmentLoader.getEnv("NOAA_CDO_TOKEN");
    Assumptions.assumeTrue(token != null && !token.isEmpty(),
        "NOAA_CDO_TOKEN required for CDO tests");

    String url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
        + "?locationid=FIPS:" + TEST_FIPS + "&limit=25&offset=1";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", TEST_FIPS);

    Map<String, String> headers = new HashMap<>();
    headers.put("token", token);

    verifyEndpoint("cdo_stations", url, new CdoStationTransformer(), dims,
        new String[]{"station_id", "station_name", "state_fips", "latitude", "longitude"},
        headers);
  }

  @Test void testCdoMonthlySummaries() throws Exception {
    String token = TestEnvironmentLoader.getEnv("NOAA_CDO_TOKEN");
    Assumptions.assumeTrue(token != null && !token.isEmpty(),
        "NOAA_CDO_TOKEN required for CDO tests");

    String url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
        + "?datasetid=GSOM&locationid=FIPS:" + TEST_FIPS
        + "&startdate=" + TEST_YEAR + "-01-01&enddate=" + TEST_YEAR + "-12-31"
        + "&limit=25&offset=1";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", TEST_FIPS);
    dims.put("year", TEST_YEAR);

    Map<String, String> headers = new HashMap<>();
    headers.put("token", token);

    verifyEndpoint("cdo_monthly_summaries", url, new CdoDataTransformer(), dims,
        new String[]{"state_fips", "station_id", "datatype", "value"},
        headers);
  }

  @Test void testCdoAnnualSummaries() throws Exception {
    String token = TestEnvironmentLoader.getEnv("NOAA_CDO_TOKEN");
    Assumptions.assumeTrue(token != null && !token.isEmpty(),
        "NOAA_CDO_TOKEN required for CDO tests");

    String url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
        + "?datasetid=GSOY&locationid=FIPS:" + TEST_FIPS
        + "&startdate=" + TEST_YEAR + "-01-01&enddate=" + TEST_YEAR + "-12-31"
        + "&limit=25&offset=1";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", TEST_FIPS);
    dims.put("year", TEST_YEAR);

    Map<String, String> headers = new HashMap<>();
    headers.put("token", token);

    verifyEndpoint("cdo_annual_summaries", url, new CdoDataTransformer(), dims,
        new String[]{"state_fips", "station_id", "datatype", "value"},
        headers);
  }

  // =========================================================================
  // EPA ENDPOINTS (aqs.epa.gov - requires EPA_AQS_EMAIL + EPA_AQS_KEY)
  // =========================================================================

  @Test void testEpaAnnualAqi() throws Exception {
    String email = TestEnvironmentLoader.getEnv("EPA_AQS_EMAIL");
    String key = TestEnvironmentLoader.getEnv("EPA_AQS_KEY");
    Assumptions.assumeTrue(email != null && key != null,
        "EPA_AQS_EMAIL + EPA_AQS_KEY required for EPA tests");

    // PM2.5 for Alabama
    String url = "https://aqs.epa.gov/data/api/annualData/byState"
        + "?email=" + email + "&key=" + key
        + "&param=88101&bdate=" + TEST_YEAR + "0101&edate=" + TEST_YEAR + "1231"
        + "&state=" + TEST_FIPS;

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", TEST_FIPS);
    dims.put("param", "88101");
    dims.put("year", TEST_YEAR);

    verifyEndpoint("epa_annual_aqi", url, new EpaAqsTransformer(), dims,
        new String[]{"state_fips", "parameter_code"},
        null);
  }

  // =========================================================================
  // HELPER METHODS
  // =========================================================================

  private void verifyEndpoint(String tableName, String urlStr,
      ResponseTransformer transformer, Map<String, String> dims,
      String[] requiredFields, Map<String, String> headers) throws Exception {

    LOGGER.info("=== Testing {} ===", tableName);

    // Step 1: Download
    LOGGER.info("  Downloading from: {}",
        urlStr.replaceAll("(key|email|token)=[^&]+", "$1=***"));
    String response = httpGet(urlStr, headers);
    assertNotNull(response, tableName + ": response should not be null");
    assertFalse(response.isEmpty(), tableName + ": response should not be empty");
    LOGGER.info("  Downloaded {} bytes", response.length());

    // Step 2: Verify valid JSON
    JsonNode rawJson = MAPPER.readTree(response);
    assertNotNull(rawJson, tableName + ": should be valid JSON");
    LOGGER.info("  Valid JSON: {} ({})", rawJson.getNodeType(),
        rawJson.isArray() ? rawJson.size() + " elements" : rawJson.size() + " fields");

    // Step 3: Transform
    RequestContext context = RequestContext.builder()
        .url(urlStr)
        .dimensionValues(dims)
        .build();

    String transformed = transformer.transform(response, context);
    assertNotNull(transformed, tableName + ": transformed should not be null");

    JsonNode rows = MAPPER.readTree(transformed);
    assertTrue(rows.isArray(), tableName + ": transformed should be array");
    assertTrue(rows.size() > 0, tableName + ": transformed should have >0 rows");
    LOGGER.info("  Transformed: {} rows", rows.size());

    // Step 4: Verify required fields
    if (requiredFields != null) {
      JsonNode firstRow = rows.get(0);
      for (String field : requiredFields) {
        assertTrue(firstRow.has(field),
            tableName + ": row missing required field '" + field + "'");
      }
    }

    // Step 5: Write to Parquet via DuckDB and verify
    writeAndVerifyParquet(tableName, transformed, rows.size());

    LOGGER.info("  {} PASSED", tableName);
  }

  private void verifyEndpointAllowEmpty(String tableName, String urlStr,
      ResponseTransformer transformer, Map<String, String> dims,
      Map<String, String> headers) throws Exception {

    LOGGER.info("=== Testing {} (may be empty) ===", tableName);

    String response = httpGet(urlStr, headers);
    assertNotNull(response, tableName + ": response should not be null");

    JsonNode rawJson = MAPPER.readTree(response);
    assertNotNull(rawJson, tableName + ": should be valid JSON");
    LOGGER.info("  Valid JSON: {}", rawJson.getNodeType());

    RequestContext context = RequestContext.builder()
        .url(urlStr)
        .dimensionValues(dims)
        .build();

    String transformed = transformer.transform(response, context);
    JsonNode rows = MAPPER.readTree(transformed);
    assertTrue(rows.isArray(), tableName + ": transformed should be array");
    LOGGER.info("  Transformed: {} rows (empty OK for alerts)", rows.size());

    if (rows.size() > 0) {
      writeAndVerifyParquet(tableName, transformed, rows.size());
    } else {
      LOGGER.info("  No active alerts - skipping parquet write (expected)");
    }

    LOGGER.info("  {} PASSED", tableName);
  }

  private void writeAndVerifyParquet(String tableName, String transformed,
      int expectedRows) throws Exception {
    Path parquetFile = tempDir.resolve(tableName + ".parquet");
    String jsonFile = tempDir.resolve(tableName + ".json").toString();
    Files.write(tempDir.resolve(tableName + ".json"), transformed.getBytes());

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Statement stmt = conn.createStatement();

      stmt.execute("COPY (SELECT * FROM read_json_auto('" + jsonFile
          + "', maximum_object_size=20000000)) TO '" + parquetFile
          + "' (FORMAT PARQUET)");

      assertTrue(Files.exists(parquetFile),
          tableName + ": parquet file should exist");
      assertTrue(Files.size(parquetFile) > 0,
          tableName + ": parquet file should be non-empty");

      ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) as cnt FROM read_parquet('" + parquetFile + "')");
      assertTrue(rs.next());
      long count = rs.getLong("cnt");
      assertEquals(expectedRows, count,
          tableName + ": parquet row count mismatch");

      LOGGER.info("  Parquet: {} bytes, {} rows -- OK",
          Files.size(parquetFile), count);
    }
  }

  private static Map<String, String> nwsHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("User-Agent", "(calcite-govdata-test, contact@example.com)");
    headers.put("Accept", "application/geo+json");
    return headers;
  }

  private static String httpGet(String urlStr, Map<String, String> headers)
      throws Exception {
    int maxRetries = 3;
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      URL url = URI.create(urlStr).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);

      if (headers != null) {
        for (Map.Entry<String, String> h : headers.entrySet()) {
          conn.setRequestProperty(h.getKey(), h.getValue());
        }
      }

      int status = conn.getResponseCode();
      if (status == 429 && attempt < maxRetries) {
        LOGGER.warn("  Rate limited (429) on attempt {}, waiting {}s before retry...",
            attempt, attempt * 2);
        Thread.sleep(attempt * 2000L);
        continue;
      }
      assertEquals(200, status,
          "HTTP " + status + " for " + urlStr.replaceAll("(key|email|token|API_KEY)=[^&]+", "$1=***"));

      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
      }
      return sb.toString();
    }
    throw new AssertionError("Exhausted retries for "
        + urlStr.replaceAll("(key|email|token|API_KEY)=[^&]+", "$1=***"));
  }
}
