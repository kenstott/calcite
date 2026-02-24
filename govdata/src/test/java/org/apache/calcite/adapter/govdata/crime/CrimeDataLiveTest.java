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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
 * Live integration tests for all crime schema datasets.
 *
 * <p>Downloads minimal data from each endpoint (1 state, 1 year),
 * transforms via the appropriate ResponseTransformer, writes to Parquet,
 * and verifies readability via DuckDB.
 *
 * <p>Covers 100% of working crime tables:
 * <ul>
 *   <li>CDE: agencies, offenses, police_employment, hate_crimes, use_of_force,
 *       crime_agency, leoka, arrests, shr, trends, supplemental</li>
 *   <li>BJS: nibrs_estimates, ncvs_personal, ncvs_personal_pop,
 *       ncvs_household, ncvs_household_pop</li>
 * </ul>
 */
@Tag("integration")
class CrimeDataLiveTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrimeDataLiveTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TEST_STATE = "AL";
  private static final String TEST_YEAR = "2022";

  @TempDir
  Path tempDir;

  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }

  // =========================================================================
  // CDE ENDPOINTS (cde.ucr.cjis.gov/LATEST/ - no auth)
  // =========================================================================

  @Test void testCdeAgencies() throws Exception {
    String apiKey = TestEnvironmentLoader.getEnv("API_DATA_GOV");
    assertNotNull(apiKey, "API_DATA_GOV required for cde_agencies");

    String url = "https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/"
        + TEST_STATE + "?API_KEY=" + apiKey;

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);

    verifyEndpoint("cde_agencies", url, new CdeAgencyTransformer(), dims,
        new String[]{"ori", "agency_name", "state_abbr"});
  }

  @Test void testCdeOffenses() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/summarized/state/"
        + TEST_STATE + "/homicide?from=01-" + TEST_YEAR + "&to=12-" + TEST_YEAR;

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("offense", "homicide");
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_offenses", url, new CdeCrimeRatesTransformer(), dims,
        new String[]{"state_abbr", "offense_code"});
  }

  @Test void testCdePoliceEmployment() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/pe?stateAbbr="
        + TEST_STATE + "&from=" + TEST_YEAR + "&to=" + TEST_YEAR;

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_police_employment", url,
        new CdePoliceEmploymentTransformer(), dims,
        new String[]{"state_abbr", "year"});
  }

  @Test void testCdeHateCrimes() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/hate-crime/state/"
        + TEST_STATE + "?from=01-" + TEST_YEAR + "&to=12-" + TEST_YEAR;

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_hate_crimes", url, new CdeHateCrimeTransformer(), dims,
        new String[]{"state_abbr", "year", "section"});
  }

  @Test void testCdeUseOfForce() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/uof?location="
        + TEST_STATE + "&year=" + TEST_YEAR;

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_use_of_force", url, new CdeHateCrimeTransformer(), dims,
        new String[]{"state_abbr", "year"});
  }

  @Test void testCdeLeoka() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/leoka/ytd?stateAbbr="
        + TEST_STATE + "&year=" + TEST_YEAR;

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_leoka", url, new CdeHateCrimeTransformer(), dims,
        new String[]{"state_abbr", "year"});
  }

  @Test void testCdeArrests() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/arrest/state/"
        + TEST_STATE + "/all?from=01-" + TEST_YEAR + "&to=12-" + TEST_YEAR
        + "&type=totals";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("offense", "all");
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_arrests", url, new CdeHateCrimeTransformer(), dims,
        new String[]{"state_abbr", "year"});
  }

  @Test void testCdeShr() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/shr/state/"
        + TEST_STATE + "?from=01-" + TEST_YEAR + "&to=12-" + TEST_YEAR
        + "&type=totals";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_shr", url, new CdeHateCrimeTransformer(), dims,
        new String[]{"state_abbr", "year"});
  }

  @Test void testCdeTrends() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/trends/national";

    Map<String, String> dims = new HashMap<>();

    verifyEndpoint("cde_trends", url, new CdeTrendsTransformer(), dims,
        new String[]{"offense_name", "trend_pct"});
  }

  @Test void testCdeSupplemental() throws Exception {
    String url = "https://cde.ucr.cjis.gov/LATEST/supplemental/state/"
        + TEST_STATE + "/property-crime?from=01-" + TEST_YEAR + "&to=12-"
        + TEST_YEAR + "&type=totals";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", TEST_STATE);
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_supplemental", url, new CdeSupplementalTransformer(), dims,
        new String[]{"state_abbr", "property_type"});
  }

  @Test void testCdeCrimeAgency() throws Exception {
    // Use a known ORI from Alabama
    String url = "https://cde.ucr.cjis.gov/LATEST/summarized/agency/"
        + "AL0010100/homicide?from=01-" + TEST_YEAR + "&to=12-" + TEST_YEAR;

    Map<String, String> dims = new HashMap<>();
    dims.put("ori", "AL0010100");
    dims.put("state_abbr", TEST_STATE);
    dims.put("offense", "homicide");
    dims.put("year", TEST_YEAR);

    verifyEndpoint("cde_crime_agency", url,
        new CdeAgencyOffenseTransformer(), dims,
        new String[]{"ori"});
  }

  // =========================================================================
  // BJS ENDPOINTS (api.ojp.gov - no auth)
  // =========================================================================

  @Test void testBjsNibrsEstimates() throws Exception {
    // Use first NIBRS endpoint: r32q-bdaw (violent incidents)
    String url = "https://api.ojp.gov/bjsdataset/v1/r32q-bdaw.json?$limit=10";

    Map<String, String> dims = new HashMap<>();
    dims.put("endpoint_id", "r32q-bdaw");

    verifyEndpoint("bjs_nibrs_estimates", url, new BjsSodaTransformer(), dims,
        new String[]{"endpoint_id"});
  }

  @Test void testBjsNcvsPersonal() throws Exception {
    String url = "https://api.ojp.gov/bjsdataset/v1/gcuy-rt5g.json"
        + "?$where=year='" + TEST_YEAR + "'&$limit=10";

    Map<String, String> dims = new HashMap<>();
    dims.put("year", TEST_YEAR);

    verifyEndpoint("bjs_ncvs_personal", url, new BjsSodaTransformer(), dims,
        null); // BJS schemas vary, just verify non-empty
  }

  @Test void testBjsNcvsPersonalPop() throws Exception {
    String url = "https://api.ojp.gov/bjsdataset/v1/r4j4-fdwx.json"
        + "?$where=year='" + TEST_YEAR + "'&$limit=10";

    Map<String, String> dims = new HashMap<>();
    dims.put("year", TEST_YEAR);

    verifyEndpoint("bjs_ncvs_personal_pop", url, new BjsSodaTransformer(), dims,
        null);
  }

  @Test void testBjsNcvsHousehold() throws Exception {
    String url = "https://api.ojp.gov/bjsdataset/v1/gkck-euys.json"
        + "?$where=year='" + TEST_YEAR + "'&$limit=10";

    Map<String, String> dims = new HashMap<>();
    dims.put("year", TEST_YEAR);

    verifyEndpoint("bjs_ncvs_household", url, new BjsSodaTransformer(), dims,
        null);
  }

  @Test void testBjsNcvsHouseholdPop() throws Exception {
    String url = "https://api.ojp.gov/bjsdataset/v1/ya4e-n9zp.json"
        + "?$where=year='" + TEST_YEAR + "'&$limit=10";

    Map<String, String> dims = new HashMap<>();
    dims.put("year", TEST_YEAR);

    verifyEndpoint("bjs_ncvs_household_pop", url, new BjsSodaTransformer(), dims,
        null);
  }

  // =========================================================================
  // HELPER METHODS
  // =========================================================================

  private void verifyEndpoint(String tableName, String urlStr,
      ResponseTransformer transformer, Map<String, String> dims,
      String[] requiredFields) throws Exception {

    LOGGER.info("=== Testing {} ===", tableName);

    // Step 1: Download
    LOGGER.info("  Downloading from: {}", urlStr.replaceAll("API_KEY=[^&]+", "API_KEY=***"));
    String response = httpGet(urlStr);
    assertNotNull(response, tableName + ": response should not be null");
    assertFalse(response.isEmpty(), tableName + ": response should not be empty");
    LOGGER.info("  Downloaded {} bytes", response.length());

    // Step 2: Verify valid JSON
    JsonNode rawJson = MAPPER.readTree(response);
    assertNotNull(rawJson, tableName + ": should be valid JSON");
    assertFalse(rawJson.has("error"), tableName + ": response contains error: " + response.substring(0, Math.min(200, response.length())));
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
    assertTrue(rows.size() > 0, tableName + ": transformed should have >0 rows, got 0");
    LOGGER.info("  Transformed: {} rows", rows.size());

    // Step 4: Verify required fields present
    if (requiredFields != null) {
      JsonNode firstRow = rows.get(0);
      for (String field : requiredFields) {
        assertTrue(firstRow.has(field),
            tableName + ": row missing required field '" + field + "'. "
            + "Available: " + firstRow.fieldNames());
      }
    }

    // Step 5: Write to Parquet via DuckDB and verify
    Path parquetFile = tempDir.resolve(tableName + ".parquet");
    String jsonFile = tempDir.resolve(tableName + ".json").toString();
    Files.write(tempDir.resolve(tableName + ".json"), transformed.getBytes());

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Statement stmt = conn.createStatement();

      // Write JSON to Parquet
      stmt.execute("COPY (SELECT * FROM read_json_auto('" + jsonFile
          + "', maximum_object_size=20000000)) TO '" + parquetFile
          + "' (FORMAT PARQUET)");

      assertTrue(Files.exists(parquetFile),
          tableName + ": parquet file should exist");
      assertTrue(Files.size(parquetFile) > 0,
          tableName + ": parquet file should be non-empty");

      // Read back from Parquet and verify
      ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) as cnt FROM read_parquet('" + parquetFile + "')");
      assertTrue(rs.next());
      long count = rs.getLong("cnt");
      assertTrue(count > 0,
          tableName + ": parquet should contain >0 rows, got " + count);

      LOGGER.info("  Parquet: {} bytes, {} rows -- OK",
          Files.size(parquetFile), count);
    }

    LOGGER.info("  {} PASSED", tableName);
  }

  private static String httpGet(String urlStr) throws Exception {
    int maxRetries = 3;
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      URL url = URI.create(urlStr).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/json");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);

      int status = conn.getResponseCode();
      if (status == 429 && attempt < maxRetries) {
        LOGGER.warn("  Rate limited (429) on attempt {}, waiting {}s before retry...",
            attempt, attempt * 2);
        Thread.sleep(attempt * 2000L);
        continue;
      }
      assertEquals(200, status,
          "HTTP " + status + " for " + urlStr.replaceAll("API_KEY=[^&]+", "API_KEY=***"));

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
    throw new AssertionError("Exhausted retries for " + urlStr.replaceAll("API_KEY=[^&]+", "API_KEY=***"));
  }
}
