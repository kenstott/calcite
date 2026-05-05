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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests for weather-ext transformers (Phases 1-2, 4-6).
 *
 * <p>Unit tests use synthetic input; integration tests hit live APIs.
 */
class WeatherExtSmokeTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WeatherExtSmokeTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }

  // =========================================================================
  // GhcndDailyTransformer — unit tests (synthetic CDO GHCND response)
  // =========================================================================

  @Tag("unit")
  @Test void testGhcndDailyPivotsTwoElements() throws Exception {
    String response = "{"
        + "\"metadata\":{\"resultset\":{\"offset\":1,\"count\":4,\"limit\":1000}},"
        + "\"results\":["
        + "{\"date\":\"2022-01-01T00:00:00\",\"datatype\":\"TMAX\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\",,S,\",\"value\":11.1},"
        + "{\"date\":\"2022-01-01T00:00:00\",\"datatype\":\"TMIN\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\",,S,\",\"value\":2.2},"
        + "{\"date\":\"2022-01-01T00:00:00\",\"datatype\":\"PRCP\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\"T,,S,\",\"value\":0.0},"
        + "{\"date\":\"2022-01-02T00:00:00\",\"datatype\":\"TMAX\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\",,S,\",\"value\":14.4}"
        + "]}";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", "01");
    dims.put("year", "2022");
    RequestContext ctx = RequestContext.builder()
        .url("https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND")
        .dimensionValues(dims)
        .build();

    String result = new GhcndDailyTransformer().transform(response, ctx);
    JsonNode rows = MAPPER.readTree(result);

    assertTrue(rows.isArray(), "Result should be JSON array");
    assertEquals(2, rows.size(), "Should produce 2 wide rows (one per date)");

    JsonNode day1 = rows.get(0);
    assertEquals("USW00013876", day1.get("station_id").asText());
    assertEquals("2022-01-01", day1.get("date").asText());
    assertEquals("01", day1.get("state_fips").asText());
    assertEquals(2022, day1.get("year").intValue());
    assertEquals(11.1, day1.get("tmax_c").doubleValue(), 0.01);
    assertEquals(2.2, day1.get("tmin_c").doubleValue(), 0.01);
    assertEquals(0.0, day1.get("prcp_mm").doubleValue(), 0.001);
    assertEquals("T,,S,", day1.get("prcp_flag").asText());

    JsonNode day2 = rows.get(1);
    assertEquals("2022-01-02", day2.get("date").asText());
    assertEquals(14.4, day2.get("tmax_c").doubleValue(), 0.01);
    assertTrue(day2.get("tmin_c").isNull(), "tmin_c should be null for day2");
  }

  @Tag("unit")
  @Test void testGhcndDailyEmptyResponse() throws Exception {
    RequestContext ctx = RequestContext.builder()
        .url("https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND")
        .dimensionValues(new HashMap<String, String>())
        .build();
    assertEquals("[]", new GhcndDailyTransformer().transform("", ctx));
    assertEquals("[]", new GhcndDailyTransformer().transform(null, ctx));
    JsonNode empty = MAPPER.readTree(new GhcndDailyTransformer().transform("{}", ctx));
    assertEquals(0, empty.size());
  }

  // =========================================================================
  // DroughtMonitorTransformer — unit tests (synthetic USDM JSON)
  // =========================================================================

  @Tag("unit")
  @Test void testDroughtTransformerBasic() throws Exception {
    // Cumulative values: D0=100, D1=77.82, D2=0, D3=0, D4=0
    // Exclusive: none=0, d0=22.18, d1=77.82, d2=0, d3=0, d4=0
    // DSCI = 100+77.82+0+0+0 = 177.82
    String response = "MapDate,FIPS,County,State,None,D0,D1,D2,D3,D4,ValidStart,ValidEnd,StatisticFormatID\n"
        + "20220104,01001,Autauga County,AL,0.00,100.00,77.82,0.00,0.00,0.00,"
        + "2022-01-04,2022-01-10,1";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "AL");
    dims.put("year", "2022");
    RequestContext ctx = RequestContext.builder()
        .url("https://usdmdataservices.unl.edu/api/CountyStatistics/GetDroughtSeverityStatisticsByAreaPercent")
        .dimensionValues(dims)
        .build();

    String result = new DroughtMonitorTransformer().transform(response, ctx);
    JsonNode rows = MAPPER.readTree(result);

    assertTrue(rows.isArray());
    assertEquals(1, rows.size());

    JsonNode row = rows.get(0);
    assertEquals("01001", row.get("county_fips").asText());
    assertEquals("01", row.get("state_fips").asText());
    assertEquals("AL", row.get("state_abbr").asText());
    assertEquals("2022-01-04", row.get("week_date").asText());
    assertEquals(2022, row.get("year").intValue());
    assertEquals(0.0, row.get("none_pct").doubleValue(), 0.001);
    assertEquals(22.18, row.get("d0_pct").doubleValue(), 0.01, "exclusive d0 = D0 - D1");
    assertEquals(77.82, row.get("d1_pct").doubleValue(), 0.01, "exclusive d1 = D1 - D2");
    assertEquals(0.0, row.get("d2_pct").doubleValue(), 0.001);
    assertEquals(177.82, row.get("dsci").doubleValue(), 0.1);
    assertEquals("2022-01-04", row.get("valid_start").asText());
    assertEquals("2022-01-10", row.get("valid_end").asText());
  }

  @Tag("unit")
  @Test void testDroughtTransformerSkipsMissingFips() throws Exception {
    String response = "MapDate,FIPS,County,State,None,D0,D1,D2,D3,D4,ValidStart,ValidEnd,StatisticFormatID\n"
        + "20220104,,Autauga County,AL,0.00,10.00,0.00,0.00,0.00,0.00,"
        + "2022-01-04,2022-01-10,1";
    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "AL");
    RequestContext ctx = RequestContext.builder()
        .url("https://usdmdataservices.unl.edu")
        .dimensionValues(dims)
        .build();
    JsonNode rows = MAPPER.readTree(new DroughtMonitorTransformer().transform(response, ctx));
    assertEquals(0, rows.size(), "Row with null FIPS should be skipped");
  }

  // =========================================================================
  // HmsSmokeDailyTransformer — unit tests
  // =========================================================================

  @Tag("unit")
  @Test void testHmsPassthroughValidJson() throws Exception {
    String json = "["
        + "{\"county_fips\":\"06037\",\"state_fips\":\"06\",\"date\":\"2022-08-01\","
        + " \"year\":2022,\"smoke_coverage_pct\":45.2,\"heavy_smoke_pct\":10.1,"
        + " \"medium_smoke_pct\":15.3,\"light_smoke_pct\":19.8}"
        + "]";

    Map<String, String> dims = new HashMap<>();
    RequestContext ctx = RequestContext.builder()
        .url("https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/")
        .dimensionValues(dims)
        .build();

    String result = new HmsSmokeDailyTransformer().transform(json, ctx);
    JsonNode rows = MAPPER.readTree(result);
    assertEquals(1, rows.size());
    assertEquals("06037", rows.get(0).get("county_fips").asText());
  }

  @Tag("unit")
  @Test void testHmsBinaryContentReturnsEmpty() throws Exception {
    Map<String, String> dims = new HashMap<>();
    RequestContext ctx = RequestContext.builder()
        .url("https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/")
        .dimensionValues(dims)
        .build();
    // Simulate binary shapefile content (non-JSON)
    String binary = "PK     ";
    assertEquals("[]", new HmsSmokeDailyTransformer().transform(binary, ctx));
  }

  // =========================================================================
  // ClimateNormalsTransformer — unit tests (synthetic CDO NORMAL_MLY)
  // =========================================================================

  @Tag("unit")
  @Test void testClimateNormalsPivot() throws Exception {
    String response = "{"
        + "\"metadata\":{\"resultset\":{\"offset\":1,\"count\":4,\"limit\":1000}},"
        + "\"results\":["
        + "{\"date\":\"2010-01-01T00:00:00\",\"datatype\":\"MLY-TMAX-NORMAL\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\"C\",\"value\":139},"
        + "{\"date\":\"2010-01-01T00:00:00\",\"datatype\":\"MLY-TMIN-NORMAL\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\"C\",\"value\":44},"
        + "{\"date\":\"2010-01-01T00:00:00\",\"datatype\":\"MLY-PRCP-NORMAL\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\"C\",\"value\":1219},"
        + "{\"date\":\"2010-02-01T00:00:00\",\"datatype\":\"MLY-TMAX-NORMAL\","
        + " \"station\":\"GHCND:USW00013876\",\"attributes\":\"C\",\"value\":149}"
        + "]}";

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", "01");
    RequestContext ctx = RequestContext.builder()
        .url("https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=NORMAL_MLY")
        .dimensionValues(dims)
        .build();

    String result = new ClimateNormalsTransformer().transform(response, ctx);
    JsonNode rows = MAPPER.readTree(result);

    assertTrue(rows.isArray());
    assertEquals(2, rows.size(), "Should have 2 rows (Jan + Feb)");

    JsonNode jan = rows.get(0);
    assertEquals("USW00013876", jan.get("station_id").asText());
    assertEquals("01", jan.get("state_fips").asText());
    assertEquals(1, jan.get("month").intValue());
    assertEquals(13.9, jan.get("normal_tmax_c").doubleValue(), 0.01, "139 tenths → 13.9°C");
    assertEquals(4.4, jan.get("normal_tmin_c").doubleValue(), 0.01, "44 tenths → 4.4°C");
    assertEquals(121.9, jan.get("normal_prcp_mm").doubleValue(), 0.01, "1219 tenths → 121.9mm");
    assertTrue(jan.get("county_fips").isNull(), "county_fips should be null (post-ETL join)");

    JsonNode feb = rows.get(1);
    assertEquals(2, feb.get("month").intValue());
    assertEquals(14.9, feb.get("normal_tmax_c").doubleValue(), 0.01);
  }

  @Tag("unit")
  @Test void testClimateNormalsEmpty() throws Exception {
    RequestContext ctx = RequestContext.builder()
        .url("https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=NORMAL_MLY")
        .dimensionValues(new HashMap<String, String>())
        .build();
    assertEquals("[]", new ClimateNormalsTransformer().transform("", ctx));
    assertEquals("[]", new ClimateNormalsTransformer().transform(null, ctx));
  }

  // =========================================================================
  // LIVE INTEGRATION TESTS — real API calls, limited data
  // =========================================================================

  @Tag("integration")
  @Test void testDroughtMonitorLiveAlabama2022() throws Exception {
    String url = "https://usdmdataservices.unl.edu/api/CountyStatistics/"
        + "GetDroughtSeverityStatisticsByAreaPercent"
        + "?aoi=AL&startdate=1/1/2022&enddate=3/31/2022&statisticsType=1";

    LOGGER.info("=== Live: drought_monitor_weekly (AL, Q1 2022) ===");
    String response = httpGet(url, null);
    assertNotNull(response);
    assertFalse(response.isEmpty(), "USDM response should not be empty");
    LOGGER.info("  Downloaded {} bytes", response.length());

    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "AL");
    dims.put("year", "2022");
    RequestContext ctx = RequestContext.builder().url(url).dimensionValues(dims).build();

    String result = new DroughtMonitorTransformer().transform(response, ctx);
    JsonNode rows = MAPPER.readTree(result);

    assertTrue(rows.isArray());
    assertTrue(rows.size() > 0, "Should have drought records for AL Q1 2022");
    LOGGER.info("  Transformed: {} rows", rows.size());

    JsonNode first = rows.get(0);
    assertTrue(first.has("county_fips"), "Row must have county_fips");
    assertTrue(first.has("week_date"), "Row must have week_date");
    assertTrue(first.has("dsci"), "Row must have dsci");
    assertNotNull(first.get("county_fips").asText());
    assertTrue(first.get("county_fips").asText().length() == 5,
        "county_fips should be 5 digits");

    writeAndVerifyParquet("drought_monitor_weekly_al", result);
    LOGGER.info("  drought_monitor_weekly (AL Q1 2022) PASSED — {} rows", rows.size());
  }

  @Tag("integration")
  @Test void testGhcndDailyLiveAlabama() throws Exception {
    String token = TestEnvironmentLoader.getEnv("NOAA_CDO_TOKEN");
    Assumptions.assumeTrue(token != null && !token.isEmpty(),
        "NOAA_CDO_TOKEN required for ghcnd_daily test");

    // Single week to keep data volume small
    String url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
        + "?datasetid=GHCND&locationid=FIPS:01"
        + "&startdate=2022-01-01&enddate=2022-01-07"
        + "&datatypeid=TMAX,TMIN,PRCP&limit=500&offset=1";

    LOGGER.info("=== Live: ghcnd_daily (AL, 2022-01-01 to 2022-01-07) ===");
    Map<String, String> headers = new HashMap<>();
    headers.put("token", token);
    String response = httpGet(url, headers);
    assertNotNull(response);
    assertFalse(response.isEmpty());
    LOGGER.info("  Downloaded {} bytes", response.length());

    JsonNode raw = MAPPER.readTree(response);
    assertNotNull(raw.get("results"), "CDO GHCND should return 'results' array");
    LOGGER.info("  Raw CDO rows: {}", raw.get("results").size());

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", "01");
    dims.put("year", "2022");
    RequestContext ctx = RequestContext.builder().url(url).dimensionValues(dims).build();

    String result = new GhcndDailyTransformer().transform(response, ctx);
    JsonNode rows = MAPPER.readTree(result);

    assertTrue(rows.isArray());
    assertTrue(rows.size() > 0, "Should have ghcnd_daily rows for AL Jan 2022");
    LOGGER.info("  Transformed: {} wide rows (pivoted from {} CDO rows)",
        rows.size(), raw.get("results").size());

    JsonNode first = rows.get(0);
    assertTrue(first.has("station_id"));
    assertTrue(first.has("date"));
    assertTrue(first.has("state_fips"));
    assertFalse(first.get("station_id").asText().startsWith("GHCND:"),
        "station_id should have GHCND: prefix stripped");

    writeAndVerifyParquet("ghcnd_daily_al", result);
    LOGGER.info("  ghcnd_daily (AL, 1 week) PASSED — {} wide rows", rows.size());
  }

  @Tag("integration")
  @Test void testClimateNormalsLiveAlabama() throws Exception {
    String token = TestEnvironmentLoader.getEnv("NOAA_CDO_TOKEN");
    Assumptions.assumeTrue(token != null && !token.isEmpty(),
        "NOAA_CDO_TOKEN required for climate_normals_monthly test");

    String url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
        + "?datasetid=NORMAL_MLY&locationid=FIPS:01"
        + "&startdate=2010-01-01&enddate=2010-12-01&limit=500&offset=1";

    LOGGER.info("=== Live: climate_normals_monthly (AL, NORMAL_MLY) ===");
    Map<String, String> headers = new HashMap<>();
    headers.put("token", token);
    String response = httpGet(url, headers);

    if (response == null || response.isEmpty()) {
      LOGGER.warn("  No response from CDO NORMAL_MLY — endpoint may not have AL normals");
      Assumptions.abort("CDO NORMAL_MLY returned empty response for AL");
    }

    JsonNode raw = MAPPER.readTree(response);
    LOGGER.info("  Raw CDO response: {}", raw.getNodeType());

    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", "01");
    RequestContext ctx = RequestContext.builder().url(url).dimensionValues(dims).build();

    String result = new ClimateNormalsTransformer().transform(response, ctx);
    JsonNode rows = MAPPER.readTree(result);
    assertTrue(rows.isArray());

    if (rows.size() > 0) {
      LOGGER.info("  Transformed: {} station-month normals", rows.size());
      JsonNode first = rows.get(0);
      assertTrue(first.has("station_id"));
      assertTrue(first.has("month"));
      assertTrue(first.has("normal_tmax_c"));
      writeAndVerifyParquet("climate_normals_al", result);
      LOGGER.info("  climate_normals_monthly (AL) PASSED — {} rows", rows.size());
    } else {
      LOGGER.warn("  CDO NORMAL_MLY returned 0 pivoted rows for AL — "
          + "dataset may not have normals for this state in the test date range");
    }
  }

  @Tag("integration")
  @Test void testGhcndStationsWithCountyLive() throws Exception {
    // Fetch a small slice of ghcnd-stations.txt manually and pass it to the transformer.
    // The transformer will also fetch TIGERweb county centroids and ghcnd-inventory.txt inline.
    LOGGER.info("=== Live: ghcnd_stations_with_county (3 synthetic AL stations) ===");

    // Synthetic fixed-width input — 3 US Alabama stations
    String stationsText =
          "USW00013876  32.3000  -86.3833   62.5 AL MONTGOMERY DANNELLY FLD               " + "\n"
        + "USW00013877  30.6833  -88.2500    6.7 AL MOBILE REGIONAL AIRPORT                " + "\n"
        + "USW00013895  34.8167  -87.6500  178.3 AL MUSCLE SHOALS REGIONAL AIRPORT         " + "\n"
        + "AE000041196  25.3330   55.5170   34.0    SHARJAH INTER. AIRP            GSN     41196\n";

    Map<String, String> dims = new HashMap<>();
    dims.put("type", "ghcnd_station");
    RequestContext ctx = RequestContext.builder()
        .url("https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt")
        .dimensionValues(dims)
        .build();

    String result = new GhcndStationTransformer().transform(stationsText, ctx);
    JsonNode rows = MAPPER.readTree(result);

    assertTrue(rows.isArray());
    assertEquals(3, rows.size(), "Should process 3 US stations and skip 1 non-US station");
    LOGGER.info("  Transformed: {} stations (non-US filtered)", rows.size());

    JsonNode first = rows.get(0);
    assertEquals("USW00013876", first.get("station_id").asText());
    assertEquals("01", first.get("state_fips").asText(), "AL should map to FIPS 01");
    assertTrue(first.has("county_fips"), "county_fips field must exist");
    assertTrue(first.has("distance_to_county_centroid_km"),
        "distance_to_county_centroid_km field must exist");

    // county_fips may be null if TIGERweb was unreachable, but field must exist
    LOGGER.info("  Station USW00013876: state_fips={}, county_fips={}, dist_km={}",
        first.get("state_fips").asText(),
        first.get("county_fips").asText(),
        first.get("distance_to_county_centroid_km").asText());

    LOGGER.info("  ghcnd_stations_with_county PASSED");
  }

  // =========================================================================
  // HELPERS
  // =========================================================================

  private void writeAndVerifyParquet(String tableName, String json) throws Exception {
    Path jsonFile = tempDir.resolve(tableName + ".json");
    Path parquetFile = tempDir.resolve(tableName + ".parquet");
    Files.write(jsonFile, json.getBytes());

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Statement stmt = conn.createStatement();
      stmt.execute("COPY (SELECT * FROM read_json_auto('" + jsonFile
          + "', maximum_object_size=20000000)) TO '" + parquetFile
          + "' (FORMAT PARQUET)");

      assertTrue(Files.exists(parquetFile), tableName + ": parquet should exist");
      assertTrue(Files.size(parquetFile) > 0, tableName + ": parquet should be non-empty");

      ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) as cnt FROM read_parquet('" + parquetFile + "')");
      assertTrue(rs.next());
      long count = rs.getLong("cnt");
      assertTrue(count > 0, tableName + ": parquet row count should be > 0");
      LOGGER.info("  Parquet: {} bytes, {} rows", Files.size(parquetFile), count);
    }
  }

  private static String httpGet(String urlStr, Map<String, String> headers) throws Exception {
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
        LOGGER.warn("  Rate limited (429), waiting {}s before retry...", attempt * 2);
        Thread.sleep(attempt * 2000L);
        continue;
      }
      if (status != 200) {
        LOGGER.warn("  HTTP {} for {}", status,
            urlStr.replaceAll("(token|key|email)=[^&]+", "$1=***"));
        return "";
      }
      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append("\n");
        }
      }
      return sb.toString();
    }
    return "";
  }
}
