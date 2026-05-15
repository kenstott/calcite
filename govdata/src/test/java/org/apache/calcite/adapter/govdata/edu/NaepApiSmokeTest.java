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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live smoke test: verifies the NAEP API returns state-level rows for all US jurisdictions.
 *
 * <p>Hits a single real URL (MAT grade 4 MRPCM 2019) and checks that:
 * <ol>
 *   <li>The response status is 200</li>
 *   <li>The result array contains the national row (jurisdiction="NP")</li>
 *   <li>The result array contains at least one state row (2-letter postal abbreviation)</li>
 *   <li>The transformer maps postal abbreviations to integer FIPS correctly</li>
 * </ol>
 *
 * <p>Run with:
 * <pre>
 * EDU_INTEGRATION_TESTS=true \
 * ./gradlew :govdata:test -PincludeTags=integration --tests "*NaepApiSmokeTest*" \
 *     --console=plain
 * </pre>
 */
@Tag("integration")
class NaepApiSmokeTest {

  private static final Logger LOG = LoggerFactory.getLogger(NaepApiSmokeTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String NAEP_URL =
      "https://www.nationsreportcard.gov/Dataservice/GetAdhocData.aspx"
      + "?type=data&subject=MAT&grade=4&subscale=MRPCM&variable=TOTAL"
      + "&jurisdiction=" + NaepJurisdictions.ALL_JURISDICTIONS
      + "&stattype=MN:MN&Year=2019";

  @Test void apiReturnsStateRows() throws Exception {
    assumeTrue("true".equalsIgnoreCase(System.getenv("EDU_INTEGRATION_TESTS")),
        "Set EDU_INTEGRATION_TESTS=true to run NAEP live API tests");

    LOG.info("Fetching: {}", NAEP_URL);
    String body = fetch(NAEP_URL);
    assertNotNull(body, "response body must not be null");
    assertFalse(body.isEmpty(), "response body must not be empty");

    JsonNode root = MAPPER.readTree(body);

    // Verify API status
    int statusCode = root.path("statusCode").asInt(-1);
    LOG.info("NAEP API statusCode: {}", statusCode);
    assertTrue(statusCode == 200, "Expected statusCode=200, got " + statusCode);

    JsonNode results = root.path("result");
    assertTrue(results.isArray(), "'result' must be an array");
    assertTrue(results.size() > 1, "Expected multiple rows (national + states), got " + results.size());
    LOG.info("Total rows returned: {}", results.size());

    // Check for national and state rows (API returns 2-letter postal abbreviations for states)
    boolean hasNational = false;
    int stateAbbrevCount = 0;

    for (JsonNode row : results) {
      String jur = row.path("jurisdiction").asText();
      if ("NP".equals(jur)) {
        hasNational = true;
        LOG.info("National row: jurisdiction={}, value={}", jur, row.path("value").asText());
      } else if (jur.length() == 2) {
        stateAbbrevCount++;
      } else {
        LOG.warn("Unexpected jurisdiction code: '{}'", jur);
      }
    }

    assertTrue(hasNational, "Response must contain a national (NP) row");
    assertTrue(stateAbbrevCount > 0, "Response must contain at least one state row with 2-letter abbreviation");
    LOG.info("State rows with postal abbreviation: {} ({} total rows)", stateAbbrevCount, results.size());

    // Verify the transformer handles the real response correctly
    Map<String, String> dims = new LinkedHashMap<String, String>();
    dims.put("subscale", "MRPCM");
    dims.put("grade", "4");
    dims.put("year", "2019");
    RequestContext ctx = RequestContext.builder()
        .url(NAEP_URL)
        .dimensionValues(dims)
        .build();

    NaepScoresResponseTransformer transformer = new NaepScoresResponseTransformer();
    String transformed = transformer.transform(body, ctx);
    JsonNode rows = MAPPER.readTree(transformed);

    assertTrue(rows.isArray(), "transformer output must be a JSON array");
    assertTrue(rows.size() > 1, "Transformer must produce multiple rows (got " + rows.size() + ")");

    // Count state rows in transformed output (jurisdiction > 0)
    long stateRowCount = 0;
    long nationalRowCount = 0;
    for (JsonNode row : rows) {
      int jur = row.path("jurisdiction").asInt(-1);
      if (jur == 0) {
        nationalRowCount++;
      } else if (jur > 0) {
        stateRowCount++;
      }
    }

    LOG.info("Transformer output: {} national rows, {} state rows", nationalRowCount, stateRowCount);
    assertTrue(nationalRowCount >= 1, "Expected at least 1 national row in transformer output");
    assertTrue(stateRowCount >= 1, "Expected at least 1 state row in transformer output");

    // NAEP reports data for all 50 states + DC + DoD schools = ~52 jurisdictions + national
    assertTrue(stateRowCount >= 40,
        "Expected at least 40 state rows (50 states + DC), got " + stateRowCount);
  }

  private static String fetch(String urlStr) throws Exception {
    URL url = URI.create(urlStr).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(15000);
    conn.setReadTimeout(30000);
    conn.setRequestProperty("Accept", "application/json");

    int code = conn.getResponseCode();
    LOG.info("HTTP {} for {}", code, urlStr);

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining("\n"));
    } finally {
      conn.disconnect();
    }
  }
}
