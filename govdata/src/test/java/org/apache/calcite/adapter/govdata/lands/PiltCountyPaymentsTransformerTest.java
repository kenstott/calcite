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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fetches the live pilt.doi.gov page — same URL shape the lands schema wires up for
 * pilt_county_payments — and checks the transformer against real HTML, not a fixture.
 * Wyoming exercises the plain COUNTY case; Vermont exercises the mixed TOWN/COUNTY case the
 * class doc calls out, plus the source's own state-wide TOTAL row that must not become a
 * phantom county row.
 */
@Tag("integration")
class PiltCountyPaymentsTransformerTest {

  private static String fetch(String fiscalYear, String stateCode) throws IOException, InterruptedException {
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("https://pilt.doi.gov/counties.cfm?fiscal_yr=" + fiscalYear
            + "&state_code=" + stateCode))
        .timeout(Duration.ofSeconds(30))
        .GET()
        .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    return response.body();
  }

  @Test void wyomingRowsHaveCountyTypeAndNoTotalRow() throws Exception {
    PiltCountyPaymentsTransformer transformer = new PiltCountyPaymentsTransformer();
    RequestContext context = RequestContext.builder()
        .dimensionValues(java.util.Map.of("year", "2024", "state_code", "WY"))
        .build();

    String json = transformer.transform(fetch("2024", "WY"), context);
    JsonNode rows = new ObjectMapper().readTree(json);

    assertTrue(rows.isArray() && rows.size() > 20, "expected ~23 Wyoming counties, got " + rows.size());
    for (JsonNode row : rows) {
      assertFalse("TOTAL".equalsIgnoreCase(row.get("area_name").asText()),
          "the state-wide TOTAL row must be dropped, not ingested as a county");
      assertEquals("COUNTY", row.get("area_type").asText());
      assertEquals("2024", row.get("fiscal_year").asText());
      assertEquals("WY", row.get("state_code").asText());
      assertTrue(row.get("payment_dollars").isIntegralNumber());
      assertTrue(row.get("total_acres").isIntegralNumber());
    }
  }

  @Test void vermontMixesTownAndCountyTypesAndDropsTotalRow() throws Exception {
    PiltCountyPaymentsTransformer transformer = new PiltCountyPaymentsTransformer();
    RequestContext context = RequestContext.builder()
        .dimensionValues(java.util.Map.of("year", "2024", "state_code", "VT"))
        .build();

    String json = transformer.transform(fetch("2024", "VT"), context);
    JsonNode rows = new ObjectMapper().readTree(json);

    boolean sawTown = false;
    boolean sawCounty = false;
    for (JsonNode row : rows) {
      String areaName = row.get("area_name").asText();
      assertFalse("TOTAL".equalsIgnoreCase(areaName),
          "the state-wide TOTAL row must be dropped, not ingested as a phantom area");
      String areaType = row.get("area_type").isNull() ? null : row.get("area_type").asText();
      sawTown |= "TOWN".equals(areaType);
      sawCounty |= "COUNTY".equals(areaType);
    }
    assertTrue(sawTown, "Vermont's page mixes in TOWN-level rows");
    assertTrue(sawCounty, "Vermont's page mixes in COUNTY-level subtotal rows");
  }
}
