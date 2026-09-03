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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Covers D-023: {@code energy.eia_fossil_fuel_production}'s own comment claims natural gas
 * production is unioned in alongside crude oil, but the table only ever fetched the crude-oil
 * endpoint, so {@code fuel_type} was always {@code 'Crude Oil'}.
 */
class EiaFossilFuelTransformerTest {

  private static final ObjectMapper M = new ObjectMapper();

  /** Pure row-mapping logic (no network): a natural-gas-shaped row must map to fuel_type
   * 'Natural Gas', proving the union path -- once wired up -- classifies rows correctly rather
   * than by which fetch produced them. */
  @Test @Tag("unit") void mapsNaturalGasRowsSeparatelyFromCrudeOil() throws Exception {
    String crude = "[{\"period\":\"2023-01\",\"duoarea\":\"STX\",\"product-name\":\"Crude Oil\","
        + "\"process\":\"FPF\",\"process-name\":\"Field Production\","
        + "\"series\":\"MCRFPTX1\",\"series-description\":\"Texas Crude Oil Production\","
        + "\"value\":\"120000\",\"units\":\"MBBL\"}]";
    String gas = "[{\"period\":\"2023-01\",\"duoarea\":\"SAR\",\"product-name\":\"Natural Gas\","
        + "\"process\":\"FPD\",\"process-name\":\"Dry Production\","
        + "\"series\":\"NA1160_SAR_2\",\"series-description\":\"Arkansas Dry Natural Gas Production\","
        + "\"value\":\"34439\",\"units\":\"MMCF\"}]";

    // Each endpoint's payload goes through transform() on its own, then the two outputs are
    // concatenated — the union happens across fetches, so what matters here is that each row
    // carries the fuel_type its own product-name implies rather than one inherited from the
    // fetch it arrived in.
    EiaFossilFuelTransformer transformer = new EiaFossilFuelTransformer();
    RequestContext ctx = RequestContext.builder().url("https://api.eia.gov/v2/test").build();
    ArrayNode result = M.createArrayNode();
    result.addAll((ArrayNode) M.readTree(transformer.transform(crude, ctx)));
    result.addAll((ArrayNode) M.readTree(transformer.transform(gas, ctx)));

    assertEquals(2, result.size());

    JsonNode crudeRow = result.get(0);
    assertEquals("Crude Oil", crudeRow.get("fuel_type").asText());
    assertEquals("TX", crudeRow.get("state_abbr").asText());
    assertEquals(2023, crudeRow.get("production_year").asInt());
    assertEquals(1, crudeRow.get("production_month").asInt());
    assertEquals(120000.0, crudeRow.get("production_volume").asDouble());

    JsonNode gasRow = result.get(1);
    assertEquals("Natural Gas", gasRow.get("fuel_type").asText());
    assertEquals("AR", gasRow.get("state_abbr").asText());
    assertEquals("MMCF", gasRow.get("production_unit").asText());
    assertEquals(34439.0, gasRow.get("production_volume").asDouble());
  }

  /** Full path: a live crude-oil response plus the live natural-gas fetch this fix adds must
   * both be present in transform()'s output, distinguished by fuel_type. Requires
   * ENERGY_EIA_API_KEY (skips otherwise, matching this module's other live-API tests). */
  @Test @Tag("integration") void transformUnionsLiveNaturalGasWithCrudeOil() throws Exception {
    TestEnvironmentLoader.ensureLoaded();
    String apiKey = TestEnvironmentLoader.getEnv("ENERGY_EIA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(),
        "ENERGY_EIA_API_KEY not set; skipping live EIA integration test");

    String periodStart = "2023-01-01";
    String periodEnd = "2023-03-31";
    String crudeUrl = "https://api.eia.gov/v2/petroleum/crd/crpdn/data?frequency=monthly"
        + "&start=" + periodStart + "&end=" + periodEnd + "&data[]=value"
        + "&api_key=" + apiKey + "&length=5000";

    java.net.HttpURLConnection conn =
        (java.net.HttpURLConnection) new java.net.URI(crudeUrl).toURL().openConnection();
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    assertEquals(200, conn.getResponseCode(), "live crude-oil fetch must succeed");
    String crudeResponse;
    try (java.io.InputStream in = conn.getInputStream()) {
      crudeResponse = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    } finally {
      conn.disconnect();
    }

    Map<String, String> dimensionValues = new HashMap<>();
    dimensionValues.put("period_start", periodStart);
    dimensionValues.put("period_end", periodEnd);
    Map<String, String> parameters = new LinkedHashMap<>();
    parameters.put("api_key", apiKey);
    parameters.put("length", "5000");
    RequestContext context = RequestContext.builder()
        .url(crudeUrl)
        .parameters(parameters)
        .dimensionValues(dimensionValues)
        .build();

    String transformed = new EiaFossilFuelTransformer().transform(crudeResponse, context);
    JsonNode rows = M.readTree(transformed);
    assertTrue(rows.isArray() && rows.size() > 0, "expected transformed rows");

    Set<String> fuelTypes = new HashSet<>();
    for (JsonNode row : rows) {
      fuelTypes.add(row.get("fuel_type").asText());
    }
    assertTrue(fuelTypes.contains("Crude Oil"), "expected Crude Oil rows: " + fuelTypes);
    assertTrue(fuelTypes.contains("Natural Gas"),
        "expected Natural Gas rows from the union fetch, got only: " + fuelTypes);
  }
}
