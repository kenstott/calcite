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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CdeSupplementalTransformer}.
 */
@Tag("unit")
class CdeSupplementalTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CdeSupplementalTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new CdeSupplementalTransformer();
    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "CA");
    dims.put("year", "2022");
    context = RequestContext.builder()
        .url("https://cde.ucr.cjis.gov/LATEST/supplemental/state/CA/property-crime"
            + "?from=01-2022&to=12-2022&type=totals")
        .dimensionValues(dims)
        .build();
  }

  @Test void testTypicalResponse() throws Exception {
    String response = "{"
        + "\"stolen_and_recovered\": {"
        + "  \"stolen_value\": {"
        + "    \"Firearms\": 34309810,"
        + "    \"Livestock\": 440448"
        + "  },"
        + "  \"recovered_value\": {"
        + "    \"Firearms\": 449433,"
        + "    \"Livestock\": 6324"
        + "  }"
        + "},"
        + "\"offense_analysis\": {\"location_counts\": null}"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(2, array.size());

    JsonNode firearms = array.get(0);
    assertEquals("CA", firearms.get("state_abbr").asText());
    assertEquals(2022, firearms.get("year").asInt());
    assertEquals("Firearms", firearms.get("property_type").asText());
    assertEquals(34309810L, firearms.get("stolen_value").asLong());
    assertEquals(449433L, firearms.get("recovered_value").asLong());

    JsonNode livestock = array.get(1);
    assertEquals("Livestock", livestock.get("property_type").asText());
    assertEquals(440448L, livestock.get("stolen_value").asLong());
    assertEquals(6324L, livestock.get("recovered_value").asLong());
  }

  @Test void testMissingRecoveredValue() throws Exception {
    String response = "{"
        + "\"stolen_and_recovered\": {"
        + "  \"stolen_value\": {"
        + "    \"Firearms\": 100000"
        + "  },"
        + "  \"recovered_value\": {}"
        + "}"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertEquals(1, array.size());
    JsonNode row = array.get(0);
    assertEquals(100000L, row.get("stolen_value").asLong());
    assertTrue(row.get("recovered_value").isNull());
  }

  @Test void testEmptyResponse() {
    assertEquals("[]", transformer.transform("", context));
    assertEquals("[]", transformer.transform(null, context));
  }

  @Test void testNoStolenAndRecovered() throws Exception {
    String response = "{\"offense_analysis\": {\"location_counts\": null}}";
    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertTrue(array.isArray());
    assertEquals(0, array.size());
  }

  @Test void testAllPropertyCategories() throws Exception {
    String response = "{"
        + "\"stolen_and_recovered\": {"
        + "  \"stolen_value\": {"
        + "    \"Firearms\": 34309810,"
        + "    \"Livestock\": 440448,"
        + "    \"Miscellaneous\": 1293494879,"
        + "    \"Household Goods\": 11283859,"
        + "    \"Consumable Goods\": 3590297,"
        + "    \"Office Equipment\": 3488395,"
        + "    \"Clothing and Furs\": 42833008,"
        + "    \"Currency, Notes, etc.\": 684467801,"
        + "    \"Jewelry and Precious Metals\": 76530141,"
        + "    \"Locally Stolen Motor Vehicles\": 901098938,"
        + "    \"Televisions, Radios, Stereos, etc.\": 109770653"
        + "  },"
        + "  \"recovered_value\": {"
        + "    \"Firearms\": 449433,"
        + "    \"Livestock\": 6324,"
        + "    \"Miscellaneous\": 25548246,"
        + "    \"Household Goods\": 176088,"
        + "    \"Consumable Goods\": 217208,"
        + "    \"Office Equipment\": 183971,"
        + "    \"Clothing and Furs\": 760809,"
        + "    \"Currency, Notes, etc.\": 344806,"
        + "    \"Jewelry and Precious Metals\": 468710,"
        + "    \"Locally Stolen Motor Vehicles\": 45318031,"
        + "    \"Televisions, Radios, Stereos, etc.\": 225841"
        + "  }"
        + "}"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertEquals(11, array.size());

    // Spot check motor vehicles
    boolean foundMv = false;
    for (JsonNode row : array) {
      if ("Locally Stolen Motor Vehicles".equals(row.get("property_type").asText())) {
        assertEquals(901098938L, row.get("stolen_value").asLong());
        assertEquals(45318031L, row.get("recovered_value").asLong());
        foundMv = true;
      }
    }
    assertTrue(foundMv, "Expected Locally Stolen Motor Vehicles row");
  }
}
