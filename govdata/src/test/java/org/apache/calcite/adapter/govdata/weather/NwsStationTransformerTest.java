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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link NwsStationTransformer} and {@link NwsAlertTransformer}.
 */
@Tag("unit")
class NwsStationTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private NwsStationTransformer stationTransformer;
  private NwsAlertTransformer alertTransformer;
  private RequestContext context;

  @BeforeEach void setUp() {
    stationTransformer = new NwsStationTransformer();
    alertTransformer = new NwsAlertTransformer();
    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "IL");
    context = RequestContext.builder()
        .url("https://api.weather.gov/stations?state=IL")
        .dimensionValues(dims)
        .build();
  }

  @Test void testStationTransformation() throws Exception {
    String response = "{"
        + "\"features\": [{"
        + "  \"properties\": {"
        + "    \"stationIdentifier\": \"KORD\","
        + "    \"name\": \"Chicago OHare International Airport\","
        + "    \"timeZone\": \"America/Chicago\","
        + "    \"forecast\": \"https://api.weather.gov/zones/forecast/ILZ014\","
        + "    \"county\": \"https://api.weather.gov/zones/county/ILC031\","
        + "    \"elevation\": {\"value\": 201.8, \"unitCode\": \"wmoUnit:m\"}"
        + "  },"
        + "  \"geometry\": {\"type\": \"Point\", \"coordinates\": [-87.9317, 41.9606]}"
        + "}]"
        + "}";

    String result = stationTransformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(1, array.size());

    JsonNode station = array.get(0);
    assertEquals("KORD", station.get("station_id").asText());
    assertEquals("Chicago OHare International Airport",
        station.get("station_name").asText());
    assertEquals("IL", station.get("state_abbr").asText());
    assertEquals(-87.9317, station.get("longitude").doubleValue(), 0.001);
    assertEquals(41.9606, station.get("latitude").doubleValue(), 0.001);
    assertEquals(201.8, station.get("elevation_m").doubleValue(), 0.1);
    assertEquals("America/Chicago", station.get("timezone").asText());
    assertEquals("ILZ014", station.get("forecast_zone").asText());
    assertEquals("ILC031", station.get("county_zone").asText());
  }

  @Test void testStationEmptyResponse() {
    assertEquals("[]", stationTransformer.transform("", context));
    assertEquals("[]", stationTransformer.transform(null, context));
  }

  @Test void testStationNoFeatures() throws Exception {
    String result = stationTransformer.transform("{}", context);
    JsonNode array = MAPPER.readTree(result);
    assertTrue(array.isArray());
    assertEquals(0, array.size());
  }

  @Test void testStationMissingElevation() throws Exception {
    String response = "{"
        + "\"features\": [{"
        + "  \"properties\": {"
        + "    \"stationIdentifier\": \"KMDW\","
        + "    \"name\": \"Chicago Midway\""
        + "  },"
        + "  \"geometry\": {\"type\": \"Point\", \"coordinates\": [-87.75, 41.78]}"
        + "}]"
        + "}";

    String result = stationTransformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertEquals(1, array.size());
    assertTrue(array.get(0).get("elevation_m").isNull());
  }

  @Test void testExtractZoneId() {
    ObjectMapper mapper = new ObjectMapper();
    com.fasterxml.jackson.databind.node.ObjectNode props = mapper.createObjectNode();
    props.put("forecast", "https://api.weather.gov/zones/forecast/ILZ014");
    assertEquals("ILZ014", NwsStationTransformer.extractZoneId(props, "forecast"));

    props.putNull("missing");
    assertNull(NwsStationTransformer.extractZoneId(props, "missing"));
    assertNull(NwsStationTransformer.extractZoneId(props, "nonexistent"));
  }

  @Test void testAlertTransformation() throws Exception {
    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "OK");
    RequestContext alertCtx = RequestContext.builder()
        .url("https://api.weather.gov/alerts?area=OK")
        .dimensionValues(dims)
        .build();

    String response = "{"
        + "\"features\": [{"
        + "  \"properties\": {"
        + "    \"id\": \"urn:oid:2.49.0.1.840.0.abc\","
        + "    \"event\": \"Tornado Warning\","
        + "    \"severity\": \"Extreme\","
        + "    \"certainty\": \"Observed\","
        + "    \"urgency\": \"Immediate\","
        + "    \"headline\": \"Tornado Warning issued\","
        + "    \"description\": \"A tornado was observed...\","
        + "    \"onset\": \"2024-01-15T14:00:00-06:00\","
        + "    \"expires\": \"2024-01-15T15:00:00-06:00\","
        + "    \"senderName\": \"NWS Norman OK\","
        + "    \"affectedZones\": ["
        + "      \"https://api.weather.gov/zones/forecast/OKZ014\","
        + "      \"https://api.weather.gov/zones/forecast/OKZ015\""
        + "    ]"
        + "  }"
        + "}]"
        + "}";

    String result = alertTransformer.transform(response, alertCtx);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(1, array.size());

    JsonNode alert = array.get(0);
    assertEquals("urn:oid:2.49.0.1.840.0.abc", alert.get("alert_id").asText());
    assertEquals("OK", alert.get("state_abbr").asText());
    assertEquals("Tornado Warning", alert.get("event").asText());
    assertEquals("Extreme", alert.get("severity").asText());
    assertEquals("Observed", alert.get("certainty").asText());
    assertEquals("Immediate", alert.get("urgency").asText());
    assertEquals("NWS Norman OK", alert.get("sender_name").asText());
    assertEquals("OKZ014,OKZ015", alert.get("affected_zones").asText());
  }

  @Test void testAlertEmptyResponse() {
    assertEquals("[]", alertTransformer.transform("", context));
    assertEquals("[]", alertTransformer.transform(null, context));
  }

  @Test void testAlertNoZones() throws Exception {
    String response = "{"
        + "\"features\": [{"
        + "  \"properties\": {"
        + "    \"id\": \"test-alert\","
        + "    \"event\": \"Heat Advisory\""
        + "  }"
        + "}]"
        + "}";

    String result = alertTransformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertEquals(1, array.size());
    assertTrue(array.get(0).get("affected_zones").isNull());
  }
}
