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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CdoDataTransformer} and {@link CdoStationTransformer}.
 */
@Tag("unit")
class CdoDataTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CdoDataTransformer dataTransformer;
  private CdoStationTransformer stationTransformer;
  private RequestContext context;

  @BeforeEach void setUp() {
    dataTransformer = new CdoDataTransformer();
    stationTransformer = new CdoStationTransformer();
    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", "17");
    context = RequestContext.builder()
        .url("https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GSOM&locationid=FIPS:17")
        .dimensionValues(dims)
        .build();
  }

  @Test void testDataTransformation() throws Exception {
    String response = "{"
        + "\"metadata\": {\"resultset\": {\"offset\": 1, \"count\": 2, \"limit\": 1000}},"
        + "\"results\": ["
        + "  {"
        + "    \"date\": \"2020-01-01T00:00:00\","
        + "    \"datatype\": \"TAVG\","
        + "    \"station\": \"GHCND:USW00094846\","
        + "    \"attributes\": \"H,,S,\","
        + "    \"value\": -2.3"
        + "  },"
        + "  {"
        + "    \"date\": \"2020-06-01T00:00:00\","
        + "    \"datatype\": \"PRCP\","
        + "    \"station\": \"GHCND:USW00094846\","
        + "    \"attributes\": \"\","
        + "    \"value\": 102.4"
        + "  }"
        + "]"
        + "}";

    String result = dataTransformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(2, array.size());

    JsonNode first = array.get(0);
    assertEquals("17", first.get("state_fips").asText());
    assertEquals("GHCND:USW00094846", first.get("station_id").asText());
    assertEquals(2020, first.get("year").intValue());
    assertEquals(1, first.get("month").intValue());
    assertEquals("TAVG", first.get("datatype").asText());
    assertEquals(-2.3, first.get("value").doubleValue(), 0.01);
    assertEquals("H,,S,", first.get("attributes").asText());

    JsonNode second = array.get(1);
    assertEquals(6, second.get("month").intValue());
    assertEquals("PRCP", second.get("datatype").asText());
    assertEquals(102.4, second.get("value").doubleValue(), 0.01);
  }

  @Test void testDataEmptyResponse() {
    assertEquals("[]", dataTransformer.transform("", context));
    assertEquals("[]", dataTransformer.transform(null, context));
  }

  @Test void testDataNoResults() throws Exception {
    // CDO returns empty object when no data
    String result = dataTransformer.transform("{}", context);
    JsonNode array = MAPPER.readTree(result);
    assertTrue(array.isArray());
    assertEquals(0, array.size());
  }

  @Test void testStationTransformation() throws Exception {
    String response = "{"
        + "\"metadata\": {\"resultset\": {\"offset\": 1, \"count\": 1, \"limit\": 1000}},"
        + "\"results\": [{"
        + "  \"id\": \"GHCND:USW00094846\","
        + "  \"name\": \"CHICAGO OHARE INTERNATIONAL AIRPORT, IL US\","
        + "  \"latitude\": 41.9606,"
        + "  \"longitude\": -87.9317,"
        + "  \"elevation\": 201.8,"
        + "  \"mindate\": \"1958-11-01\","
        + "  \"maxdate\": \"2024-12-31\","
        + "  \"datacoverage\": 1.0"
        + "}]"
        + "}";

    String result = stationTransformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(1, array.size());

    JsonNode station = array.get(0);
    assertEquals("GHCND:USW00094846", station.get("station_id").asText());
    assertEquals("CHICAGO OHARE INTERNATIONAL AIRPORT, IL US",
        station.get("station_name").asText());
    assertEquals("17", station.get("state_fips").asText());
    assertEquals(41.9606, station.get("latitude").doubleValue(), 0.001);
    assertEquals(-87.9317, station.get("longitude").doubleValue(), 0.001);
    assertEquals(201.8, station.get("elevation").doubleValue(), 0.1);
    assertEquals("1958-11-01", station.get("min_date").asText());
    assertEquals("2024-12-31", station.get("max_date").asText());
    assertEquals(1.0, station.get("datacoverage").doubleValue(), 0.01);
  }

  @Test void testStationEmptyResponse() {
    assertEquals("[]", stationTransformer.transform("", context));
    assertEquals("[]", stationTransformer.transform(null, context));
  }

  @Test void testStationMissingElevation() throws Exception {
    String response = "{"
        + "\"metadata\": {\"resultset\": {\"offset\": 1, \"count\": 1, \"limit\": 1000}},"
        + "\"results\": [{"
        + "  \"id\": \"GHCND:USC00111577\","
        + "  \"name\": \"CHICAGO BOTANICAL GARDEN, IL US\","
        + "  \"latitude\": 42.15,"
        + "  \"longitude\": -87.79,"
        + "  \"mindate\": \"2000-01-01\","
        + "  \"maxdate\": \"2024-12-31\","
        + "  \"datacoverage\": 0.85"
        + "}]"
        + "}";

    String result = stationTransformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertEquals(1, array.size());
    assertTrue(array.get(0).get("elevation").isNull());
  }
}
