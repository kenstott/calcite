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
 * Unit tests for {@link CdeAgencyTransformer}.
 */
@Tag("unit")
class CdeAgencyTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CdeAgencyTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new CdeAgencyTransformer();
    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "CA");
    context = RequestContext.builder()
        .url("https://api.usa.gov/crime/fbi/sapi/api/agency/byStateAbbr/CA")
        .dimensionValues(dims)
        .build();
  }

  @Test void testBasicTransformation() throws Exception {
    String response = "{"
        + "\"KERN\": [{"
        + "  \"ori\": \"CA0150000\","
        + "  \"agency_name\": \"Kern County Sheriff\","
        + "  \"agency_type_name\": \"County\","
        + "  \"state_abbr\": \"CA\","
        + "  \"is_nibrs\": true,"
        + "  \"nibrs_start_date\": \"2021-01-01\","
        + "  \"latitude\": 35.3733,"
        + "  \"longitude\": -118.9915"
        + "}],"
        + "\"LOS ANGELES\": [{"
        + "  \"ori\": \"CA0190100\","
        + "  \"agency_name\": \"LAPD\","
        + "  \"agency_type_name\": \"City\","
        + "  \"state_abbr\": \"CA\","
        + "  \"is_nibrs\": false,"
        + "  \"latitude\": 34.0522,"
        + "  \"longitude\": -118.2437"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(2, array.size());

    // Check Kern county agency
    JsonNode kern = findByOri(array, "CA0150000");
    assertEquals("Kern County Sheriff", kern.get("agency_name").asText());
    assertEquals("County", kern.get("agency_type_name").asText());
    assertEquals("CA", kern.get("state_abbr").asText());
    assertEquals("Kern", kern.get("county_name").asText());
    assertTrue(kern.get("is_nibrs").booleanValue());
    assertEquals(35.3733, kern.get("latitude").doubleValue(), 0.001);

    // Check Los Angeles agency
    JsonNode la = findByOri(array, "CA0190100");
    assertEquals("Los Angeles", la.get("county_name").asText());
    assertEquals("City", la.get("agency_type_name").asText());
  }

  @Test void testEmptyResponse() {
    assertEquals("[]", transformer.transform("", context));
    assertEquals("[]", transformer.transform(null, context));
  }

  @Test void testEmptyObject() throws Exception {
    String result = transformer.transform("{}", context);
    JsonNode array = MAPPER.readTree(result);
    assertTrue(array.isArray());
    assertEquals(0, array.size());
  }

  @Test void testTitleCase() {
    assertEquals("Los Angeles", CdeAgencyTransformer.toTitleCase("LOS ANGELES"));
    assertEquals("Kern", CdeAgencyTransformer.toTitleCase("KERN"));
    assertEquals("San Bernardino", CdeAgencyTransformer.toTitleCase("SAN BERNARDINO"));
    assertEquals("", CdeAgencyTransformer.toTitleCase(""));
  }

  private JsonNode findByOri(JsonNode array, String ori) {
    for (JsonNode node : array) {
      if (ori.equals(node.get("ori").asText())) {
        return node;
      }
    }
    throw new AssertionError("No agency found with ORI: " + ori);
  }
}
