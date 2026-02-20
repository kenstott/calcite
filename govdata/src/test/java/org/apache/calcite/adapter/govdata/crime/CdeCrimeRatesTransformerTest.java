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
 * Unit tests for {@link CdeCrimeRatesTransformer}.
 */
@Tag("unit")
class CdeCrimeRatesTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CdeCrimeRatesTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new CdeCrimeRatesTransformer();
    Map<String, String> dims = new HashMap<>();
    dims.put("state_abbr", "CA");
    dims.put("offense", "homicide");
    dims.put("year", "2022");
    context = RequestContext.builder()
        .url("https://api.usa.gov/crime/fbi/sapi/api/summarized/state/CA/homicide")
        .dimensionValues(dims)
        .build();
  }

  @Test void testArrayResponse() throws Exception {
    String response = "[{"
        + "\"date\": \"01-2022\","
        + "\"rate\": 5.2,"
        + "\"cleared_rate\": 2.1,"
        + "\"us_rate\": 6.3,"
        + "\"us_cleared_rate\": 2.8,"
        + "\"population\": 39538223,"
        + "\"ori_coverage_pct\": 98.5"
        + "}]";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(1, array.size());

    JsonNode row = array.get(0);
    assertEquals("CA", row.get("state_abbr").asText());
    assertEquals("homicide", row.get("offense_code").asText());
  }

  @Test void testObjectWithResultsResponse() throws Exception {
    String response = "{"
        + "\"results\": [{"
        + "  \"date\": \"01-2022\","
        + "  \"rate\": 5.2,"
        + "  \"population\": 39538223"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(1, array.size());
    assertEquals("CA", array.get(0).get("state_abbr").asText());
    assertEquals("homicide", array.get(0).get("offense_code").asText());
  }

  @Test void testEmptyResponse() {
    assertEquals("[]", transformer.transform("", context));
    assertEquals("[]", transformer.transform(null, context));
  }

  @Test void testEmptyArray() throws Exception {
    String result = transformer.transform("[]", context);
    JsonNode array = MAPPER.readTree(result);
    assertTrue(array.isArray());
    assertEquals(0, array.size());
  }

  @Test void testNestedMonthKeyedResponse() throws Exception {
    String response = "{"
        + "\"01-2022\": {"
        + "  \"rate\": 5.2,"
        + "  \"cleared_rate\": 2.1"
        + "},"
        + "\"02-2022\": {"
        + "  \"rate\": 4.8,"
        + "  \"cleared_rate\": 1.9"
        + "}"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(2, array.size());

    // Both rows should have state_abbr and offense_code enriched
    for (JsonNode row : array) {
      assertEquals("CA", row.get("state_abbr").asText());
      assertEquals("homicide", row.get("offense_code").asText());
      assertTrue(row.has("month"));
    }
  }
}
