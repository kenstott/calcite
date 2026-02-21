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
 * Unit tests for {@link EpaAqsTransformer}.
 */
@Tag("unit")
class EpaAqsTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private EpaAqsTransformer transformer;
  private RequestContext context;

  @BeforeEach void setUp() {
    transformer = new EpaAqsTransformer();
    Map<String, String> dims = new HashMap<>();
    dims.put("state_fips", "06");
    dims.put("param", "88101");
    context = RequestContext.builder()
        .url("https://aqs.epa.gov/data/api/annualData/byState?param=88101&state=06")
        .dimensionValues(dims)
        .build();
  }

  @Test void testBasicTransformation() throws Exception {
    String response = "{"
        + "\"Header\": [{\"status\": \"Success\", \"rows\": 2}],"
        + "\"Data\": ["
        + "  {"
        + "    \"state_code\": \"06\","
        + "    \"county_code\": \"037\","
        + "    \"parameter_code\": \"88101\","
        + "    \"parameter_name\": \"PM2.5 - Local Conditions\","
        + "    \"year\": 2020,"
        + "    \"arithmetic_mean\": 10.5,"
        + "    \"first_max_value\": 45.2,"
        + "    \"observation_count\": 350,"
        + "    \"valid_day_count\": 340,"
        + "    \"aqi\": 44"
        + "  },"
        + "  {"
        + "    \"state_code\": \"06\","
        + "    \"county_code\": \"059\","
        + "    \"parameter_code\": \"88101\","
        + "    \"parameter_name\": \"PM2.5 - Local Conditions\","
        + "    \"year\": 2020,"
        + "    \"arithmetic_mean\": 8.3,"
        + "    \"first_max_value\": 32.1,"
        + "    \"observation_count\": 360,"
        + "    \"valid_day_count\": 355,"
        + "    \"aqi\": 35"
        + "  }"
        + "]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(2, array.size());

    JsonNode first = array.get(0);
    assertEquals("06", first.get("state_fips").asText());
    assertEquals("06037", first.get("county_fips").asText());
    assertEquals("88101", first.get("parameter_code").asText());
    assertEquals("PM2.5 - Local Conditions", first.get("parameter_name").asText());
    assertEquals(2020, first.get("year").intValue());
    assertEquals(10.5, first.get("arithmetic_mean").doubleValue(), 0.01);
    assertEquals(45.2, first.get("first_max_value").doubleValue(), 0.01);
    assertEquals(350, first.get("observation_count").intValue());
    assertEquals(340, first.get("valid_day_count").intValue());
    assertEquals(44, first.get("aqi").intValue());

    JsonNode second = array.get(1);
    assertEquals("06059", second.get("county_fips").asText());
    assertEquals(8.3, second.get("arithmetic_mean").doubleValue(), 0.01);
  }

  @Test void testEmptyResponse() {
    assertEquals("[]", transformer.transform("", context));
    assertEquals("[]", transformer.transform(null, context));
  }

  @Test void testNoData() throws Exception {
    String response = "{"
        + "\"Header\": [{\"status\": \"Success\", \"rows\": 0}],"
        + "\"Data\": []"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertTrue(array.isArray());
    assertEquals(0, array.size());
  }

  @Test void testErrorStatus() throws Exception {
    String response = "{"
        + "\"Header\": [{\"status\": \"Failed\", \"error\": [\"Invalid parameter\"]}],"
        + "\"Data\": []"
        + "}";

    String result = transformer.transform(response, context);
    assertEquals("[]", result);
  }

  @Test void testMissingCountyCode() throws Exception {
    String response = "{"
        + "\"Header\": [{\"status\": \"Success\", \"rows\": 1}],"
        + "\"Data\": [{"
        + "  \"state_code\": \"06\","
        + "  \"parameter_code\": \"88101\","
        + "  \"parameter_name\": \"PM2.5\","
        + "  \"year\": 2020,"
        + "  \"arithmetic_mean\": 10.5,"
        + "  \"first_max_value\": 45.2,"
        + "  \"observation_count\": 350,"
        + "  \"valid_day_count\": 340"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertEquals(1, array.size());
    assertTrue(array.get(0).get("county_fips").isNull());
    assertTrue(array.get(0).get("aqi").isNull());
  }
}
