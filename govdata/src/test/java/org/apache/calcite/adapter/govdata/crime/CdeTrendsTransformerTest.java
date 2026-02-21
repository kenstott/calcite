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

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CdeTrendsTransformer}.
 */
@Tag("unit")
class CdeTrendsTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CdeTrendsTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new CdeTrendsTransformer();
    context = RequestContext.builder()
        .url("https://cde.ucr.cjis.gov/LATEST/trends/national")
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();
  }

  @Test void testTypicalResponse() throws Exception {
    String response = "{"
        + "\"crime_trends\": {"
        + "  \"cde_crime_trends\": {"
        + "    \"trends\": {"
        + "      \"Murder\": \"-18.4\","
        + "      \"Robbery\": \"-18.0\","
        + "      \"Violent Crime\": \"-9.9\""
        + "    },"
        + "    \"current_range\": \"November 2024 - October 2025\","
        + "    \"last_refresh_date\": \"February 15, 2026\""
        + "  }"
        + "}"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(3, array.size());

    // Check first row
    JsonNode row = array.get(0);
    assertEquals("Murder", row.get("offense_name").asText());
    assertEquals(-18.4, row.get("trend_pct").asDouble(), 0.01);
    assertEquals("November 2024 - October 2025", row.get("current_range").asText());
    assertEquals("February 15, 2026", row.get("last_refresh_date").asText());
  }

  @Test void testEmptyResponse() {
    assertEquals("[]", transformer.transform("", context));
    assertEquals("[]", transformer.transform(null, context));
  }

  @Test void testMissingTrendsSection() throws Exception {
    String response = "{\"crime_trends\": {}}";
    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertTrue(array.isArray());
    assertEquals(0, array.size());
  }

  @Test void testPositiveTrend() throws Exception {
    String response = "{"
        + "\"crime_trends\": {"
        + "  \"cde_crime_trends\": {"
        + "    \"trends\": {"
        + "      \"Motor Vehicle Theft\": \"12.3\""
        + "    },"
        + "    \"current_range\": \"Jan 2025 - Dec 2025\","
        + "    \"last_refresh_date\": \"January 1, 2026\""
        + "  }"
        + "}"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);
    assertEquals(1, array.size());
    assertEquals(12.3, array.get(0).get("trend_pct").asDouble(), 0.01);
  }
}
