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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FredResponseTransformer}.
 */
@Tag("unit")
class FredResponseTransformerTest {

  private FredResponseTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new FredResponseTransformer();
    context = RequestContext.builder()
        .url("https://api.stlouisfed.org/fred/series/observations")
        .build();
  }

  @Test
  void testObservationsResponse() {
    String response = "{"
        + "\"realtime_start\": \"2024-01-01\","
        + "\"realtime_end\": \"2024-12-31\","
        + "\"count\": 2,"
        + "\"observations\": ["
        + "  {\"date\": \"2024-01-01\", \"value\": \"100.0\"},"
        + "  {\"date\": \"2024-02-01\", \"value\": \"101.5\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("2024-01-01"));
    assertTrue(result.contains("100.0"));
    assertTrue(result.contains("2024-02-01"));
    assertTrue(result.contains("101.5"));
  }

  @Test
  void testSeriessResponse() {
    // Note: FRED uses "seriess" (double s) not "series"
    String response = "{"
        + "\"count\": 2,"
        + "\"seriess\": ["
        + "  {\"id\": \"GDP\", \"title\": \"Gross Domestic Product\"},"
        + "  {\"id\": \"UNRATE\", \"title\": \"Unemployment Rate\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("GDP"));
    assertTrue(result.contains("Gross Domestic Product"));
    assertTrue(result.contains("UNRATE"));
  }

  @Test
  void testCategoriesResponse() {
    String response = "{"
        + "\"categories\": ["
        + "  {\"id\": 1, \"name\": \"Production & Business Activity\"},"
        + "  {\"id\": 2, \"name\": \"Prices\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("Production & Business Activity"));
    assertTrue(result.contains("Prices"));
  }

  @Test
  void testEmptyObservationsArray() {
    String response = "{"
        + "\"count\": 0,"
        + "\"observations\": []"
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test
  void testZeroCountResponse() {
    String response = "{"
        + "\"count\": 0,"
        + "\"limit\": 1000,"
        + "\"offset\": 0"
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test
  void testApiKeyError() {
    String response = "{"
        + "\"error_code\": 400,"
        + "\"error_message\": \"Bad Request. Variable api_key is not set.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("api_key"));
    assertTrue(ex.getMessage().contains("API key"));
  }

  @Test
  void testRateLimitError() {
    String response = "{"
        + "\"error_code\": 429,"
        + "\"error_message\": \"Too Many Requests. Limit exceeded.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().toLowerCase().contains("rate limit"));
  }

  @Test
  void testNotFoundError() {
    String response = "{"
        + "\"error_code\": 404,"
        + "\"error_message\": \"Not Found\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test
  void testSeriesNotFoundReturnsEmpty() {
    String response = "{"
        + "\"error_code\": 400,"
        + "\"error_message\": \"Bad Request. The series ID provided is not valid.\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test
  void testServerError() {
    String response = "{"
        + "\"error_code\": 500,"
        + "\"error_message\": \"Internal Server Error\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("500"));
    assertTrue(ex.getMessage().contains("server error"));
  }

  @Test
  void testEmptyResponse() {
    String result = transformer.transform("", context);

    assertEquals("[]", result);
  }

  @Test
  void testNullResponse() {
    String result = transformer.transform(null, context);

    assertEquals("[]", result);
  }

  @Test
  void testSingleSeriesObjectWrappedInArray() {
    // Some endpoints return a single object instead of an array
    String response = "{"
        + "\"id\": \"GDP\","
        + "\"title\": \"Gross Domestic Product\","
        + "\"observation_start\": \"1947-01-01\""
        + "}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("GDP"));
  }

  @Test
  void testContextDimensionsForDebugging() {
    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("series", "GDP");
    dimensions.put("year", "2024");

    RequestContext contextWithDimensions = RequestContext.builder()
        .url("https://api.stlouisfed.org/fred/series/observations")
        .dimensionValues(dimensions)
        .build();

    String response = "{"
        + "\"error_code\": 502,"
        + "\"error_message\": \"Bad Gateway\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, contextWithDimensions));

    assertTrue(ex.getMessage().contains("502"));
  }

  @Test
  void testReleasesResponse() {
    String response = "{"
        + "\"releases\": ["
        + "  {\"id\": 10, \"name\": \"Consumer Price Index\"},"
        + "  {\"id\": 53, \"name\": \"Gross Domestic Product\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("Consumer Price Index"));
    assertTrue(result.contains("Gross Domestic Product"));
  }

  @Test
  void testTagsResponse() {
    String response = "{"
        + "\"tags\": ["
        + "  {\"name\": \"gdp\", \"group_id\": \"freq\"},"
        + "  {\"name\": \"monthly\", \"group_id\": \"freq\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("gdp"));
    assertTrue(result.contains("monthly"));
  }

  @Test
  void testMultipleObservations() {
    String response = "{"
        + "\"count\": 12,"
        + "\"observations\": ["
        + "  {\"date\": \"2024-01-01\", \"value\": \"100\"},"
        + "  {\"date\": \"2024-02-01\", \"value\": \"101\"},"
        + "  {\"date\": \"2024-03-01\", \"value\": \"102\"},"
        + "  {\"date\": \"2024-04-01\", \"value\": \"103\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("2024-01-01"));
    assertTrue(result.contains("2024-04-01"));
  }
}
