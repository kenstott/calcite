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
 * Unit tests for {@link BlsResponseTransformer}.
 */
@Tag("unit")
class BlsResponseTransformerTest {

  private BlsResponseTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new BlsResponseTransformer();
    context = RequestContext.builder()
        .url("https://api.bls.gov/publicAPI/v2/timeseries/data/")
        .build();
  }

  @Test void testSuccessfulResponse() {
    String response = "{"
        + "\"status\": \"REQUEST_SUCCEEDED\","
        + "\"responseTime\": 150,"
        + "\"message\": [],"
        + "\"Results\": {"
        + "  \"series\": [{"
        + "    \"seriesID\": \"LAUCN040010000000005\","
        + "    \"data\": [{\"year\": \"2024\", \"value\": \"3.5\"}]"
        + "  }]"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("LAUCN040010000000005"));
    assertTrue(result.contains("2024"));
    assertTrue(result.contains("3.5"));
  }

  @Test void testEmptySeriesArray() {
    String response = "{"
        + "\"status\": \"REQUEST_SUCCEEDED\","
        + "\"Results\": {"
        + "  \"series\": []"
        + "}}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testMissingSeriesArray() {
    String response = "{"
        + "\"status\": \"REQUEST_SUCCEEDED\","
        + "\"Results\": {}"
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testRequestFailed() {
    // Use a generic error that doesn't trigger special handling
    String response = "{"
        + "\"status\": \"REQUEST_FAILED\","
        + "\"responseTime\": 50,"
        + "\"message\": [\"Database connection error\"],"
        + "\"Results\": {}"
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("Database connection error"));
  }

  @Test void testRateLimitError() {
    String response = "{"
        + "\"status\": \"REQUEST_FAILED\","
        + "\"message\": [\"Rate limit exceeded. Please try again later.\"],"
        + "\"Results\": {}"
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().toLowerCase().contains("rate limit"));
  }

  @Test void testInvalidSeriesReturnsEmpty() {
    String response = "{"
        + "\"status\": \"REQUEST_FAILED\","
        + "\"message\": [\"Invalid series ID: INVALID123\"],"
        + "\"Results\": {}"
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testNoDataAvailableReturnsEmpty() {
    String response = "{"
        + "\"status\": \"REQUEST_FAILED\","
        + "\"message\": [\"No data available for the requested time period\"],"
        + "\"Results\": {}"
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testEmptyResponse() {
    String result = transformer.transform("", context);

    assertEquals("[]", result);
  }

  @Test void testNullResponse() {
    String result = transformer.transform(null, context);

    assertEquals("[]", result);
  }

  @Test void testUnknownStatusStillExtractsData() {
    // Unknown status should still try to extract data
    String response = "{"
        + "\"status\": \"UNKNOWN_STATUS\","
        + "\"Results\": {"
        + "  \"series\": [{\"seriesID\": \"TEST\"}]"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("TEST"));
  }

  @Test void testMultipleSeriesRecords() {
    String response = "{"
        + "\"status\": \"REQUEST_SUCCEEDED\","
        + "\"Results\": {"
        + "  \"series\": ["
        + "    {\"seriesID\": \"SERIES1\", \"data\": []},"
        + "    {\"seriesID\": \"SERIES2\", \"data\": []},"
        + "    {\"seriesID\": \"SERIES3\", \"data\": []}"
        + "  ]"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("SERIES1"));
    assertTrue(result.contains("SERIES2"));
    assertTrue(result.contains("SERIES3"));
  }

  @Test void testMultipleErrorMessages() {
    String response = "{"
        + "\"status\": \"REQUEST_FAILED\","
        + "\"message\": [\"Error 1\", \"Error 2\", \"Error 3\"],"
        + "\"Results\": {}"
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    // Should concatenate messages
    assertTrue(ex.getMessage().contains("Error 1"));
    assertTrue(ex.getMessage().contains("Error 2"));
    assertTrue(ex.getMessage().contains("Error 3"));
  }

  @Test void testContextDimensionsForDebugging() {
    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("series", "LAUCN040010000000005");
    dimensions.put("year", "2024");

    RequestContext contextWithDimensions = RequestContext.builder()
        .url("https://api.bls.gov/publicAPI/v2/timeseries/data/")
        .dimensionValues(dimensions)
        .build();

    String response = "{"
        + "\"status\": \"REQUEST_FAILED\","
        + "\"message\": [\"Server error\"],"
        + "\"Results\": {}"
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, contextWithDimensions));

    assertTrue(ex.getMessage().contains("Server error"));
  }
}
