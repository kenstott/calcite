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
package org.apache.calcite.adapter.govdata.sec;

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
 * Unit tests for {@link AlphaVantageResponseTransformer}.
 */
@Tag("unit")
class AlphaVantageResponseTransformerTest {

  private AlphaVantageResponseTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new AlphaVantageResponseTransformer();
    context = RequestContext.builder()
        .url("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL")
        .build();
  }

  @Test void testDailyTimeSeriesResponse() {
    String response = "{"
        + "\"Meta Data\": {"
        + "  \"1. Information\": \"Daily Prices (open, high, low, close) and Volumes\","
        + "  \"2. Symbol\": \"AAPL\","
        + "  \"3. Last Refreshed\": \"2024-12-16\""
        + "},"
        + "\"Time Series (Daily)\": {"
        + "  \"2024-12-16\": {"
        + "    \"1. open\": \"250.00\","
        + "    \"2. high\": \"252.00\","
        + "    \"3. low\": \"249.00\","
        + "    \"4. close\": \"251.50\","
        + "    \"5. volume\": \"45000000\""
        + "  },"
        + "  \"2024-12-13\": {"
        + "    \"1. open\": \"248.00\","
        + "    \"2. high\": \"250.00\","
        + "    \"3. low\": \"247.00\","
        + "    \"4. close\": \"249.50\","
        + "    \"5. volume\": \"38000000\""
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    // Check normalized field names
    assertTrue(result.contains("\"open\""));
    assertTrue(result.contains("\"high\""));
    assertTrue(result.contains("\"low\""));
    assertTrue(result.contains("\"close\""));
    assertTrue(result.contains("\"volume\""));
    // Check date is included
    assertTrue(result.contains("\"date\""));
    assertTrue(result.contains("2024-12-16"));
    assertTrue(result.contains("2024-12-13"));
  }

  @Test void testFieldNameNormalization() {
    String response = "{"
        + "\"Time Series (Daily)\": {"
        + "  \"2024-12-16\": {"
        + "    \"1. open\": \"100.00\","
        + "    \"2. high\": \"105.00\""
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    // Should normalize "1. open" to "open"
    assertTrue(result.contains("\"open\""));
    assertTrue(result.contains("\"high\""));
    // Should NOT contain numbered prefixes
    assertTrue(!result.contains("1. open"));
    assertTrue(!result.contains("2. high"));
  }

  @Test void testErrorMessageResponse() {
    String response = "{"
        + "\"Error Message\": \"Invalid API call. Please retry or visit the documentation.\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testRateLimitNote() {
    String response = "{"
        + "\"Note\": \"Thank you for using Alpha Vantage! Our standard API call frequency "
        + "is 5 calls per minute and 500 calls per day.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().toLowerCase().contains("rate limit"));
  }

  @Test void testDemoKeyInformation() {
    String response = "{"
        + "\"Information\": \"The **demo** API key is for demo purposes only. "
        + "Please claim your free API key at https://www.alphavantage.co/support/#api-key\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("demo key"));
  }

  @Test void testDailyLimitExceeded() {
    String response = "{"
        + "\"Note\": \"You have exceeded the 25 requests per day limit for free API keys.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().toLowerCase().contains("rate limit"));
  }

  @Test void testGlobalQuoteResponse() {
    String response = "{"
        + "\"Global Quote\": {"
        + "  \"01. symbol\": \"AAPL\","
        + "  \"02. open\": \"250.00\","
        + "  \"03. high\": \"252.00\","
        + "  \"04. low\": \"249.00\","
        + "  \"05. price\": \"251.50\","
        + "  \"06. volume\": \"45000000\""
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("AAPL"));
    assertTrue(result.contains("250.00"));
  }

  @Test void testSearchResultsResponse() {
    String response = "{"
        + "\"bestMatches\": ["
        + "  {\"1. symbol\": \"AAPL\", \"2. name\": \"Apple Inc.\", \"3. type\": \"Equity\"},"
        + "  {\"1. symbol\": \"APLE\", \"2. name\": \"Apple Hospitality REIT\", \"3. type\": \"Equity\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("AAPL"));
    assertTrue(result.contains("Apple Inc."));
  }

  @Test void testWeeklyTimeSeriesResponse() {
    String response = "{"
        + "\"Meta Data\": {\"2. Symbol\": \"AAPL\"},"
        + "\"Time Series (Weekly)\": {"
        + "  \"2024-12-13\": {"
        + "    \"1. open\": \"248.00\","
        + "    \"4. close\": \"251.50\""
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("2024-12-13"));
    assertTrue(result.contains("248.00"));
  }

  @Test void testInvalidSymbolReturnsEmpty() {
    String response = "{"
        + "\"Error Message\": \"Invalid symbol XXYZZ123.\""
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

  @Test void testNoRecognizedDataStructure() {
    String response = "{"
        + "\"Meta Data\": {\"Symbol\": \"AAPL\"},"
        + "\"unknownField\": {}"
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testContextDimensionsForDebugging() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("symbol", "AAPL");
    dimensions.put("function", "TIME_SERIES_DAILY");

    RequestContext contextWithDimensions = RequestContext.builder()
        .url("https://www.alphavantage.co/query")
        .dimensionValues(dimensions)
        .build();

    String response = "{"
        + "\"Note\": \"API call frequency limit reached.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, contextWithDimensions));

    assertTrue(ex.getMessage().toLowerCase().contains("rate limit"));
  }

  @Test void testIntradayTimeSeriesResponse() {
    String response = "{"
        + "\"Meta Data\": {\"2. Symbol\": \"AAPL\"},"
        + "\"Time Series (5min)\": {"
        + "  \"2024-12-16 16:00:00\": {"
        + "    \"1. open\": \"251.00\","
        + "    \"4. close\": \"251.50\""
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("2024-12-16 16:00:00"));
    assertTrue(result.contains("251.00"));
  }
}
