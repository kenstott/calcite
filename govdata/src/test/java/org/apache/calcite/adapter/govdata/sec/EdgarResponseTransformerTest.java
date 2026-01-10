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
 * Unit tests for {@link EdgarResponseTransformer}.
 */
@Tag("unit")
class EdgarResponseTransformerTest {

  private EdgarResponseTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new EdgarResponseTransformer();
    context = RequestContext.builder()
        .url("https://data.sec.gov/submissions/CIK0000320193.json")
        .build();
  }

  @Test void testSuccessfulSubmissionsResponse() {
    String response = "{"
        + "\"cik\": \"0000320193\","
        + "\"entityType\": \"operating\","
        + "\"sic\": \"3571\","
        + "\"sicDescription\": \"Electronic Computers\","
        + "\"name\": \"Apple Inc.\","
        + "\"tickers\": [\"AAPL\"],"
        + "\"exchanges\": [\"Nasdaq\"],"
        + "\"filings\": {"
        + "  \"recent\": {"
        + "    \"accessionNumber\": [\"0000320193-24-000001\", \"0000320193-24-000002\"],"
        + "    \"filingDate\": [\"2024-01-15\", \"2024-03-01\"],"
        + "    \"form\": [\"10-K\", \"10-Q\"],"
        + "    \"primaryDocument\": [\"aapl-20231230.htm\", \"aapl-20231231.htm\"]"
        + "  },"
        + "  \"files\": []"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("10-K"));
    assertTrue(result.contains("10-Q"));
    assertTrue(result.contains("2024-01-15"));
    assertTrue(result.contains("0000320193-24-000001"));
  }

  @Test void testEmptyFilingsArray() {
    String response = "{"
        + "\"cik\": \"0000320193\","
        + "\"name\": \"Apple Inc.\","
        + "\"filings\": {"
        + "  \"recent\": {"
        + "    \"form\": []"
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testMissingFilingsNode() {
    String response = "{"
        + "\"cik\": \"0000320193\","
        + "\"name\": \"Apple Inc.\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testMissingRecentFilings() {
    String response = "{"
        + "\"cik\": \"0000320193\","
        + "\"name\": \"Apple Inc.\","
        + "\"filings\": {"
        + "  \"files\": []"
        + "}}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testRateLimitError() {
    String response = "{"
        + "\"error\": \"Rate limit exceeded. SEC allows 10 requests per second.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().toLowerCase().contains("rate limit"));
  }

  @Test void testNotFoundError() {
    String response = "{"
        + "\"message\": \"CIK not found in SEC database.\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testServerError() {
    String response = "{"
        + "\"error\": \"Internal server error occurred.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("server error"));
  }

  @Test void testEmptyResponse() {
    String result = transformer.transform("", context);

    assertEquals("[]", result);
  }

  @Test void testNullResponse() {
    String result = transformer.transform(null, context);

    assertEquals("[]", result);
  }

  @Test void testNonEdgarStructurePassthrough() {
    // Non-EDGAR JSON should be returned as-is
    String response = "{\"data\": [1, 2, 3]}";

    String result = transformer.transform(response, context);

    assertEquals(response, result);
  }

  @Test void testContextDimensionsIncludedInError() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("cik", "0000320193");
    dimensions.put("ticker", "AAPL");

    RequestContext contextWithDimensions = RequestContext.builder()
        .url("https://data.sec.gov/submissions/CIK0000320193.json")
        .dimensionValues(dimensions)
        .build();

    String response = "{"
        + "\"error\": \"Internal server error occurred.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, contextWithDimensions));

    assertTrue(ex.getMessage().contains("server error"));
  }

  @Test void testMultipleFilingTypes() {
    String response = "{"
        + "\"cik\": \"0000320193\","
        + "\"name\": \"Apple Inc.\","
        + "\"filings\": {"
        + "  \"recent\": {"
        + "    \"form\": [\"10-K\", \"10-Q\", \"8-K\", \"4\", \"SC 13G\"],"
        + "    \"filingDate\": [\"2024-01-15\", \"2024-03-01\", \"2024-02-15\", \"2024-02-01\", \"2024-01-20\"],"
        + "    \"accessionNumber\": [\"A1\", \"A2\", \"A3\", \"A4\", \"A5\"],"
        + "    \"primaryDocument\": [\"d1.htm\", \"d2.htm\", \"d3.htm\", \"d4.htm\", \"d5.htm\"]"
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("10-K"));
    assertTrue(result.contains("10-Q"));
    assertTrue(result.contains("8-K"));
    assertTrue(result.contains("SC 13G"));
  }

  @Test void testGenericError() {
    String response = "{"
        + "\"message\": \"Some unexpected API error message\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("unexpected API error"));
  }
}
