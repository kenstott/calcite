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
package org.apache.calcite.adapter.govdata.geo;

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
 * Unit tests for {@link CensusResponseTransformer}.
 */
@Tag("unit")
class CensusResponseTransformerTest {

  private CensusResponseTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new CensusResponseTransformer();
    context = RequestContext.builder()
        .url("https://api.census.gov/data/2020/acs/acs5")
        .build();
  }

  @Test
  void testTableFormatResponse() {
    // Census Data API returns 2D array with headers in first row
    String response = "["
        + "[\"NAME\", \"B01001_001E\", \"state\", \"county\"],"
        + "[\"Los Angeles County, California\", \"10014009\", \"06\", \"037\"],"
        + "[\"Cook County, Illinois\", \"5150233\", \"17\", \"031\"]"
        + "]";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    // Check that headers became field names
    assertTrue(result.contains("\"NAME\""));
    assertTrue(result.contains("\"B01001_001E\""));
    assertTrue(result.contains("\"state\""));
    assertTrue(result.contains("\"county\""));
    // Check values
    assertTrue(result.contains("Los Angeles County, California"));
    assertTrue(result.contains("10014009"));
    assertTrue(result.contains("06"));
  }

  @Test
  void testTableFormatMultipleRows() {
    String response = "["
        + "[\"NAME\", \"POP\", \"state\"],"
        + "[\"California\", \"39538223\", \"06\"],"
        + "[\"Texas\", \"29145505\", \"48\"],"
        + "[\"Florida\", \"21538187\", \"12\"],"
        + "[\"New York\", \"20201249\", \"36\"]"
        + "]";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("California"));
    assertTrue(result.contains("Texas"));
    assertTrue(result.contains("Florida"));
    assertTrue(result.contains("New York"));
    assertTrue(result.contains("39538223"));
  }

  @Test
  void testTableFormatHeadersOnly() {
    // Only headers, no data rows
    String response = "["
        + "[\"NAME\", \"POP\", \"state\"]"
        + "]";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test
  void testErrorObjectResponse() {
    String response = "{"
        + "\"error\": \"Your request did not return any results. Check your variables and geographies.\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test
  void testArrayErrorResponse() {
    // Census sometimes returns errors as an array with a single string
    String response = "[\"error: unknown variable 'INVALID_VAR'\"]";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("unknown variable"));
  }

  @Test
  void testUnknownVariableError() {
    String response = "{"
        + "\"error\": \"error: unknown variable 'B99999_001E'\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("unknown variable"));
  }

  @Test
  void testInvalidGeographyReturnsEmpty() {
    String response = "{"
        + "\"error\": \"Invalid geography XYZZY requested\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test
  void testRateLimitError() {
    String response = "{"
        + "\"error\": \"Rate limit exceeded. Please slow down your requests.\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().toLowerCase().contains("rate limit"));
  }

  @Test
  void testGeographyApiResponse() {
    String response = "{"
        + "\"geos\": ["
        + "  {\"name\": \"California\", \"geoId\": \"0400000US06\", \"state\": \"06\"},"
        + "  {\"name\": \"Texas\", \"geoId\": \"0400000US48\", \"state\": \"48\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("California"));
    assertTrue(result.contains("0400000US06"));
    assertTrue(result.contains("Texas"));
  }

  @Test
  void testVariablesApiResponse() {
    String response = "{"
        + "\"variables\": ["
        + "  {\"name\": \"B01001_001E\", \"label\": \"Total Population\"},"
        + "  {\"name\": \"B01001_002E\", \"label\": \"Male Population\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("B01001_001E"));
    assertTrue(result.contains("Total Population"));
    assertTrue(result.contains("Male Population"));
  }

  @Test
  void testSingleRecordWrappedInArray() {
    String response = "{"
        + "\"geoId\": \"0400000US06\","
        + "\"name\": \"California\","
        + "\"fips\": \"06\""
        + "}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("California"));
    assertTrue(result.contains("0400000US06"));
  }

  @Test
  void testEmptyArrayResponse() {
    String response = "[]";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
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
  void testArrayOfObjects() {
    // Already in the expected format - should pass through unchanged
    String response = "["
        + "{\"NAME\": \"California\", \"POP\": \"39538223\"},"
        + "{\"NAME\": \"Texas\", \"POP\": \"29145505\"}"
        + "]";

    String result = transformer.transform(response, context);

    // Just verify it contains the data (JSON formatting may vary)
    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("California"));
    assertTrue(result.contains("Texas"));
    assertTrue(result.contains("39538223"));
  }

  @Test
  void testContextDimensionsIncludedInError() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2020");
    dimensions.put("for", "state:06");

    RequestContext contextWithDimensions = RequestContext.builder()
        .url("https://api.census.gov/data/2020/acs/acs5")
        .dimensionValues(dimensions)
        .build();

    String response = "{"
        + "\"error\": \"Unknown error occurred\""
        + "}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, contextWithDimensions));

    assertTrue(ex.getMessage().contains("Census API error"));
  }

  @Test
  void testDataFieldWrapper() {
    String response = "{"
        + "\"data\": ["
        + "  {\"id\": 1, \"value\": \"test1\"},"
        + "  {\"id\": 2, \"value\": \"test2\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("test1"));
    assertTrue(result.contains("test2"));
  }

  @Test
  void testResultsFieldWrapper() {
    String response = "{"
        + "\"results\": ["
        + "  {\"name\": \"California\"},"
        + "  {\"name\": \"Texas\"}"
        + "]}";

    String result = transformer.transform(response, context);

    assertTrue(result.contains("California"));
    assertTrue(result.contains("Texas"));
  }

  @Test
  void testNoDataErrorMessage() {
    String response = "{"
        + "\"error\": \"No data found for the specified query parameters.\""
        + "}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }
}
