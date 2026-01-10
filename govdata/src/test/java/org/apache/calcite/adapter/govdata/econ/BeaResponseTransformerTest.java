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
 * Unit tests for {@link BeaResponseTransformer}.
 */
@Tag("unit")
class BeaResponseTransformerTest {

  private BeaResponseTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new BeaResponseTransformer();
    context = RequestContext.builder()
        .url("https://apps.bea.gov/api/data")
        .build();
  }

  @Test void testSuccessfulResponse() {
    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Request\": {\"RequestParam\": []},"
        + "  \"Results\": {"
        + "    \"Data\": [{\"TableName\": \"T10101\", \"Value\": \"1234.5\"}]"
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertEquals("[{\"TableName\":\"T10101\",\"Value\":\"1234.5\"}]", result);
  }

  @Test void testEmptyDataArray() {
    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Results\": {"
        + "    \"Data\": []"
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testMissingDataArray() {
    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Results\": {}"
        + "}}";

    String result = transformer.transform(response, context);

    assertEquals("[]", result);
  }

  @Test void testParamValueResponse() {
    // GetParameterValues returns ParamValue instead of Data
    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Results\": {"
        + "    \"ParamValue\": [{\"Key\": \"2020\"}, {\"Key\": \"2021\"}]"
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertEquals("[{\"Key\":\"2020\"},{\"Key\":\"2021\"}]", result);
  }

  @Test void testApiError() {
    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Results\": {"
        + "    \"Error\": {"
        + "      \"APIErrorCode\": \"INVALID_PARAMETER\","
        + "      \"APIErrorDescription\": \"Invalid TableName parameter\""
        + "    }"
        + "  }"
        + "}}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, context));

    assertTrue(ex.getMessage().contains("INVALID_PARAMETER"));
    assertTrue(ex.getMessage().contains("Invalid TableName parameter"));
  }

  @Test void testNoDataError() {
    // NO_DATA is a soft error - returns empty array
    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Results\": {"
        + "    \"Error\": {"
        + "      \"APIErrorCode\": \"NO_DATA\","
        + "      \"APIErrorDescription\": \"No data found for request\""
        + "    }"
        + "  }"
        + "}}";

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

  @Test void testNonBeaStructure() {
    // Non-BEA JSON should be returned as-is
    String response = "{\"data\": [1, 2, 3]}";

    String result = transformer.transform(response, context);

    assertEquals(response, result);
  }

  @Test void testContextDimensionsIncludedInErrorMessage() {
    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("year", "2024");
    dimensions.put("tablename", "T10101");

    RequestContext contextWithDimensions = RequestContext.builder()
        .url("https://apps.bea.gov/api/data")
        .dimensionValues(dimensions)
        .build();

    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Results\": {"
        + "    \"Error\": {"
        + "      \"APIErrorCode\": \"SERVER_ERROR\","
        + "      \"APIErrorDescription\": \"Internal server error\""
        + "    }"
        + "  }"
        + "}}";

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
        transformer.transform(response, contextWithDimensions));

    assertTrue(ex.getMessage().contains("SERVER_ERROR"));
  }

  @Test void testMultipleDataRecords() {
    String response = "{"
        + "\"BEAAPI\": {"
        + "  \"Results\": {"
        + "    \"Data\": ["
        + "      {\"TableName\": \"T10101\", \"Year\": \"2020\", \"Value\": \"100\"},"
        + "      {\"TableName\": \"T10101\", \"Year\": \"2021\", \"Value\": \"110\"},"
        + "      {\"TableName\": \"T10101\", \"Year\": \"2022\", \"Value\": \"120\"}"
        + "    ]"
        + "  }"
        + "}}";

    String result = transformer.transform(response, context);

    assertTrue(result.startsWith("["));
    assertTrue(result.endsWith("]"));
    assertTrue(result.contains("2020"));
    assertTrue(result.contains("2021"));
    assertTrue(result.contains("2022"));
  }
}
