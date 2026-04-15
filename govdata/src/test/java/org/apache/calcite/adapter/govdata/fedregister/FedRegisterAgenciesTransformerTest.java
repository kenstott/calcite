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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FedRegisterAgenciesTransformer}.
 */
@Tag("unit")
class FedRegisterAgenciesTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private FedRegisterAgenciesTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new FedRegisterAgenciesTransformer();
    context = RequestContext.builder()
        .url("https://api.federalregister.gov/v1/agencies.json")
        .dimensionValues(Collections.emptyMap())
        .build();
  }

  @Test void testBasicTransformation() throws Exception {
    String response = "["
        + "{"
        + "  \"id\": 199,"
        + "  \"name\": \"Environmental Protection Agency\","
        + "  \"short_name\": \"EPA\","
        + "  \"slug\": \"environmental-protection-agency\","
        + "  \"url\": \"https://www.federalregister.gov/agencies/environmental-protection-agency\","
        + "  \"parent_id\": null"
        + "},"
        + "{"
        + "  \"id\": 252,"
        + "  \"name\": \"Office of Air and Radiation\","
        + "  \"short_name\": \"OAR\","
        + "  \"slug\": \"office-of-air-and-radiation\","
        + "  \"url\": \"https://www.federalregister.gov/agencies/office-of-air-and-radiation\","
        + "  \"parent_id\": 199"
        + "}"
        + "]";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(2, array.size());

    JsonNode epa = array.get(0);
    assertEquals(199, epa.get("id").intValue());
    assertEquals("Environmental Protection Agency", epa.get("name").asText());
    assertEquals("EPA", epa.get("short_name").asText());
    assertEquals("environmental-protection-agency", epa.get("slug").asText());
    assertTrue(epa.get("parent_id").isNull());

    JsonNode oar = array.get(1);
    assertEquals(252, oar.get("id").intValue());
    assertEquals(199, oar.get("parent_id").intValue());
  }

  @Test void testEmptyResponse() {
    assertEquals("[]", transformer.transform("", context));
    assertEquals("[]", transformer.transform(null, context));
  }

  @Test void testNotArrayResponse() {
    assertEquals("[]", transformer.transform("{\"key\": \"value\"}", context));
  }

  @Test void testMissingOptionalFields() throws Exception {
    String response = "[{"
        + "  \"id\": 100,"
        + "  \"name\": \"Test Agency\","
        + "  \"slug\": \"test-agency\""
        + "}]";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertEquals(1, array.size());
    JsonNode agency = array.get(0);
    assertEquals(100, agency.get("id").intValue());
    assertEquals("Test Agency", agency.get("name").asText());
    assertEquals("test-agency", agency.get("slug").asText());
    assertTrue(agency.get("short_name").isNull());
    assertTrue(agency.get("url").isNull());
    assertTrue(agency.get("parent_id").isNull());
  }

  @Test void testNonNumericIdIgnored() throws Exception {
    // id is not a number — should still produce a row with null id
    String response = "[{"
        + "  \"id\": \"not-a-number\","
        + "  \"name\": \"Test Agency\","
        + "  \"slug\": \"test-agency\""
        + "}]";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertNotNull(array);
    assertTrue(array.isArray());
    assertEquals(1, array.size());
    assertTrue(array.get(0).get("id").isNull());
  }

  @Test void testEmptyNameExcludedFromText() throws Exception {
    // Empty string name should be normalized to null
    String response = "[{"
        + "  \"id\": 1,"
        + "  \"name\": \"\","
        + "  \"slug\": \"test\""
        + "}]";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertEquals(1, array.size());
    assertTrue(array.get(0).get("name").isNull());
  }
}
