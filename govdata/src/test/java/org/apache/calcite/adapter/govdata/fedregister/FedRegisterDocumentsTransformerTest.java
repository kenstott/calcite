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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FedRegisterDocumentsTransformer}.
 */
@Tag("unit")
class FedRegisterDocumentsTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private FedRegisterDocumentsTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new FedRegisterDocumentsTransformer();
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("doc_type", "RULE");
    dims.put("year", "2023");
    context = RequestContext.builder()
        .url("https://api.federalregister.gov/v1/documents.json?conditions[type][]=RULE")
        .dimensionValues(dims)
        .build();
  }

  @Test void testEmptyResponse() {
    assertEquals("[]", transformer.transform("", context));
    assertEquals("[]", transformer.transform(null, context));
  }

  @Test void testSinglePageNoNextPage() throws Exception {
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-07234\","
        + "  \"title\": \"Clean Air Act Standards\","
        + "  \"type\": \"RULE\","
        + "  \"abstract\": \"Final rule under CAA section 111.\","
        + "  \"publication_date\": \"2023-04-10\","
        + "  \"effective_on\": \"2023-06-09\","
        + "  \"action\": \"Final rule\","
        + "  \"agencies\": [{"
        + "    \"name\": \"Environmental Protection Agency\","
        + "    \"slug\": \"environmental-protection-agency\""
        + "  }],"
        + "  \"cfr_references\": [{\"title\": 40, \"part\": 60}],"
        + "  \"regulation_id_numbers\": [\"2060-AT46\"],"
        + "  \"significant\": true,"
        + "  \"docket_ids\": [\"EPA-HQ-OAR-2021-0208\"],"
        + "  \"president\": null,"
        + "  \"signing_date\": null,"
        + "  \"full_text_xml_url\": \"https://www.govinfo.gov/content/pkg/FR-2023-04-10/xml/2023-07234.xml\","
        + "  \"body_html_url\": \"https://www.federalregister.gov/documents/2023/04/10/2023-07234/clean-air-act-standards\""
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.isArray());
    assertEquals(1, array.size());

    JsonNode doc = array.get(0);
    assertEquals("2023-07234", doc.get("document_number").asText());
    assertEquals("Clean Air Act Standards", doc.get("title").asText());
    assertEquals("RULE", doc.get("doc_type").asText());
    assertEquals("Final rule under CAA section 111.", doc.get("abstract").asText());
    assertEquals("2023-04-10", doc.get("publication_date").asText());
    assertEquals("2023-06-09", doc.get("effective_on").asText());
    assertEquals("Final rule", doc.get("action").asText());
    assertEquals("Environmental Protection Agency", doc.get("agency_names").asText());
    assertEquals("environmental-protection-agency", doc.get("agency_slugs").asText());
    assertEquals("[{\"title\":40,\"part\":60}]", doc.get("cfr_references").asText());
    assertEquals("2060-AT46", doc.get("rin").asText());
    assertTrue(doc.get("significant").booleanValue());
    assertEquals("[\"EPA-HQ-OAR-2021-0208\"]", doc.get("docket_ids").asText());
    assertTrue(doc.get("president").isNull());
    assertTrue(doc.get("signing_date").isNull());
    assertNotNull(doc.get("full_text_xml_url").asText());
    assertNotNull(doc.get("body_html_url").asText());
  }

  @Test void testMultipleAgencies() throws Exception {
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-00001\","
        + "  \"agencies\": ["
        + "    {\"name\": \"EPA\", \"slug\": \"epa\"},"
        + "    {\"name\": \"Army Corps of Engineers\", \"slug\": \"army-corps-of-engineers\"}"
        + "  ]"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    JsonNode doc = array.get(0);
    assertEquals("EPA, Army Corps of Engineers", doc.get("agency_names").asText());
    assertEquals("epa,army-corps-of-engineers", doc.get("agency_slugs").asText());
  }

  @Test void testNoAgencies() throws Exception {
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-00001\","
        + "  \"agencies\": []"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    JsonNode doc = array.get(0);
    assertTrue(doc.get("agency_names").isNull());
    assertTrue(doc.get("agency_slugs").isNull());
  }

  @Test void testRinFirstEntryOnly() throws Exception {
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-00001\","
        + "  \"regulation_id_numbers\": [\"2060-AT46\", \"2060-AT47\"]"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertEquals("2060-AT46", array.get(0).get("rin").asText());
  }

  @Test void testRinEmptyArray() throws Exception {
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-00001\","
        + "  \"regulation_id_numbers\": []"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.get(0).get("rin").isNull());
  }

  @Test void testSignificantNullWhenAbsent() throws Exception {
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-00001\""
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.get(0).get("significant").isNull());
  }

  @Test void testSignificantFalse() throws Exception {
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-00001\","
        + "  \"significant\": false"
        + "}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertTrue(array.get(0).has("significant"));
    assertTrue(!array.get(0).get("significant").isNull());
    assertEquals(false, array.get(0).get("significant").booleanValue());
  }

  @Test void testPresidentialDocFields() throws Exception {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("doc_type", "PRESDOC");
    dims.put("year", "2023");
    RequestContext presCtx = RequestContext.builder()
        .url("https://api.federalregister.gov/v1/documents.json?conditions[type][]=PRESDOC")
        .dimensionValues(dims)
        .build();

    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{"
        + "  \"document_number\": \"2023-00100\","
        + "  \"president\": {\"name\": \"Joseph R. Biden Jr.\"},"
        + "  \"signing_date\": \"2023-01-15\""
        + "}]"
        + "}";

    String result = transformer.transform(response, presCtx);
    JsonNode array = MAPPER.readTree(result);

    assertEquals("PRESDOC", array.get(0).get("doc_type").asText());
    // president field is an object — asText() returns the nested representation
    assertNotNull(array.get(0).get("president"));
    assertEquals("2023-01-15", array.get(0).get("signing_date").asText());
  }

  @Test void testPaginationStopsOnNullNextPage() throws Exception {
    // When next_page_url is absent, transformer should return only first page results
    // (HTTP calls for pagination cannot succeed in unit tests, but next_page_url=null stops it)
    String response = "{"
        + "\"count\": 2,"
        + "\"total_pages\": 1,"
        + "\"results\": ["
        + "  {\"document_number\": \"2023-00001\"},"
        + "  {\"document_number\": \"2023-00002\"}"
        + "]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertEquals(2, array.size());
    assertEquals("2023-00001", array.get(0).get("document_number").asText());
    assertEquals("2023-00002", array.get(1).get("document_number").asText());
  }

  @Test void testMalformedJsonReturnsEmpty() {
    String result = transformer.transform("not valid json {{{", context);
    assertEquals("[]", result);
  }

  @Test void testDocTypeFromContextNotResponse() throws Exception {
    // doc_type should come from context.dimensionValues, not the response "type" field
    String response = "{"
        + "\"count\": 1,"
        + "\"total_pages\": 1,"
        + "\"results\": [{\"document_number\": \"2023-00001\", \"type\": \"IGNORED\"}]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode array = MAPPER.readTree(result);

    assertEquals("RULE", array.get(0).get("doc_type").asText());
  }
}
