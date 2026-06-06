/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CisaAdvisoryResponseTransformer}.
 */
@Tag("unit")
class CisaAdvisoryResponseTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CisaAdvisoryResponseTransformer transformer;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    transformer = new CisaAdvisoryResponseTransformer();
    context = RequestContext.builder()
        .url("https://raw.githubusercontent.com/cisagov/CSAF/develop/"
            + "csaf_files/IT/white/2026/va-26-155-01.json")
        .build();
  }

  @Test void testFullAdvisory() throws Exception {
    String response = "{"
        + "\"document\": {"
        + "  \"title\": \"Acme Widget Critical Flaw\","
        + "  \"tracking\": {"
        + "    \"id\": \"VA-26-155-01\","
        + "    \"current_release_date\": \"2026-06-04T15:31:57Z\""
        + "  },"
        + "  \"aggregate_severity\": { \"text\": \"Critical\" }"
        + "},"
        + "\"vulnerabilities\": ["
        + "  { \"cve\": \"CVE-2024-1234\","
        + "    \"scores\": [ { \"cvss_v3\": { \"baseScore\": 9.8, \"baseSeverity\": \"CRITICAL\" } } ] },"
        + "  { \"cve\": \"CVE-2024-5678\","
        + "    \"scores\": [ { \"cvss_v31\": { \"baseScore\": 7.5, \"baseSeverity\": \"HIGH\" } } ] }"
        + "],"
        + "\"product_tree\": { \"branches\": ["
        + "  { \"name\": \"Acme\", \"branches\": ["
        + "    { \"name\": \"Widget Server\" },"
        + "    { \"name\": \"Widget Client\" }"
        + "  ] }"
        + "] }"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode rows = MAPPER.readTree(result);

    assertTrue(rows.isArray(), "result must be a JSON array");
    assertEquals(1, rows.size(), "exactly one row expected");

    JsonNode row = rows.get(0);
    assertEquals("VA-26-155-01", row.get("advisory_id").asText());
    assertEquals("cisa", row.get("source").asText());
    assertEquals("Acme Widget Critical Flaw", row.get("title").asText());
    assertEquals("2026-06-04", row.get("published_date").asText());
    assertEquals("CRITICAL", row.get("severity").asText());
    assertEquals("CVE-2024-1234|CVE-2024-5678", row.get("cve_ids").asText());

    assertNotNull(row.get("affected_systems"));
    assertTrue(!row.get("affected_systems").isNull(), "affected_systems must be non-null");
    String affected = row.get("affected_systems").asText();
    assertTrue(affected.contains("Acme"), "affected_systems should include branch names");
    assertTrue(affected.length() <= 200, "affected_systems truncated to 200 chars");
  }

  @Test void testSeverityFromScoresWhenNoAggregate() throws Exception {
    // No aggregate_severity -> max baseSeverity across scores (HIGH > MEDIUM)
    String response = "{"
        + "\"document\": {"
        + "  \"title\": \"No Aggregate\","
        + "  \"tracking\": {"
        + "    \"id\": \"VA-26-100-01\","
        + "    \"current_release_date\": \"2026-01-15T00:00:00Z\""
        + "  }"
        + "},"
        + "\"vulnerabilities\": ["
        + "  { \"cve\": \"CVE-2025-0001\","
        + "    \"scores\": [ { \"cvss_v3\": { \"baseSeverity\": \"MEDIUM\" } } ] },"
        + "  { \"cve\": \"CVE-2025-0002\","
        + "    \"scores\": [ { \"cvss_v31\": { \"baseSeverity\": \"HIGH\" } } ] }"
        + "]"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode row = MAPPER.readTree(result).get(0);

    assertEquals("VA-26-100-01", row.get("advisory_id").asText());
    assertEquals("2026-01-15", row.get("published_date").asText());
    assertEquals("HIGH", row.get("severity").asText());
    assertEquals("CVE-2025-0001|CVE-2025-0002", row.get("cve_ids").asText());
  }

  @Test void testNoVulnerabilities() throws Exception {
    String response = "{"
        + "\"document\": {"
        + "  \"title\": \"Informational Advisory\","
        + "  \"tracking\": {"
        + "    \"id\": \"VA-26-200-01\","
        + "    \"current_release_date\": \"2026-03-20T12:00:00Z\""
        + "  }"
        + "}"
        + "}";

    String result = transformer.transform(response, context);
    JsonNode rows = MAPPER.readTree(result);

    assertEquals(1, rows.size());
    JsonNode row = rows.get(0);
    assertEquals("VA-26-200-01", row.get("advisory_id").asText());
    assertEquals("cisa", row.get("source").asText());
    assertEquals("2026-03-20", row.get("published_date").asText());
    assertTrue(row.get("cve_ids").isNull(), "cve_ids must be null with no vulnerabilities");
    assertTrue(row.get("severity").isNull(), "severity must be null with no aggregate or scores");
    assertTrue(row.get("affected_systems").isNull(),
        "affected_systems must be null with no product_tree");
  }

  @Test void testEmptyResponse() {
    String result = transformer.transform("", context);
    assertEquals("[]", result);
  }
}
