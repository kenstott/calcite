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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the NVD junction-table transformers emit one row per distinct fact.
 */
@Tag("unit")
class NvdVulnJunctionDedupTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String CPE = "cpe:2.3:a:acme:widget:1.0:*:*:*:*:*:*:*";

  private static String response() {
    return "{\"vulnerabilities\":[{\"cve\":{\"id\":\"CVE-2024-1234\","
        + "\"published\":\"2024-05-01T00:00:00.000\","
        + "\"weaknesses\":["
        + "{\"source\":\"nvd@nist.gov\",\"description\":[{\"value\":\"CWE-79\"}]},"
        + "{\"source\":\"vendor\",\"description\":[{\"value\":\"CWE-79\"},{\"value\":\"CWE-89\"}]}],"
        + "\"configurations\":[{\"nodes\":["
        + "{\"cpeMatch\":[{\"vulnerable\":true,\"criteria\":\"" + CPE + "\"}]},"
        + "{\"cpeMatch\":[{\"vulnerable\":true,\"criteria\":\"" + CPE + "\"},"
        + "{\"vulnerable\":false,\"criteria\":\"" + CPE + "\"}]}]}]}}]}";
  }

  @Test void testCweDuplicatesAcrossSourcesCollapse() throws Exception {
    RequestContext ctx = RequestContext.builder().url("https://example.test/nvd").build();
    JsonNode rows = MAPPER.readTree(new NvdVulnCwesResponseTransformer().transform(response(), ctx));
    assertEquals(2, rows.size());
    assertEquals("CWE-79", rows.get(0).get("cwe_id").asText());
    assertEquals("CWE-89", rows.get(1).get("cwe_id").asText());
  }

  @Test void testCpeDuplicatesCollapseButVulnerableFlagDistinguishes() throws Exception {
    RequestContext ctx = RequestContext.builder().url("https://example.test/nvd").build();
    JsonNode rows = MAPPER.readTree(new NvdVulnCpesResponseTransformer().transform(response(), ctx));
    assertEquals(2, rows.size());
  }

  @Test void testJunctionRowsRouteByPublishedYearNotCveIdYear() throws Exception {
    String resp = "{\"vulnerabilities\":[{\"cve\":{\"id\":\"CVE-2025-9999\","
        + "\"published\":\"2026-02-10T00:00:00.000\","
        + "\"weaknesses\":[{\"source\":\"nvd@nist.gov\",\"description\":[{\"value\":\"CWE-79\"}]}],"
        + "\"configurations\":[{\"nodes\":[{\"cpeMatch\":[{\"vulnerable\":true,\"criteria\":\""
        + CPE + "\"}]}]}]}}]}";
    RequestContext ctx = RequestContext.builder().url("https://example.test/nvd").build();
    JsonNode cwes = MAPPER.readTree(new NvdVulnCwesResponseTransformer().transform(resp, ctx));
    JsonNode cpes = MAPPER.readTree(new NvdVulnCpesResponseTransformer().transform(resp, ctx));
    assertEquals(1, cwes.size());
    assertEquals(2026, cwes.get(0).get("pub_year").asInt());
    assertEquals(1, cwes.get(0).get("quarter").asInt());
    assertEquals(1, cpes.size());
    assertEquals(2026, cpes.get(0).get("pub_year").asInt());
  }
}
