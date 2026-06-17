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
package org.apache.calcite.adapter.govdata.cyber;

import org.apache.calcite.adapter.govdata.cyber.vuln.CisaAdvisoryResponseTransformer;
import org.apache.calcite.adapter.govdata.cyber.vuln.CisaKevResponseTransformer;
import org.apache.calcite.adapter.govdata.cyber.vuln.CweResponseTransformer;
import org.apache.calcite.adapter.govdata.cyber.vuln.GithubSaResponseTransformer;
import org.apache.calcite.adapter.govdata.cyber.vuln.NvdResponseTransformer;
import org.apache.calcite.adapter.govdata.cyber.vuln.OsvResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Phase 1 cyber_vuln transformer infrastructure.
 *
 * <p>These tests hit live APIs and require {@code CYBER_INTEGRATION_TESTS=true}.
 * They do not write parquet — they validate that the transformers produce
 * non-empty, well-formed JSON from the actual endpoints.
 *
 * <p>Required: network access to NVD, CISA, and cwe.mitre.org.
 * Optional: {@code CYBER_NVD_API_KEY} for higher NVD rate limits.
 */
@Tag("integration")
class CyberVulnIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CyberVulnIntegrationTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int TIMEOUT_MS = 30_000;

  @BeforeAll static void checkEnvironment() {
    Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("CYBER_INTEGRATION_TESTS")),
        "CYBER_INTEGRATION_TESTS=true required to run cyber integration tests");
    LOGGER.info("Running cyber_vuln integration tests");
  }

  // ── CWE catalog ───────────────────────────────────────────────────────────

  @Test void testCweCatalogFetchAndTransform() throws Exception {
    LOGGER.info("CWE: fetching cwec_latest.xml.zip");
    String xml = fetchZipXml("https://cwe.mitre.org/data/xml/cwec_latest.xml.zip");
    assertNotNull(xml, "CWE ZIP/XML should be fetchable");
    assertFalse(xml.isEmpty(), "CWE XML content should not be empty");
    assertTrue(xml.contains("<Weakness"), "CWE XML should contain Weakness elements");

    CweResponseTransformer transformer = new CweResponseTransformer();
    RequestContext ctx = RequestContext.builder()
        .url("https://cwe.mitre.org/data/xml/cwec_latest.xml.zip")
        .headers(Collections.<String, String>emptyMap())
        .parameters(Collections.<String, String>emptyMap())
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();

    String result = transformer.transform(xml, ctx);
    assertNotNull(result);
    assertNotEquals("[]", result, "Should have parsed at least some CWE entries");

    JsonNode rows = MAPPER.readTree(result);
    assertTrue(rows.isArray(), "Result should be a JSON array");
    assertTrue(rows.size() > 100, "Expected > 100 CWE entries, got: " + rows.size());

    LOGGER.info("CWE: got {} entries", rows.size());

    // Spot-check a known entry — CWE-79 (XSS) should be present
    boolean foundXss = false;
    for (JsonNode row : rows) {
      if ("CWE-79".equals(row.path("cwe_id").asText())) {
        foundXss = true;
        assertNotNull(row.path("name").asText(null), "CWE-79 should have a name");
        assertNotNull(row.path("cwe_url").asText(null), "CWE-79 should have a URL");
        break;
      }
    }
    assertTrue(foundXss, "CWE-79 (XSS) should be present in catalog");
  }

  // ── CISA KEV ──────────────────────────────────────────────────────────────

  @Test void testCisaKevFetchAndTransform() throws Exception {
    LOGGER.info("CISA KEV: fetching catalog");
    String json = fetchJson(
        "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
        null);
    assertNotNull(json, "KEV catalog should be fetchable");
    assertFalse(json.isEmpty(), "KEV JSON should not be empty");

    CisaKevResponseTransformer transformer = new CisaKevResponseTransformer();
    RequestContext ctx = RequestContext.builder()
        .url("https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json")
        .headers(Collections.<String, String>emptyMap())
        .parameters(Collections.<String, String>emptyMap())
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();

    String result = transformer.transform(json, ctx);
    assertNotNull(result);
    assertNotEquals("[]", result, "KEV should have entries");

    JsonNode rows = MAPPER.readTree(result);
    assertTrue(rows.isArray());
    assertTrue(rows.size() > 100, "Expected > 100 KEV entries, got: " + rows.size());

    LOGGER.info("CISA KEV: got {} entries", rows.size());

    // Verify required columns on first row
    JsonNode first = rows.get(0);
    assertNotNull(first.path("cve_id").asText(null), "cve_id should be present");
    assertNotNull(first.path("catalog_version").asText(null), "catalog_version should be present");

    // notes_urls should use pipe delimiter (or be null if notes was empty)
    String notesUrls = first.path("notes_urls").asText(null);
    if (notesUrls != null) {
      assertFalse(notesUrls.contains(";"),
          "notes_urls should use | delimiter, not semicolon: " + notesUrls);
    }
  }

  // ── NVD CVE delta (7-day window) ──────────────────────────────────────────

  @Test void testNvdDeltaFetchAndTransform() throws Exception {
    // Use a 7-day window to keep the response small and avoid rate limits
    String sevenDaysAgo = daysAgo(7);
    String today = daysAgo(0);
    String url = "https://services.nvd.nist.gov/rest/json/cves/2.0"
        + "?lastModStartDate=" + sevenDaysAgo + "T00:00:00.000"
        + "&lastModEndDate=" + today + "T23:59:59.999"
        + "&resultsPerPage=50";

    LOGGER.info("NVD delta: fetching 7-day window [{} to {}]", sevenDaysAgo, today);

    String nvdApiKey = System.getenv("CYBER_NVD_API_KEY");
    String json = fetchJson(url, nvdApiKey);
    assertNotNull(json, "NVD API should respond");
    assertFalse(json.isEmpty());

    NvdResponseTransformer transformer = new NvdResponseTransformer();

    java.util.Map<String, String> headers = new java.util.LinkedHashMap<String, String>();
    if (nvdApiKey != null && !nvdApiKey.isEmpty()) {
      headers.put("apiKey", nvdApiKey);
    }
    java.util.Map<String, String> params = new java.util.LinkedHashMap<String, String>();
    params.put("lastModStartDate", sevenDaysAgo + "T00:00:00.000");
    params.put("lastModEndDate", today + "T23:59:59.999");

    RequestContext ctx = RequestContext.builder()
        .url(url)
        .headers(headers)
        .parameters(params)
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();

    String result = transformer.transform(json, ctx);
    assertNotNull(result);

    JsonNode rows = MAPPER.readTree(result);
    assertTrue(rows.isArray(), "Result should be JSON array");
    LOGGER.info("NVD delta: got {} CVE rows from 7-day window", rows.size());

    if (rows.size() > 0) {
      JsonNode first = rows.get(0);
      assertNotNull(first.path("cve_id").asText(null), "cve_id should be present");
      assertEquals("nvd", first.path("source").asText(), "source should be 'nvd'");
      assertTrue(first.path("cve_id").asText("").startsWith("CVE-"),
          "cve_id should start with CVE-");
    }
  }

  // ── Phase 2: OSV ─────────────────────────────────────────────────────────

  @Test void testOsvFetchAndTransform() throws Exception {
    // Requires CYBER_OSV_ECOSYSTEMS=<small ecosystem> to keep the test fast.
    // Default 10 ecosystems would take several minutes; Hex (~200 entries) is ideal.
    String ecosystems = System.getenv("CYBER_OSV_ECOSYSTEMS");
    Assumptions.assumeTrue(ecosystems != null && !ecosystems.isEmpty(),
        "Set CYBER_OSV_ECOSYSTEMS=Hex (or similar) to run OSV smoke test");

    LOGGER.info("OSV: smoke test for ecosystems={}", ecosystems);

    OsvResponseTransformer transformer = new OsvResponseTransformer();
    RequestContext ctx = RequestContext.builder()
        .url("https://osv-vulnerabilities.storage.googleapis.com")
        .headers(Collections.<String, String>emptyMap())
        .parameters(Collections.<String, String>emptyMap())
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();

    String result = transformer.transform("", ctx);
    assertNotNull(result, "OSV result should not be null");

    JsonNode rows = MAPPER.readTree(result);
    assertTrue(rows.isArray(), "OSV result should be JSON array");
    LOGGER.info("OSV: got {} rows for ecosystems={}", rows.size(), ecosystems);

    // Spot-check first row has required fields
    if (rows.size() > 0) {
      JsonNode first = rows.get(0);
      assertNotNull(first.path("osv_id").asText(null), "osv_id should be present");
      assertNotNull(first.path("modified").asText(null), "modified should be present");
      assertNotNull(first.path("ecosystem").asText(null), "ecosystem should be present");
    }
    assertTrue(rows.size() > 0, "Expected at least one OSV row for ecosystem: " + ecosystems);
  }

  // ── Phase 2: GitHub Security Advisories ──────────────────────────────────

  @Test void testGithubSaTransformPage() throws Exception {
    // The framework now drives the GraphQL fetch + CURSOR pagination; GithubSaResponseTransformer
    // only reshapes one page envelope into vuln_cross_refs rows. Feed a canned page (one advisory
    // with a GHSA and a CVE identifier) and assert exactly the CVE pair is emitted.
    String page = "{\"data\":{\"securityAdvisories\":{"
        + "\"pageInfo\":{\"hasNextPage\":false,\"endCursor\":\"Y3Vyc29yOjE=\"},"
        + "\"nodes\":[{\"ghsaId\":\"GHSA-xxxx-yyyy-zzzz\","
        + "\"publishedAt\":\"2024-01-01T00:00:00Z\",\"severity\":\"HIGH\",\"summary\":\"test\","
        + "\"identifiers\":["
        + "{\"type\":\"GHSA\",\"value\":\"GHSA-xxxx-yyyy-zzzz\"},"
        + "{\"type\":\"CVE\",\"value\":\"CVE-2024-12345\"}],"
        + "\"references\":[{\"url\":\"https://example.com/advisory\"}]}]}}}";

    GithubSaResponseTransformer transformer = new GithubSaResponseTransformer();
    RequestContext ctx = RequestContext.builder()
        .url("https://api.github.com/graphql")
        .headers(Collections.<String, String>emptyMap())
        .parameters(Collections.<String, String>emptyMap())
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();

    String result = transformer.transform(page, ctx);
    JsonNode rows = MAPPER.readTree(result);
    assertTrue(rows.isArray(), "GithubSA result should be a JSON array");
    assertEquals(1, rows.size(), "only the CVE identifier should yield a row (GHSA id is skipped)");

    JsonNode row = rows.get(0);
    assertEquals("CVE-2024-12345", row.path("cve_id").asText(), "cve_id");
    assertEquals("GHSA-xxxx-yyyy-zzzz", row.path("external_id").asText(), "external_id (GHSA)");
    assertEquals("ghsa", row.path("external_source").asText(), "external_source");
    assertEquals("https://example.com/advisory", row.path("url").asText(), "url");
  }

  // ── Phase 2: CISA Advisories ──────────────────────────────────────────────

  @Test void testCisaAdvisoryFetchAndTransform() throws Exception {
    LOGGER.info("CisaAdvisory: fetching advisory RSS feed");

    String rss = fetchHtml("https://www.cisa.gov/cybersecurity-advisories/all.xml");
    Assumptions.assumeTrue(rss != null && !rss.isEmpty(),
        "CISA advisory RSS feed must be reachable");

    CisaAdvisoryResponseTransformer transformer = new CisaAdvisoryResponseTransformer();
    RequestContext ctx = RequestContext.builder()
        .url("https://www.cisa.gov/cybersecurity-advisories/all.xml")
        .headers(Collections.<String, String>emptyMap())
        .parameters(Collections.<String, String>emptyMap())
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();

    String result = transformer.transform(rss, ctx);
    assertNotNull(result, "CisaAdvisory result should not be null");

    JsonNode rows = MAPPER.readTree(result);
    assertTrue(rows.isArray(), "CisaAdvisory result should be JSON array");
    LOGGER.info("CisaAdvisory: got {} advisory rows", rows.size());

    if (rows.size() > 0) {
      JsonNode first = rows.get(0);
      assertNotNull(first.path("advisory_id").asText(null), "advisory_id should be present");
      assertEquals("cisa", first.path("source").asText(), "source should be 'cisa'");
      assertNotNull(first.path("url").asText(null), "url should be present");
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private String fetchJson(String urlStr, String apiKey) throws Exception {
    HttpURLConnection conn =
        (HttpURLConnection) URI.create(urlStr).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(TIMEOUT_MS);
    conn.setReadTimeout(TIMEOUT_MS);
    conn.setRequestProperty("Accept", "application/json");
    if (apiKey != null && !apiKey.isEmpty()) {
      conn.setRequestProperty("apiKey", apiKey);
    }
    int status = conn.getResponseCode();
    if (status != 200) {
      LOGGER.warn("HTTP {} for {}", status, urlStr);
      return null;
    }
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    reader.close();
    conn.disconnect();
    return sb.toString();
  }

  private String fetchZipXml(String urlStr) throws Exception {
    java.net.URL url = URI.create(urlStr).toURL();
    java.io.InputStream is = url.openStream();
    java.util.zip.ZipInputStream zis = new java.util.zip.ZipInputStream(is);
    java.util.zip.ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      if (entry.getName().endsWith(".xml")) {
        byte[] bytes = readBytes(zis);
        zis.close();
        return new String(bytes, StandardCharsets.UTF_8);
      }
    }
    zis.close();
    return null;
  }

  private byte[] readBytes(java.io.InputStream in) throws Exception {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      baos.write(buf, 0, n);
    }
    return baos.toByteArray();
  }

  private String fetchHtml(String urlStr) throws Exception {
    HttpURLConnection conn =
        (HttpURLConnection) URI.create(urlStr).toURL().openConnection();
    conn.setInstanceFollowRedirects(true);
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(TIMEOUT_MS);
    conn.setReadTimeout(TIMEOUT_MS);
    conn.setRequestProperty("Accept", "text/html");
    int status = conn.getResponseCode();
    if (status != 200) {
      LOGGER.warn("HTTP {} for {}", status, urlStr);
      return null;
    }
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    reader.close();
    conn.disconnect();
    return sb.toString();
  }

  private String daysAgo(int days) {
    java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    cal.add(java.util.Calendar.DAY_OF_YEAR, -days);
    return String.format("%04d-%02d-%02d",
        cal.get(java.util.Calendar.YEAR),
        cal.get(java.util.Calendar.MONTH) + 1,
        cal.get(java.util.Calendar.DAY_OF_MONTH));
  }
}
