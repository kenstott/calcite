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
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms NVD CVE 2.0 API responses into flat rows, handling pagination.
 *
 * <p>The NVD API returns pages of up to 2,000 CVEs. This transformer fetches all
 * subsequent pages and streams rows incrementally to avoid accumulating the full
 * dataset in heap. Only one page's Jackson tree is live at a time.
 *
 * <p>CVSS priority: V3.1 preferred, V3.0 fallback, then V2.0. V4.0 stored as
 * additional columns when present.
 *
 * <p>Input: first-page response from
 * {@code https://services.nvd.nist.gov/rest/json/cves/2.0}
 * <pre>
 * {
 *   "resultsPerPage": 2000,
 *   "startIndex": 0,
 *   "totalResults": 347000,
 *   "vulnerabilities": [
 *     { "cve": { "id": "CVE-2021-44228", ... } },
 *     ...
 *   ]
 * }
 * </pre>
 */
public class NvdResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NvdResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int PAGE_SIZE = 2000;
  private static final int TIMEOUT_MS = 60_000;
  private static final String NVD_BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0";
  /** Delay between pagination requests to stay within NVD rate limits (6 req/30s with key). */
  private static final long RATE_DELAY_MS = 600L;

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NVD: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (root.has("message")) {
        String msg = root.path("message").asText();
        LOGGER.error("NVD API error: {}", msg);
        throw new RuntimeException("NVD API error: " + msg);
      }

      int totalResults = root.path("totalResults").asInt(0);
      int startIndex = root.path("startIndex").asInt(0);
      int resultsPerPage = root.path("resultsPerPage").asInt(PAGE_SIZE);

      LOGGER.info("NVD: totalResults={} startIndex={} resultsPerPage={}",
          totalResults, startIndex, resultsPerPage);

      ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * 1024 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int[] rowCount = {0};
      writePage(root, gen, rowCount);

      // Fetch remaining pages; each page's tree is GC-eligible after writePage returns
      String apiKey = context.getHeaders().get("apiKey");
      int nextStart = startIndex + resultsPerPage;
      while (nextStart < totalResults) {
        String pageResponse = fetchPage(nextStart, context, apiKey);
        if (pageResponse == null) {
          LOGGER.warn("NVD: pagination stopped at startIndex={}", nextStart);
          break;
        }
        JsonNode pageRoot = MAPPER.readTree(pageResponse);
        int fetched = pageRoot.path("resultsPerPage").asInt(PAGE_SIZE);
        writePage(pageRoot, gen, rowCount);
        nextStart += fetched;

        if (rowCount[0] % 10000 == 0) {
          LOGGER.info("NVD: wrote {} rows so far (total={})", rowCount[0], totalResults);
        }

        sleepQuietly(RATE_DELAY_MS);
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("NVD: returning {} CVE rows (totalResults={})", rowCount[0], totalResults);
      return baos.toString(StandardCharsets.UTF_8.name());

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("NVD: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse NVD response: " + e.getMessage(), e);
    }
  }

  private void writePage(JsonNode pageRoot, JsonGenerator gen, int[] rowCount) throws Exception {
    JsonNode vulnerabilities = pageRoot.path("vulnerabilities");
    if (!vulnerabilities.isArray()) {
      return;
    }
    for (JsonNode entry : vulnerabilities) {
      JsonNode cve = entry.path("cve");
      if (cve.isMissingNode()) {
        continue;
      }
      ObjectNode row = flattenCve(cve);
      gen.writeTree(row);
      rowCount[0]++;
    }
  }

  private ObjectNode flattenCve(JsonNode cve) {
    ObjectNode row = MAPPER.createObjectNode();

    row.put("cve_id", textOrNull(cve, "id"));
    row.put("published", textOrNull(cve, "published"));
    row.put("last_modified", textOrNull(cve, "lastModified"));
    row.put("vuln_status", textOrNull(cve, "vulnStatus"));
    row.put("description_en", extractEnDescription(cve));

    // CVSS scores: V31 preferred, V30 fallback, then V2
    extractCvssScores(cve, row);

    row.put("cwe_ids", extractCweIds(cve));
    row.put("source", "nvd");

    return row;
  }

  private String extractEnDescription(JsonNode cve) {
    JsonNode descs = cve.path("descriptions");
    if (!descs.isArray()) {
      return null;
    }
    for (JsonNode d : descs) {
      if ("en".equals(d.path("lang").asText())) {
        return d.path("value").asText(null);
      }
    }
    return null;
  }

  private void extractCvssScores(JsonNode cve, ObjectNode row) {
    JsonNode metrics = cve.path("metrics");

    // Try V31 first
    JsonNode v31 = firstMetric(metrics, "cvssMetricV31");
    if (v31 != null) {
      JsonNode data = v31.path("cvssData");
      row.put("cvss_v31_score", doubleOrNull(data, "baseScore"));
      row.put("cvss_v31_vector", textOrNull(data, "vectorString"));
      row.put("cvss_v31_severity", textOrNull(v31, "baseSeverity"));
    } else {
      // Try V30
      JsonNode v30 = firstMetric(metrics, "cvssMetricV30");
      if (v30 != null) {
        JsonNode data = v30.path("cvssData");
        row.put("cvss_v31_score", doubleOrNull(data, "baseScore"));
        row.put("cvss_v31_vector", textOrNull(data, "vectorString"));
        row.put("cvss_v31_severity", textOrNull(v30, "baseSeverity"));
      } else {
        row.putNull("cvss_v31_score");
        row.putNull("cvss_v31_vector");
        row.putNull("cvss_v31_severity");
      }
    }

    // V2
    JsonNode v2 = firstMetric(metrics, "cvssMetricV2");
    if (v2 != null) {
      JsonNode data = v2.path("cvssData");
      row.put("cvss_v2_score", doubleOrNull(data, "baseScore"));
      row.put("cvss_v2_vector", textOrNull(data, "vectorString"));
    } else {
      row.putNull("cvss_v2_score");
      row.putNull("cvss_v2_vector");
    }

    // V40
    JsonNode v40 = firstMetric(metrics, "cvssMetricV40");
    if (v40 != null) {
      JsonNode data = v40.path("cvssData");
      row.put("cvss_v40_score", doubleOrNull(data, "baseScore"));
    } else {
      row.putNull("cvss_v40_score");
    }
  }

  private JsonNode firstMetric(JsonNode metrics, String key) {
    JsonNode arr = metrics.path(key);
    if (arr.isArray() && arr.size() > 0) {
      return arr.get(0);
    }
    return null;
  }

  private String extractCweIds(JsonNode cve) {
    JsonNode weaknesses = cve.path("weaknesses");
    if (!weaknesses.isArray()) {
      return null;
    }
    List<String> ids = new ArrayList<String>();
    for (JsonNode w : weaknesses) {
      JsonNode descs = w.path("description");
      if (!descs.isArray()) {
        continue;
      }
      for (JsonNode d : descs) {
        String val = d.path("value").asText(null);
        if (val != null && val.startsWith("CWE-")) {
          ids.add(val);
        }
      }
    }
    if (ids.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ids.size(); i++) {
      if (i > 0) {
        sb.append("|");
      }
      sb.append(ids.get(i));
    }
    return sb.toString();
  }

  private String fetchPage(int startIndex, RequestContext context, String apiKey) {
    try {
      // Reconstruct URL with pagination params
      StringBuilder url = new StringBuilder(NVD_BASE_URL);
      url.append("?startIndex=").append(startIndex);
      url.append("&resultsPerPage=").append(PAGE_SIZE);

      // Preserve any original query parameters except startIndex/resultsPerPage
      for (java.util.Map.Entry<String, String> entry : context.getParameters().entrySet()) {
        String key = entry.getKey();
        if (!"startIndex".equals(key) && !"resultsPerPage".equals(key)) {
          url.append("&").append(key).append("=").append(entry.getValue());
        }
      }

      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url.toString()).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("Accept", "application/json");
      if (apiKey != null && !apiKey.isEmpty()) {
        conn.setRequestProperty("apiKey", apiKey);
      }

      int status = conn.getResponseCode();
      if (status == 403 || status == 429) {
        LOGGER.warn("NVD: rate limit ({}) at startIndex={}, sleeping 30s", status, startIndex);
        sleepQuietly(30_000L);
        conn.disconnect();
        return fetchPage(startIndex, context, apiKey);
      }
      if (status != 200) {
        LOGGER.warn("NVD: HTTP {} fetching startIndex={}", status, startIndex);
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

    } catch (Exception e) {
      LOGGER.warn("NVD: error fetching startIndex={}: {}", startIndex, e.getMessage());
      return null;
    }
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }

  private static Double doubleOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    return v.asDouble();
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
