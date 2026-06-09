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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms one page of NVD CVE 2.0 API response into flat rows.
 *
 * <p>Pagination is driven by the file adapter's built-in OFFSET pagination
 * (configured in cyber-vuln-schema.yaml: type=OFFSET, limitParam=resultsPerPage,
 * offsetParam=startIndex, pageSize=2000, countPath=totalResults).
 * This transformer is called once per page.
 *
 * <p>CVSS priority: V3.1 preferred, V3.0 fallback, then V2.0. V4.0 stored as
 * additional columns when present.
 */
public class NvdResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NvdResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

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

      int startIndex = root.path("startIndex").asInt(0);
      int totalResults = root.path("totalResults").asInt(0);

      ByteArrayOutputStream baos = new ByteArrayOutputStream(4 * 1024 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();
      int[] rowCount = {0};
      writePage(root, gen, rowCount);
      gen.writeEndArray();
      gen.close();

      LOGGER.info("NVD: {} CVE rows from page startIndex={} (total={})",
          rowCount[0], startIndex, totalResults);
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
    String publishedDate = textOrNull(cve, "published");
    row.put("published", publishedDate);
    // Emit pub_year/pub_month as real row values so the Iceberg writer can route each row into
    // its [type, year, quarter] partition. The writer's effectiveYearField/effectiveMonthField
    // fan-out reads these per-row BEFORE DuckDB evaluates any computed-column expression, so the
    // value must already be present on the row (an expression-only column is still null here).
    if (publishedDate != null && publishedDate.length() >= 7) {
      try {
        row.put("pub_year", Integer.parseInt(publishedDate.substring(0, 4)));
        row.put("pub_month", Integer.parseInt(publishedDate.substring(5, 7)));
      } catch (NumberFormatException ignored) {
        // unparseable published date — leave pub_year/pub_month unset
      }
    }
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
      row.put("cvss_v31_severity", textOrNull(data, "baseSeverity"));
    } else {
      // Try V30
      JsonNode v30 = firstMetric(metrics, "cvssMetricV30");
      if (v30 != null) {
        JsonNode data = v30.path("cvssData");
        row.put("cvss_v31_score", doubleOrNull(data, "baseScore"));
        row.put("cvss_v31_vector", textOrNull(data, "vectorString"));
        row.put("cvss_v31_severity", textOrNull(data, "baseSeverity"));
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
}
