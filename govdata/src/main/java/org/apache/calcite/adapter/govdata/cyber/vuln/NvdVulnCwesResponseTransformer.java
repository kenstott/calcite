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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Transforms NVD CVE 2.0 API responses into the vulnerability_cwes junction table:
 * one row per (cve_id, cwe_id) pair, materialized at ingest time.
 *
 * <p>Relies on the rawCache hit from the co-located {@code vulnerabilities} table's
 * fetch — no additional API calls are made when the cache is warm.
 */
public class NvdVulnCwesResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NvdVulnCwesResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NVD CWEs: empty response");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode rows = MAPPER.createArrayNode();

      JsonNode vulnerabilities = root.path("vulnerabilities");
      if (!vulnerabilities.isArray()) {
        return "[]";
      }

      for (JsonNode entry : vulnerabilities) {
        JsonNode cve = entry.path("cve");
        if (cve.isMissingNode()) {
          continue;
        }
        String cveId = cve.path("id").asText(null);
        if (cveId == null) {
          continue;
        }
        List<String> cweIds = extractCweIds(cve);
        for (String cweId : cweIds) {
          ObjectNode row = MAPPER.createObjectNode();
          row.put("cve_id", cveId);
          row.put("cwe_id", cweId);
          // Emit pub_year/pub_month as real row values for partition routing (the writer's
          // fan-out reads them per-row before DuckDB evaluates any computed expression). CWE
          // rows have no published date, so derive the year from CVE-YYYY-NNNN; quarter = Q1.
          String[] cveParts = cveId.split("-");
          if (cveParts.length >= 2) {
            try {
              row.put("pub_year", Integer.parseInt(cveParts[1]));
            } catch (NumberFormatException ignored) {
              // non-standard CVE id — leave pub_year unset
            }
          }
          row.put("pub_month", 1);
          rows.add(row);
        }
      }

      LOGGER.info("NVD CWEs: {} (cve_id, cwe_id) rows", rows.size());
      return rows.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("NVD CWEs: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse NVD CWE response: " + e.getMessage(), e);
    }
  }

  private List<String> extractCweIds(JsonNode cve) {
    List<String> ids = new ArrayList<String>();
    JsonNode weaknesses = cve.path("weaknesses");
    if (!weaknesses.isArray()) {
      return ids;
    }
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
    return ids;
  }
}
