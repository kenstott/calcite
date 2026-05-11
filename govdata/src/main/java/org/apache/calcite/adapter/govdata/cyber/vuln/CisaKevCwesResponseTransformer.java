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

/**
 * Transforms CISA KEV catalog responses into the kev_cwes junction table:
 * one row per (cve_id, cwe_id) pair, materialized at ingest time.
 *
 * <p>Relies on the rawCache hit from the co-located {@code kev_catalog} table's
 * fetch — no additional API calls are made when the cache is warm.
 */
public class CisaKevCwesResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CisaKevCwesResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CISA KEV CWEs: empty response");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode vulns = root.path("vulnerabilities");
      if (!vulns.isArray()) {
        LOGGER.warn("CISA KEV CWEs: no 'vulnerabilities' array");
        return "[]";
      }

      ArrayNode rows = MAPPER.createArrayNode();
      for (JsonNode v : vulns) {
        String cveId = v.path("cveID").asText(null);
        if (cveId == null) {
          continue;
        }
        JsonNode cwesNode = v.path("cwes");
        if (!cwesNode.isArray() || cwesNode.size() == 0) {
          continue;
        }
        for (JsonNode c : cwesNode) {
          String cweId = c.asText(null);
          if (cweId != null && !cweId.isEmpty()) {
            ObjectNode row = MAPPER.createObjectNode();
            row.put("cve_id", cveId);
            row.put("cwe_id", cweId);
            rows.add(row);
          }
        }
      }

      LOGGER.info("CISA KEV CWEs: {} (cve_id, cwe_id) rows", rows.size());
      return rows.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("CISA KEV CWEs: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse CISA KEV CWE response: " + e.getMessage(), e);
    }
  }
}
