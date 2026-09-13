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
 * Transforms NVD CVE 2.0 API responses into the vulnerability_cpes junction table:
 * one row per (cve_id, cpe_criteria) pair from configurations[].nodes[].cpeMatch[],
 * with vendor and product parsed from the CPE 2.3 string.
 *
 * <p>CPE 2.3 format: cpe:2.3:{part}:{vendor}:{product}:{version}:{update}:{edition}
 * :{language}:{sw_edition}:{target_sw}:{target_hw}:{other}
 *
 * <p>Relies on the rawCache hit from the co-located {@code vulnerabilities} table's
 * fetch — no additional API calls are made when the cache is warm.
 */
public class NvdVulnCpesResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NvdVulnCpesResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NVD CPEs: empty response");
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

        Integer pubYear = null;
        Integer pubMonth = null;
        Integer pubQuarter = null;
        String[] cveParts = cveId.split("-");
        if (cveParts.length >= 2) {
          try {
            pubYear = Integer.parseInt(cveParts[1]);
          } catch (NumberFormatException ignored) {
            // non-standard CVE id
          }
        }

        // Extract month and quarter from published timestamp (ISO 8601: YYYY-MM-DDTHH:MM:SSZ)
        String published = cve.path("published").asText(null);
        if (published != null && published.length() >= 7) {
          try {
            // Parse month from "YYYY-MM-DD..." format
            String monthStr = published.substring(5, 7);
            pubMonth = Integer.parseInt(monthStr);
            if (pubMonth >= 1 && pubMonth <= 12) {
              pubQuarter = (pubMonth - 1) / 3 + 1;
            }
          } catch (NumberFormatException ignored) {
            // malformed date
          }
        }

        JsonNode configurations = cve.path("configurations");
        if (!configurations.isArray()) {
          continue;
        }

        for (JsonNode config : configurations) {
          JsonNode nodes = config.path("nodes");
          if (!nodes.isArray()) {
            continue;
          }
          for (JsonNode node : nodes) {
            JsonNode cpeMatches = node.path("cpeMatch");
            if (!cpeMatches.isArray()) {
              continue;
            }
            for (JsonNode match : cpeMatches) {
              String criteria = match.path("criteria").asText(null);
              if (criteria == null) {
                continue;
              }

              String[] parts = parseCpe23(criteria);
              if (parts == null) {
                continue;
              }

              ObjectNode row = MAPPER.createObjectNode();
              row.put("cve_id", cveId);
              row.put("cpe_criteria", criteria);
              row.put("part", parts[0]);
              row.put("vendor", parts[1]);
              row.put("product", parts[2]);
              row.put("version", parts[3]);
              row.put("vulnerable", match.path("vulnerable").asBoolean(false));

              String versionStartInc = match.path("versionStartIncluding").asText(null);
              if (versionStartInc != null) {
                row.put("version_start_including", versionStartInc);
              } else {
                row.putNull("version_start_including");
              }
              String versionStartExc = match.path("versionStartExcluding").asText(null);
              if (versionStartExc != null) {
                row.put("version_start_excluding", versionStartExc);
              } else {
                row.putNull("version_start_excluding");
              }
              String versionEndInc = match.path("versionEndIncluding").asText(null);
              if (versionEndInc != null) {
                row.put("version_end_including", versionEndInc);
              } else {
                row.putNull("version_end_including");
              }
              String versionEndExc = match.path("versionEndExcluding").asText(null);
              if (versionEndExc != null) {
                row.put("version_end_excluding", versionEndExc);
              } else {
                row.putNull("version_end_excluding");
              }

              if (pubYear != null) {
                row.put("pub_year", pubYear);
              }
              if (pubMonth != null) {
                row.put("pub_month", pubMonth);
              } else {
                row.putNull("pub_month");
              }
              if (pubQuarter != null) {
                row.put("quarter", pubQuarter);
              } else {
                row.putNull("quarter");
              }
              rows.add(row);
            }
          }
        }
      }

      LOGGER.info("NVD CPEs: {} (cve_id, cpe) rows", rows.size());
      return rows.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("NVD CPEs: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse NVD CPE response: " + e.getMessage(), e);
    }
  }

  private String[] parseCpe23(String criteria) {
    if (criteria == null || !criteria.startsWith("cpe:2.3:")) {
      return null;
    }
    String[] parts = criteria.split(":", -1);
    if (parts.length < 6) {
      return null;
    }
    return new String[] { parts[2], parts[3], parts[4], parts[5] };
  }
}
