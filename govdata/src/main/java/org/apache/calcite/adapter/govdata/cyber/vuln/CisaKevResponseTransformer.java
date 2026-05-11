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
 * Transforms CISA Known Exploited Vulnerabilities (KEV) catalog responses.
 *
 * <p>The KEV endpoint returns a single JSON object (not a paginated API):
 * <pre>
 * {
 *   "title": "CISA Known Exploited Vulnerabilities Catalog",
 *   "catalogVersion": "2024.10.03",
 *   "dateReleased": "2024-10-03T13:51:19.0000Z",
 *   "count": 1222,
 *   "vulnerabilities": [
 *     {
 *       "cveID": "CVE-2021-27104",
 *       "vendorProject": "Accellion",
 *       "product": "FTA",
 *       "vulnerabilityName": "...",
 *       "dateAdded": "2021-11-03",
 *       "shortDescription": "...",
 *       "requiredAction": "...",
 *       "dueDate": "2021-11-17",
 *       "knownRansomwareCampaignUse": "Unknown",
 *       "notes": "https://example.com; https://other.com",
 *       "cwes": ["CWE-78"]
 *     },
 *     ...
 *   ]
 * }
 * </pre>
 *
 * <p>Key quirks:
 * <ul>
 *   <li>{@code notes} is a semicolon-delimited string of URLs, not an array</li>
 *   <li>{@code cwes} array may be empty on older entries</li>
 *   <li>No ETag / delta endpoint — diff on {@code catalogVersion}</li>
 * </ul>
 *
 * <p>The {@code catalogVersion} is stored per-row so downstream SQL can detect
 * when the catalog has been refreshed.
 */
public class CisaKevResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CisaKevResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CISA KEV: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      String catalogVersion = root.path("catalogVersion").asText(null);
      int count = root.path("count").asInt(0);

      LOGGER.info("CISA KEV: catalogVersion={} count={}", catalogVersion, count);

      JsonNode vulns = root.path("vulnerabilities");
      if (!vulns.isArray()) {
        LOGGER.warn("CISA KEV: no 'vulnerabilities' array in response");
        return "[]";
      }

      ArrayNode rows = MAPPER.createArrayNode();
      for (JsonNode v : vulns) {
        rows.add(flattenEntry(v, catalogVersion));
      }

      LOGGER.info("CISA KEV: returning {} KEV rows", rows.size());
      return rows.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("CISA KEV: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse CISA KEV response: " + e.getMessage(), e);
    }
  }

  private ObjectNode flattenEntry(JsonNode v, String catalogVersion) {
    ObjectNode row = MAPPER.createObjectNode();

    row.put("cve_id", textOrNull(v, "cveID"));
    row.put("vendor_project", textOrNull(v, "vendorProject"));
    row.put("product", textOrNull(v, "product"));
    row.put("vulnerability_name", textOrNull(v, "vulnerabilityName"));
    row.put("date_added", textOrNull(v, "dateAdded"));
    row.put("short_description", textOrNull(v, "shortDescription"));
    row.put("required_action", textOrNull(v, "requiredAction"));
    row.put("due_date", textOrNull(v, "dueDate"));
    row.put("known_ransomware_use", textOrNull(v, "knownRansomwareCampaignUse"));

    // notes is a semicolon-delimited string of URLs
    row.put("notes_urls", splitNotes(v.path("notes").asText(null)));

    // cwes may be an empty array on older entries
    row.put("cwes", joinCwes(v.path("cwes")));

    row.put("catalog_version", catalogVersion);

    return row;
  }

  private String splitNotes(String notes) {
    if (notes == null || notes.trim().isEmpty()) {
      return null;
    }
    String[] parts = notes.split(";");
    List<String> urls = new ArrayList<String>();
    for (String part : parts) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        urls.add(trimmed);
      }
    }
    if (urls.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < urls.size(); i++) {
      if (i > 0) {
        sb.append("|");
      }
      sb.append(urls.get(i));
    }
    return sb.toString();
  }

  private String joinCwes(JsonNode cwesNode) {
    if (!cwesNode.isArray() || cwesNode.size() == 0) {
      return null;
    }
    List<String> ids = new ArrayList<String>();
    for (JsonNode c : cwesNode) {
      String val = c.asText(null);
      if (val != null && !val.isEmpty()) {
        ids.add(val);
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
}
