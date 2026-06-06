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
 * Transforms a single CISA CSAF 2.0 advisory JSON document into one structured row.
 *
 * <p>The advisory files are fetched by the ETL engine from CISA's official CSAF
 * GitHub repository ({@code cisagov/CSAF}, branch {@code develop}), at
 * {@code https://raw.githubusercontent.com/cisagov/CSAF/develop/csaf_files/IT/white/{year}/{id}.json}
 * (and the parallel {@code OT/white} tree). The set of per-year advisory paths is
 * resolved by {@link CsafAdvisoryDimensionResolver} from each tree's {@code index.txt}
 * (or {@code changes.csv}) listing.
 *
 * <p>The {@code response} parameter is exactly ONE CSAF 2.0 JSON document, shaped like:
 * <pre>
 * {
 *   "document": {
 *     "title": "...",
 *     "tracking": { "id": "VA-26-155-01", "current_release_date": "2026-06-04T15:31:57Z" },
 *     "aggregate_severity": { "text": "Critical" }
 *   },
 *   "vulnerabilities": [
 *     { "cve": "CVE-2024-1234",
 *       "scores": [ { "cvss_v3": { "baseScore": 9.8, "baseSeverity": "CRITICAL" } } ] }
 *   ],
 *   "product_tree": { "branches": [...] }
 * }
 * </pre>
 *
 * <p>Produces a JSON array containing exactly ONE row for the {@code advisories} table:
 * {@code advisory_id}, {@code source} (literal {@code "cisa"}), {@code title},
 * {@code published_date} (YYYY-MM-DD), {@code severity}, {@code cve_ids} (pipe-delimited),
 * {@code affected_systems}. No network calls are made here; the engine already fetched
 * the file.
 */
public class CisaAdvisoryResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CisaAdvisoryResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int AFFECTED_SYSTEMS_MAX = 200;

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("CisaAdvisory: empty response from {}", context.getUrl());
      return "[]";
    }

    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (Exception e) {
      throw new RuntimeException(
          "CisaAdvisory: failed to parse CSAF JSON from " + context.getUrl()
          + ": " + e.getMessage(), e);
    }

    JsonNode document = root.path("document");
    JsonNode tracking = document.path("tracking");

    String advisoryId = textOrNull(tracking, "id");
    String title = textOrNull(document, "title");
    String publishedDate = toIsoDate(textOrNull(tracking, "current_release_date"));

    JsonNode vulnerabilities = root.path("vulnerabilities");
    String severity = parseSeverity(document, vulnerabilities);
    String cveIds = parseCveIds(vulnerabilities);
    String affectedSystems = parseAffectedSystems(root.path("product_tree"));

    ObjectNode row = MAPPER.createObjectNode();
    row.put("advisory_id", advisoryId);
    row.put("source", "cisa");
    row.put("title", title);
    row.put("published_date", publishedDate);
    row.put("severity", severity);
    row.put("cve_ids", cveIds);
    row.put("affected_systems", affectedSystems);

    ArrayNode rows = MAPPER.createArrayNode();
    rows.add(row);

    LOGGER.debug("CisaAdvisory: parsed advisory {}", advisoryId);
    return rows.toString();
  }

  /** Returns the first 10 chars (YYYY-MM-DD) of an ISO timestamp, or null. */
  private static String toIsoDate(String isoTimestamp) {
    if (isoTimestamp == null || isoTimestamp.length() < 10) {
      return null;
    }
    return isoTimestamp.substring(0, 10);
  }

  /**
   * Resolves severity: prefers {@code document.aggregate_severity.text}, otherwise
   * the maximum {@code baseSeverity} across {@code vulnerabilities[].scores[]}
   * ({@code cvss_v3} / {@code cvss_v31}). Returns an upper-cased value in
   * {CRITICAL, HIGH, MEDIUM, LOW}, or null if none found.
   */
  private static String parseSeverity(JsonNode document, JsonNode vulnerabilities) {
    String aggregate = textOrNull(document.path("aggregate_severity"), "text");
    if (aggregate != null) {
      return aggregate.toUpperCase();
    }

    String best = null;
    if (vulnerabilities.isArray()) {
      for (JsonNode vuln : vulnerabilities) {
        JsonNode scores = vuln.path("scores");
        if (!scores.isArray()) {
          continue;
        }
        for (JsonNode score : scores) {
          String sev = textOrNull(score.path("cvss_v3"), "baseSeverity");
          if (sev == null) {
            sev = textOrNull(score.path("cvss_v31"), "baseSeverity");
          }
          if (sev != null) {
            best = pickHigher(best, sev.toUpperCase());
          }
        }
      }
    }
    return best;
  }

  private static String pickHigher(String current, String candidate) {
    return severityRank(candidate) > severityRank(current) ? candidate : current;
  }

  private static int severityRank(String sev) {
    if (sev == null) {
      return -1;
    }
    if ("CRITICAL".equals(sev)) {
      return 4;
    }
    if ("HIGH".equals(sev)) {
      return 3;
    }
    if ("MEDIUM".equals(sev)) {
      return 2;
    }
    if ("LOW".equals(sev)) {
      return 1;
    }
    return 0;
  }

  /** Pipe-delimited distinct CVE IDs from {@code vulnerabilities[].cve}, in order. */
  private static String parseCveIds(JsonNode vulnerabilities) {
    if (!vulnerabilities.isArray()) {
      return null;
    }
    List<String> ids = new ArrayList<String>();
    for (JsonNode vuln : vulnerabilities) {
      String cve = textOrNull(vuln, "cve");
      if (cve != null && !ids.contains(cve)) {
        ids.add(cve);
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

  /**
   * Best-effort affected_systems: joins distinct product_tree branch names with
   * {@code "; "}, truncated to {@value #AFFECTED_SYSTEMS_MAX} characters. Returns null
   * if no branch names are present.
   */
  private static String parseAffectedSystems(JsonNode productTree) {
    JsonNode branches = productTree.path("branches");
    if (!branches.isArray() || branches.size() == 0) {
      return null;
    }

    List<String> names = new ArrayList<String>();
    collectBranchNames(branches, names);
    if (names.isEmpty()) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < names.size(); i++) {
      if (i > 0) {
        sb.append("; ");
      }
      sb.append(names.get(i));
      if (sb.length() >= AFFECTED_SYSTEMS_MAX) {
        break;
      }
    }
    String result = sb.toString();
    return result.length() > AFFECTED_SYSTEMS_MAX
        ? result.substring(0, AFFECTED_SYSTEMS_MAX)
        : result;
  }

  /** Recursively collects distinct branch {@code name} values, depth-first. */
  private static void collectBranchNames(JsonNode branches, List<String> names) {
    if (!branches.isArray()) {
      return;
    }
    for (JsonNode branch : branches) {
      String name = textOrNull(branch, "name");
      if (name != null && !names.contains(name)) {
        names.add(name);
      }
      collectBranchNames(branch.path("branches"), names);
    }
  }

  private static String textOrNull(JsonNode node, String field) {
    if (node == null || node.isMissingNode()) {
      return null;
    }
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }
}
