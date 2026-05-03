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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms College Scorecard program-level (CIP 4-digit) responses.
 *
 * <p>Each institution record in the Scorecard API response contains a
 * {@code programs.cip_4_digit} array of program objects. This transformer
 * expands that array: one output row per (institution, program, credential).
 * "PrivacySuppressed" values are mapped to null.
 *
 * <p>The {@code year} dimension is injected into every output row.
 */
public class CollegeScorecardProgramsResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CollegeScorecardProgramsResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PRIVACY_SUPPRESSED = "PrivacySuppressed";
  private static final String PROGRAMS_KEY = "latest.programs.cip_4_digit";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Scorecard Programs: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      JsonNode results = root.isArray() ? root : root.path("results");
      if (!results.isArray()) {
        LOGGER.debug("Scorecard Programs: no results array for {}", context.getUrl());
        return "[]";
      }

      String year = context.getDimensionValues().get("year");

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode institution : results) {
        if (!institution.isObject()) {
          continue;
        }

        // The programs array is nested under the dot-notation key "programs.cip_4_digit"
        JsonNode programs = institution.path(PROGRAMS_KEY);
        if (!programs.isArray() || programs.size() == 0) {
          continue;
        }

        for (JsonNode prog : programs) {
          if (!prog.isObject()) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();

          // Year dimension
          if (year != null) {
            try {
              row.put("year", Integer.parseInt(year));
            } catch (NumberFormatException e) {
              row.put("year", year);
            }
          }

          row.set("unit_id",           nullSafe(prog.path("unit_id")));
          row.set("ope6_id",           nullSafe(prog.path("ope6_id")));
          row.put("cip_code",          textOrNull(prog, "code"));
          row.put("cip_title",         textOrNull(prog, "title"));

          JsonNode credential = prog.path("credential");
          row.set("credential_level",  nullSafe(credential.path("level")));
          row.put("credential_title",  textOrNull(credential, "title"));

          JsonNode counts = prog.path("counts");
          row.set("ipeds_awards",      nullSafe(counts.path("ipeds_awards1")));

          JsonNode e1yr = prog.path("earnings").path("1_yr");
          row.set("median_earnings_1yr",       nullSafe(e1yr.path("overall_median_earnings")));
          row.set("pell_median_earnings_1yr",  nullSafe(e1yr.path("pell_median_earnings")));
          row.set("nonpell_median_earnings_1yr", nullSafe(e1yr.path("nonpell_median_earnings")));

          JsonNode e4yr = prog.path("earnings").path("4_yr");
          row.set("median_earnings_4yr", nullSafe(e4yr.path("overall_median_earnings")));

          JsonNode e5yr = prog.path("earnings").path("5_yr");
          row.set("median_earnings_5yr", nullSafe(e5yr.path("overall_median_earnings")));

          out.add(row);
        }
      }

      LOGGER.debug("Scorecard Programs: {} program rows for year={}", out.size(), year);
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("Scorecard Programs: transform failed for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private static JsonNode nullSafe(JsonNode node) {
    if (node == null || node.isMissingNode()) {
      return MAPPER.nullNode();
    }
    if (node.isTextual() && PRIVACY_SUPPRESSED.equals(node.asText())) {
      return MAPPER.nullNode();
    }
    return node;
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode v = node.path(field);
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    String s = v.asText(null);
    if (s == null || s.isEmpty() || PRIVACY_SUPPRESSED.equals(s)) {
      return null;
    }
    return s;
  }
}
