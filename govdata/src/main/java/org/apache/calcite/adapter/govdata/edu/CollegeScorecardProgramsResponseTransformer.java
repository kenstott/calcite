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

import java.util.Iterator;

/**
 * Transforms College Scorecard program-level (CIP 4-digit) responses.
 *
 * <p>Each institution record in the Scorecard API response contains a
 * {@code <year>.programs.cip_4_digit} array of program objects, holding that year's data file. This transformer
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
  private static final String PROGRAMS_SUFFIX = ".programs.cip_4_digit";

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
      if (year == null) {
        throw new IllegalStateException("Scorecard Programs: year dimension is required");
      }
      String programsKey = year + PROGRAMS_SUFFIX;

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode institution : results) {
        if (!institution.isObject()) {
          continue;
        }

        // The programs array is under the flat dot-notation key. The API omits the key when
        // the year's data file has no field-of-study data, so only another year's key is an error.
        Iterator<String> keys = institution.fieldNames();
        while (keys.hasNext()) {
          String key = keys.next();
          if (key.endsWith(PROGRAMS_SUFFIX) && !key.equals(programsKey)) {
            throw new IllegalStateException("Scorecard Programs: response holds " + key
                + " where " + programsKey + " was requested");
          }
        }
        JsonNode programs = institution.path(programsKey);
        if (!programs.isArray() || programs.size() == 0) {
          continue;
        }

        for (JsonNode prog : programs) {
          if (!prog.isObject()) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();

          // Year dimension
          row.put("year", Integer.parseInt(year));

          // DQ-012: log when PK components are missing; write null so outer joins work
          JsonNode unitIdNode = prog.path("unit_id");
          if (unitIdNode.isMissingNode() || unitIdNode.isNull()) {
            LOGGER.warn("Scorecard Programs: unit_id missing in program record, year={}", year);
          }
          row.set("unit_id", nullSafe(unitIdNode));
          row.set("ope6_id", nullSafe(prog.path("ope6_id")));

          String cipCode = textOrNull(prog, "code");
          if (cipCode == null) {
            LOGGER.warn("Scorecard Programs: cip_code missing for unit_id={}, year={}",
                unitIdNode.asText("?"), year);
          }
          row.put("cip_code",  cipCode);
          row.put("cip_title", textOrNull(prog, "title"));

          // DQ-013: explicitly convert credential.level to integer
          JsonNode credential = prog.path("credential");
          JsonNode levelNode = credential.path("level");
          if (!levelNode.isMissingNode() && !levelNode.isNull()) {
            if (levelNode.isInt() || levelNode.isLong()) {
              row.put("credential_level", levelNode.asInt());
            } else if (levelNode.isTextual()) {
              try {
                row.put("credential_level", Integer.parseInt(levelNode.asText()));
              } catch (NumberFormatException e) {
                LOGGER.warn("Scorecard Programs: non-integer credential_level '{}', storing null",
                    levelNode.asText());
                row.putNull("credential_level");
              }
            } else {
              row.putNull("credential_level");
            }
          } else {
            row.putNull("credential_level");
          }
          row.put("credential_title", textOrNull(credential, "title"));

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
      throw new RuntimeException("College Scorecard Programs: failed to parse response for "
          + context.getUrl(), e);
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
