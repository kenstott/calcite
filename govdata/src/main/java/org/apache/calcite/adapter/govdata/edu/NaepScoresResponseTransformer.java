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
 * Transforms Nation's Report Card (NAEP) API responses.
 *
 * <p>The NAEP GetAdhocData endpoint returns {@code {"result": [...]}} where each
 * element has: year, subject, grade, scale, jurisdiction (e.g. "NP"), jurisLabel,
 * variable, varValueLabel, value (mean score), isStatDisplayable.
 * This transformer remaps those fields to the naep_scores schema columns.
 */
public class NaepScoresResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NaepScoresResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Maps subscale code to subject code for the naep_scores schema. */
  private static String subscaleToSubject(String subscale) {
    if ("MRPCM".equals(subscale)) {
      return "MAT";
    } else if ("RRPCM".equals(subscale)) {
      return "RED";
    }
    return subscale;
  }

  /** Maps NAEP jurisdiction code to integer FIPS (NP = 0 = national public). */
  private static int jurisdictionToFips(String jurisdiction) {
    if (jurisdiction == null || "NP".equals(jurisdiction)) {
      return 0;
    }
    try {
      return Integer.parseInt(jurisdiction);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NAEP: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Surface API errors as warnings rather than silent empty results
      if (root.has("statusCode") && root.path("statusCode").asInt(200) != 200) {
        LOGGER.warn("NAEP: API error for {}: {}", context.getUrl(), root.path("result").asText());
        return "[]";
      }

      JsonNode result = root.isArray() ? root : root.path("result");
      if (!result.isArray()) {
        LOGGER.debug("NAEP: no result array in response for {}", context.getUrl());
        return "[]";
      }

      String subscale = context.getDimensionValues().get("subscale");
      String subject = subscaleToSubject(subscale);
      String gradeStr = context.getDimensionValues().get("grade");

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : result) {
        if (!record.isObject()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();

        // jurisdiction (int FIPS, 0 = national)
        String jurCode = record.path("jurisdiction").asText("NP");
        row.put("jurisdiction", jurisdictionToFips(jurCode));

        // jurisdiction_name
        if (record.has("jurisLabel")) {
          row.put("jurisdiction_name", record.path("jurisLabel").asText());
        }

        // year — prefer from response, fall back to dimension
        if (record.has("year") && !record.path("year").isNull()) {
          row.put("year", record.path("year").asInt());
        } else {
          String yearDim = context.getDimensionValues().get("year");
          if (yearDim != null) {
            try {
              row.put("year", Integer.parseInt(yearDim));
            } catch (NumberFormatException e) {
              LOGGER.warn("NAEP: non-integer year dimension '{}'", yearDim);
            }
          }
        }

        // subject derived from subscale dimension
        row.put("subject", subject);

        // grade — prefer from response, fall back to dimension
        if (record.has("grade") && !record.path("grade").isNull()) {
          row.put("grade", record.path("grade").asInt());
        } else if (gradeStr != null) {
          try {
            row.put("grade", Integer.parseInt(gradeStr));
          } catch (NumberFormatException e) {
            LOGGER.warn("NAEP: non-integer grade dimension '{}'", gradeStr);
          }
        }

        // variable_type and subgroup_name
        row.put("variable_type", record.path("variable").asText("TOTAL"));
        row.put("subgroup_name", record.path("varValueLabel").asText("All students"));

        // avg_score (the mean scale score)
        if (record.has("value") && !record.path("value").isNull()) {
          row.put("avg_score", record.path("value").asDouble());
        } else {
          row.putNull("avg_score");
        }

        // std_error, pct_* fields not provided by this endpoint
        row.putNull("std_error");
        row.putNull("pct_below_basic");
        row.putNull("pct_basic");
        row.putNull("pct_proficient");
        row.putNull("pct_advanced");
        row.putNull("sample_size");

        // is_displayable
        if (record.has("isStatDisplayable")) {
          row.put("is_displayable", record.path("isStatDisplayable").asInt());
        } else {
          row.putNull("is_displayable");
        }

        out.add(row);
      }

      LOGGER.debug("NAEP: extracted {} records for subscale={}, grade={}", out.size(), subscale, gradeStr);
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("NAEP: transform failed for {}: {}", context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
