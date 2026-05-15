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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms NAEP GetAdhocData ALD stattype responses into naep_achievement_levels rows.
 *
 * <p>Each call returns one level (BA/BC/PR/AD) for a given subject/grade/year combination.
 * The {@code level} dimension value is mapped to the human-readable string stored in the table.
 */
public class NaepAchievementLevelsResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NaepAchievementLevelsResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static String subscaleToSubject(String subscale) {
    if ("MRPCM".equals(subscale)) {
      return "MAT";
    } else if ("RRPCM".equals(subscale)) {
      return "RED";
    }
    return subscale;
  }

  private static int jurisdictionToFips(String jurisdiction) {
    return NaepJurisdictions.toFips(jurisdiction);
  }

  /** Maps ALD stattype suffix to the level string stored in the table. */
  private static String levelCode(String code) {
    if ("BA".equals(code)) {
      return "below_basic";
    } else if ("BC".equals(code)) {
      return "basic";
    } else if ("PR".equals(code)) {
      return "proficient";
    } else if ("AD".equals(code)) {
      return "advanced";
    }
    return code;
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NAEP-ALD: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      String subscale = context.getDimensionValues().get("subscale");
      String subject = subscaleToSubject(subscale);
      String gradeStr = context.getDimensionValues().get("grade");
      String level = levelCode(context.getDimensionValues().get("level"));
      ArrayNode out = MAPPER.createArrayNode();

      try (JsonParser parser = MAPPER.createParser(response)) {
        JsonToken first = parser.nextToken();
        if (first == JsonToken.START_OBJECT) {
          JsonToken t;
          while ((t = parser.nextToken()) != JsonToken.END_OBJECT && t != null) {
            if (t == JsonToken.FIELD_NAME) {
              String field = parser.currentName();
              parser.nextToken();
              if ("statusCode".equals(field)) {
                int code = parser.getIntValue();
                if (code != 200) {
                  LOGGER.warn("NAEP-ALD: API error for {}: status {}", context.getUrl(), code);
                  return "[]";
                }
              } else if ("result".equals(field)) {
                processResultArray(parser, out, subject, gradeStr, level, context);
              } else {
                parser.skipChildren();
              }
            }
          }
        } else if (first == JsonToken.START_ARRAY) {
          processResultArray(parser, out, subject, gradeStr, level, context);
        } else {
          LOGGER.debug("NAEP-ALD: no result array in response for {}", context.getUrl());
          return "[]";
        }
      }

      LOGGER.debug("NAEP-ALD: extracted {} records for subscale={}, grade={}, level={}",
          out.size(), subscale, gradeStr, level);
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("NAEP-ALD: transform failed for {}: {}", context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private void processResultArray(JsonParser parser, ArrayNode out, String subject,
      String gradeStr, String level, RequestContext context) throws Exception {
    while (parser.nextToken() == JsonToken.START_OBJECT) {
      JsonNode record = MAPPER.readTree(parser);
      if (!record.isObject()) {
        continue;
      }
      ObjectNode row = MAPPER.createObjectNode();

      String jurCode = record.path("jurisdiction").asText("NP");
      row.put("jurisdiction", jurisdictionToFips(jurCode));

      if (record.has("jurisLabel")) {
        row.put("jurisdiction_name", record.path("jurisLabel").asText());
      }

      if (record.has("year") && !record.path("year").isNull()) {
        row.put("year", record.path("year").asInt());
      } else {
        String yearDim = context.getDimensionValues().get("year");
        if (yearDim != null) {
          try {
            row.put("year", Integer.parseInt(yearDim));
          } catch (NumberFormatException e) {
            LOGGER.warn("NAEP-ALD: non-integer year dimension '{}'", yearDim);
          }
        }
      }

      row.put("subject", subject);

      if (record.has("grade") && !record.path("grade").isNull()) {
        row.put("grade", record.path("grade").asInt());
      } else if (gradeStr != null) {
        try {
          row.put("grade", Integer.parseInt(gradeStr));
        } catch (NumberFormatException e) {
          LOGGER.warn("NAEP-ALD: non-integer grade dimension '{}'", gradeStr);
        }
      }

      row.put("variable_type", record.path("variable").asText("TOTAL"));
      row.put("subgroup_name", record.path("varValueLabel").asText("All students"));
      row.put("level", level);

      if (record.has("value") && !record.path("value").isNull()) {
        row.put("pct", record.path("value").asDouble());
      } else {
        row.putNull("pct");
      }

      if (record.has("isStatDisplayable")) {
        row.put("is_displayable", record.path("isStatDisplayable").asInt());
      } else {
        row.putNull("is_displayable");
      }

      out.add(row);
    }
  }
}
