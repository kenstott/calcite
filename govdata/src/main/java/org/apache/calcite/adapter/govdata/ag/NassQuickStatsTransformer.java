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
package org.apache.calcite.adapter.govdata.ag;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transformer for the USDA NASS QuickStats API, shared by nass_crop_production
 * and nass_livestock_inventory (QuickStats returns a uniform record shape across
 * sectors).
 *
 * <p>Response envelope is {@code {"data":[ {...39 fields...}, ... ]}}. Each record
 * is flattened to the canonical snake_case column set declared by the
 * {@code nass_columns} template in ag-schema.yaml. Two fields need normalization:
 * <ul>
 *   <li>{@code Value} — comma thousands separators and whitespace-padded
 *       suppression markers ("(D)","(Z)","(X)","(NA)"). Numeric values become a
 *       parseable string; suppression markers set {@code value}=null and record
 *       the marker in {@code value_flag}.</li>
 *   <li>{@code CV (%)} — same treatment, mapped to {@code cv_pct}.</li>
 * </ul>
 *
 * <p>Error handling follows the no-silent-handling rule: a body reporting
 * {@code exceeds limit=50000} is a chunking design error and is thrown so it is
 * visible; any other {@code error} (e.g. "no data" for a state with no crops) is a
 * legitimately-empty result and yields an empty array.
 *
 * @see ResponseTransformer
 */
public class NassQuickStatsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NassQuickStatsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Direct string field mappings: QuickStats key -> output column. */
  private static final String[][] STRING_FIELDS = {
      {"source_desc", "source_desc"},
      {"sector_desc", "sector_desc"},
      {"group_desc", "group_desc"},
      {"commodity_desc", "commodity_desc"},
      {"class_desc", "class_desc"},
      {"prodn_practice_desc", "prodn_practice_desc"},
      {"util_practice_desc", "util_practice_desc"},
      {"statisticcat_desc", "statisticcat_desc"},
      {"domain_desc", "domain_desc"},
      {"domaincat_desc", "domaincat_desc"},
      {"short_desc", "short_desc"},
      {"unit_desc", "unit_desc"},
      {"agg_level_desc", "agg_level_desc"},
      {"state_alpha", "state_alpha"},
      {"state_fips_code", "state_fips_code"},
      {"state_ansi", "state_ansi"},
      {"state_name", "state_name"},
      {"asd_code", "asd_code"},
      {"asd_desc", "asd_desc"},
      {"county_ansi", "county_ansi"},
      {"county_code", "county_code"},
      {"county_name", "county_name"},
      {"freq_desc", "freq_desc"},
      {"reference_period_desc", "reference_period_desc"},
      {"load_time", "load_time"},
  };

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NASS: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      JsonNode error = root.path("error");
      if (!error.isMissingNode() && !error.isNull()) {
        String message = error.isArray() && error.size() > 0
            ? error.get(0).asText() : error.asText();
        if (message != null && message.toLowerCase().contains("exceeds limit")) {
          // Chunk too large — a dimensioning error we must not hide.
          throw new RuntimeException("NASS query exceeded the 50000-row limit; "
              + "chunk further. dimensions=" + context.getDimensionValues()
              + " message=" + message);
        }
        // Any other error (e.g. no data for this state/year) is legitimately empty.
        LOGGER.debug("NASS: no data for {} ({})", context.getDimensionValues(), message);
        return "[]";
      }

      JsonNode data = root.path("data");
      if (data.isMissingNode() || !data.isArray()) {
        LOGGER.debug("NASS: no data array for {}", context.getDimensionValues());
        return "[]";
      }

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : data) {
        out.add(transformRecord(record));
      }
      LOGGER.debug("NASS: transformed {} records for {}", out.size(),
          context.getDimensionValues());
      return out.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("NASS: failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform NASS response: " + e.getMessage(), e);
    }
  }

  private ObjectNode transformRecord(JsonNode record) {
    ObjectNode result = MAPPER.createObjectNode();

    JsonNode yearNode = record.get("year");
    if (yearNode != null && !yearNode.isNull()) {
      result.put("year", yearNode.asInt());
    } else {
      result.putNull("year");
    }

    for (int i = 0; i < STRING_FIELDS.length; i++) {
      result.put(STRING_FIELDS[i][1], getTextValue(record, STRING_FIELDS[i][0]));
    }

    // Value -> value (numeric string or null) + value_flag (suppression marker)
    String rawValue = getTextValue(record, "Value");
    String flag = suppressionMarker(rawValue);
    if (flag != null) {
      result.putNull("value");
      result.put("value_flag", flag);
    } else {
      result.put("value", normalizeNumeric(rawValue));
      result.putNull("value_flag");
    }

    // CV (%) -> cv_pct (numeric string or null); suppression markers drop to null
    String rawCv = getTextValue(record, "CV (%)");
    result.put("cv_pct", suppressionMarker(rawCv) != null ? null : normalizeNumeric(rawCv));

    return result;
  }

  /**
   * Returns the trimmed suppression marker if the value is a NASS non-numeric
   * placeholder ("(D)", "(Z)", "(X)", "(NA)", "(S)", empty), else null.
   */
  private String suppressionMarker(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
      return trimmed;
    }
    return null;
  }

  /** Strips whitespace and thousands separators for DOUBLE parsing; null if empty. */
  private String normalizeNumeric(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return trimmed.replace(",", "");
  }

  private String getTextValue(JsonNode node, String fieldName) {
    JsonNode field = node.get(fieldName);
    if (field == null || field.isNull()) {
      return null;
    }
    String text = field.asText();
    if (text == null) {
      return null;
    }
    String trimmed = text.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
