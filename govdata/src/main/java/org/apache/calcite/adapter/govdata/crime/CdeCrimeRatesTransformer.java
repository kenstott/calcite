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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Transforms FBI CDE summarized offense rate responses into flat monthly rows.
 *
 * <p>The CDE /summarized/state/{state}/{offense} endpoint returns a nested structure with
 * top-level sections: {@code offenses}, {@code tooltips}, {@code populations},
 * {@code cde_properties}. Rate data lives under {@code offenses.rates} keyed by
 * "{StateName} Offenses", "{StateName} Clearances", "United States Offenses",
 * "United States Clearances" → month ("MM-YYYY") → value.
 *
 * <p>Output: One row per calendar month with state_abbr, offense_code, month,
 * offense_rate, clearance_rate, national_offense_rate, national_clearance_rate,
 * population, and population_coverage_pct.
 */
public class CdeCrimeRatesTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CdeCrimeRatesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE Crime: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (root.isArray()) {
        return transformArrayResponse(root, context);
      }
      if (!root.isObject()) {
        LOGGER.warn("CDE Crime: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      // Section-based response: offenses / tooltips / populations / cde_properties
      if (root.has("offenses")) {
        return transformSectionedResponse(root, context);
      }

      // Fallback: results wrapper or month-keyed flat object
      return transformObjectResponse(root, context);

    } catch (Exception e) {
      LOGGER.error("CDE Crime: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private String transformSectionedResponse(JsonNode root, RequestContext context) {
    String stateAbbr = context.getDimensionValues().get("state_abbr");
    String offenseCode = context.getDimensionValues().get("offense");

    ArrayNode result = MAPPER.createArrayNode();

    JsonNode offensesSection = root.get("offenses");
    JsonNode tooltipsSection = root.get("tooltips");
    JsonNode populationsSection = root.get("populations");

    if (offensesSection == null || !offensesSection.has("rates")) {
      LOGGER.warn("CDE Crime: No offenses.rates in response for {}", context.getUrl());
      return result.toString();
    }

    JsonNode rates = offensesSection.get("rates");
    JsonNode stateOffenseRates = null;
    JsonNode stateClearanceRates = null;
    JsonNode usOffenseRates = null;
    JsonNode usClearanceRates = null;

    Iterator<Map.Entry<String, JsonNode>> rateIt = rates.fields();
    while (rateIt.hasNext()) {
      Map.Entry<String, JsonNode> e = rateIt.next();
      String key = e.getKey();
      if ("United States Offenses".equals(key)) {
        usOffenseRates = e.getValue();
      } else if ("United States Clearances".equals(key)) {
        usClearanceRates = e.getValue();
      } else if (key.endsWith(" Offenses")) {
        stateOffenseRates = e.getValue();
      } else if (key.endsWith(" Clearances")) {
        stateClearanceRates = e.getValue();
      }
    }

    if (stateOffenseRates == null) {
      LOGGER.warn("CDE Crime: Could not find state offense rates for {}", context.getUrl());
      return result.toString();
    }

    // Find state-level population and coverage series (first non-"United States" key)
    JsonNode coverageByMonth = findFirstNonUS(tooltipsSection, "Percent of Population Coverage");
    JsonNode popByMonth = findFirstNonUS(populationsSection, "population");

    Iterator<Map.Entry<String, JsonNode>> monthIt = stateOffenseRates.fields();
    while (monthIt.hasNext()) {
      Map.Entry<String, JsonNode> monthEntry = monthIt.next();
      String month = monthEntry.getKey();

      ObjectNode row = MAPPER.createObjectNode();
      row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
      row.put("offense_code", offenseCode != null ? offenseCode : "");
      row.put("month", month);
      putDoubleFromMap(row, "offense_rate", stateOffenseRates, month);
      putDoubleFromMap(row, "clearance_rate", stateClearanceRates, month);
      putDoubleFromMap(row, "national_offense_rate", usOffenseRates, month);
      putDoubleFromMap(row, "national_clearance_rate", usClearanceRates, month);
      putLongFromMap(row, "population", popByMonth, month);
      putDoubleFromMap(row, "population_coverage_pct", coverageByMonth, month);
      result.add(row);
    }

    LOGGER.debug("CDE Crime: Transformed {} monthly records for state={}, offense={}",
        result.size(), stateAbbr, offenseCode);
    return result.toString();
  }

  /** Navigates {@code root[sectionKey][first non-"United States" child]} and returns it. */
  private static JsonNode findFirstNonUS(JsonNode root, String sectionKey) {
    if (root == null) {
      return null;
    }
    JsonNode section = root.get(sectionKey);
    if (section == null || !section.isObject()) {
      return null;
    }
    Iterator<Map.Entry<String, JsonNode>> it = section.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> e = it.next();
      if (!"United States".equals(e.getKey()) && e.getValue().isObject()) {
        return e.getValue();
      }
    }
    return null;
  }

  private static void putDoubleFromMap(ObjectNode row, String key, JsonNode map, String mapKey) {
    if (map != null && map.has(mapKey)) {
      JsonNode v = map.get(mapKey);
      if (v != null && v.isNumber()) {
        row.put(key, v.doubleValue());
        return;
      }
    }
    row.putNull(key);
  }

  private static void putLongFromMap(ObjectNode row, String key, JsonNode map, String mapKey) {
    if (map != null && map.has(mapKey)) {
      JsonNode v = map.get(mapKey);
      if (v != null && v.isNumber()) {
        row.put(key, v.longValue());
        return;
      }
    }
    row.putNull(key);
  }

  private String transformArrayResponse(JsonNode root, RequestContext context) {
    String stateAbbr = context.getDimensionValues().get("state_abbr");
    String offenseCode = context.getDimensionValues().get("offense");

    ArrayNode result = MAPPER.createArrayNode();
    for (JsonNode item : root) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
      row.put("offense_code", offenseCode != null ? offenseCode : "");
      Iterator<Map.Entry<String, JsonNode>> fields = item.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        row.set(field.getKey(), field.getValue());
      }
      result.add(row);
    }
    return result.toString();
  }

  private String transformObjectResponse(JsonNode root, RequestContext context) {
    String stateAbbr = context.getDimensionValues().get("state_abbr");
    String offenseCode = context.getDimensionValues().get("offense");

    ArrayNode result = MAPPER.createArrayNode();

    JsonNode results = root.get("results");
    if (results != null && results.isArray()) {
      for (JsonNode item : results) {
        result.add(buildRow(item, stateAbbr, offenseCode));
      }
    } else {
      Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String key = entry.getKey();
        JsonNode value = entry.getValue();
        if ("pagination".equals(key) || "message".equals(key)) {
          continue;
        }
        if (value.isObject()) {
          ObjectNode row = MAPPER.createObjectNode();
          row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
          row.put("offense_code", offenseCode != null ? offenseCode : "");
          row.put("month", key);
          copyNumericFields(row, value);
          result.add(row);
        } else if (value.isArray()) {
          for (JsonNode item : value) {
            result.add(buildRow(item, stateAbbr, offenseCode));
          }
        }
      }
    }
    return result.toString();
  }

  private ObjectNode buildRow(JsonNode item, String stateAbbr, String offenseCode) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
    row.put("offense_code", offenseCode != null ? offenseCode : "");
    putStringField(row, "month", item, "date", "month");
    putDoubleField(row, "offense_rate", item, "rate", "offense_rate", "actual_rate");
    putDoubleField(row, "clearance_rate", item, "cleared_rate", "clearance_rate");
    putDoubleField(row, "national_offense_rate", item, "us_rate", "national_rate",
        "national_offense_rate");
    putDoubleField(row, "national_clearance_rate", item, "us_cleared_rate",
        "national_clearance_rate");
    putLongField(row, "population", item, "population");
    putDoubleField(row, "population_coverage_pct", item, "ori_coverage_pct",
        "population_coverage_pct", "coverage_pct");
    return row;
  }

  private static void copyNumericFields(ObjectNode row, JsonNode source) {
    Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode value = field.getValue();
      if (value.isNumber()) {
        row.put(field.getKey(), value.doubleValue());
      } else if (value.isTextual()) {
        row.put(field.getKey(), value.asText());
      }
    }
  }

  private static void putStringField(ObjectNode row, String targetKey,
      JsonNode source, String... sourceKeys) {
    for (String key : sourceKeys) {
      JsonNode value = source.get(key);
      if (value != null && !value.isNull() && value.isTextual()) {
        row.put(targetKey, value.asText());
        return;
      }
    }
  }

  private static void putDoubleField(ObjectNode row, String targetKey,
      JsonNode source, String... sourceKeys) {
    for (String key : sourceKeys) {
      JsonNode value = source.get(key);
      if (value != null && !value.isNull()) {
        if (value.isNumber()) {
          row.put(targetKey, value.doubleValue());
          return;
        }
        try {
          row.put(targetKey, Double.parseDouble(value.asText()));
          return;
        } catch (NumberFormatException e) {
          // continue
        }
      }
    }
  }

  private static void putLongField(ObjectNode row, String targetKey,
      JsonNode source, String... sourceKeys) {
    for (String key : sourceKeys) {
      JsonNode value = source.get(key);
      if (value != null && !value.isNull()) {
        if (value.isNumber()) {
          row.put(targetKey, value.longValue());
          return;
        }
        try {
          row.put(targetKey, Long.parseLong(value.asText()));
          return;
        } catch (NumberFormatException e) {
          // continue
        }
      }
    }
  }
}
