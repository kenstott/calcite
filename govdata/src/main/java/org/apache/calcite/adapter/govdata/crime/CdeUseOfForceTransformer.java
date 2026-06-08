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
 * Transforms FBI CDE use-of-force responses into flat rows.
 *
 * <p>The CDE /uof endpoint returns two sections:
 * <ul>
 *   <li>{@code participation} — flat object: par_agencies, total_agencies, percent_agencies</li>
 *   <li>{@code reports_submission} — nested: category → month ("MM-YYYY") → count</li>
 * </ul>
 *
 * <p>Output: One row per section+label with state_abbr, year, section, category, label, value.
 */
public class CdeUseOfForceTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdeUseOfForceTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE UoF: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      String stateAbbr = context.getDimensionValues().get("state_abbr");
      String yearDim = context.getDimensionValues().get("year");
      int year = 0;
      if (yearDim != null) {
        try {
          year = Integer.parseInt(yearDim);
        } catch (NumberFormatException e) {
          // leave as 0
        }
      }

      ArrayNode result = MAPPER.createArrayNode();

      if (!root.isObject()) {
        LOGGER.warn("CDE UoF: Unexpected response type {} for {}", root.getNodeType(),
            context.getUrl());
        return "[]";
      }

      Iterator<Map.Entry<String, JsonNode>> sections = root.fields();
      while (sections.hasNext()) {
        Map.Entry<String, JsonNode> sectionEntry = sections.next();
        String section = sectionEntry.getKey();
        JsonNode sectionData = sectionEntry.getValue();

        if (!sectionData.isObject()) {
          continue;
        }

        // participation section: flat label → numeric value
        if ("participation".equals(section)) {
          Iterator<Map.Entry<String, JsonNode>> labels = sectionData.fields();
          while (labels.hasNext()) {
            Map.Entry<String, JsonNode> labelEntry = labels.next();
            String label = labelEntry.getKey();
            JsonNode valueNode = labelEntry.getValue();
            if (valueNode.isNumber()) {
              result.add(buildRow(stateAbbr, year, section, null, label,
                  valueNode.doubleValue()));
            }
          }
          continue;
        }

        // reports_submission and similar: category → month → count
        Iterator<Map.Entry<String, JsonNode>> categories = sectionData.fields();
        while (categories.hasNext()) {
          Map.Entry<String, JsonNode> catEntry = categories.next();
          String category = catEntry.getKey();
          JsonNode catData = catEntry.getValue();

          if (catData.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> labels = catData.fields();
            while (labels.hasNext()) {
              Map.Entry<String, JsonNode> labelEntry = labels.next();
              String label = labelEntry.getKey();
              JsonNode valueNode = labelEntry.getValue();
              if (valueNode.isNumber()) {
                result.add(buildRow(stateAbbr, year, section, category, label,
                    valueNode.doubleValue()));
              }
            }
          } else if (catData.isNumber()) {
            result.add(buildRow(stateAbbr, year, section, null, category,
                catData.doubleValue()));
          }
        }
      }

      LOGGER.debug("CDE UoF: Transformed {} records for state={}, year={}",
          result.size(), stateAbbr, year);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE UoF: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private static ObjectNode buildRow(String stateAbbr, int year,
      String section, String category, String label, double value) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
    if (year > 0) {
      row.put("year", year);
    }
    row.put("section", section);
    if (category != null) {
      row.put("category", category);
    } else {
      row.putNull("category");
    }
    row.put("label", label);
    row.put("value", value);
    return row;
  }
}
