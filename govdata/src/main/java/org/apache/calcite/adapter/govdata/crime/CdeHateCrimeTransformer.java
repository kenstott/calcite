/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * Transforms FBI CDE hate crime and use-of-force responses into flat rows.
 *
 * <p>Both the hate crime and use-of-force endpoints return nested JSON with
 * sections, categories, and labels. This transformer flattens the hierarchy
 * into rows with section, category, label, and count/value columns.
 *
 * <p>Input example (hate crime):
 * <pre>{@code
 * {
 *   "bias_section": {
 *     "victim_type": {
 *       "Individual": 1234,
 *       "Business": 56
 *     },
 *     "offense_type": { ... }
 *   },
 *   "incident_section": { ... }
 * }
 * }</pre>
 *
 * <p>Output: Flat rows with state_abbr, year, section, category, label, count.
 */
public class CdeHateCrimeTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdeHateCrimeTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE HateCrime: Empty response for {}", context.getUrl());
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

      if (root.isArray()) {
        // Direct array - pass through with enrichment
        for (JsonNode item : root) {
          ObjectNode row = MAPPER.createObjectNode();
          row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
          if (year > 0) {
            row.put("year", year);
          }
          Iterator<Map.Entry<String, JsonNode>> fields = item.fields();
          while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            row.set(field.getKey(), field.getValue());
          }
          result.add(row);
        }
      } else if (root.isObject()) {
        // Nested sections -> categories -> labels
        flattenSections(root, result, stateAbbr, year);
      }

      LOGGER.debug("CDE HateCrime: Transformed {} records for state={}, year={}",
          result.size(), stateAbbr, year);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE HateCrime: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private void flattenSections(JsonNode root, ArrayNode result,
      String stateAbbr, int year) {
    Iterator<Map.Entry<String, JsonNode>> sections = root.fields();
    while (sections.hasNext()) {
      Map.Entry<String, JsonNode> sectionEntry = sections.next();
      String section = sectionEntry.getKey();
      JsonNode sectionData = sectionEntry.getValue();

      // Skip non-object metadata fields
      if (!sectionData.isObject()) {
        continue;
      }

      Iterator<Map.Entry<String, JsonNode>> categories = sectionData.fields();
      while (categories.hasNext()) {
        Map.Entry<String, JsonNode> catEntry = categories.next();
        String category = catEntry.getKey();
        JsonNode categoryData = catEntry.getValue();

        if (categoryData.isObject()) {
          // Category contains label -> count mappings
          Iterator<Map.Entry<String, JsonNode>> labels = categoryData.fields();
          while (labels.hasNext()) {
            Map.Entry<String, JsonNode> labelEntry = labels.next();
            String label = labelEntry.getKey();
            JsonNode countNode = labelEntry.getValue();

            ObjectNode row = MAPPER.createObjectNode();
            row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
            if (year > 0) {
              row.put("year", year);
            }
            row.put("section", section);
            row.put("category", category);
            row.put("label", label);

            if (countNode.isNumber()) {
              row.put("count", countNode.longValue());
            } else if (countNode.isTextual()) {
              try {
                row.put("count", Long.parseLong(countNode.asText()));
              } catch (NumberFormatException e) {
                // For use-of-force, value may be double
                try {
                  row.put("value", Double.parseDouble(countNode.asText()));
                } catch (NumberFormatException e2) {
                  row.put("value", countNode.asText());
                }
              }
            }

            result.add(row);
          }
        } else if (categoryData.isNumber()) {
          // Direct section -> label -> count (no intermediate category)
          ObjectNode row = MAPPER.createObjectNode();
          row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
          if (year > 0) {
            row.put("year", year);
          }
          row.put("section", section);
          row.put("category", "");
          row.put("label", category);
          row.put("count", categoryData.longValue());
          result.add(row);
        }
      }
    }
  }
}
