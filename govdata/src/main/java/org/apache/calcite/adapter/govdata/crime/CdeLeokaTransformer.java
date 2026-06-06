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
 * Transforms FBI CDE LEOKA year-to-date chart responses into flat rows.
 *
 * <p>Input: {@code [{ "leoka_chart_ytd": { "data": { "chart_data": {
 * "weapons": { "Handgun": 36, ... }, ... } } } }]}. Each chart category maps
 * to label/count pairs.
 *
 * <p>Output: flat rows with state_abbr, year, section="chart_data", category
 * (chart key), label, count.
 */
public class CdeLeokaTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdeLeokaTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE LEOKA: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode element = root.isArray() ? (root.size() > 0 ? root.get(0) : null) : root;
      JsonNode chartData = element == null ? null
          : element.path("leoka_chart_ytd").path("data").path("chart_data");
      if (chartData == null || !chartData.isObject()) {
        LOGGER.debug("CDE LEOKA: No chart_data for {}", context.getUrl());
        return "[]";
      }

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
      Iterator<Map.Entry<String, JsonNode>> categories = chartData.fields();
      while (categories.hasNext()) {
        Map.Entry<String, JsonNode> catEntry = categories.next();
        JsonNode categoryData = catEntry.getValue();
        if (!categoryData.isObject()) {
          continue;
        }
        Iterator<Map.Entry<String, JsonNode>> labels = categoryData.fields();
        while (labels.hasNext()) {
          Map.Entry<String, JsonNode> labelEntry = labels.next();
          JsonNode countNode = labelEntry.getValue();
          if (!countNode.isNumber()) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();
          row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
          if (year > 0) {
            row.put("year", year);
          }
          row.put("section", "chart_data");
          row.put("category", catEntry.getKey());
          row.put("label", labelEntry.getKey());
          row.put("count", countNode.longValue());
          result.add(row);
        }
      }

      LOGGER.debug("CDE LEOKA: Transformed {} records for state={}, year={}",
          result.size(), stateAbbr, year);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE LEOKA: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
