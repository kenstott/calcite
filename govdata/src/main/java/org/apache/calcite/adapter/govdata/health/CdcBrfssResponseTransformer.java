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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms CDC BRFSS data from the Socrata endpoint (dttw-5yxu).
 *
 * <p>Response: top-level JSON array. Each element is a BRFSS prevalence row.
 * Schema: year, state (from locationabbr), question, category, response, pct, sample_size.
 */
public class CdcBrfssResponseTransformer implements ResponseTransformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    try {
      String json = response;
      JsonNode root = MAPPER.readTree(json);
      ArrayNode out = MAPPER.createArrayNode();

      if (!root.isArray()) {
        return "[]";
      }

      for (JsonNode record : root) {
        ObjectNode row = MAPPER.createObjectNode();
        put(row, "year", text(record, "year"));
        put(row, "state", text(record, "locationabbr"));
        put(row, "question", text(record, "question"));
        put(row, "category", text(record, "class"));
        put(row, "topic", text(record, "topic"));
        put(row, "response", text(record, "response"));
        put(row, "pct", text(record, "data_value"));
        put(row, "sample_size", text(record, "sample_size"));
        put(row, "break_out", text(record, "break_out"));
        put(row, "break_out_category", text(record, "break_out_category"));
        put(row, "type", "cdc_brfss");
        out.add(row);
      }

      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform CDC BRFSS response", e);
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText(null);
  }

  private static void put(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }
}
