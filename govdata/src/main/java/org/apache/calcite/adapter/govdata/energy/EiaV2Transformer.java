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
package org.apache.calcite.adapter.govdata.energy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EiaV2Transformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected static final Logger LOGGER = LoggerFactory.getLogger(EiaV2Transformer.class);

  protected JsonNode extractDataArray(String response) throws Exception {
    JsonNode root = MAPPER.readTree(response);
    JsonNode responseNode = root.get("response");
    if (responseNode != null && !responseNode.isNull()) {
      JsonNode data = responseNode.get("data");
      if (data != null && data.isArray()) {
        return data;
      }
    }
    // Raw-cache hit: HttpSource rewrites paginated EIA v2 responses into a merged
    // {"results":[...]} envelope, so the cached file no longer carries the original
    // response.data wrapper. Read the merged records back from that envelope.
    JsonNode results = root.get("results");
    if (results != null && results.isArray()) {
      return results;
    }
    // Fall back to treating the entire input as an array
    if (root.isArray()) {
      return root;
    }
    return MAPPER.createArrayNode();
  }

  protected int parseYear(String period) {
    if (period == null || period.isEmpty()) {
      return 0;
    }
    String[] parts = period.split("-");
    try {
      return Integer.parseInt(parts[0]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  protected Integer parseMonth(String period) {
    if (period == null || period.isEmpty()) {
      return null;
    }
    String[] parts = period.split("-");
    if (parts.length < 2) {
      return null;
    }
    try {
      int month = Integer.parseInt(parts[1]);
      return month;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  protected String parseDate(String period) {
    if (period == null || period.isEmpty()) {
      return null;
    }
    // Only return as-is if it matches YYYY-MM-DD
    if (period.matches("\\d{4}-\\d{2}-\\d{2}")) {
      return period;
    }
    return null;
  }

  protected Double getDouble(JsonNode node, String... keys) {
    for (String key : keys) {
      JsonNode value = node.get(key);
      if (value != null && !value.isNull()) {
        Double d = safeDouble(value);
        if (d != null) {
          return d;
        }
      }
    }
    return null;
  }

  protected Long getLong(JsonNode node, String... keys) {
    for (String key : keys) {
      JsonNode value = node.get(key);
      if (value != null && !value.isNull()) {
        if (value.isNumber()) {
          return value.longValue();
        }
        try {
          return Long.parseLong(value.asText().trim());
        } catch (NumberFormatException e) {
          // try next key
        }
      }
    }
    return null;
  }

  protected String getString(JsonNode node, String... keys) {
    for (String key : keys) {
      JsonNode value = node.get(key);
      if (value != null && !value.isNull() && value.isTextual()) {
        return value.asText();
      }
    }
    return null;
  }

  protected Double safeDouble(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      return node.doubleValue();
    }
    if (node.isTextual()) {
      String text = node.asText().trim();
      if (text.isEmpty()) {
        return null;
      }
      try {
        return Double.parseDouble(text);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
