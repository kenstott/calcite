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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin base class for openFDA response transformers.
 *
 * <p>HttpSource handles pagination; this transformer receives one page's JSON at a time.
 * Each page looks like: {@code {"meta": {...}, "results": [...]}}
 * Subclasses implement {@link #flattenRecord} to map one {@code results[i]} object
 * to the output row shape expected by the schema.
 */
abstract class AbstractOpenFdaResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOpenFdaResponseTransformer.class);
  static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      if (root.has("error")) {
        String msg = root.path("error").asText();
        LOGGER.warn("openFDA API error for {}: {}", getClass().getSimpleName(), msg);
        return "[]";
      }

      JsonNode results = root.path("results");
      if (!results.isArray() || results.size() == 0) {
        return "[]";
      }

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : results) {
        try {
          ObjectNode row = MAPPER.createObjectNode();
          flattenRecord(record, row);
          out.add(row);
        } catch (Exception e) {
          LOGGER.debug("Skipping malformed {} record: {}", getClass().getSimpleName(), e.getMessage());
        }
      }
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("{} transform failed: {}", getClass().getSimpleName(), e.getMessage());
      return "[]";
    }
  }

  /** Map one results[] entry into the flat output row. */
  protected abstract void flattenRecord(JsonNode record, ObjectNode row);

  /** Get first string element of a JSON array node, or null. */
  protected static String firstText(JsonNode node, String fieldName) {
    JsonNode arr = node.path(fieldName);
    if (arr.isArray() && arr.size() > 0) {
      return nullIfEmpty(arr.get(0).asText(null));
    }
    return nullIfEmpty(arr.asText(null));
  }

  /** Get nested first-array-element: node.path(parent).path(child)[0]. */
  protected static String nestedFirstText(JsonNode node, String parent, String child) {
    JsonNode parentNode = node.path(parent);
    if (parentNode.isArray() && parentNode.size() > 0) {
      parentNode = parentNode.get(0);
    }
    return firstText(parentNode, child);
  }

  /** Join all string elements of a JSON array with commas. */
  protected static String joinArray(JsonNode node, String fieldName) {
    JsonNode arr = node.path(fieldName);
    if (!arr.isArray() || arr.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (JsonNode item : arr) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(item.asText(""));
    }
    String result = sb.toString().trim();
    return result.isEmpty() ? null : result;
  }

  /** Put a string field, omitting null/blank values. */
  protected static void put(ObjectNode row, String key, String value) {
    if (value != null && !value.isEmpty()) {
      row.put(key, value);
    } else {
      row.putNull(key);
    }
  }

  protected static String text(JsonNode node, String fieldName) {
    return nullIfEmpty(node.path(fieldName).asText(null));
  }

  private static String nullIfEmpty(String s) {
    if (s == null || s.isEmpty() || "null".equals(s)) {
      return null;
    }
    return s;
  }
}
