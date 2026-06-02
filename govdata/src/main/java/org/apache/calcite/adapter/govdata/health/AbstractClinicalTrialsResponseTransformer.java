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
 * Base class for transforming clinicaltrials.gov API responses.
 *
 * <p>Recognises two response shapes:
 * <ul>
 *   <li>Live API: {@code { "studies": [ { "protocolSection": {...} }, ... ] }} — each element
 *       wraps a {@code protocolSection}.</li>
 *   <li>Cached merged envelope (after HttpSource accumulator with {@code dataPath: studies}):
 *       {@code { "results": [ { "protocolSection": {...} }, ... ] }} — same per-element shape.</li>
 * </ul>
 * Subclasses receive the protocolSection node per study and write rows to an output array.
 */
public abstract class AbstractClinicalTrialsResponseTransformer implements ResponseTransformer {
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode records = root.path("studies");
      if (records.isMissingNode() || !records.isArray()) {
        records = root.path("results");
      }
      ArrayNode out = MAPPER.createArrayNode();

      for (JsonNode study : records) {
        JsonNode ps = study.path("protocolSection");
        flattenStudy(ps, out);
      }

      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform clinical trials response", e);
    }
  }

  protected abstract void flattenStudy(JsonNode protocolSection, ArrayNode out);

  protected static void put(ObjectNode row, String key, Object value) {
    if (value == null) {
      row.putNull(key);
    } else if (value instanceof String) {
      row.put(key, (String) value);
    } else if (value instanceof Integer) {
      row.put(key, (Integer) value);
    } else if (value instanceof Boolean) {
      row.put(key, (Boolean) value);
    } else {
      row.put(key, String.valueOf(value));
    }
  }

  protected static String text(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    JsonNode value = node.path(field);
    return value.isMissingNode() ? null : value.asText(null);
  }

  protected static String nestedText(JsonNode node, String path) {
    if (node == null) {
      return null;
    }
    String[] parts = path.split("\\.");
    JsonNode current = node;
    for (String part : parts) {
      current = current.path(part);
      if (current.isMissingNode()) {
        return null;
      }
    }
    return current.asText(null);
  }

  protected static String firstText(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    JsonNode arr = node.path(field);
    if (arr.isArray() && arr.size() > 0) {
      return arr.get(0).asText(null);
    }
    return null;
  }

  protected static Integer firstInt(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    JsonNode arr = node.path(field);
    if (arr.isArray() && arr.size() > 0) {
      JsonNode first = arr.get(0);
      if (first.isIntegralNumber()) {
        return first.asInt();
      }
    }
    return null;
  }

  protected static Integer asInt(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    JsonNode value = node.path(field);
    if (value.isIntegralNumber()) {
      return value.asInt();
    }
    return null;
  }

  protected static String truncate(String value, int maxLength) {
    if (value == null) {
      return null;
    }
    return value.length() > maxLength ? value.substring(0, maxLength) : value;
  }
}
