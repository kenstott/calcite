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

import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 *
 * <p>Implements {@link PerRecordResponseTransformer} so HttpSource's streamFromRawCache path
 * can stream gigabytes of cached studies without loading the whole envelope into a Java
 * String (which would OOM at the ~2 GiB Java array limit). The cache reader calls
 * {@link #transformRecordToMany} once per study element; the legacy {@link #transform(String,
 * RequestContext)} entry point remains for live (paginated) HTTP responses where individual
 * page bodies fit comfortably in memory.
 */
public abstract class AbstractClinicalTrialsResponseTransformer
    implements PerRecordResponseTransformer {
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

  /**
   * Not supported for clinical-trials transformers — they are fan-out (one study → 0..N rows),
   * so they override {@link #transformRecordToMany} instead. Required only because the
   * interface declares {@code transformRecord} as non-default.
   */
  @Override
  public void transformRecord(Map<String, Object> row, RequestContext context) {
    throw new UnsupportedOperationException(
        "Use transformRecordToMany for fan-out clinical-trials transformers");
  }

  @Override
  public List<Map<String, Object>> transformRecordToMany(
      Map<String, Object> source, RequestContext context) {
    JsonNode study = MAPPER.valueToTree(source);
    JsonNode ps = study.path("protocolSection");
    ArrayNode out = MAPPER.createArrayNode();
    flattenStudy(ps, out);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(out.size());
    for (JsonNode node : out) {
      @SuppressWarnings("unchecked")
      Map<String, Object> row = MAPPER.convertValue(node, Map.class);
      rows.add(row);
    }
    return rows;
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
