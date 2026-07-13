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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for the HUD USER API response transformers ({@code fmr}/{@code il}
 * {@code statedata} endpoints). Both wrap their payload in a top-level
 * {@code {"data": ...}} envelope; subclasses receive that {@code data} node and
 * emit schema-shaped rows. The {@code year} and {@code state} partition values
 * come from the request dimensions, so subclasses never emit them.
 */
abstract class AbstractHudJsonTransformer implements ResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Emits one or more rows from the HUD {@code data} node into {@code out}. */
  protected abstract void emitRows(JsonNode data, ArrayNode out);

  @Override public final String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode data = root.path("data");
      if (data.isMissingNode() || data.isNull()) {
        JsonNode error = root.path("error");
        if (!error.isMissingNode() && !error.isNull()) {
          throw new RuntimeException("HUD API error: " + error.asText());
        }
        logger.warn("{}: no 'data' node in response (first 200 chars: {})",
            getClass().getSimpleName(),
            response.substring(0, Math.min(200, response.length())));
        return "[]";
      }
      ArrayNode out = MAPPER.createArrayNode();
      emitRows(data, out);
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(getClass().getSimpleName() + " transform failed: "
          + e.getMessage(), e);
    }
  }

  // --- shared field helpers -------------------------------------------------

  /** Puts a text value, or null when the field is missing/empty. */
  protected static void putText(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || v.asText().isEmpty()) {
      row.putNull(col);
    } else {
      row.put(col, v.asText());
    }
  }

  /** Puts an integer value, tolerating numeric-in-string; null when absent/blank/non-numeric. */
  protected static void putInt(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    if (v.isNumber()) {
      row.put(col, v.asInt());
      return;
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      row.putNull(col);
      return;
    }
    try {
      row.put(col, (int) Double.parseDouble(s));
    } catch (NumberFormatException e) {
      row.putNull(col);
    }
  }

  /** First 2 digits of a HUD 10-digit FIPS code, or null. */
  protected static String stateFipsOf(String fips) {
    return fips != null && fips.length() >= 2 ? fips.substring(0, 2) : null;
  }

  /** First 5 digits of a HUD 10-digit FIPS code (state+county), or null. */
  protected static String countyFipsOf(String fips) {
    return fips != null && fips.length() >= 5 ? fips.substring(0, 5) : null;
  }

  /** Left-pads a numeric state id to a 2-digit FIPS string, or null. */
  protected static String padStateFips(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      return null;
    }
    return s.length() == 1 ? "0" + s : s;
  }
}
