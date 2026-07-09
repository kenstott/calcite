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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for the FEMA OpenFEMA response transformers.
 *
 * <p>The engine pages the API with the OFFSET paginator ({@code $top}/{@code $skip}) and hands
 * this transformer the raw per-page envelope
 * {@code {"metadata": {...}, "<EntityName>": [ {record}, ... ]}}. Because a
 * {@code responseTransformer} is configured, {@code HttpSource} does not apply the schema
 * {@code dataPath} to the output (see {@code HttpSource} line ~2118), so each subclass extracts
 * its own record array here and returns a bare JSON array of schema-shaped rows.
 *
 * <p>The {@code year} partition value comes from the {@code year} dimension (the API is filtered
 * to one year per fetch), so subclasses never emit a {@code year} column.
 */
abstract class AbstractOpenFemaTransformer implements ResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** The OpenFEMA entity name wrapping the record array (e.g. {@code DisasterDeclarationsSummaries}). */
  protected abstract String entityName();

  /** Maps one raw OpenFEMA record into the schema row. */
  protected abstract void mapRow(JsonNode record, ObjectNode row);

  @Override public final String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode records = root.isArray() ? root : root.path(entityName());
      if (!records.isArray()) {
        // A FEMA "technical difficulties" HTML page or an error envelope reached us as a
        // successful body; surface it rather than silently emit zero rows.
        JsonNode errors = root.path("errors");
        if (errors.isArray() && errors.size() > 0) {
          throw new RuntimeException(entityName() + ": OpenFEMA error: " + errors.toString());
        }
        logger.warn("{}: no '{}' array in response (first 200 chars: {})",
            entityName(), entityName(),
            response.substring(0, Math.min(200, response.length())));
        return "[]";
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : records) {
        if (!record.isObject()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        mapRow(record, row);
        out.add(row);
      }
      logger.debug("{}: transformed {} records", entityName(), out.size());
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(entityName() + " transform failed: " + e.getMessage(), e);
    }
  }

  // --- shared field helpers -------------------------------------------------

  protected static void putText(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asText());
    }
  }

  protected static void putInt(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || (v.isTextual() && v.asText().isEmpty())) {
      row.putNull(col);
    } else {
      row.put(col, v.asInt());
    }
  }

  protected static void putDouble(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || (v.isTextual() && v.asText().isEmpty())) {
      row.putNull(col);
    } else {
      row.put(col, v.asDouble());
    }
  }

  protected static void putBool(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asBoolean());
    }
  }

  /**
   * Emits the date part ({@code YYYY-MM-DD}) of an OpenFEMA ISO-8601 datetime string
   * (e.g. {@code 2024-08-13T00:00:00.000Z}).
   */
  protected static void putDate(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    String s = v.asText();
    if (s.isEmpty()) {
      row.putNull(col);
    } else if (s.length() >= 10) {
      row.put(col, s.substring(0, 10));
    } else {
      row.put(col, s);
    }
  }

  /**
   * Builds a 5-digit county FIPS from a 2-digit state FIPS and a county code that is either the
   * 3-digit county part or an already-complete 5-digit code. Returns {@code null} when the parts
   * cannot form a 5-digit code (rather than a padded guess).
   */
  protected static String buildCountyFips(String stateFips, String countyCode) {
    if (countyCode == null || countyCode.isEmpty()) {
      return null;
    }
    String c = countyCode.trim();
    if (c.length() == 5) {
      return c;
    }
    if (stateFips == null || stateFips.length() != 2) {
      return null;
    }
    if (c.length() > 3) {
      return null;
    }
    StringBuilder sb = new StringBuilder(stateFips);
    for (int i = c.length(); i < 3; i++) {
      sb.append('0');
    }
    sb.append(c);
    return sb.toString();
  }

  /** Left-pads a numeric state code to 2 digits (FIPS), or {@code null} if absent. */
  protected static String padStateFips(String raw) {
    if (raw == null) {
      return null;
    }
    String s = raw.trim();
    if (s.isEmpty()) {
      return null;
    }
    if (s.length() == 1) {
      return "0" + s;
    }
    return s.length() >= 2 ? s.substring(0, 2) : s;
  }

  protected static String text(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    return v.isMissingNode() || v.isNull() ? null : v.asText();
  }
}
