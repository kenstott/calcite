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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;

/**
 * Base transformer for Urban Institute Education Data Portal API responses.
 *
 * <p>All Urban Institute API pages return:
 * {@code {"count": N, "next": "...", "previous": null, "results": [...]}}
 * This class extracts the {@code results} array and delegates per-record
 * augmentation to subclasses via {@link #augmentRecord}.
 *
 * <p>Responses can be very large (100k+ records). This class uses streaming
 * JSON parsing to process one record at a time, avoiding full-tree allocation.
 */
abstract class AbstractUrbanInstituteResponseTransformer implements PerRecordResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractUrbanInstituteResponseTransformer.class);
  static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public final String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Urban Institute: empty response for {}", context.getUrl());
      return "[]";
    }

    try (JsonParser parser = MAPPER.getFactory().createParser(response)) {
      JsonToken first = parser.nextToken();
      if (first == JsonToken.START_ARRAY) {
        return streamArray(parser, context);
      }
      if (first == JsonToken.START_OBJECT) {
        while (parser.nextToken() != null) {
          if ("results".equals(parser.currentName())
              && parser.nextToken() == JsonToken.START_ARRAY) {
            return streamArray(parser, context);
          }
          parser.skipChildren();
        }
      }
      LOGGER.warn("Urban Institute: no results array in response for {}", context.getUrl());
      return "[]";
    } catch (Exception e) {
      LOGGER.error("Urban Institute: transform failed for {}: {}", context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  // Streams the already-opened START_ARRAY, emitting one record at a time.
  private String streamArray(JsonParser parser, RequestContext context) throws Exception {
    StringWriter sw = new StringWriter(1 << 16);
    try (JsonGenerator gen = MAPPER.getFactory().createGenerator(sw)) {
      gen.writeStartArray();
      while (parser.nextToken() == JsonToken.START_OBJECT) {
        ObjectNode row = MAPPER.readTree(parser);
        augmentRecord(row, context);
        MAPPER.writeTree(gen, row);
      }
      gen.writeEndArray();
    }
    return sw.toString();
  }

  @Override public void transformRecord(Map<String, Object> row, RequestContext context) {
    // no-op: subclasses that need per-row augmentation must override this method
  }

  /**
   * Augment a single row after extraction (String-based path).
   * Default implementation is a no-op; subclasses override to inject dimension values
   * or rename fields.
   */
  protected void augmentRecord(ObjectNode row, RequestContext context) {
    // no-op by default
  }

  /** Return text value of a node field, or null. */
  protected static String text(JsonNode node, String field) {
    JsonNode v = node.path(field);
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    String s = v.asText(null);
    return (s == null || s.isEmpty() || "null".equals(s)) ? null : s;
  }
}
