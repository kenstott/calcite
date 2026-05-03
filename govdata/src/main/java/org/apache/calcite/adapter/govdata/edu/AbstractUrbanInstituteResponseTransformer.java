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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base transformer for Urban Institute Education Data Portal API responses.
 *
 * <p>All Urban Institute API pages return:
 * {@code {"count": N, "next": "...", "previous": null, "results": [...]}}
 * This class extracts the {@code results} array and delegates per-record
 * augmentation to subclasses via {@link #augmentRecord}.
 */
abstract class AbstractUrbanInstituteResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractUrbanInstituteResponseTransformer.class);
  static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public final String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Urban Institute: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Handle direct array responses
      if (root.isArray()) {
        return augmentAll((ArrayNode) root, context);
      }

      JsonNode results = root.path("results");
      if (!results.isArray()) {
        LOGGER.warn("Urban Institute: no results array in response for {}", context.getUrl());
        return "[]";
      }

      return augmentAll((ArrayNode) results, context);

    } catch (Exception e) {
      LOGGER.error("Urban Institute: transform failed for {}: {}", context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private String augmentAll(ArrayNode results, RequestContext context) {
    ArrayNode out = MAPPER.createArrayNode();
    for (JsonNode record : results) {
      if (!record.isObject()) {
        continue;
      }
      ObjectNode row = (ObjectNode) record.deepCopy();
      augmentRecord(row, context);
      out.add(row);
    }
    return out.toString();
  }

  /**
   * Augment a single row after extraction.
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
