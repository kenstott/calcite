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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms Bureau of Justice Statistics SODA API responses.
 *
 * <p>SODA (Socrata Open Data API) returns JSON arrays directly.
 * This transformer validates the response and enriches records with
 * the {@code endpoint_id} dimension for multi-endpoint tables like
 * {@code bjs_nibrs_estimates}.
 *
 * <p>For empty or error responses, returns an empty array.
 */
public class BjsSodaTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BjsSodaTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("BJS SODA: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check for error responses (SODA returns error objects, not arrays)
      if (root.isObject()) {
        if (root.has("error") || root.has("message")) {
          String errorMsg = root.has("error")
              ? root.get("error").asText()
              : root.get("message").asText();
          LOGGER.warn("BJS SODA: API error for {}: {}", context.getUrl(), errorMsg);
          return "[]";
        }
        // Single object - wrap in array
        LOGGER.debug("BJS SODA: Wrapping single object in array");
        return "[" + root.toString() + "]";
      }

      if (!root.isArray()) {
        LOGGER.warn("BJS SODA: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      if (root.size() == 0) {
        LOGGER.debug("BJS SODA: Empty array for {}", context.getUrl());
        return "[]";
      }

      // Enrich with endpoint_id if present in dimensions
      String endpointId = context.getDimensionValues().get("endpoint_id");
      if (endpointId != null) {
        ArrayNode enriched = MAPPER.createArrayNode();
        for (JsonNode item : root) {
          if (item.isObject()) {
            ObjectNode row = ((ObjectNode) item).deepCopy();
            row.put("endpoint_id", endpointId);
            enriched.add(row);
          } else {
            enriched.add(item);
          }
        }
        LOGGER.debug("BJS SODA: Enriched {} records with endpoint_id={}",
            enriched.size(), endpointId);
        return enriched.toString();
      }

      LOGGER.debug("BJS SODA: Passing through {} records", root.size());
      return root.toString();

    } catch (Exception e) {
      LOGGER.error("BJS SODA: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
