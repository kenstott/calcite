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

import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;

/**
 * Transforms Bureau of Justice Statistics SODA API responses.
 *
 * <p>SODA (Socrata Open Data API) returns JSON arrays directly.
 * This transformer validates each fetched page and enriches records with
 * the {@code endpoint_id} dimension for multi-endpoint tables like
 * {@code bjs_nibrs_estimates}.
 *
 * <p>Implements {@link PerRecordResponseTransformer} because the raw cache of a paginated
 * source holds a merged {@code {"results":[...]}} envelope rather than a SODA response;
 * {@code HttpSource} streams its records through {@link #transformRecord}.
 *
 * <p>An empty body, an error object or any other non-array response fails the fetch, so a
 * failed page never ends pagination as if the data were complete.
 */
public class BjsSodaTransformer implements PerRecordResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT_ID = "endpoint_id";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new RuntimeException("BJS SODA: Empty response for " + context.getUrl());
    }

    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new RuntimeException("BJS SODA: Failed to parse response for " + context.getUrl(), e);
    }

    if (!root.isArray()) {
      JsonNode error = root.has("error") ? root.get("error") : root.get("message");
      throw new RuntimeException("BJS SODA: Expected a JSON array from " + context.getUrl()
          + ", got " + (error != null ? "error: " + error.asText() : root.getNodeType()));
    }

    String endpointId = context.getDimensionValues().get(ENDPOINT_ID);
    if (endpointId != null) {
      for (JsonNode item : root) {
        if (item.isObject()) {
          ((ObjectNode) item).put(ENDPOINT_ID, endpointId);
        }
      }
    }
    return root.toString();
  }

  @Override public void transformRecord(Map<String, Object> row, RequestContext context) {
    String endpointId = context.getDimensionValues().get(ENDPOINT_ID);
    if (endpointId != null) {
      row.put(ENDPOINT_ID, endpointId);
    }
  }
}
