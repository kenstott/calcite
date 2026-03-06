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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms OpenFIGI batch API responses into flat instrument records.
 *
 * <p>OpenFIGI's mapping API returns an array of result arrays — one result set
 * per item in the batch request. Each result set contains matching instruments
 * or an error object.
 *
 * <p>Response structure:
 * <pre>{@code
 * [
 *   {
 *     "data": [
 *       {
 *         "figi": "BBG000B9XRY4",
 *         "name": "APPLE INC",
 *         "ticker": "AAPL",
 *         "exchCode": "US",
 *         "marketSector": "Equity",
 *         "securityType": "Common Stock",
 *         "securityType2": "Common Stock",
 *         "securityDescription": "AAPL",
 *         "compositeFIGI": "BBG000B9XRY4",
 *         "shareClassFIGI": "BBG001S5N8V8"
 *       }
 *     ]
 *   },
 *   {
 *     "error": "No identifier found."
 *   }
 * ]
 * }</pre>
 *
 * @see ResponseTransformer
 */
public class FigiResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FigiResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("FIGI: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check for top-level error (rate limit, auth failure)
      if (root.isObject()) {
        return handleTopLevelError(root, context);
      }

      if (!root.isArray()) {
        LOGGER.warn("FIGI: Unexpected response type for {}", context.getUrl());
        return "[]";
      }

      // Flatten batch results: array of {data: [...]} or {error: "..."}
      ArrayNode flatRecords = MAPPER.createArrayNode();
      int errorCount = 0;

      for (JsonNode resultSet : root) {
        // Check for per-item error
        JsonNode error = resultSet.path("error");
        if (!error.isMissingNode()) {
          errorCount++;
          LOGGER.trace("FIGI: Batch item error: {}", error.asText());
          continue;
        }

        // Extract data array
        JsonNode data = resultSet.path("data");
        if (data.isArray()) {
          for (JsonNode instrument : data) {
            flatRecords.add(instrument);
          }
        }
      }

      if (errorCount > 0) {
        LOGGER.debug("FIGI: {} batch items had no match out of {} total",
            errorCount, root.size());
      }

      LOGGER.debug("FIGI: Extracted {} instrument records from {} batch items",
          flatRecords.size(), root.size());
      return flatRecords.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("FIGI: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse OpenFIGI response: " + e.getMessage(), e);
    }
  }

  /**
   * Handles top-level error responses (not per-item batch errors).
   */
  private String handleTopLevelError(JsonNode root, RequestContext context) {
    String status = root.path("status").asText("");
    String message = root.path("message").asText(
        root.path("error").asText("Unknown error"));

    String dimensionInfo = context.getDimensionValues().isEmpty()
        ? ""
        : " [dimensions: " + context.getDimensionValues() + "]";

    // Rate limit (HTTP 429)
    if ("429".equals(status) || message.toLowerCase().contains("rate limit")) {
      LOGGER.warn("FIGI: Rate limit exceeded for {}{}", context.getUrl(), dimensionInfo);
      throw new RuntimeException("OpenFIGI rate limit exceeded: " + message);
    }

    // Auth error
    if ("401".equals(status) || "403".equals(status)) {
      LOGGER.error("FIGI: Authentication error for {}: {}", context.getUrl(), message);
      throw new RuntimeException("OpenFIGI auth error " + status + ": " + message);
    }

    LOGGER.error("FIGI API error for {}{}: {}", context.getUrl(), dimensionInfo, message);
    throw new RuntimeException("OpenFIGI API error: " + message);
  }
}
