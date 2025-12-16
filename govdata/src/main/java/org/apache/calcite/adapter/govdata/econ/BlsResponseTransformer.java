/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms Bureau of Labor Statistics (BLS) API responses.
 *
 * <p>BLS API responses have the following structure:
 * <pre>{@code
 * {
 *   "status": "REQUEST_SUCCEEDED",
 *   "responseTime": 150,
 *   "message": [],
 *   "Results": {
 *     "series": [
 *       {
 *         "seriesID": "LAUCN040010000000005",
 *         "data": [ ... ]
 *       }
 *     ]
 *   }
 * }
 * }</pre>
 *
 * <p>Error responses contain:
 * <pre>{@code
 * {
 *   "status": "REQUEST_FAILED",
 *   "responseTime": 50,
 *   "message": ["Invalid series ID"],
 *   "Results": {}
 * }
 * }</pre>
 *
 * <p>This transformer:
 * <ul>
 *   <li>Checks the status field for REQUEST_SUCCEEDED/REQUEST_FAILED</li>
 *   <li>Extracts the series array from Results for downstream processing</li>
 *   <li>Handles rate limit errors and suggests retry</li>
 *   <li>Returns empty array for no-data responses</li>
 * </ul>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class BlsResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlsResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** BLS status indicating successful request. */
  private static final String STATUS_SUCCEEDED = "REQUEST_SUCCEEDED";

  /** BLS status indicating failed request. */
  private static final String STATUS_FAILED = "REQUEST_FAILED";

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("BLS: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check status field
      String status = root.path("status").asText("UNKNOWN");

      if (STATUS_SUCCEEDED.equals(status)) {
        return extractSuccessData(root, context);
      } else if (STATUS_FAILED.equals(status)) {
        return handleFailedRequest(root, context);
      } else {
        // Unknown status - log warning but try to extract data anyway
        LOGGER.warn("BLS: Unknown status '{}' for {}", status, context.getUrl());
        return extractSuccessData(root, context);
      }

    } catch (RuntimeException e) {
      // Re-throw runtime exceptions (including our API error exceptions)
      throw e;
    } catch (Exception e) {
      LOGGER.error("BLS: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse BLS response: " + e.getMessage(), e);
    }
  }

  /**
   * Extracts the series data from a successful BLS response.
   *
   * @param root The parsed JSON root node
   * @param context Request context for logging
   * @return JSON string of the series array
   */
  private String extractSuccessData(JsonNode root, RequestContext context) {
    JsonNode results = root.path("Results");
    JsonNode series = results.path("series");

    if (series.isMissingNode() || !series.isArray()) {
      LOGGER.debug("BLS: No series data in response for {}", context.getUrl());
      return "[]";
    }

    if (series.isEmpty()) {
      LOGGER.debug("BLS: Empty series array for {}", context.getUrl());
      return "[]";
    }

    LOGGER.debug("BLS: Extracted {} series records", series.size());
    return series.toString();
  }

  /**
   * Handles a failed BLS API request.
   *
   * @param root The parsed JSON root node
   * @param context Request context for logging
   * @return Empty array for recoverable errors
   * @throws RuntimeException for non-recoverable errors
   */
  private String handleFailedRequest(JsonNode root, RequestContext context) {
    // Extract error message(s)
    JsonNode messageNode = root.path("message");
    String message;

    if (messageNode.isArray() && !messageNode.isEmpty()) {
      // BLS often returns messages as array
      StringBuilder sb = new StringBuilder();
      for (JsonNode msg : messageNode) {
        if (sb.length() > 0) {
          sb.append("; ");
        }
        sb.append(msg.asText());
      }
      message = sb.toString();
    } else {
      message = messageNode.asText("No error message provided");
    }

    // Check for specific error types
    String lowerMessage = message.toLowerCase();

    // Rate limit errors - should trigger retry
    if (lowerMessage.contains("rate limit") || lowerMessage.contains("too many requests")) {
      LOGGER.warn("BLS: Rate limit exceeded for {} - {}", context.getUrl(), message);
      throw new RuntimeException("BLS rate limit exceeded: " + message);
    }

    // Invalid series - return empty (no data for this series)
    if (lowerMessage.contains("invalid series") || lowerMessage.contains("series not found")) {
      LOGGER.debug("BLS: Invalid/unknown series for {} - {}", context.getUrl(), message);
      return "[]";
    }

    // No data available - return empty
    if (lowerMessage.contains("no data") || lowerMessage.contains("data not available")) {
      LOGGER.debug("BLS: No data available for {} - {}", context.getUrl(), message);
      return "[]";
    }

    // Log dimension values for debugging
    String dimensionInfo = context.getDimensionValues().isEmpty()
        ? ""
        : " [dimensions: " + context.getDimensionValues() + "]";

    LOGGER.error("BLS API error: {}{}", message, dimensionInfo);
    throw new RuntimeException("BLS API error: " + message);
  }
}
