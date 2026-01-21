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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms Federal Reserve Economic Data (FRED) API responses.
 *
 * <p>FRED API responses vary by endpoint:
 *
 * <h4>Series Observations (fred/series/observations)</h4>
 * <pre>{@code
 * {
 *   "realtime_start": "2024-01-01",
 *   "realtime_end": "2024-12-31",
 *   "observation_start": "2020-01-01",
 *   "observation_end": "2024-12-31",
 *   "units": "lin",
 *   "output_type": 1,
 *   "file_type": "json",
 *   "order_by": "observation_date",
 *   "sort_order": "asc",
 *   "count": 60,
 *   "offset": 0,
 *   "limit": 100000,
 *   "observations": [
 *     { "realtime_start": "...", "realtime_end": "...", "date": "...", "value": "..." }
 *   ]
 * }
 * }</pre>
 *
 * <h4>Series Search (fred/series/search)</h4>
 * <pre>{@code
 * {
 *   "realtime_start": "...",
 *   "realtime_end": "...",
 *   "order_by": "search_rank",
 *   "sort_order": "desc",
 *   "count": 100,
 *   "offset": 0,
 *   "limit": 1000,
 *   "seriess": [
 *     { "id": "GDP", "title": "...", "observation_start": "...", ... }
 *   ]
 * }
 * }</pre>
 *
 * <h4>Error Response</h4>
 * <pre>{@code
 * {
 *   "error_code": 400,
 *   "error_message": "Bad Request. Variable api_key is not set."
 * }
 * }</pre>
 *
 * <p>This transformer:
 * <ul>
 *   <li>Checks for error_code and error_message fields</li>
 *   <li>Extracts observations or seriess arrays as appropriate</li>
 *   <li>Handles rate limit (429) and bad request (400) errors</li>
 *   <li>Returns empty array for no-data responses</li>
 * </ul>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class FredResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FredResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("FRED: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check for API error
      if (root.has("error_code") || root.has("error_message")) {
        return handleErrorResponse(root, context);
      }

      // Try to extract data array based on response type
      return extractDataArray(root, context);

    } catch (RuntimeException e) {
      // Re-throw runtime exceptions (including our API error exceptions)
      throw e;
    } catch (Exception e) {
      LOGGER.error("FRED: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse FRED response: " + e.getMessage(), e);
    }
  }

  /**
   * Handles a FRED API error response.
   *
   * @param root The parsed JSON root node
   * @param context Request context for logging
   * @return Empty array for recoverable errors
   * @throws RuntimeException for non-recoverable errors
   */
  private String handleErrorResponse(JsonNode root, RequestContext context) {
    int errorCode = root.path("error_code").asInt(0);
    String errorMessage = root.path("error_message").asText("Unknown error");

    // Log dimension values for debugging
    String dimensionInfo = context.getDimensionValues().isEmpty()
        ? ""
        : " [dimensions: " + context.getDimensionValues() + "]";

    // Handle specific error codes
    switch (errorCode) {
    case 429:
      // Rate limit exceeded
      LOGGER.warn("FRED: Rate limit exceeded (429) for {}{}", context.getUrl(), dimensionInfo);
      throw new RuntimeException("FRED rate limit exceeded: " + errorMessage);

    case 400:
      // Bad request - could be invalid API key or invalid parameters
      if (errorMessage.toLowerCase().contains("api_key")) {
        LOGGER.error("FRED: API key error for {}: {}", context.getUrl(), errorMessage);
        throw new RuntimeException("FRED API key error: " + errorMessage);
      }
      // Other bad requests might be recoverable (e.g., series not found)
      if (errorMessage.toLowerCase().contains("series")
          || errorMessage.toLowerCase().contains("not found")) {
        LOGGER.debug("FRED: Series not found for {}{}: {}",
            context.getUrl(), dimensionInfo, errorMessage);
        return "[]";
      }
      LOGGER.error("FRED API error (400) for {}{}: {}",
          context.getUrl(), dimensionInfo, errorMessage);
      throw new RuntimeException("FRED API error: " + errorMessage);

    case 404:
      // Not found - return empty
      LOGGER.debug("FRED: Resource not found (404) for {}{}", context.getUrl(), dimensionInfo);
      return "[]";

    case 500:
    case 502:
    case 503:
    case 504:
      // Server errors - should trigger retry
      LOGGER.warn("FRED: Server error ({}) for {}: {}",
          errorCode, context.getUrl(), errorMessage);
      throw new RuntimeException("FRED server error " + errorCode + ": " + errorMessage);

    default:
      LOGGER.error("FRED API error ({}) for {}{}: {}",
          errorCode, context.getUrl(), dimensionInfo, errorMessage);
      throw new RuntimeException("FRED API error " + errorCode + ": " + errorMessage);
    }
  }

  /**
   * Extracts the appropriate data array from a successful FRED response.
   *
   * <p>FRED responses can contain different data arrays depending on the endpoint:
   * <ul>
   *   <li>observations - Series data points</li>
   *   <li>seriess - Series metadata (note: FRED uses "seriess" not "series")</li>
   *   <li>categories - Category information</li>
   *   <li>releases - Release information</li>
   *   <li>sources - Source information</li>
   *   <li>tags - Tag information</li>
   * </ul>
   *
   * @param root The parsed JSON root node
   * @param context Request context for logging
   * @return JSON string of the extracted data array
   */
  private String extractDataArray(JsonNode root, RequestContext context) {
    // Check common FRED response arrays in order of likelihood
    String[] dataFields = {"observations", "seriess", "categories", "releases", "sources", "tags"};

    for (String field : dataFields) {
      JsonNode data = root.path(field);
      if (!data.isMissingNode() && data.isArray()) {
        if (data.isEmpty()) {
          LOGGER.debug("FRED: Empty {} array for {}", field, context.getUrl());
          return "[]";
        }
        LOGGER.debug("FRED: Extracted {} {} records", data.size(), field);
        return data.toString();
      }
    }

    // No known data array found - check if this might be a single-object response
    // (some FRED endpoints return a single object rather than an array)
    if (root.has("id") && root.has("title")) {
      // Single series metadata object - wrap in array
      LOGGER.debug("FRED: Wrapping single series object in array");
      return "[" + root.toString() + "]";
    }

    // Check count field - if count is 0, this is an expected empty response
    if (root.has("count") && root.path("count").asInt(0) == 0) {
      LOGGER.debug("FRED: Zero count in response for {}", context.getUrl());
      return "[]";
    }

    // No recognized data structure
    LOGGER.warn("FRED: No recognized data array in response for {}. Response keys: {}",
        context.getUrl(), iteratorToString(root.fieldNames()));

    // Return the whole response as-is for caller to handle
    return response(root);
  }

  /**
   * Safely converts an iterator to a string for logging.
   */
  private static String iteratorToString(java.util.Iterator<String> iterator) {
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    while (iterator.hasNext()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(iterator.next());
      first = false;
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Returns the JSON string representation of a node.
   */
  private static String response(JsonNode node) {
    return node.toString();
  }
}
