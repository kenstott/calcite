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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Transforms U.S. Census Bureau API responses.
 *
 * <p>Census Bureau API responses come in two formats:
 *
 * <h4>Data API Format (most common)</h4>
 * <p>Returns a 2D array where the first row contains headers and subsequent rows
 * contain data:
 * <pre>{@code
 * [
 *   ["NAME", "B01001_001E", "state", "county"],
 *   ["Los Angeles County, California", "10014009", "06", "037"],
 *   ["Cook County, Illinois", "5150233", "17", "031"],
 *   ...
 * ]
 * }</pre>
 *
 * <h4>Error Response</h4>
 * <pre>{@code
 * {
 *   "error": "Your request did not return any results. Check your variables and geographies."
 * }
 * }</pre>
 * or
 * <pre>{@code
 * [
 *   "error: unknown variable 'INVALID_VAR'"
 * ]
 * }</pre>
 *
 * <h4>Geography API Format</h4>
 * <pre>{@code
 * {
 *   "geos": [
 *     {"name": "state:06", "geoId": "0400000US06", ...},
 *     ...
 *   ]
 * }
 * }</pre>
 *
 * <p>This transformer:
 * <ul>
 *   <li>Converts 2D array format to array of objects with column names</li>
 *   <li>Handles error responses (both object and array formats)</li>
 *   <li>Extracts data from wrapper objects when present</li>
 *   <li>Returns empty array for no-data responses</li>
 * </ul>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class CensusResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CensusResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Census: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check for error object
      if (root.isObject() && root.has("error")) {
        return handleError(root.path("error").asText(), context);
      }

      // Check for array response
      if (root.isArray()) {
        return handleArrayResponse(root, context);
      }

      // Check for wrapped object response (geography API, metadata API)
      if (root.isObject()) {
        return handleObjectResponse(root, context);
      }

      LOGGER.warn("Census: Unexpected response type for {}", context.getUrl());
      return response;

    } catch (RuntimeException e) {
      // Re-throw runtime exceptions
      throw e;
    } catch (Exception e) {
      LOGGER.error("Census: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse Census response: " + e.getMessage(), e);
    }
  }

  /**
   * Handles an error message from Census API.
   *
   * @param message The error message
   * @param context Request context for logging
   * @return Empty array for recoverable errors
   * @throws RuntimeException for non-recoverable errors
   */
  private String handleError(String message, RequestContext context) {
    String lowerMessage = message.toLowerCase();

    // Log dimension values for debugging
    String dimensionInfo = context.getDimensionValues().isEmpty()
        ? ""
        : " [dimensions: " + context.getDimensionValues() + "]";

    // No results - common when query parameters don't match any data
    if (lowerMessage.contains("did not return any results")
        || lowerMessage.contains("no data")) {
      LOGGER.debug("Census: No results for {}{} - {}",
          context.getUrl(), dimensionInfo, message);
      return "[]";
    }

    // Unknown variable - invalid field requested
    if (lowerMessage.contains("unknown variable")) {
      LOGGER.warn("Census: Unknown variable for {}{} - {}",
          context.getUrl(), dimensionInfo, message);
      throw new RuntimeException("Census API error: " + message);
    }

    // Invalid geography
    if (lowerMessage.contains("invalid") && lowerMessage.contains("geography")) {
      LOGGER.debug("Census: Invalid geography for {}{} - {}",
          context.getUrl(), dimensionInfo, message);
      return "[]";
    }

    // Rate limit (Census API has rate limits)
    if (lowerMessage.contains("rate limit") || lowerMessage.contains("too many requests")) {
      LOGGER.warn("Census: Rate limit exceeded for {}{}", context.getUrl(), dimensionInfo);
      throw new RuntimeException("Census rate limit exceeded: " + message);
    }

    // General error
    LOGGER.error("Census API error for {}{}: {}", context.getUrl(), dimensionInfo, message);
    throw new RuntimeException("Census API error: " + message);
  }

  /**
   * Handles an array response from Census API.
   *
   * <p>Census data API returns a 2D array where the first row is headers.
   * This method converts it to an array of objects.
   *
   * @param root The parsed JSON array node
   * @param context Request context for logging
   * @return JSON array string of data objects
   */
  private String handleArrayResponse(JsonNode root, RequestContext context) {
    if (root.isEmpty()) {
      LOGGER.debug("Census: Empty array for {}", context.getUrl());
      return "[]";
    }

    // Check if first element is an error string
    JsonNode first = root.get(0);
    if (first.isTextual()) {
      String text = first.asText();
      if (text.toLowerCase().startsWith("error")) {
        return handleError(text, context);
      }
    }

    // Check if this is a 2D array (headers + data rows)
    if (first.isArray()) {
      return convertTableToObjects(root, context);
    }

    // Already an array of objects
    LOGGER.debug("Census: Extracted {} records", root.size());
    return root.toString();
  }

  /**
   * Converts Census 2D table format to array of objects.
   *
   * <p>Input format:
   * <pre>{@code
   * [
   *   ["NAME", "POP", "state"],
   *   ["California", "39538223", "06"],
   *   ["Texas", "29145505", "48"]
   * ]
   * }</pre>
   *
   * <p>Output format:
   * <pre>{@code
   * [
   *   {"NAME": "California", "POP": "39538223", "state": "06"},
   *   {"NAME": "Texas", "POP": "29145505", "state": "48"}
   * ]
   * }</pre>
   *
   * @param table The 2D array node
   * @param context Request context for logging
   * @return JSON array string of objects
   */
  private String convertTableToObjects(JsonNode table, RequestContext context) {
    if (table.size() < 2) {
      // Only headers, no data
      LOGGER.debug("Census: Table has only headers, no data for {}", context.getUrl());
      return "[]";
    }

    // Extract headers from first row
    JsonNode headerRow = table.get(0);
    String[] headers = new String[headerRow.size()];
    for (int i = 0; i < headerRow.size(); i++) {
      headers[i] = headerRow.get(i).asText();
    }

    // Convert data rows to objects
    ArrayNode resultArray = MAPPER.createArrayNode();
    for (int rowIndex = 1; rowIndex < table.size(); rowIndex++) {
      JsonNode row = table.get(rowIndex);
      ObjectNode obj = MAPPER.createObjectNode();

      for (int colIndex = 0; colIndex < headers.length && colIndex < row.size(); colIndex++) {
        obj.set(headers[colIndex], row.get(colIndex));
      }

      resultArray.add(obj);
    }

    LOGGER.debug("Census: Converted table with {} columns and {} data rows",
        headers.length, resultArray.size());
    return resultArray.toString();
  }

  /**
   * Handles an object response from Census API.
   *
   * <p>Some Census endpoints return wrapped objects. This method extracts
   * the data array from known wrapper structures.
   *
   * @param root The parsed JSON object node
   * @param context Request context for logging
   * @return JSON array string of data
   */
  private String handleObjectResponse(JsonNode root, RequestContext context) {
    // Check for known wrapper fields
    String[] dataFields = {"geos", "variables", "data", "result", "results", "items"};

    for (String field : dataFields) {
      JsonNode data = root.path(field);
      if (!data.isMissingNode() && data.isArray()) {
        LOGGER.debug("Census: Extracted {} records from '{}'", data.size(), field);
        return data.toString();
      }
    }

    // Check if this might be a single record that should be wrapped
    if (root.has("geoId") || root.has("name") || root.has("fips")) {
      LOGGER.debug("Census: Wrapping single record in array");
      return "[" + root.toString() + "]";
    }

    // Return as-is if no recognized structure
    LOGGER.debug("Census: Returning object response as-is. Fields: {}",
        iteratorToString(root.fieldNames()));
    return root.toString();
  }

  /**
   * Safely converts an iterator to a string for logging.
   */
  private static String iteratorToString(Iterator<String> iterator) {
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
}
