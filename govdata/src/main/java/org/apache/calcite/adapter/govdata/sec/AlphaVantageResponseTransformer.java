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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Transforms Alpha Vantage API responses.
 *
 * <p>Alpha Vantage API responses vary by function:
 *
 * <h4>Time Series Daily</h4>
 * <pre>{@code
 * {
 *   "Meta Data": {
 *     "1. Information": "Daily Prices (open, high, low, close) and Volumes",
 *     "2. Symbol": "AAPL",
 *     "3. Last Refreshed": "2024-12-16",
 *     "4. Output Size": "Full size",
 *     "5. Time Zone": "US/Eastern"
 *   },
 *   "Time Series (Daily)": {
 *     "2024-12-16": {
 *       "1. open": "250.00",
 *       "2. high": "252.00",
 *       "3. low": "249.00",
 *       "4. close": "251.50",
 *       "5. volume": "45000000"
 *     },
 *     ...
 *   }
 * }
 * }</pre>
 *
 * <h4>Error Responses</h4>
 * <pre>{@code
 * {
 *   "Error Message": "Invalid API call. Please retry or visit the documentation."
 * }
 * }</pre>
 *
 * <h4>Rate Limit Note</h4>
 * <pre>{@code
 * {
 *   "Note": "Thank you for using Alpha Vantage! Our standard API call frequency
 *           is 5 calls per minute and 500 calls per day."
 * }
 * }</pre>
 *
 * <h4>Information Message</h4>
 * <pre>{@code
 * {
 *   "Information": "The **demo** API key is for demo purposes only. Please claim
 *                   your free API key at https://www.alphavantage.co/support/#api-key"
 * }
 * }</pre>
 *
 * <p>This transformer:
 * <ul>
 *   <li>Checks for Error Message, Note, and Information error fields</li>
 *   <li>Extracts Time Series data and converts to array format</li>
 *   <li>Handles rate limit (5/min free tier, 25/day for demo key)</li>
 *   <li>Returns empty array for no-data responses</li>
 * </ul>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class AlphaVantageResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlphaVantageResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("AlphaVantage: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check for error messages
      if (root.has("Error Message")) {
        return handleError(root.path("Error Message").asText(), "Error Message", context);
      }

      // Check for rate limit note
      if (root.has("Note")) {
        return handleError(root.path("Note").asText(), "Note", context);
      }

      // Check for information message (demo key limit)
      if (root.has("Information")) {
        return handleError(root.path("Information").asText(), "Information", context);
      }

      // Try to extract time series data
      return extractTimeSeriesData(root, context);

    } catch (RuntimeException e) {
      // Re-throw runtime exceptions
      throw e;
    } catch (Exception e) {
      LOGGER.error("AlphaVantage: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse Alpha Vantage response: " + e.getMessage(), e);
    }
  }

  /**
   * Handles an error message from Alpha Vantage.
   *
   * @param message The error message
   * @param field The field name containing the error
   * @param context Request context for logging
   * @return Empty array for recoverable errors
   * @throws RuntimeException for non-recoverable errors
   */
  private String handleError(String message, String field, RequestContext context) {
    String lowerMessage = message.toLowerCase();

    // Log dimension values for debugging
    String dimensionInfo = context.getDimensionValues().isEmpty()
        ? ""
        : " [dimensions: " + context.getDimensionValues() + "]";

    // Rate limit errors
    if (lowerMessage.contains("api call frequency")
        || lowerMessage.contains("rate limit")
        || lowerMessage.contains("calls per minute")
        || lowerMessage.contains("calls per day")
        || lowerMessage.contains("25 requests per day")) {
      LOGGER.warn("AlphaVantage: Rate limit exceeded for {}{} - {}",
          context.getUrl(), dimensionInfo, message);
      throw new RuntimeException("Alpha Vantage rate limit: " + message);
    }

    // Demo key limitation
    if (lowerMessage.contains("demo") && lowerMessage.contains("api key")) {
      LOGGER.warn("AlphaVantage: Demo key limitation for {}{} - {}",
          context.getUrl(), dimensionInfo, message);
      throw new RuntimeException("Alpha Vantage demo key limitation: " + message);
    }

    // Invalid symbol
    if (lowerMessage.contains("invalid api call") || lowerMessage.contains("invalid symbol")) {
      LOGGER.debug("AlphaVantage: Invalid symbol for {}{} - {}",
          context.getUrl(), dimensionInfo, message);
      return "[]";
    }

    // No data found
    if (lowerMessage.contains("no data") || lowerMessage.contains("not found")) {
      LOGGER.debug("AlphaVantage: No data for {}{} - {}",
          context.getUrl(), dimensionInfo, message);
      return "[]";
    }

    // General error
    LOGGER.error("AlphaVantage API {} for {}{}: {}",
        field, context.getUrl(), dimensionInfo, message);
    throw new RuntimeException("Alpha Vantage API error: " + message);
  }

  /**
   * Extracts time series data from a successful Alpha Vantage response.
   *
   * <p>Alpha Vantage returns data as a map of date to values. This method
   * converts it to an array format suitable for downstream processing.
   *
   * @param root The parsed JSON root node
   * @param context Request context for logging
   * @return JSON array string of time series data
   */
  private String extractTimeSeriesData(JsonNode root, RequestContext context) {
    // Look for time series field (varies by API function)
    String[] timeSeriesFields = {
        "Time Series (Daily)",
        "Time Series (Weekly)",
        "Time Series (Monthly)",
        "Weekly Adjusted Time Series",
        "Monthly Adjusted Time Series",
        "Time Series (5min)",
        "Time Series (15min)",
        "Time Series (30min)",
        "Time Series (60min)"
    };

    for (String field : timeSeriesFields) {
      JsonNode timeSeries = root.path(field);
      if (!timeSeries.isMissingNode() && timeSeries.isObject()) {
        return convertTimeSeriesMapToArray(timeSeries, field, context);
      }
    }

    // Check for other data arrays (e.g., Global Quote, Search Results)
    if (root.has("Global Quote")) {
      JsonNode quote = root.path("Global Quote");
      if (!quote.isMissingNode() && quote.isObject()) {
        LOGGER.debug("AlphaVantage: Extracted Global Quote data");
        return "[" + quote.toString() + "]";
      }
    }

    if (root.has("bestMatches")) {
      JsonNode matches = root.path("bestMatches");
      if (matches.isArray()) {
        LOGGER.debug("AlphaVantage: Extracted {} search matches", matches.size());
        return matches.toString();
      }
    }

    // No recognized data structure
    LOGGER.warn("AlphaVantage: No recognized time series data for {}. Fields: {}",
        context.getUrl(), iteratorToString(root.fieldNames()));
    return "[]";
  }

  /**
   * Converts Alpha Vantage time series map format to array format.
   *
   * <p>Input format: {"2024-12-16": {"1. open": "100.00", ...}, ...}
   * <p>Output format: [{"date": "2024-12-16", "open": "100.00", ...}, ...]
   *
   * @param timeSeries The time series map node
   * @param field The field name (for logging)
   * @param context Request context for logging
   * @return JSON array string
   */
  private String convertTimeSeriesMapToArray(JsonNode timeSeries, String field,
      RequestContext context) {

    ArrayNode arrayNode = MAPPER.createArrayNode();
    Iterator<Map.Entry<String, JsonNode>> entries = timeSeries.fields();

    while (entries.hasNext()) {
      Map.Entry<String, JsonNode> entry = entries.next();
      String date = entry.getKey();
      JsonNode dayData = entry.getValue();

      ObjectNode record = MAPPER.createObjectNode();
      record.put("date", date);

      // Normalize Alpha Vantage field names (remove number prefixes)
      Iterator<Map.Entry<String, JsonNode>> fields = dayData.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> fieldEntry = fields.next();
        String originalName = fieldEntry.getKey();
        String normalizedName = normalizeFieldName(originalName);
        record.set(normalizedName, fieldEntry.getValue());
      }

      arrayNode.add(record);
    }

    LOGGER.debug("AlphaVantage: Extracted {} records from {}", arrayNode.size(), field);
    return arrayNode.toString();
  }

  /**
   * Normalizes Alpha Vantage field names by removing number prefixes.
   *
   * <p>Examples:
   * <ul>
   *   <li>"1. open" -&gt; "open"</li>
   *   <li>"2. high" -&gt; "high"</li>
   *   <li>"5. volume" -&gt; "volume"</li>
   * </ul>
   *
   * @param fieldName The original field name
   * @return Normalized field name
   */
  private String normalizeFieldName(String fieldName) {
    // Pattern: "N. fieldname" -> "fieldname"
    if (fieldName.length() > 3 && Character.isDigit(fieldName.charAt(0))) {
      int dotIndex = fieldName.indexOf(". ");
      if (dotIndex > 0 && dotIndex < fieldName.length() - 2) {
        return fieldName.substring(dotIndex + 2);
      }
    }
    return fieldName;
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
