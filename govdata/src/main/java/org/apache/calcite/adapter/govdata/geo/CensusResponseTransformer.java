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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
 * <p><b>Memory:</b> the 2-D data-API table — by far the largest shape (the
 * intltrade {@code statehs}/{@code hs} timeseries returns ~2M rows per month) —
 * is transformed by <i>streaming</i>: a {@link JsonParser} reads the input row by
 * row and a {@link JsonGenerator} writes the output objects directly, so the full
 * parsed node tree and the rebuilt object array are never held in memory at once.
 * Only the small error/wrapper/object-array shapes fall back to a full tree parse.
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class CensusResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CensusResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Census Bureau API suppression/unavailability sentinel integers.
  // These must be coerced to NULL before writing — storing them as real
  // numeric values would corrupt aggregations and fail range checks.
  //   -666666666  suppressed (privacy or reliability threshold not met)
  //   -333333333  median in open-ended top/bottom interval
  //   -222222222  not applicable for this geography / estimate type
  //   -999999999  unknown
  private static final java.util.Set<String> CENSUS_SENTINELS =
      new java.util.HashSet<String>(java.util.Arrays.asList(
          "-666666666", "-333333333", "-222222222", "-999999999"));

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Census: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      // Peek at the leading tokens to detect the large 2-D table shape without
      // materialising the whole document. Only this shape is streamed; every
      // other shape (errors, wrapper objects, already-object arrays) is small
      // for these endpoints and handled via a full tree parse below.
      JsonFactory factory = MAPPER.getFactory();
      boolean isTable = false;
      boolean isArray = false;
      boolean emptyArray = false;
      JsonParser peek = factory.createParser(response);
      try {
        if (peek.nextToken() == JsonToken.START_ARRAY) {
          isArray = true;
          JsonToken second = peek.nextToken();
          if (second == JsonToken.START_ARRAY) {
            isTable = true;
          } else if (second == JsonToken.END_ARRAY) {
            emptyArray = true;
          }
        }
      } finally {
        peek.close();
      }

      if (isTable) {
        return streamTableToObjects(response, context);
      }
      if (emptyArray) {
        LOGGER.debug("Census: Empty array for {}", context.getUrl());
        return "[]";
      }

      JsonNode root = MAPPER.readTree(response);

      // Check for error object
      if (root.isObject() && root.has("error")) {
        return handleError(root.path("error").asText(), context);
      }

      // Non-table array (array of strings/objects — e.g. an error string or an
      // already-object array)
      if (isArray || root.isArray()) {
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
   * Handles a non-table array response from Census API.
   *
   * <p>The large 2-D table shape (headers row + data rows) is intercepted and
   * streamed in {@link #transform}; by the time this method is reached the array's
   * first element is known not to be an array. It is either an error string or an
   * already-object array, both of which are small.
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

    // Already an array of objects (or non-error scalars)
    LOGGER.debug("Census: Extracted {} records", root.size());
    return root.toString();
  }

  /**
   * Streams the Census 2-D table format to an array of objects.
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
   * <p>The header row is buffered (a handful of column names); data rows are read
   * and written one at a time, so the millions of rows a monthly trade fetch
   * returns are never all resident as parsed nodes.
   *
   * @param response The raw JSON response
   * @param context Request context (supplies the {@code naics_var} rename)
   * @return JSON array string of objects
   */
  private String streamTableToObjects(String response, RequestContext context)
      throws IOException {
    JsonFactory factory = MAPPER.getFactory();
    StringWriter out = new StringWriter();
    String[] headers;
    int rowCount = 0;

    JsonParser parser = factory.createParser(response);
    JsonGenerator gen = factory.createGenerator(out);
    try {
      parser.nextToken();  // START_ARRAY (outer table)
      parser.nextToken();  // START_ARRAY (header row)
      gen.writeStartArray();

      headers = readHeaderRow(parser, context);

      // Remaining inner arrays are data rows.
      JsonToken tok;
      while ((tok = parser.nextToken()) != JsonToken.END_ARRAY && tok != null) {
        if (tok == JsonToken.START_ARRAY) {
          writeRowObject(parser, gen, headers);
          rowCount++;
        } else if (tok.isStructStart()) {
          // Defensive: unexpected object element inside a 2-D table — skip it.
          parser.skipChildren();
        }
      }

      gen.writeEndArray();
    } finally {
      gen.close();
      parser.close();
    }

    LOGGER.debug("Census: streamed table with {} columns and {} data rows",
        headers.length, rowCount);
    return out.toString();
  }

  /**
   * Reads the header row into a column-name array, applying the vintage-varying
   * request-variable rename.
   *
   * <p>Vintage-varying request variables (e.g. the Nonemployer Statistics NAICS
   * variable NAICS2007/2012/2017/2022, injected per data reference year by
   * NonemployerNaicsDimensionResolver) are renamed to a stable canonical header so
   * the materialization column expression is vintage-independent. Only applies to
   * pipelines that declare a naics_var dimension; cbp and the rest are unaffected.
   *
   * @param parser Positioned at the header row's {@code START_ARRAY}
   * @param context Request context (supplies {@code naics_var})
   * @return The column names
   */
  private String[] readHeaderRow(JsonParser parser, RequestContext context)
      throws IOException {
    List<String> hdr = new ArrayList<String>();
    JsonToken t;
    while ((t = parser.nextToken()) != JsonToken.END_ARRAY && t != null) {
      hdr.add(parser.getValueAsString());
    }
    String[] headers = hdr.toArray(new String[0]);

    String naicsVar = context.getDimensionValues().get("naics_var");
    if (naicsVar != null) {
      for (int i = 0; i < headers.length; i++) {
        if (naicsVar.equals(headers[i])) {
          headers[i] = "NAICS";
        }
      }
    }
    return headers;
  }

  /**
   * Writes a single data row (an inner array) as a JSON object keyed by headers.
   *
   * <p>Textual Census suppression/unavailability sentinels are coerced to null;
   * all other values are copied verbatim (numbers stay numbers). Cells beyond the
   * header count are ignored, mirroring the min(headers, row) bound of the prior
   * tree-based conversion.
   *
   * @param parser Positioned at the data row's {@code START_ARRAY}
   * @param gen Output generator
   * @param headers Column names
   */
  private void writeRowObject(JsonParser parser, JsonGenerator gen, String[] headers)
      throws IOException {
    gen.writeStartObject();
    int col = 0;
    JsonToken t;
    while ((t = parser.nextToken()) != JsonToken.END_ARRAY && t != null) {
      if (col < headers.length) {
        String name = headers[col];
        if (t == JsonToken.VALUE_STRING) {
          String v = parser.getText();
          if (CENSUS_SENTINELS.contains(v)) {
            gen.writeNullField(name);
          } else {
            gen.writeStringField(name, v);
          }
        } else {
          gen.writeFieldName(name);
          gen.copyCurrentStructure(parser);
        }
      } else if (t.isStructStart()) {
        // Extra cell beyond the header count — consume and discard.
        parser.skipChildren();
      }
      col++;
    }
    gen.writeEndObject();
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
  @SuppressWarnings("UnusedVariable")
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
