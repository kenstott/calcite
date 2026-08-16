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
 * Transforms Bureau of Economic Analysis (BEA) API responses.
 *
 * <p>BEA API responses have a nested structure:
 * <pre>{@code
 * {
 *   "BEAAPI": {
 *     "Request": { ... },
 *     "Results": {
 *       "Data": [ ... ]   // Success: array of data records
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Error responses contain:
 * <pre>{@code
 * {
 *   "BEAAPI": {
 *     "Results": {
 *       "Error": {
 *         "APIErrorCode": "...",
 *         "APIErrorDescription": "..."
 *       }
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>This transformer:
 * <ul>
 *   <li>Checks for API errors in the response</li>
 *   <li>Extracts the Data array for downstream processing</li>
 *   <li>Handles missing data gracefully (returns empty array)</li>
 * </ul>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class BeaResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeaResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("BEA: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode beaApi = root.path("BEAAPI");

      if (beaApi.isMissingNode()) {
        // Response doesn't have expected BEA structure
        LOGGER.warn("BEA: Response missing BEAAPI root node for {}", context.getUrl());
        return response; // Return as-is for non-standard responses
      }

      JsonNode results = beaApi.path("Results");

      // Check for API error. Some BEA datasets (observed on MNE/FDI) wrap Error in a
      // single-element array rather than a bare object — unwrap it the same way Results
      // itself is unwrapped below for GDPbyIndustry-style array responses.
      JsonNode error = results.path("Error");
      if (error.isArray() && error.size() > 0) {
        error = error.get(0);
      }
      if (!error.isMissingNode()) {
        // Some BEA error payloads use XML-attribute-style keys ("@APIErrorCode") instead of
        // plain ones; try both rather than silently falling back to a placeholder.
        JsonNode codeNode = error.path("APIErrorCode");
        if (codeNode.isMissingNode()) {
          codeNode = error.path("@APIErrorCode");
        }
        JsonNode descNode = error.path("APIErrorDescription");
        if (descNode.isMissingNode()) {
          descNode = error.path("@APIErrorDescription");
        }
        // Neither known key shape matched: surface the raw node rather than a useless
        // "UNKNOWN: No description" placeholder, so the actual cause is diagnosable.
        String errorCode = codeNode.isMissingNode() ? "UNKNOWN(raw=" + error + ")"
            : codeNode.asText("UNKNOWN");
        String errorDesc = descNode.isMissingNode() ? "" : descNode.asText("No description");

        // Some error codes are expected (e.g., no data for requested parameters)
        // Error code 101 with "Unknown error" typically means invalid parameter combination
        boolean isNoDataError = "NO_DATA".equals(errorCode)
            || "PARAMETER_EMPTY".equals(errorCode)
            || ("101".equals(errorCode) && errorDesc.toLowerCase().contains("unknown error"));
        if (isNoDataError) {
          LOGGER.debug("BEA: No data available for request: {} - {}",
              errorCode, errorDesc);
          return "[]";
        }

        // Log dimension values for debugging
        String dimensionInfo = context.getDimensionValues().isEmpty()
            ? ""
            : " [dimensions: " + context.getDimensionValues() + "]";

        LOGGER.error("BEA API error: {} - {}{}", errorCode, errorDesc, dimensionInfo);
        throw new RuntimeException("BEA API error " + errorCode + ": " + errorDesc);
      }

      // Extract data array - handle both dict and array Results structures
      // Some BEA APIs return Results as a dict: Results.Data
      // Others (like GDPbyIndustry) return Results as an array: Results[0].Data
      JsonNode dataSource = results;
      if (results.isArray() && results.size() > 0) {
        // Results is an array, get first element
        dataSource = results.get(0);
        LOGGER.debug("BEA: Results is an array, using first element");
      }

      JsonNode data = dataSource.path("Data");

      // Handle single object Data (ITA API returns single object for single-year queries)
      if (!data.isMissingNode() && data.isObject()) {
        LOGGER.debug("BEA: Data is single object, wrapping in array");
        return "[" + data.toString() + "]";
      }

      if (data.isMissingNode() || !data.isArray()) {
        // Check for ParamValue (used by GetParameterValues API calls)
        JsonNode paramValue = dataSource.path("ParamValue");
        if (!paramValue.isMissingNode() && paramValue.isArray()) {
          LOGGER.debug("BEA: Returning ParamValue array with {} elements", paramValue.size());
          return paramValue.toString();
        }

        // Log what IS in the Results to help debug missing data issues
        if (LOGGER.isDebugEnabled()) {
          StringBuilder fields = new StringBuilder();
          java.util.Iterator<String> fieldNames = dataSource.fieldNames();
          while (fieldNames.hasNext()) {
            if (fields.length() > 0) {
              fields.append(", ");
            }
            fields.append(fieldNames.next());
          }
          LOGGER.debug("BEA: No Data array in response for {} - Results contains: [{}]{}",
              context.getUrl(), fields.toString(),
              context.getDimensionValues().isEmpty() ? ""
                  : " [dimensions: " + context.getDimensionValues() + "]");
        }
        return "[]";
      }

      LOGGER.debug("BEA: Extracted {} data records", data.size());
      return data.toString();

    } catch (RuntimeException e) {
      // Re-throw runtime exceptions (including our API error exceptions)
      throw e;
    } catch (Exception e) {
      LOGGER.error("BEA: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse BEA response: " + e.getMessage(), e);
    }
  }
}
