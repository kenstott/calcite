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

      // Check for API error
      JsonNode error = results.path("Error");
      if (!error.isMissingNode()) {
        String errorCode = error.path("APIErrorCode").asText("UNKNOWN");
        String errorDesc = error.path("APIErrorDescription").asText("No description");

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
