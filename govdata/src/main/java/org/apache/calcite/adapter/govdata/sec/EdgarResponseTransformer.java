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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms SEC EDGAR API responses.
 *
 * <p>SEC EDGAR API responses contain company submission data with the following structure:
 * <pre>{@code
 * {
 *   "cik": "0000320193",
 *   "entityType": "operating",
 *   "sic": "3571",
 *   "sicDescription": "Electronic Computers",
 *   "name": "Apple Inc.",
 *   "tickers": ["AAPL"],
 *   "exchanges": ["Nasdaq"],
 *   "filings": {
 *     "recent": {
 *       "accessionNumber": [...],
 *       "filingDate": [...],
 *       "form": [...],
 *       "primaryDocument": [...]
 *     },
 *     "files": [...]
 *   }
 * }
 * }</pre>
 *
 * <p>Error responses from SEC are typically HTTP errors (404, 429 for rate limit).
 * The SEC limits requests to 10 per second.
 *
 * <p>This transformer:
 * <ul>
 *   <li>Validates the response contains expected EDGAR structure</li>
 *   <li>Extracts filings.recent data for downstream processing</li>
 *   <li>Handles rate limit (429) errors from SEC</li>
 *   <li>Returns empty array for missing or invalid CIKs</li>
 * </ul>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class EdgarResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdgarResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EDGAR: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check for error response (SEC sometimes returns JSON errors)
      if (root.has("error") || root.has("message")) {
        return handleErrorResponse(root, context);
      }

      // Validate expected EDGAR structure
      if (!root.has("cik")) {
        LOGGER.warn("EDGAR: Response missing 'cik' field for {}", context.getUrl());
        return response; // Return as-is for non-standard responses
      }

      // Extract company information
      String cik = root.path("cik").asText();
      String entityName = root.path("name").asText();
      LOGGER.debug("EDGAR: Processing submissions for {} (CIK: {})", entityName, cik);

      // Check for filings data
      JsonNode filings = root.path("filings");
      if (filings.isMissingNode()) {
        LOGGER.debug("EDGAR: No filings data for CIK {}", cik);
        return "[]";
      }

      // Extract recent filings
      JsonNode recent = filings.path("recent");
      if (recent.isMissingNode()) {
        LOGGER.debug("EDGAR: No recent filings for CIK {}", cik);
        return "[]";
      }

      // Convert to array of filing records for processing
      JsonNode forms = recent.path("form");
      if (!forms.isArray() || forms.isEmpty()) {
        LOGGER.debug("EDGAR: Empty filings array for CIK {}", cik);
        return "[]";
      }

      LOGGER.debug("EDGAR: Extracted {} filing records for CIK {}",
          forms.size(), cik);

      // Return the recent filings as-is (it's already structured data)
      return recent.toString();

    } catch (RuntimeException e) {
      // Re-throw runtime exceptions
      throw e;
    } catch (Exception e) {
      LOGGER.error("EDGAR: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse EDGAR response: " + e.getMessage(), e);
    }
  }

  /**
   * Handles an error response from SEC EDGAR.
   *
   * @param root The parsed JSON root node
   * @param context Request context for logging
   * @return Empty array for recoverable errors
   * @throws RuntimeException for non-recoverable errors
   */
  private String handleErrorResponse(JsonNode root, RequestContext context) {
    String errorMessage = root.has("message")
        ? root.path("message").asText()
        : root.path("error").asText("Unknown error");

    String lowerMessage = errorMessage.toLowerCase();

    // Log dimension values for debugging
    String dimensionInfo = context.getDimensionValues().isEmpty()
        ? ""
        : " [dimensions: " + context.getDimensionValues() + "]";

    // Rate limit error - SEC allows 10 requests per second
    if (lowerMessage.contains("rate limit") || lowerMessage.contains("too many requests")) {
      LOGGER.warn("EDGAR: Rate limit exceeded (10 req/sec) for {}{}", context.getUrl(), dimensionInfo);
      throw new RuntimeException("EDGAR rate limit exceeded: " + errorMessage);
    }

    // Not found - typically invalid CIK
    if (lowerMessage.contains("not found") || lowerMessage.contains("no data")) {
      LOGGER.debug("EDGAR: Entity not found for {}{}", context.getUrl(), dimensionInfo);
      return "[]";
    }

    // Server error
    if (lowerMessage.contains("server error") || lowerMessage.contains("internal error")) {
      LOGGER.warn("EDGAR: Server error for {}: {}", context.getUrl(), errorMessage);
      throw new RuntimeException("EDGAR server error: " + errorMessage);
    }

    LOGGER.error("EDGAR API error: {}{}", errorMessage, dimensionInfo);
    throw new RuntimeException("EDGAR API error: " + errorMessage);
  }
}
