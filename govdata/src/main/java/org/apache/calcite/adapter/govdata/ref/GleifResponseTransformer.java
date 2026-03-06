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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms GLEIF API responses from JSON:API format into flat records.
 *
 * <p>GLEIF uses JSON:API format where entity data is nested under
 * {@code data[].attributes}. This transformer extracts the attributes
 * into flat records suitable for tabular processing.
 *
 * <p>Response structure:
 * <pre>{@code
 * {
 *   "data": [
 *     {
 *       "type": "lei-records",
 *       "id": "5493001KJTIIGC8Y1R12",
 *       "attributes": {
 *         "lei": "5493001KJTIIGC8Y1R12",
 *         "entity": {
 *           "legalName": { "name": "ACME Corp", "language": "en" },
 *           "jurisdiction": "US-DE",
 *           "status": "ACTIVE",
 *           ...
 *         },
 *         "registration": {
 *           "initialRegistrationDate": "2014-01-01",
 *           "lastUpdateDate": "2024-01-01",
 *           "nextRenewalDate": "2025-01-01",
 *           "status": "ISSUED"
 *         }
 *       }
 *     }
 *   ],
 *   "links": {
 *     "next": "https://api.gleif.org/api/v1/lei-records?page[after]=..."
 *   }
 * }
 * }</pre>
 *
 * @see ResponseTransformer
 */
public class GleifResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GleifResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("GLEIF: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // Check for error response
      JsonNode errors = root.path("errors");
      if (!errors.isMissingNode() && errors.isArray() && errors.size() > 0) {
        return handleErrorResponse(errors, context);
      }

      // Extract data array (JSON:API format)
      JsonNode data = root.path("data");
      if (data.isMissingNode()) {
        LOGGER.warn("GLEIF: Response missing 'data' field for {}", context.getUrl());
        return "[]";
      }

      // Handle single object (detail endpoint)
      if (data.isObject()) {
        ObjectNode flat = flattenLeiRecord(data);
        if (flat != null) {
          return "[" + flat.toString() + "]";
        }
        return "[]";
      }

      if (!data.isArray()) {
        LOGGER.warn("GLEIF: Unexpected data type for {}", context.getUrl());
        return "[]";
      }

      if (data.isEmpty()) {
        LOGGER.debug("GLEIF: Empty data array for {}", context.getUrl());
        return "[]";
      }

      // Flatten each record from JSON:API format
      ArrayNode flatRecords = MAPPER.createArrayNode();
      for (JsonNode record : data) {
        ObjectNode flat = flattenLeiRecord(record);
        if (flat != null) {
          flatRecords.add(flat);
        }
      }

      LOGGER.debug("GLEIF: Extracted {} records from {}", flatRecords.size(), context.getUrl());
      return flatRecords.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("GLEIF: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse GLEIF response: " + e.getMessage(), e);
    }
  }

  /**
   * Flattens a JSON:API LEI record into a flat object.
   */
  private ObjectNode flattenLeiRecord(JsonNode record) {
    JsonNode attributes = record.path("attributes");
    if (attributes.isMissingNode()) {
      return null;
    }

    ObjectNode flat = MAPPER.createObjectNode();

    // LEI identifier
    flat.put("lei", attributes.path("lei").asText(""));

    // Entity information
    JsonNode entity = attributes.path("entity");
    if (!entity.isMissingNode()) {
      JsonNode legalName = entity.path("legalName");
      flat.put("legal_name", legalName.path("name").asText(""));
      flat.put("legal_name_language", legalName.path("language").asText(""));

      flat.put("jurisdiction", entity.path("jurisdiction").asText(""));
      flat.put("entity_status", entity.path("status").asText(""));

      // Legal form
      JsonNode legalForm = entity.path("legalForm");
      flat.put("entity_legal_form", legalForm.path("id").asText(""));

      // Registration authority (links to SEC CIK via RA000602)
      JsonNode registrationAuthority = entity.path("registrationAuthority");
      flat.put("registration_authority_id",
          registrationAuthority.path("id").asText(""));
      flat.put("registration_authority_entity_id",
          registrationAuthority.path("otherRegistrationAuthorityEntityID").asText(
              registrationAuthority.path("registrationAuthorityEntityID").asText("")));

      // Headquarters address
      JsonNode hqAddress = entity.path("headquartersAddress");
      flat.put("headquarters_country", hqAddress.path("country").asText(""));
      flat.put("headquarters_city", hqAddress.path("city").asText(""));

      // Legal (registered) address
      JsonNode legalAddress = entity.path("legalAddress");
      flat.put("registered_country", legalAddress.path("country").asText(""));
      flat.put("registered_city", legalAddress.path("city").asText(""));
    }

    // Registration information
    JsonNode registration = attributes.path("registration");
    if (!registration.isMissingNode()) {
      flat.put("registration_date",
          registration.path("initialRegistrationDate").asText(""));
      flat.put("last_update",
          registration.path("lastUpdateDate").asText(""));
      flat.put("next_renewal",
          registration.path("nextRenewalDate").asText(""));
    }

    return flat;
  }

  /**
   * Handles JSON:API error responses from GLEIF.
   */
  private String handleErrorResponse(JsonNode errors, RequestContext context) {
    JsonNode firstError = errors.get(0);
    String status = firstError.path("status").asText("unknown");
    String title = firstError.path("title").asText("Unknown error");
    String detail = firstError.path("detail").asText("");

    String dimensionInfo = context.getDimensionValues().isEmpty()
        ? ""
        : " [dimensions: " + context.getDimensionValues() + "]";

    // Rate limit
    if ("429".equals(status)) {
      LOGGER.warn("GLEIF: Rate limit exceeded for {}{}", context.getUrl(), dimensionInfo);
      throw new RuntimeException("GLEIF rate limit exceeded: " + title);
    }

    // Not found
    if ("404".equals(status)) {
      LOGGER.debug("GLEIF: Resource not found for {}{}", context.getUrl(), dimensionInfo);
      return "[]";
    }

    // Server errors
    if (status.startsWith("5")) {
      LOGGER.warn("GLEIF: Server error ({}) for {}: {}", status, context.getUrl(), title);
      throw new RuntimeException("GLEIF server error " + status + ": " + title);
    }

    LOGGER.error("GLEIF API error ({}) for {}{}: {} - {}",
        status, context.getUrl(), dimensionInfo, title, detail);
    throw new RuntimeException("GLEIF API error " + status + ": " + title);
  }
}
