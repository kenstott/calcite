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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms the Federal Register agencies list response into flat rows.
 *
 * <p>Input: JSON array from {@code https://api.federalregister.gov/v1/agencies.json}
 * <pre>
 * [
 *   {
 *     "id": 199,
 *     "name": "Environmental Protection Agency",
 *     "short_name": "EPA",
 *     "slug": "environmental-protection-agency",
 *     "url": "https://www.federalregister.gov/agencies/environmental-protection-agency",
 *     "parent_id": null
 *   },
 *   ...
 * ]
 * </pre>
 *
 * <p>Output: flat JSON array suitable for parquet materialization.
 * The response is already a flat array so transformation is minimal —
 * primarily field selection and null normalization.
 */
public class FedRegisterAgenciesTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FedRegisterAgenciesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("FedRegister Agencies: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isArray()) {
        LOGGER.warn("FedRegister Agencies: expected JSON array, got {}", root.getNodeType());
        return "[]";
      }

      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode agency : root) {
        ObjectNode row = MAPPER.createObjectNode();

        // id — integer
        JsonNode id = agency.get("id");
        if (id != null && id.isNumber()) {
          row.put("id", id.intValue());
        } else {
          row.putNull("id");
        }

        row.put("name", getTextOrNull(agency, "name"));
        row.put("short_name", getTextOrNull(agency, "short_name"));
        row.put("slug", getTextOrNull(agency, "slug"));
        row.put("url", getTextOrNull(agency, "url"));

        // parent_id — integer or null
        JsonNode parentId = agency.get("parent_id");
        if (parentId != null && parentId.isNumber()) {
          row.put("parent_id", parentId.intValue());
        } else {
          row.putNull("parent_id");
        }

        result.add(row);
      }

      LOGGER.info("FedRegister Agencies: transformed {} agencies", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("FedRegister Agencies: failed to parse response: {}", e.getMessage());
      return "[]";
    }
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull() || value.isMissingNode()) {
      return null;
    }
    String text = value.asText();
    return text.isEmpty() ? null : text;
  }
}
