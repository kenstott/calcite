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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms BLM ArcGIS FeatureServer responses into {@code blm_field_offices} rows.
 *
 * <p>Input: ArcGIS query JSON from
 * {@code admin_boundaries/BLM_Natl_AdminUnit_Generalized/FeatureServer/0/query}.
 * {@code ADMIN_ST} is a 2-character state abbreviation (not FIPS). {@code GIS_ACRES} is
 * not available in this endpoint.
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "ADM_UNIT_CD": "UTMO",
 *         "ADMU_NAME": "Moab Field Office",
 *         "BLM_ORG_TYPE": "Field Office",
 *         "ADMIN_ST": "UT"
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code blm_field_offices} schema.
 */
public class BlmFieldOfficeTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BlmFieldOfficeTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("blm_field_offices: empty response from BLM ArcGIS");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      if (root.path("exceededTransferLimit").asBoolean(false)) {
        throw new RuntimeException(
            "blm_field_offices: ArcGIS response exceeded transfer limit — "
                + "increase resultRecordCount in lands-schema.yaml or add resultOffset pagination");
      }

      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("blm_field_offices: no 'features' array in ArcGIS response");
        return "[]";
      }

      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        if (attrs.isMissingNode()) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("office_code", textOrNull(attrs, "ADM_UNIT_CD"));
        row.put("office_name", textOrNull(attrs, "ADMU_NAME"));
        row.put("office_type", textOrNull(attrs, "BLM_ORG_TYPE"));
        row.put("state_abbr", textOrNull(attrs, "ADMIN_ST"));
        result.add(row);
      }

      LOGGER.debug("blm_field_offices: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("blm_field_offices: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("blm_field_offices transform failed", e);
    }
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }
}
