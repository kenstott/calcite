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
 * Transforms USDA FS FACTS ArcGIS FeatureServer responses into {@code timber_sales} rows.
 *
 * <p>Input: ArcGIS query JSON from {@code EDW_TimberSaleProgram_01/FeatureServer/0/query}
 * filtered by {@code SALEYEAR={sale_year}}.
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "CONTRACT_ID": "OR095000008",
 *         "SALE_NAME": "Desolation Salvage Sale",
 *         "SALEYEAR": 2022,
 *         "FOREST": "0607",
 *         "COUNTY_FIPS": "41043",
 *         "SALETYPE": "Lump Sum",
 *         "VOLUME": 4250.0,
 *         "APPRAISED_VALUE": 318750.0,
 *         "SALE_VALUE": 382500.0,
 *         "SPECIES_GROUP": "Softwood",
 *         "ADVERTISED_DATE": 1651363200000,
 *         "AWARD_DATE": 1659139200000
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Epoch millisecond timestamps are converted to ISO date strings.
 * Output: JSON array string with columns matching the {@code timber_sales} schema.
 */
public class UsfsFactsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsfsFactsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("timber_sales: empty response from USDA FS FACTS ArcGIS");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      if (root.path("exceededTransferLimit").asBoolean(false)) {
        throw new RuntimeException(
            "timber_sales: ArcGIS response exceeded transfer limit (resultRecordCount=5000) — "
                + "increase resultRecordCount in lands-schema.yaml or add resultOffset pagination");
      }

      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("timber_sales: no 'features' array in ArcGIS response");
        return "[]";
      }

      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        if (attrs.isMissingNode()) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("contract_id", textOrNull(attrs, "CONTRACT_ID"));
        row.put("sale_name", textOrNull(attrs, "SALE_NAME"));
        row.put("sale_year", intOrNull(attrs, "SALEYEAR"));
        row.put("forest_id", textOrNull(attrs, "FOREST"));
        row.put("state_fips", resolveStateFips(attrs));
        row.put("county_fips", textOrNull(attrs, "COUNTY_FIPS"));
        row.put("sale_type", textOrNull(attrs, "SALETYPE"));
        row.put("volume_mbf", doubleOrNull(attrs, "VOLUME"));
        row.put("appraised_value", doubleOrNull(attrs, "APPRAISED_VALUE"));
        row.put("sale_value", doubleOrNull(attrs, "SALE_VALUE"));
        row.put("species_group", textOrNull(attrs, "SPECIES_GROUP"));
        row.put("advertised_date", epochToDate(attrs, "ADVERTISED_DATE"));
        row.put("award_date", epochToDate(attrs, "AWARD_DATE"));
        result.add(row);
      }

      LOGGER.debug("timber_sales: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("timber_sales: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("timber_sales transform failed", e);
    }
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private Integer intOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asInt();
  }

  private Double doubleOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asDouble();
  }

  private String resolveStateFips(JsonNode attrs) {
    String countyFips = textOrNull(attrs, "COUNTY_FIPS");
    if (countyFips != null && countyFips.length() >= 2) {
      return countyFips.substring(0, 2);
    }
    return null;
  }

  private String epochToDate(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    long epochMs = val.asLong();
    if (epochMs == 0) {
      return null;
    }
    java.time.LocalDate date =
        java.time.Instant.ofEpochMilli(epochMs)
            .atZone(java.time.ZoneOffset.UTC)
            .toLocalDate();
    return date.toString();
  }
}
