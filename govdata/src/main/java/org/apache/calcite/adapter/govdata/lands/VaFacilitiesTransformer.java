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
 * Transforms the HIFLD Veterans Health Administration Medical Facilities ArcGIS
 * FeatureServer response into {@code va_facilities} rows.
 *
 * <p>Input: ArcGIS query JSON, one object per facility under {@code features[].attributes}
 * (see {@code lands-schema.yaml}'s {@code outFields} for the exact field set requested).
 * {@code FIPS} is the 5-digit county FIPS; {@code state_fips} is derived as its leading
 * 2 characters rather than requested separately (the service does not expose it directly).
 */
public class VaFacilitiesTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VaFacilitiesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("va_facilities: empty response from HIFLD ArcGIS FeatureServer");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      if (root.path("exceededTransferLimit").asBoolean(false)) {
        throw new RuntimeException(
            "va_facilities: ArcGIS response exceeded transfer limit — "
                + "increase resultRecordCount in lands-schema.yaml or add resultOffset pagination");
      }

      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("va_facilities: no 'features' array in ArcGIS response");
        return "[]";
      }

      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        if (attrs.isMissingNode()) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("station_number", textOrNull(attrs, "STA_NO"));
        row.put("facility_name", textOrNull(attrs, "NAME"));
        row.put("street_address", textOrNull(attrs, "ADDRESS"));
        row.put("street_address2", textOrNull(attrs, "ADDRESS2"));
        row.put("city", textOrNull(attrs, "CITY"));

        String countyFips = textOrNull(attrs, "FIPS");
        row.put("state_fips", countyFips != null && countyFips.length() >= 2
            ? countyFips.substring(0, 2) : null);
        row.put("state_abbr", textOrNull(attrs, "STATE"));
        row.put("zip", textOrNull(attrs, "ZIP"));
        row.put("county_name", textOrNull(attrs, "COUNTY"));
        row.put("county_fips", countyFips);
        row.put("latitude", doubleOrNull(attrs, "LATITUDE"));
        row.put("longitude", doubleOrNull(attrs, "LONGITUDE"));
        row.put("facility_type", textOrNull(attrs, "PRIM_SVC"));
        row.put("naics_code", textOrNull(attrs, "NAICSCODE"));
        row.put("naics_description", textOrNull(attrs, "NAICSDESCR"));
        row.put("visn", textOrNull(attrs, "VISN"));
        row.put("congressional_district", textOrNull(attrs, "CD115"));
        row.put("phone", textOrNull(attrs, "PHONE"));
        result.add(row);
      }

      LOGGER.debug("va_facilities: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("va_facilities: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("va_facilities transform failed", e);
    }
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private static Double doubleOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asDouble();
  }
}
