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
package org.apache.calcite.adapter.govdata.housing;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps HUD Open Data {@code Opportunity_Zones} FeatureServer features into {@code opportunity_zones}
 * rows. {@code GEOID10} is the 11-digit tract GEOID; {@code STATE} (2-digit) + {@code COUNTY}
 * (3-digit) compose the 5-digit county FIPS; {@code Rural} is {@code Y}/{@code N}.
 */
public class HudOpportunityZonesTransformer extends AbstractArcGisFeatureTransformer {

  @Override protected Map<String, Object> mapAttributes(JsonNode attrs) {
    String geoid = text(attrs, "GEOID10");
    if (geoid == null) {
      return null;
    }
    String state = text(attrs, "STATE");
    String county = text(attrs, "COUNTY");
    String rural = text(attrs, "Rural");
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("census_tract", geoid);
    row.put("state_fips", state);
    row.put("county_fips", state != null && county != null ? state + county : null);
    row.put("state_abbr", text(attrs, "STUSAB"));
    row.put("tract_code", text(attrs, "TRACT"));
    row.put("state_name", text(attrs, "STATE_NAME"));
    row.put("is_rural", rural == null ? null
        : Boolean.valueOf("Y".equalsIgnoreCase(rural) || "1".equals(rural)
            || "Yes".equalsIgnoreCase(rural)));
    return row;
  }
}
