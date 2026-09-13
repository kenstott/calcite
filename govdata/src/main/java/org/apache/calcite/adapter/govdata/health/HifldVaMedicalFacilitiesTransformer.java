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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.govdata.housing.AbstractArcGisFeatureTransformer;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps the HIFLD Veterans Health Administration Medical Facilities FeatureServer's
 * attributes into {@code health.va_medical_facilities} rows. Reuses the same paginating
 * base ({@link AbstractArcGisFeatureTransformer}) as sibling ArcGIS-sourced tables in
 * the housing schema (see HudOpportunityZonesTransformer).
 *
 * <p>Source field names come from VA's own VAST database column vocabulary (STA_NO,
 * S_ABBR, S_ADD1, etc.) — see this table's schema comment for the meaning of each.
 */
public class HifldVaMedicalFacilitiesTransformer extends AbstractArcGisFeatureTransformer {

  @Override protected Map<String, Object> mapAttributes(JsonNode attrs) {
    String staNo = text(attrs, "STA_NO");
    if (staNo == null) {
      return null;
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("station_number", staNo);
    row.put("parent_station_number", text(attrs, "PAR_STA_NO"));
    row.put("facility_type", text(attrs, "S_ABBR"));
    row.put("facility_name", text(attrs, "STA_NAME"));
    row.put("street_address", text(attrs, "S_ADD1"));
    row.put("city", text(attrs, "S_CITY"));
    row.put("state", text(attrs, "S_STATE"));
    row.put("zip", text(attrs, "S_ZIP"));
    row.put("county_name", text(attrs, "CNAME"));
    row.put("latitude", attrs.has("LAT") ? dbl(attrs.get("LAT")) : null);
    row.put("longitude", attrs.has("LON") ? dbl(attrs.get("LON")) : null);
    row.put("homeless_poc", text(attrs, "Homeless_POC"));
    row.put("homeless_poc_email", text(attrs, "Homeless_POC_Email"));
    return row;
  }
}
