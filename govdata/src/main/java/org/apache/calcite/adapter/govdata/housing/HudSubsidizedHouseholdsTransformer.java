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
 * Maps HUD Open Data {@code Snapshot_HUD_Picture_Subsidized_Households} FeatureServer features
 * (the December-2020 "Summary of All HUD Programs" tract snapshot) into {@code hud_subsidized_housing}
 * rows. {@code code} is the 11-digit tract GEOID; {@code CNTY_FIPS} is the 5-digit county FIPS.
 * People/income/percent fields are null-suppressed by HUD for small cells.
 */
public class HudSubsidizedHouseholdsTransformer extends AbstractArcGisFeatureTransformer {

  @Override protected Map<String, Object> mapAttributes(JsonNode attrs) {
    String geoid = text(attrs, "code");
    if (geoid == null) {
      return null;
    }
    String cnty = text(attrs, "CNTY_FIPS");
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("census_tract", geoid);
    row.put("state_fips", firstChars(cnty != null ? cnty : geoid, 2));
    row.put("county_fips", cnty != null ? cnty : firstChars(geoid, 5));
    row.put("state_abbr", text(attrs, "State_ABB"));
    row.put("tract_name", text(attrs, "NAME"));
    row.put("program", text(attrs, "program_label"));
    row.put("total_units", intg(attrs.path("total_units")));
    row.put("people_total", intg(attrs.path("people_total")));
    row.put("people_per_unit", dbl(attrs.path("people_per_unit")));
    row.put("avg_hh_income", intg(attrs.path("hh_income")));
    row.put("pct_disabled", dbl(attrs.path("pct_disabled_all")));
    return row;
  }

  private static String firstChars(String s, int n) {
    return s != null && s.length() >= n ? s.substring(0, n) : null;
  }
}
