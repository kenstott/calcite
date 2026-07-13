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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps HUD {@code fmr/statedata/{state}} responses into {@code fair_market_rents}
 * rows. The {@code data.counties[]} array carries county-grain FMRs (New England
 * entries may be sub-county town areas, kept distinct by {@code fips_code}); the
 * {@code data.metroareas[]} array is intentionally ignored (metro rollups are
 * derivable from the county rows and would duplicate them).
 */
public class HudFairMarketRentsTransformer extends AbstractHudJsonTransformer {

  @Override protected void emitRows(JsonNode data, ArrayNode out) {
    JsonNode counties = data.path("counties");
    if (!counties.isArray()) {
      return;
    }
    for (JsonNode c : counties) {
      if (!c.isObject()) {
        continue;
      }
      String fips = text(c, "fips_code");
      if (fips == null) {
        continue;
      }
      ObjectNode row = MAPPER.createObjectNode();
      row.put("state_fips", stateFipsOf(fips));
      row.put("county_fips", countyFipsOf(fips));
      row.put("hud_area_code", fips);
      putText(row, "county_name", c, "county_name");
      putText(row, "town_name", c, "town_name");
      putText(row, "metro_name", c, "metro_name");
      putText(row, "smallarea_status", c, "smallarea_status");
      putInt(row, "fmr_percentile", c, "FMR Percentile");
      putInt(row, "fmr_efficiency", c, "Efficiency");
      putInt(row, "fmr_1br", c, "One-Bedroom");
      putInt(row, "fmr_2br", c, "Two-Bedroom");
      putInt(row, "fmr_3br", c, "Three-Bedroom");
      putInt(row, "fmr_4br", c, "Four-Bedroom");
      out.add(row);
    }
  }

  private static String text(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    return v.isMissingNode() || v.isNull() || v.asText().isEmpty() ? null : v.asText();
  }
}
