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

import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transforms USDA FS FACTS ArcGIS MapServer responses into {@code timber_sales} rows.
 *
 * <p>Input: ArcGIS query JSON from {@code EDW_TimberHarvest_01/MapServer/{layer}/query}. This is
 * harvest activity data (activity-level, not sale contracts). The service hard-caps
 * {@code maxRecordCount} at 2000 regardless of the requested {@code resultRecordCount} and
 * signals truncation via {@code exceededTransferLimit} — paginating on that signal (via
 * {@link AbstractArcGisFeatureTransformer}) is required; a single unpaginated request silently
 * returns only the first 2000 rows of a year that can hold 4x that.
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "facts_id": "OR095000008",
 *         "sale_name": "Desolation Salvage",
 *         "fy_completed": "2022",
 *         "fy_awarded": "2021",
 *         "admin_forest_code": "0601",
 *         "admin_forest_name": "Willamette National Forest",
 *         "admin_district_code": "0605",
 *         "admin_district_name": "McKenzie River Ranger District",
 *         "state_abbr": "OR",
 *         "gis_acres": 45.3,
 *         "activity_code": "6101",
 *         "activity_name": "Timber Sale Preparation",
 *         "treatment_type": "Salvage",
 *         "nbr_units_accomplished": 450.0,
 *         "uom": "CCF",
 *         "cost_per_uom": 85.0
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Output: one row per feature, columns matching the {@code timber_sales} schema.
 */
public class UsfsFactsTransformer extends AbstractArcGisFeatureTransformer {

  @Override protected Map<String, Object> mapAttributes(JsonNode attrs) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("facts_id", text(attrs, "facts_id"));
    row.put("sale_name", text(attrs, "sale_name"));
    row.put("fy_completed", intg(attrs.path("fy_completed")));
    row.put("fy_awarded", intg(attrs.path("fy_awarded")));
    row.put("forest_code", text(attrs, "admin_forest_code"));
    row.put("forest_name", text(attrs, "admin_forest_name"));
    row.put("district_code", text(attrs, "admin_district_code"));
    row.put("district_name", text(attrs, "admin_district_name"));
    row.put("state_abbr", text(attrs, "state_abbr"));
    row.put("gis_acres", dbl(attrs.path("gis_acres")));
    row.put("activity_code", text(attrs, "activity_code"));
    row.put("activity_name", text(attrs, "activity_name"));
    row.put("treatment_type", text(attrs, "treatment_type"));
    row.put("units_accomplished", dbl(attrs.path("nbr_units_accomplished")));
    row.put("uom", text(attrs, "uom"));
    row.put("cost_per_uom", dbl(attrs.path("cost_per_uom")));
    return row;
  }
}
