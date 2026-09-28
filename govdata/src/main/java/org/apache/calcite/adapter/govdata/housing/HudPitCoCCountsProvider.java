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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Fetches HUD's national Point-in-Time (PIT) homeless count workbook at Continuum of Care (CoC)
 * grain (one row per CoC per year, melted into long format) and maps it into
 * {@code hud_pit_counts_by_coc} rows. See {@link AbstractHudPitCountsDataProvider} for the shared
 * xlsb-parsing and melt logic.
 */
public class HudPitCoCCountsProvider extends AbstractHudPitCountsDataProvider {

  private static final String URL =
      "https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb";

  private static final Set<String> ID_COLUMNS = new HashSet<String>(Arrays.asList(
      "CoC Number", "CoC Name", "CoC Category", "Count Types"));

  @Override protected String workbookUrl() {
    return URL;
  }

  @Override protected Set<String> idColumnNames() {
    return ID_COLUMNS;
  }

  @Override protected Map<String, Object> buildRow(int year, Map<String, String> idValues,
      String metricName, double count) {
    // HUD's national total row leaves 'CoC Number' blank (only 'CoC Name' = 'Total' identifies
    // it); substitute a stable sentinel so (year, coc_number, metric_name) stays a non-null PK,
    // and skip the state_abbr derivation for it - "TO" would be a false state code.
    String cocNumber = idValues.get("CoC Number");
    boolean isNationalTotal = cocNumber == null || cocNumber.trim().isEmpty();
    if (isNationalTotal) {
      cocNumber = "TOTAL";
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("type", "hud_pit_counts_by_coc");
    row.put("year", Integer.valueOf(year));
    row.put("coc_number", cocNumber);
    row.put("coc_name", idValues.get("CoC Name"));
    row.put("coc_category", idValues.get("CoC Category"));
    row.put("count_types", idValues.get("Count Types"));
    row.put("state_abbr", isNationalTotal || cocNumber.length() < 2
        ? null : cocNumber.substring(0, 2));
    row.put("metric_name", metricName);
    row.put("count", Integer.valueOf((int) Math.round(count)));
    return row;
  }
}
