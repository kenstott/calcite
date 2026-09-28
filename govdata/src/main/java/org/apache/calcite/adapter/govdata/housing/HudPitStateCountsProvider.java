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
 * Fetches HUD's national Point-in-Time (PIT) homeless count workbook at state grain (one row per
 * state/territory per year, melted into long format) and maps it into
 * {@code hud_pit_counts_by_state} rows. See {@link AbstractHudPitCountsDataProvider} for the
 * shared xlsb-parsing and melt logic.
 */
public class HudPitStateCountsProvider extends AbstractHudPitCountsDataProvider {

  private static final String URL =
      "https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-State.xlsb";

  private static final Set<String> ID_COLUMNS = new HashSet<String>(Arrays.asList(
      "State", "Number of CoCs"));

  @Override protected String workbookUrl() {
    return URL;
  }

  @Override protected Set<String> idColumnNames() {
    return ID_COLUMNS;
  }

  @Override protected Map<String, Object> buildRow(int year, Map<String, String> idValues,
      String metricName, double count) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("type", "hud_pit_counts_by_state");
    row.put("year", Integer.valueOf(year));
    row.put("state_abbr", idValues.get("State"));
    row.put("number_of_cocs", parseIntOrNull(idValues.get("Number of CoCs")));
    row.put("metric_name", metricName);
    row.put("count", Integer.valueOf((int) Math.round(count)));
    return row;
  }

  private static Integer parseIntOrNull(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.valueOf((int) Math.round(Double.parseDouble(raw.trim())));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
