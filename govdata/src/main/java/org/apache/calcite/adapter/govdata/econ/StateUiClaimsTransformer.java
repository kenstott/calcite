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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps a row of the DOL Employment and Training Administration ETA 539 bulk file
 * ({@code https://oui.doleta.gov/unemploy/csv/ar539.csv}) into the {@code state_ui_claims} table.
 *
 * <p>The file has headers {@code st, rptdate, c1..c23}. Only the columns whose meaning was
 * checked against published figures are mapped: c3 initial claims and c8 continued weeks
 * claimed (Louisiana, report week 2026-10-17: 1,179 and 5,295, as quoted by the state), the
 * neighbouring federal-employee and ex-servicemember columns, and c18/c19 covered employment
 * and the insured unemployment rate.
 */
public class StateUiClaimsTransformer implements RowTransformer {

  /** Output column -> source header. */
  private static final String[][] STRING_FIELDS = {
      {"state", "st"},
      {"week_ending", "rptdate"},
      {"continued_week_ending", "c2"},
  };

  private static final String[][] INT_FIELDS = {
      {"week_number", "c1"},
      {"initial_claims", "c3"},
      {"initial_claims_ucfe", "c4"},
      {"initial_claims_ucx", "c5"},
      {"continued_weeks_claimed", "c8"},
      {"continued_weeks_claimed_ucfe", "c9"},
      {"continued_weeks_claimed_ucx", "c10"},
      {"covered_employment", "c18"},
  };

  @Override public List<Map<String, Object>> transform(Map<String, Object> row,
      RowContext context) {
    Map<String, Object> out = new LinkedHashMap<String, Object>();
    for (String[] m : STRING_FIELDS) {
      out.put(m[0], text(row.get(m[1])));
    }
    for (String[] m : INT_FIELDS) {
      String v = text(row.get(m[1]));
      out.put(m[0], v == null ? null : Long.valueOf(v));
    }
    String rate = text(row.get("c19"));
    out.put("insured_unemployment_rate", rate == null ? null : Double.valueOf(rate));
    return Collections.<Map<String, Object>>singletonList(out);
  }

  private static String text(Object o) {
    if (o == null) {
      return null;
    }
    String s = o.toString().trim();
    return s.isEmpty() ? null : s;
  }
}
