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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Generates the {@code ref.fiscal_year} dimension: one row per US federal fiscal year, the
 * conformed FK target for anything keyed by fiscal year. The federal fiscal year begins Oct 1
 * and is labelled by the calendar year in which it ends — FY2024 runs 2023-10-01 … 2024-09-30,
 * with quarters Q1 Oct-Dec, Q2 Jan-Mar, Q3 Apr-Jun, Q4 Jul-Sep.
 *
 * <p>The span covers every fiscal year touched by the {@code ref.calendar} date spine: the
 * calendar starts 2010-01-01 (inside FY2010) and extends two years past the current year, so
 * fiscal years FY2010 through FY(currentYear + 3) are emitted. The table is tiny (~20 rows) and
 * fully regenerated (overwrite) on each run.
 */
public class FiscalYearDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiscalYearDataProvider.class);

  /** Calendar start of the spine (2010-01-01) falls inside FY2010, the first fiscal year. */
  private static final int FIRST_FISCAL_YEAR = 2010;
  /** Calendar extends currentYear + 2; the last such day (Dec of currentYear+2) is in FY(currentYear+3). */
  private static final int FORWARD_FISCAL_YEARS = 3;

  public FiscalYearDataProvider() {
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) {
    int lastFiscalYear = resolveCurrentYear() + FORWARD_FISCAL_YEARS;
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int fy = FIRST_FISCAL_YEAR; fy <= lastFiscalYear; fy++) {
      rows.add(buildRow(fy));
    }
    LOGGER.info("FiscalYearDataProvider: generated {} fiscal years (FY{}..FY{})",
        rows.size(), FIRST_FISCAL_YEAR, lastFiscalYear);
    return rows.iterator();
  }

  private int resolveCurrentYear() {
    String override = System.getProperty("GOVDATA_CURRENT_YEAR");
    if (override != null && !override.isEmpty()) {
      return Integer.parseInt(override.trim());
    }
    return LocalDate.now(ZoneOffset.UTC).getYear();
  }

  private Map<String, Object> buildRow(int fiscalYear) {
    LocalDate start = LocalDate.of(fiscalYear - 1, 10, 1);         // Oct 1 of prior calendar year
    LocalDate end = LocalDate.of(fiscalYear, 9, 30);               // Sep 30 of the FY's calendar year
    LocalDate q1Start = start;                                     // Oct 1
    LocalDate q2Start = LocalDate.of(fiscalYear, 1, 1);            // Jan 1
    LocalDate q3Start = LocalDate.of(fiscalYear, 4, 1);           // Apr 1
    LocalDate q4Start = LocalDate.of(fiscalYear, 7, 1);           // Jul 1

    Map<String, Object> r = new HashMap<String, Object>();
    r.put("fiscal_year", fiscalYear);
    r.put("fiscal_year_label", "FY" + fiscalYear);
    r.put("start_date", start.toString());        // type date (ISO string coerces to DATE)
    r.put("end_date", end.toString());
    r.put("start_iso_date", start.toString());    // string form → FK into ref.calendar.iso_date
    r.put("end_iso_date", end.toString());
    r.put("calendar_years", (fiscalYear - 1) + "-" + fiscalYear);
    r.put("days_in_fiscal_year", (int) (end.toEpochDay() - start.toEpochDay()) + 1);
    r.put("q1_start_date", q1Start.toString());
    r.put("q2_start_date", q2Start.toString());
    r.put("q3_start_date", q3Start.toString());
    r.put("q4_start_date", q4Start.toString());
    r.put("prior_fiscal_year", fiscalYear - 1);
    r.put("next_fiscal_year", fiscalYear + 1);
    return r;
  }
}
