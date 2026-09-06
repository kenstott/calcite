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
package org.apache.calcite.adapter.govdata.transport;

import java.util.regex.Pattern;

/**
 * Appends PHMSA gas-distribution annual mileage report rows for 2010-2016 —
 * published only as whole-workbook XLSX files (one per year, single sheet
 * named {@code GD AR <year>}) — to the 2017+ per-year CSV rows already merged
 * by {@code extractPattern}.
 *
 * <p>Confirmed live: the 2010-2014 workbooks (237 columns) have no COMMODITY
 * field at all — it was added starting with the 2015 workbook (278 columns,
 * matching the 2017+ CSV header exactly) — so rows from 2010-2014 carry a
 * NULL commodity via the base class's by-name column lookup rather than a
 * guessed value. Every other target field (REPORT_NUMBER, OPERATOR_ID,
 * MMILES_TOTAL, NUM_SRVCS_TOTAL, the MMILES_BY_DCD_* decade columns,
 * REPORT_SUBMISSION_TYPE) is present in every year 2010-2016, and
 * REPORT_NUMBER remains a clean unique key throughout (no duplicates found
 * for any inspected year).
 */
public class PhmsaGasDistributionMileageHistoricalTransformer
    extends PhmsaHistoricalMileageXlsxTransformer {

  private static final Pattern XLSX_ENTRY = Pattern.compile("annual_gas_distribution_(\\d{4})\\.xlsx");

  @Override protected Pattern xlsxEntryPattern() {
    return XLSX_ENTRY;
  }

  @Override protected int firstHistoricalYear() {
    return 2010;
  }

  @Override protected int lastHistoricalYear() {
    return 2016;
  }

  @Override protected String sheetName(int year) {
    return "GD AR " + year;
  }

  @Override protected int headerRowIndex() {
    return 2;
  }

  @Override protected String[] neededColumns() {
    return new String[] {
        "REPORT_YEAR", "REPORT_NUMBER", "OPERATOR_ID", "OPERATOR_NAME",
        "OFFICE_ADDRESS_STATE", "COMMODITY", "MMILES_TOTAL", "NUM_SRVCS_TOTAL",
        "MMILES_BY_DCD_PRE1940", "MMILES_BY_DCD_2010_TO_2019", "MMILES_BY_DCD_2020_TO_2029",
        "REPORT_SUBMISSION_TYPE"
    };
  }
}
