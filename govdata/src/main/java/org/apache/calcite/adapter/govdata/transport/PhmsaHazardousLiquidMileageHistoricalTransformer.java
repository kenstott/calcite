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
 * Appends PHMSA hazardous-liquid annual mileage report rows for 2010-2016 —
 * published only as whole-workbook XLSX files (one per year, sheet
 * {@code HL AR Part A to E}, identical 88-column layout confirmed live across
 * the whole 2010-2016 span and matching the 2017+ CSV era's own header
 * exactly) — to the 2017+ Part-A-to-E CSV rows already merged by
 * {@code extractPattern}.
 */
public class PhmsaHazardousLiquidMileageHistoricalTransformer
    extends PhmsaHistoricalMileageXlsxTransformer {

  private static final Pattern XLSX_ENTRY = Pattern.compile("annual_hazardous_liquid_(\\d{4})\\.xlsx");

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
    return "HL AR Part A to E";
  }

  @Override protected int headerRowIndex() {
    return 2;
  }

  @Override protected String[] neededColumns() {
    return new String[] {
        "REPORT_YEAR", "REPORT_NUMBER", "OPERATOR_ID", "PARTA2NAMEOFCOMP", "PARTA4STATE",
        "PARTA5COMMODITY", "PARTBHCAONSHORE", "PARTBHCAOFFSHORE", "PARTBHCATOTAL",
        "PARTDTOTALMILES", "PARTDONTOTAL", "PARTDOFFTOTAL", "PARTEPRE40TOTAL",
        "PARTE2010TOTAL", "PARTE2020TOTAL", "PARTEUNKNTOTAL", "REPORT_SUBMISSION_TYPE"
    };
  }
}
