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
 * Appends PHMSA gas-transmission-and-gathering annual mileage report rows for
 * 2010-2016 — published only as whole-workbook XLSX files (one per year,
 * sheet {@code GT AR Part A to D}) — to the 2017+ Part-A-to-D CSV rows already
 * merged by {@code extractPattern}.
 *
 * <p>Confirmed live: 2010-2011 workbooks carry 116 columns (vs. 154 for
 * 2012-2016 and the 2017+ CSV era), but every target field this warehouse
 * uses (REPORT_NUMBER, OPERATOR_ID, PARTA5COMMODITY, the HCA mileage summary,
 * PARTDTTOTAL/PARTDGTOTAL/PARTDTOTALMILES, REPORT_SUBMISSION_TYPE) is present
 * in every year from 2010 on — the narrower 2010-2011 layout only omits
 * columns this table never ingests anyway.
 */
public class PhmsaGasTransmissionMileageHistoricalTransformer
    extends PhmsaHistoricalMileageXlsxTransformer {

  private static final Pattern XLSX_ENTRY =
      Pattern.compile("annual_gas_transmission_gathering_(\\d{4})\\.xlsx");

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
    return "GT AR Part A to D";
  }

  @Override protected int headerRowIndex() {
    return 2;
  }

  @Override protected String[] neededColumns() {
    return new String[] {
        "REPORT_YEAR", "REPORT_NUMBER", "OPERATOR_ID", "PARTA2NAMEOFCOMP", "PARTA4STATE",
        "PARTA5COMMODITY", "PARTBHCAONSHORE", "PARTBHCAOFFSHORE", "PARTBHCATOTAL",
        "PARTDTTOTAL", "PARTDGTOTAL", "PARTDTOTALMILES", "REPORT_SUBMISSION_TYPE"
    };
  }
}
