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
package org.apache.calcite.adapter.govdata.census;

import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Median and mean household income, 1967 to present, in current and constant dollars (CPS ASEC
 * historical Table H-5, {@code h05.xlsx}), "All Races" block.
 */
public class CpsHouseholdIncomeHistoryTransformer extends CpsHistoricalXlsxTransformer {

  private static final Pattern LINK = Pattern.compile(
      "href=\"((?:https:)?//www2\\.census\\.gov/[^\"]*/h05\\.xlsx)\"");
  private static final Pattern BASE_YEAR = Pattern.compile("current and (\\d{4}) C-CPI-U");

  @Override protected Pattern workbookLink() {
    return LINK;
  }

  @Override protected ArrayNode parseWorkbook(XSSFWorkbook workbook) {
    int baseYear = -1;
    for (int r = 0; r < 12 && baseYear < 0; r++) {
      Row row = workbook.getSheetAt(0).getRow(r);
      String a = row == null ? null : text(row.getCell(0));
      Matcher m = a == null ? null : BASE_YEAR.matcher(a);
      if (m != null && m.find()) {
        baseYear = Integer.parseInt(m.group(1));
      }
    }
    if (baseYear < 0) {
      throw new IllegalStateException("H-5: constant-dollar base year not found in the header");
    }
    final int base = baseYear;
    return allRacesRows(workbook.getSheetAt(0), (row, node) -> {
      node.put("constant_dollar_year", base);
      put(node, "households_thousands", row.getCell(1), 0);
      put(node, "median_income_current_dollars", row.getCell(2), 0);
      put(node, "median_income_constant_dollars", row.getCell(3), 0);
      put(node, "mean_income_current_dollars", row.getCell(4), 0);
      put(node, "mean_income_constant_dollars", row.getCell(5), 0);
    });
  }
}
