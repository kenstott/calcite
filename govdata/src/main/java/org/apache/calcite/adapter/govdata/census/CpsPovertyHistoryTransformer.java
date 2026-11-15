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

import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.util.regex.Pattern;

/**
 * Official U.S. poverty status of people, 1959 to present (CPS ASEC historical Table 2,
 * {@code hstpov2.xlsx}), "All Races" block.
 */
public class CpsPovertyHistoryTransformer extends CpsHistoricalXlsxTransformer {

  private static final Pattern LINK = Pattern.compile(
      "href=\"((?:https:)?//www2\\.census\\.gov/[^\"]*/hstpov2\\.xlsx)\"");

  @Override protected Pattern workbookLink() {
    return LINK;
  }

  @Override protected ArrayNode parseWorkbook(XSSFWorkbook workbook) {
    return allRacesRows(workbook.getSheetAt(0), (row, node) -> {
      put(node, "people_total", row.getCell(1), 0);
      put(node, "people_below_poverty", row.getCell(2), 0);
      put(node, "poverty_rate_pct", row.getCell(3), 1);
      put(node, "family_people_total", row.getCell(4), 0);
      put(node, "family_people_below_poverty", row.getCell(5), 0);
      put(node, "family_people_poverty_rate_pct", row.getCell(6), 1);
      put(node, "female_householder_people_total", row.getCell(7), 0);
      put(node, "female_householder_people_below_poverty", row.getCell(8), 0);
      put(node, "female_householder_poverty_rate_pct", row.getCell(9), 1);
      put(node, "unrelated_individuals_total", row.getCell(10), 0);
      put(node, "unrelated_individuals_below_poverty", row.getCell(11), 0);
      put(node, "unrelated_individuals_poverty_rate_pct", row.getCell(12), 1);
    });
  }
}
