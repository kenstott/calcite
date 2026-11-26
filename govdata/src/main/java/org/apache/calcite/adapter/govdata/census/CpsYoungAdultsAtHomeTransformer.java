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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Young adults living at home by sex and age group, 1960 to present (CPS Table AD-1,
 * {@code ad1.xls}). Long format: one row per year, vintage, age group and sex.
 */
public class CpsYoungAdultsAtHomeTransformer extends CpsAdultsHistoryTransformer {

  @Override protected void parse(String pageHtml, String pageUrl, ArrayNode out)
      throws IOException {
    try (HSSFWorkbook workbook = openWorkbook(pageHtml, "ad1.xls")) {
      parseSheet(workbook.getSheetAt(0), out);
    }
  }

  static void parseSheet(Sheet sheet, ArrayNode out) {
    String ageGroup = null;
    boolean headersChecked = false;
    Set<String> seen = new HashSet<>();
    Set<String> ageGroups = new HashSet<>();
    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      if ("Men".equals(text(sheet, r, 1))) {
        require("Women".equals(text(sheet, r, 5)), "column 5 header is not Women");
        require("Total".equals(text(sheet, r + 2, 1))
            && "Percent".equals(text(sheet, r + 2, 3))
            && "Total".equals(text(sheet, r + 2, 5))
            && "Percent".equals(text(sheet, r + 2, 7)), "unexpected column headers");
        headersChecked = true;
        continue;
      }
      String label = text(sheet, r, 0);
      if (label == null) {
        continue;
      }
      if (".18 to 24 years".equals(label)) {
        ageGroup = "18-24";
        continue;
      }
      if (".25 to 34 years".equals(label)) {
        ageGroup = "25-34";
        continue;
      }
      if (!label.startsWith("..")) {
        continue;
      }
      Matcher m = YEAR_LABEL.matcher(label);
      require(m.matches() && ageGroup != null && headersChecked, "unexpected row " + label);
      int year = Integer.parseInt(m.group(1));
      String vintage = seen.add(ageGroup + year) ? "primary" : "alternate";
      String dataSource = m.group(2) == null ? "cps" : "decennial_census";
      ageGroups.add(ageGroup);
      Row row = sheet.getRow(r);
      addRow(out, row, year, vintage, ageGroup, "male", dataSource, 1);
      addRow(out, row, year, vintage, ageGroup, "female", dataSource, 5);
    }
    require(ageGroups.size() == 2, "expected two age groups, found " + ageGroups);
  }

  private static void addRow(ArrayNode out, Row row, int year, String vintage, String ageGroup,
      String sex, String dataSource, int col) {
    ObjectNode node = out.addObject();
    node.put("year", year);
    node.put("vintage", vintage);
    node.put("age_group", ageGroup);
    node.put("sex", sex);
    node.put("data_source", dataSource);
    CpsHistoricalXlsxTransformer.put(node, "population_thousands", row.getCell(col), 0);
    CpsHistoricalXlsxTransformer.put(node, "child_of_householder_thousands",
        row.getCell(col + 1), 0);
    CpsHistoricalXlsxTransformer.put(node, "child_of_householder_pct", row.getCell(col + 2), 1);
  }

  private static void require(boolean ok, String message) {
    if (!ok) {
      throw new IllegalStateException("AD-1: " + message);
    }
  }
}
