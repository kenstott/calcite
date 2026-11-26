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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Living arrangements of adults, 1967 to present (CPS Table AD-3). {@code ad3.xls} covers all
 * adults 18 and over (counts and percents, both sexes together); the five age-group workbooks
 * carry the group's population and percents by sex. Long format: one row per year, vintage, age
 * group, sex and living arrangement.
 */
public class CpsAdultLivingArrangementsTransformer extends CpsAdultsHistoryTransformer {

  /** Workbook file name to age group, for the workbooks broken out by sex. */
  private static final String[][] AGE_WORKBOOKS = {
      {"ad3-18-24.xls", "18-24"},
      {"ad3-25-34.xls", "25-34"},
      {"ad3-35-64.xls", "35-64"},
      {"ad3-65-74.xls", "65-74"},
      {"ad3-75andover.xls", "75+"},
  };

  @Override protected void parse(String pageHtml, String pageUrl, ArrayNode out)
      throws IOException {
    try (HSSFWorkbook workbook = openWorkbook(pageHtml, "ad3.xls")) {
      parseAllAdultsSheet(workbook.getSheetAt(0), out);
    }
    for (String[] entry : AGE_WORKBOOKS) {
      try (HSSFWorkbook workbook = openWorkbook(pageHtml, entry[0])) {
        parseAgeGroupSheet(workbook.getSheetAt(0), entry[1], out);
      }
    }
  }

  /** Parses {@code ad3.xls}: a Number/Percent column pair per living arrangement. */
  static void parseAllAdultsSheet(Sheet sheet, ArrayNode out) {
    int header = headerRow(sheet);
    require("Total, 18 and over".equals(text(sheet, header - 1, 1))
        && "Number".equals(text(sheet, header, 1)), "unexpected 18+ headers");
    Map<Integer, String> columns = arrangementColumns(sheet, header - 1, 3);
    for (int c : columns.keySet()) {
      require("Number".equals(text(sheet, header, c))
          && "Percent".equals(text(sheet, header, c + 1)), "unexpected pair at column " + c);
    }
    Set<Integer> seen = new HashSet<>();
    for (int r = header + 1; r <= sheet.getLastRowNum(); r++) {
      Integer year = yearOf(sheet, r);
      if (year == null) {
        continue;
      }
      String vintage = seen.add(year) ? "primary" : "alternate";
      Row row = sheet.getRow(r);
      for (Map.Entry<Integer, String> column : columns.entrySet()) {
        ObjectNode node = node(out, year, vintage, "18+", "total", column.getValue());
        CpsHistoricalXlsxTransformer.put(node, "age_group_population_thousands",
            row.getCell(1), 0);
        CpsHistoricalXlsxTransformer.put(node, "people_thousands",
            row.getCell(column.getKey()), 0);
        putPercent(node, row.getCell(column.getKey() + 1));
      }
    }
    require(!seen.isEmpty(), "no year rows in the 18+ sheet");
  }

  /** Parses an age-group workbook: a Men/Women percent column pair per living arrangement. */
  static void parseAgeGroupSheet(Sheet sheet, String ageGroup, ArrayNode out) {
    int header = headerRow(sheet);
    require("Total".equals(text(sheet, header - 1, 3)), "unexpected headers for " + ageGroup);
    Map<Integer, String> columns = arrangementColumns(sheet, header - 1, 5);
    for (int c = 1; c <= 3; c += 2) {
      require("Men".equals(text(sheet, header, c))
          && "Women".equals(text(sheet, header, c + 1)), "unexpected pair at column " + c);
    }
    for (int c : columns.keySet()) {
      require("Men".equals(text(sheet, header, c))
          && "Women".equals(text(sheet, header, c + 1)), "unexpected pair at column " + c);
    }
    Set<Integer> seen = new HashSet<>();
    for (int r = header + 1; r <= sheet.getLastRowNum(); r++) {
      Integer year = yearOf(sheet, r);
      if (year == null) {
        continue;
      }
      String vintage = seen.add(year) ? "primary" : "alternate";
      Row row = sheet.getRow(r);
      String[] sexes = {"male", "female"};
      for (int s = 0; s < sexes.length; s++) {
        for (Map.Entry<Integer, String> column : columns.entrySet()) {
          ObjectNode node = node(out, year, vintage, ageGroup, sexes[s], column.getValue());
          CpsHistoricalXlsxTransformer.put(node, "age_group_population_thousands",
              row.getCell(1 + s), 0);
          node.putNull("people_thousands");
          putPercent(node, row.getCell(column.getKey() + s));
        }
      }
    }
    require(!seen.isEmpty(), "no year rows for " + ageGroup);
  }

  private static int headerRow(Sheet sheet) {
    for (int r = 1; r <= sheet.getLastRowNum(); r++) {
      if ("Year".equals(text(sheet, r, 0))) {
        return r;
      }
    }
    throw new IllegalStateException("AD-3: no Year header row");
  }

  /** Column index to living arrangement code, read from the header labels. */
  private static Map<Integer, String> arrangementColumns(Sheet sheet, int labelRow, int from) {
    Map<Integer, String> columns = new LinkedHashMap<>();
    Row row = sheet.getRow(labelRow);
    for (int c = from; c < row.getLastCellNum(); c++) {
      String label = text(sheet, labelRow, c);
      if (label != null) {
        columns.put(c, arrangementCode(label));
      }
    }
    require(columns.size() >= 5, "found only " + columns.values());
    return columns;
  }

  private static String arrangementCode(String label) {
    // Header labels carry a trailing footnote number, e.g. "Living with spouse1".
    String name = label.replaceAll("\\d+$", "").trim();
    switch (name) {
    case "Living alone":
      return "living_alone";
    case "Living with spouse":
      return "living_with_spouse";
    case "Child of householder":
      return "child_of_householder";
    case "Living with partner":
      return "living_with_partner";
    case "Living with other relatives":
      return "living_with_other_relatives";
    case "Living with nonrelatives":
      return "living_with_nonrelatives";
    default:
      throw new IllegalStateException("AD-3: unknown living arrangement column " + label);
    }
  }

  /** The year of a data row, or null for a row that is not one. */
  private static Integer yearOf(Sheet sheet, int r) {
    String label = text(sheet, r, 0);
    Matcher m = label == null ? null : YEAR_LABEL.matcher(label);
    if (m != null && m.matches()) {
      return Integer.valueOf(m.group(1));
    }
    Row row = sheet.getRow(r);
    require(row == null || !isNumber(row, 1), "numbers on a row with no year label: " + r);
    return null;
  }

  private static ObjectNode node(ArrayNode out, int year, String vintage, String ageGroup,
      String sex, String arrangement) {
    ObjectNode node = out.addObject();
    node.put("year", year);
    node.put("vintage", vintage);
    node.put("age_group", ageGroup);
    node.put("sex", sex);
    node.put("living_arrangement", arrangement);
    return node;
  }

  /** The source prints "Z" for a percent that rounds to zero (its own footnote). */
  private static void putPercent(ObjectNode node, Cell cell) {
    if (cell != null && cell.getCellType() == CellType.STRING
        && "Z".equals(cell.getStringCellValue().trim())) {
      node.put("percent", 0.0);
    } else {
      CpsHistoricalXlsxTransformer.put(node, "percent", cell, 1);
    }
  }

  private static void require(boolean ok, String message) {
    if (!ok) {
      throw new IllegalStateException("AD-3: " + message);
    }
  }
}
