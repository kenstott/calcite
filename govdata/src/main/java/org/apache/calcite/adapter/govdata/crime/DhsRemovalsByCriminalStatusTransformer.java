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
package org.apache.calcite.adapter.govdata.crime;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.util.ArrayList;
import java.util.List;

/**
 * OHSS Yearbook Table 42, "DHS Removals by Known Criminal Status and Region and Country of
 * Nationality". The sheet is wide: a fiscal year spans three columns (Total, Criminal,
 * Non-Criminal) and rows run through a REGION block then a COUNTRY block, each starting with a
 * Total row. Output is one row per fiscal year x geography, with geo_level 'total', 'region' or
 * 'country'.
 */
public class DhsRemovalsByCriminalStatusTransformer extends OhssEnforcementXlsxTransformer {

  @Override protected String parseWorkbook(XSSFWorkbook workbook) {
    Sheet sheet = sheet(workbook, "Table 42");
    int yearRow = -1;
    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      Row row = row(sheet, r);
      if (row != null && "Region and country of nationality".equals(text(row.getCell(0)))) {
        yearRow = r;
        break;
      }
    }
    if (yearRow < 0) {
      throw new IllegalStateException("Table 42: no header row");
    }
    // Each year heads a Total/Criminal/Non-Criminal triple; record the year's first column.
    List<int[]> years = new ArrayList<>();
    Row headRow = row(sheet, yearRow);
    for (int c = 1; c < headRow.getLastCellNum(); c++) {
      Long y = number(headRow.getCell(c));
      if (y != null) {
        years.add(new int[] {y.intValue(), c});
      }
    }
    if (years.isEmpty()) {
      throw new IllegalStateException("Table 42: no fiscal-year columns");
    }
    ArrayNode out = MAPPER.createArrayNode();
    String level = null;
    for (int r = yearRow + 2; r <= sheet.getLastRowNum(); r++) {
      Row row = row(sheet, r);
      String name = row == null ? null : text(row.getCell(0));
      if (name == null) {
        continue;
      }
      if ("REGION".equals(name)) {
        level = "region";
        continue;
      }
      if ("COUNTRY".equals(name)) {
        level = "country";
        continue;
      }
      // Footnotes follow the last country row and carry no figures.
      if (level == null || row.getCell(1) == null || text(row.getCell(1)) == null
          && number(row.getCell(1)) == null) {
        if (level != null) {
          break;
        }
        continue;
      }
      if ("Total".equals(name)) {
        if ("country".equals(level)) {
          continue; // the country block repeats the grand total
        }
        emit(out, years, row, "total", "Total");
      } else {
        emit(out, years, row, level, name);
      }
    }
    if (out.size() == 0) {
      throw new IllegalStateException("Table 42: no rows parsed");
    }
    return out.toString();
  }

  private static void emit(ArrayNode out, List<int[]> years, Row row, String level, String name) {
    for (int[] y : years) {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("fiscal_year", y[0]);
      node.put("geo_level", level);
      node.put("geography", name);
      put(node, "removals_total", number(row.getCell(y[1])));
      put(node, "removals_criminal", number(row.getCell(y[1] + 1)));
      put(node, "removals_non_criminal", number(row.getCell(y[1] + 2)));
      out.add(node);
    }
  }
}
