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

/**
 * OHSS Yearbook Table 39, "DHS Repatriations: Fiscal Years 1892 to ...": one row per fiscal year
 * with administrative returns, enforcement returns, removals, Title 42 expulsions and the
 * total of removals and returns. Cells the source marks "NA" or "X" become NULL.
 */
public class DhsRepatriationsByYearTransformer extends OhssEnforcementXlsxTransformer {

  @Override protected String parseWorkbook(XSSFWorkbook workbook) {
    Sheet sheet = sheet(workbook, "Table 39");
    ArrayNode out = MAPPER.createArrayNode();
    int header = -1;
    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      Row row = row(sheet, r);
      if (row != null && "Year".equals(text(row.getCell(0)))) {
        header = r;
        break;
      }
    }
    if (header < 0) {
      throw new IllegalStateException("Table 39: no 'Year' header row");
    }
    for (int r = header + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = row(sheet, r);
      Long year = row == null ? null : number(row.getCell(0));
      if (year == null) {
        continue;
      }
      ObjectNode node = MAPPER.createObjectNode();
      node.put("fiscal_year", year.intValue());
      put(node, "administrative_returns", number(row.getCell(1)));
      put(node, "enforcement_returns", number(row.getCell(2)));
      put(node, "removals", number(row.getCell(3)));
      put(node, "title_42_expulsions", number(row.getCell(4)));
      put(node, "total_removals_and_returns", number(row.getCell(5)));
      out.add(node);
    }
    if (out.size() == 0) {
      throw new IllegalStateException("Table 39: no fiscal-year rows parsed");
    }
    return out.toString();
  }
}
