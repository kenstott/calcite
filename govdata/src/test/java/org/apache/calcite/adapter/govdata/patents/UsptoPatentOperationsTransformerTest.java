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
package org.apache.calcite.adapter.govdata.patents;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link UsptoPatentOperationsTransformer#parse}. */
@Tag("unit")
class UsptoPatentOperationsTransformerTest {

  private static XSSFWorkbook workbook(String unmappedLabel) {
    XSSFWorkbook wb = new XSSFWorkbook();
    Sheet sheet = wb.createSheet("Historical Data");
    CreationHelper helper = wb.getCreationHelper();
    CellStyle dateStyle = wb.createCellStyle();
    dateStyle.setDataFormat(helper.createDataFormat().getFormat("m/d/yyyy"));
    Row dates = sheet.createRow(3);
    LocalDate[] months = {LocalDate.of(2024, 9, 1), LocalDate.of(2024, 10, 1)};
    for (int i = 0; i < months.length; i++) {
      dates.createCell(i + 1).setCellValue(Date.valueOf(months[i].toString()));
      dates.getCell(i + 1).setCellStyle(dateStyle);
    }
    dates.createCell(3); // trailing blank future month
    Row inv = sheet.createRow(5);
    inv.createCell(0).setCellValue("UPR Unexamined Inventory");
    inv.createCell(1).setCellValue(800000);
    inv.createCell(2).setCellValue(790000);
    Row attr = sheet.createRow(6);
    attr.createCell(0).setCellValue("Current Attrition rate**");
    attr.createCell(1).setCellValue("-");
    attr.createCell(2).setCellValue(0.07);
    sheet.createRow(7).createCell(0)
        .setCellValue("UPR Traditional Total Pendency by Technology Center");
    Row tc = sheet.createRow(8);
    tc.createCell(0).setCellValue(1600);
    tc.createCell(1).setCellValue(34.6);
    if (unmappedLabel != null) {
      Row bad = sheet.createRow(9);
      bad.createCell(0).setCellValue(unmappedLabel);
      bad.createCell(1).setCellValue(1);
    }
    return wb;
  }

  @Test void unpivotsMonthsAndTechnologyCenters() throws Exception {
    try (XSSFWorkbook wb = workbook(null)) {
      JsonNode rows = UsptoPatentOperationsTransformer.parse(wb);
      // 2 inventory + 1 attrition ('-' skipped) + 1 TC pendency (second month blank)
      assertEquals(4, rows.size());
      JsonNode oct = null;
      for (JsonNode n : rows) {
        if ("upr_unexamined_inventory".equals(n.get("measure").asText())
            && "2024-10-01".equals(n.get("month").asText())) {
          oct = n;
        }
      }
      assertEquals(2025, oct.get("fiscal_year").asInt());
      assertEquals(790000.0, oct.get("value").asDouble(), 0.001);
      assertEquals("all", oct.get("tech_center").asText());
      JsonNode last = rows.get(3);
      assertEquals("tc_traditional_total_pendency", last.get("measure").asText());
      assertEquals("1600", last.get("tech_center").asText());
      assertEquals("months", last.get("unit").asText());
      assertEquals(2024, last.get("fiscal_year").asInt());
    }
  }

  @Test void unmappedMeasureFails() throws Exception {
    try (XSSFWorkbook wb = workbook("Brand New Measure")) {
      assertThrows(IllegalStateException.class, () -> UsptoPatentOperationsTransformer.parse(wb));
    }
  }
}
