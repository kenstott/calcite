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
package org.apache.calcite.adapter.govdata.energy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the EIA-923 Page 1 streaming parser. */
@Tag("unit")
class Eia923PlantGenerationTransformerTest {
  private static final String[] MONTHS = {
      "January", "February", "March", "April", "May", "June", "July", "August",
      "September", "October", "November", "December"};

  private static File workbook(Path dir, boolean dropNetgen) throws Exception {
    File f = dir.resolve("w.xlsx").toFile();
    try (XSSFWorkbook wb = new XSSFWorkbook()) {
      Sheet other = wb.createSheet("Page 2 Stocks Data");
      other.createRow(0).createCell(0).setCellValue("Plant Id");
      Sheet s = wb.createSheet("Page 1 Generation and Fuel Data");
      s.createRow(0).createCell(0).setCellValue("EIA-923 banner");
      Row h = s.createRow(4);
      String[] fixed = {"Plant Id", "Nuclear Unit Id", "Plant Name", "Operator Id",
          "Plant State", "Reported\r\nPrime Mover", "Reported\r\nFuel Type Code"};
      int c = 0;
      for (String x : fixed) {
        h.createCell(c++).setCellValue(x);
      }
      for (String m : MONTHS) {
        if (!dropNetgen || !"March".equals(m)) {
          h.createCell(c++).setCellValue("Netgen\r\n" + m);
        }
      }
      h.createCell(c).setCellValue("YEAR");
      Row r = s.createRow(5);
      c = 0;
      r.createCell(c++).setCellValue(8573);
      r.createCell(c++).setCellValue(".");
      r.createCell(c++).setCellValue("Hoover Dam");
      r.createCell(c++).setCellValue(100);
      r.createCell(c++).setCellValue("NV");
      r.createCell(c++).setCellValue("HY");
      r.createCell(c++).setCellValue("WAT");
      for (int m = 0; m < 12; m++) {
        if (!dropNetgen || m != 2) {
          r.createCell(c++).setCellValue(100.5 * (m + 1));
        }
      }
      r.createCell(c).setCellValue(2024);
      try (FileOutputStream out = new FileOutputStream(f)) {
        wb.write(out);
      }
    }
    return f;
  }

  @Test void unpivotsTwelveMonths(@TempDir Path dir) throws Exception {
    ArrayNode rows = Eia923PlantGenerationTransformer.readSheet(workbook(dir, false), 2024);
    assertEquals(12, rows.size());
    JsonNode mar = rows.get(2);
    assertEquals(8573, mar.get("plant_id").asInt());
    assertEquals(3, mar.get("generation_month").asInt());
    assertEquals(2024, mar.get("generation_year").asInt());
    assertEquals(301.5, mar.get("net_generation_mwh").asDouble(), 1e-9);
    assertEquals("HY", mar.get("prime_mover").asText());
    assertTrue(mar.get("nuclear_unit_id").isNull());
  }

  @Test void missingNetgenColumnFailsLoudly(@TempDir Path dir) throws Exception {
    File f = workbook(dir, true);
    assertThrows(IllegalStateException.class,
        () -> Eia923PlantGenerationTransformer.readSheet(f, 2024));
  }

  @Test void yearMismatchFailsLoudly(@TempDir Path dir) throws Exception {
    File f = workbook(dir, false);
    assertThrows(IllegalStateException.class,
        () -> Eia923PlantGenerationTransformer.readSheet(f, 2023));
  }
}
