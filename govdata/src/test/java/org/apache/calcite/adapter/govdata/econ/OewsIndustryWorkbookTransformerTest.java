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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class OewsIndustryWorkbookTransformerTest {

  private static void row(Sheet sheet, int r, Object... values) {
    Row row = sheet.createRow(r);
    for (int c = 0; c < values.length; c++) {
      if (values[c] instanceof Number) {
        row.createCell(c).setCellValue(((Number) values[c]).doubleValue());
      } else if (values[c] != null) {
        row.createCell(c).setCellValue(values[c].toString());
      }
    }
  }

  private static File write(Workbook wb, String suffix) throws Exception {
    File f = Files.createTempFile("oews-ind-test-", suffix).toFile();
    f.deleteOnExit();
    try (OutputStream out = new FileOutputStream(f)) {
      wb.write(out);
    }
    wb.close();
    return f;
  }

  /** Pre-2019 layout: no area/area_type/own_code; the 00-0000 total row is skipped. */
  private static Workbook legacy(Workbook wb) {
    Sheet s = wb.createSheet("natsector_dl");
    row(s, 0, "naics", "naics_title", "occ_code", "occ_title", "occ_group", "tot_emp");
    row(s, 1, "11", "Agriculture", "00-0000", "Industry Total", "total", 424020);
    row(s, 2, "11", "Agriculture", "11-0000", "Management", "major", 7860);
    row(s, 3, "11", "Agriculture", "11-1000", "Top Executives", "minor", 4000);
    row(s, 4, "31-33", "Manufacturing", "15-0000", "Computer", "major", "**");
    row(s, 5, "31-33", "Manufacturing", "17-0000", "Engineering", "major", 1000000);
    row(s, 6, 113300, "Logging", "19-0000", "Science", "major", 70);
    return wb;
  }

  /** 2019+ layout: upper-case header with area_type, sector 99. */
  private static Workbook modern(Workbook wb) {
    Sheet s = wb.createSheet("natsector_M2024_dl");
    row(s, 0, "AREA", "AREA_TYPE", "NAICS", "OWN_CODE", "OCC_CODE", "TOT_EMP");
    row(s, 1, 99, 1, "99", "1235", "11-0000", 500);
    row(s, 2, 99, 2, "99", "1235", "13-0000", 400);
    row(s, 3, 99, 1, "48-49", "5", "53-0000", 1234);
    return wb;
  }

  @Test void industryCodeUsesFlatFileSectorEncoding() {
    assertEquals("11--12", OewsIndustryWorkbookTransformer.industryCode("11"));
    assertEquals("51--52", OewsIndustryWorkbookTransformer.industryCode("51"));
    assertEquals("31--34", OewsIndustryWorkbookTransformer.industryCode("31-33"));
    assertEquals("44--46", OewsIndustryWorkbookTransformer.industryCode("44-45"));
    assertEquals("48--50", OewsIndustryWorkbookTransformer.industryCode("48-49"));
    assertEquals("99-100", OewsIndustryWorkbookTransformer.industryCode("99"));
    assertEquals("3250A1", OewsIndustryWorkbookTransformer.industryCode("3250A1"));
    assertThrows(IllegalStateException.class,
        () -> OewsIndustryWorkbookTransformer.industryCode("1234"));
  }

  /** The xlsx reader wraps a collector's IllegalStateException in an IOException. */
  private static void assertCause(IOException e) {
    assertTrue(e.getCause() instanceof IllegalStateException, String.valueOf(e.getCause()));
  }

  private static List<Map<String, Object>> read(File f, boolean xlsx, int year)
      throws Exception {
    OewsIndustryWorkbookTransformer.IndustryCollector c =
        new OewsIndustryWorkbookTransformer.IndustryCollector(year);
    c.beginFile(f.getName());
    if (xlsx) {
      OewsStateWorkbookTransformer.readXlsx(f, c);
    } else {
      OewsStateWorkbookTransformer.readXls(f, c);
    }
    return c.finish(f.getName());
  }

  @Test void readsLegacyXls() throws Exception {
    List<Map<String, Object>> rows = read(write(legacy(new HSSFWorkbook()), ".xls"), false, 2012);
    assertEquals(3, rows.size());
    Map<String, Object> first = rows.get(0);
    assertEquals("OEUN000000011--12" + "110000" + "01", first.get("series"));
    assertEquals(25, ((String) first.get("series")).length());
    assertEquals(2012, first.get("year"));
    assertEquals("11--12", first.get("industry_code"));
    assertEquals("110000", first.get("occupation_code"));
    assertEquals(7860.0, first.get("employment"));
    assertEquals("31--34", rows.get(1).get("industry_code"));
    assertEquals("113300", rows.get(2).get("industry_code"));
  }

  @Test void readsModernXlsxNationalAreaOnly() throws Exception {
    List<Map<String, Object>> rows =
        read(write(modern(new XSSFWorkbook()), ".xlsx"), true, 2024);
    assertEquals(2, rows.size());
    assertEquals("99-100", rows.get(0).get("industry_code"));
    assertEquals("48--50", rows.get(1).get("industry_code"));
    assertEquals(1234.0, rows.get(1).get("employment"));
  }

  @Test void identicalDuplicateAcrossWorkbooksEmittedOnce() throws Exception {
    OewsIndustryWorkbookTransformer.IndustryCollector c =
        new OewsIndustryWorkbookTransformer.IndustryCollector(2024);
    File f = write(legacy(new XSSFWorkbook()), ".xlsx");
    c.beginFile("nat3d");
    OewsStateWorkbookTransformer.readXlsx(f, c);
    c.beginFile("nat4d");
    OewsStateWorkbookTransformer.readXlsx(f, c);
    assertEquals(3, c.finish("t").size());
  }

  @Test void conflictingDuplicateFailsLoudly() throws Exception {
    OewsIndustryWorkbookTransformer.IndustryCollector c =
        new OewsIndustryWorkbookTransformer.IndustryCollector(2024);
    c.beginFile("nat3d");
    OewsStateWorkbookTransformer.readXlsx(write(legacy(new XSSFWorkbook()), ".xlsx"), c);
    Workbook other = new XSSFWorkbook();
    Sheet s = other.createSheet("nat4d");
    row(s, 0, "naics", "occ_code", "tot_emp");
    row(s, 1, "11", "11-0000", 1);
    File f = write(other, ".xlsx");
    c.beginFile("nat4d");
    assertCause(assertThrows(IOException.class,
        () -> OewsStateWorkbookTransformer.readXlsx(f, c)));
  }

  @Test void unknownEmploymentMarkerFailsLoudly() throws Exception {
    Workbook wb = new XSSFWorkbook();
    Sheet s = wb.createSheet("x");
    row(s, 0, "naics", "occ_code", "tot_emp");
    row(s, 1, "11", "11-0000", "#");
    File f = write(wb, ".xlsx");
    assertCause(assertThrows(IOException.class, () -> read(f, true, 2024)));
  }

  @Test void missingHeaderColumnFailsLoudly() throws Exception {
    Workbook wb = new XSSFWorkbook();
    Sheet s = wb.createSheet("x");
    row(s, 0, "naics", "occ_code");
    File f = write(wb, ".xlsx");
    assertCause(assertThrows(IOException.class, () -> read(f, true, 2024)));
  }
}
