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
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class OewsStateWorkbookTransformerTest {

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
    File f = Files.createTempFile("oews-test-", suffix).toFile();
    f.deleteOnExit();
    try (OutputStream out = new FileOutputStream(f)) {
      wb.write(out);
    }
    wb.close();
    return f;
  }

  /** Pre-2014 layout: no area_type/naics/own_code, area is text FIPS, ** is suppression. */
  private static Workbook legacy(Workbook wb) {
    Sheet s = wb.createSheet("state_dl");
    row(s, 0, "area", "st", "state", "occ_code", "occ_title", "group", "tot_emp");
    row(s, 1, "01", "AL", "Alabama", "00-0000", "All Occupations", "total", 1807480);
    row(s, 2, "01", "AL", "Alabama", "17-0000", "Architecture", "major", 27500);
    row(s, 3, "06", "CA", "California", "15-1132", "Software Developers, Applications", "",
        100000);
    row(s, 4, "78", "VI", "Virgin Islands", "15-1133", "Systems", "", "**");
    row(s, 5, "06", "CA", "California", "15-1121", "Computer Systems Analysts", "", 5);
    return wb;
  }

  /** 2019+ layout: header in lower case with area_type/naics/own_code, numeric area. */
  private static Workbook modern(Workbook wb) {
    Sheet s = wb.createSheet("All May 2021 data");
    Sheet docs = wb.createSheet("Field Descriptions");
    row(docs, 0, "Field", "Definition");
    row(docs, 1, "occ_code", "17-0000");
    row(s, 0, "area", "area_title", "area_type", "naics", "own_code", "occ_code", "tot_emp");
    row(s, 1, 1, "Alabama", 2, "000000", "1235", "19-0000", 12000);
    row(s, 2, 72, "Puerto Rico", 3, "000000", "1235", "15-1252", 3000);
    row(s, 3, 1, "Alabama", 2, "541000", "1235", "19-0000", 999);
    row(s, 4, 1, "Alabama", 2, "000000", "5", "19-0000", 998);
    row(s, 5, 99, "Somewhere", 4, "000000", "1235", "19-0000", 997);
    return wb;
  }

  @Test void readsLegacyXls() throws Exception {
    File f = write(legacy(new HSSFWorkbook()), ".xls");
    OewsStateWorkbookTransformer.Collector c = new OewsStateWorkbookTransformer.Collector(2010);
    OewsStateWorkbookTransformer.readXls(f, c);
    List<Map<String, Object>> rows = c.finish("test");
    assertEquals(2, rows.size());
    assertEquals("OEUS0100000000000170000" + "01", rows.get(0).get("series"));
    assertEquals(2010, rows.get(0).get("year"));
    assertEquals("01", rows.get(0).get("state_fips"));
    assertEquals("170000", rows.get(0).get("occupation_code"));
    assertEquals(27500.0, rows.get(0).get("employment"));
    assertEquals("151132", rows.get(1).get("occupation_code"));
    assertEquals("Software Developers, Applications", rows.get(1).get("occupation_name"));
  }

  @Test void readsLegacyLayoutXlsx() throws Exception {
    File f = write(legacy(new XSSFWorkbook()), ".xlsx");
    OewsStateWorkbookTransformer.Collector c = new OewsStateWorkbookTransformer.Collector(2014);
    OewsStateWorkbookTransformer.readXlsx(f, c);
    assertEquals(2, c.finish("test").size());
  }

  @Test void filtersModernXlsxByAreaTypeIndustryAndOwnership() throws Exception {
    File f = write(modern(new XSSFWorkbook()), ".xlsx");
    OewsStateWorkbookTransformer.Collector c = new OewsStateWorkbookTransformer.Collector(2024);
    OewsStateWorkbookTransformer.readXlsx(f, c);
    List<Map<String, Object>> rows = c.finish("test");
    assertEquals(2, rows.size());
    assertEquals("01", rows.get(0).get("state_fips"));
    assertEquals("190000", rows.get(0).get("occupation_code"));
    assertEquals("72", rows.get(1).get("state_fips"));
    assertEquals("151252", rows.get(1).get("occupation_code"));
  }

  @Test void unknownEmploymentMarkerFailsLoudly() throws Exception {
    Workbook wb = new XSSFWorkbook();
    Sheet s = wb.createSheet("state_dl");
    row(s, 0, "area", "occ_code", "tot_emp");
    row(s, 1, "01", "17-0000", "#");
    File f = write(wb, ".xlsx");
    OewsStateWorkbookTransformer.Collector c = new OewsStateWorkbookTransformer.Collector(2020);
    assertThrows(Exception.class, () -> OewsStateWorkbookTransformer.readXlsx(f, c));
  }

  @Test void duplicateAreaOccupationFailsLoudly() throws Exception {
    Workbook wb = new XSSFWorkbook();
    Sheet s = wb.createSheet("state_dl");
    row(s, 0, "area", "occ_code", "tot_emp");
    row(s, 1, "01", "17-0000", 5);
    row(s, 2, "01", "17-0000", 6);
    File f = write(wb, ".xlsx");
    OewsStateWorkbookTransformer.Collector c = new OewsStateWorkbookTransformer.Collector(2020);
    assertThrows(Exception.class, () -> OewsStateWorkbookTransformer.readXlsx(f, c));
  }
}
