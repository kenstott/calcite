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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the List 1 workbook parsing of {@link CbsaDelineationProvider}. */
@Tag("unit")
class CbsaDelineationProviderTest {

  private static final String[] HEADER = {"CBSA Code", "Metropolitan Division Code", "CSA Code",
      "CBSA Title", "Metropolitan/Micropolitan Statistical Area", "Metropolitan Division Title",
      "CSA Title", "County/County Equivalent", "State Name", "FIPS State Code",
      "FIPS County Code", "Central/Outlying County"};

  private static byte[] workbook(Workbook wb, String title, String[]... data) throws IOException {
    Sheet s = wb.createSheet("List 1");
    s.createRow(0).createCell(0).setCellValue("Table with row headers in column A");
    s.createRow(1).createCell(0).setCellValue(title);
    Row h = s.createRow(2);
    for (int i = 0; i < HEADER.length; i++) {
      h.createCell(i).setCellValue(HEADER[i]);
    }
    int r = 3;
    for (String[] d : data) {
      Row row = s.createRow(r++);
      for (int i = 0; i < d.length; i++) {
        if (!d[i].isEmpty()) {
          row.createCell(i).setCellValue(d[i]);
        }
      }
    }
    r++; // blank row, then the notes
    s.createRow(r).createCell(0).setCellValue("Source: Census Bureau");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    wb.write(out);
    wb.close();
    return out.toByteArray();
  }

  private static final String[] NY = {"35620", "35614", "35620", "New York-Newark-Jersey City, "
      + "NY-NJ-PA", "Metropolitan Statistical Area", "New York-Jersey City-White Plains, NY-NJ",
      "New York-Newark, NY-NJ-CT-PA", "Bronx County", "New York", "36", "005", "Central"};
  private static final String[] ABERDEEN = {"10100", "", "", "Aberdeen, SD",
      "Micropolitan Statistical Area", "", "", "Brown County", "South Dakota", "46", "013",
      "Central"};

  @Test void parsesXlsWithDivisionAndWithout() throws Exception {
    byte[] xls = workbook(new HSSFWorkbook(),
        "List 1. CORE BASED STATISTICAL AREAS (CBSAs), JULY 2015", NY, ABERDEEN);
    List<Map<String, Object>> rows = CbsaDelineationProvider.parse(new ByteArrayInputStream(xls),
        2015);
    assertEquals(2, rows.size());
    Map<String, Object> ny = rows.get(0);
    assertEquals(2015, ny.get("vintage"));
    assertEquals("2015-07", ny.get("delineation_date"));
    assertEquals("36005", ny.get("county_fips"));
    assertEquals("36", ny.get("state_fips"));
    assertEquals("metropolitan", ny.get("cbsa_type"));
    assertEquals("35614", ny.get("metro_division_code"));
    assertEquals("35620", ny.get("csa_code"));
    assertEquals("central", ny.get("central_outlying"));
    Map<String, Object> ab = rows.get(1);
    assertEquals("46013", ab.get("county_fips"));
    assertEquals("micropolitan", ab.get("cbsa_type"));
    assertNull(ab.get("metro_division_code"));
    assertNull(ab.get("csa_code"));
    assertNull(ab.get("csa_title"));
  }

  @Test void parsesXlsx() throws Exception {
    byte[] xlsx = workbook(new XSSFWorkbook(),
        "List 1. CORE BASED STATISTICAL AREAS (CBSAs), JULY 2023", ABERDEEN);
    List<Map<String, Object>> rows =
        CbsaDelineationProvider.parse(new ByteArrayInputStream(xlsx), 2023);
    assertEquals(1, rows.size());
    assertEquals("2023-07", rows.get(0).get("delineation_date"));
  }

  @Test void zeroPadsFipsCodes() throws Exception {
    String[] al = ABERDEEN.clone();
    al[9] = "1";
    al[10] = "7";
    byte[] xls = workbook(new HSSFWorkbook(), "List 1. X, MARCH 2020", al);
    assertEquals("01007", CbsaDelineationProvider.parse(new ByteArrayInputStream(xls), 2020)
        .get(0).get("county_fips"));
  }

  @Test void badCbsaCodeFails() throws Exception {
    String[] bad = ABERDEEN.clone();
    bad[0] = "1010";
    byte[] xls = workbook(new HSSFWorkbook(), "List 1. X, MARCH 2020", bad);
    assertThrows(IOException.class,
        () -> CbsaDelineationProvider.parse(new ByteArrayInputStream(xls), 2020));
  }

  @Test void badCentralOutlyingFails() throws Exception {
    String[] bad = ABERDEEN.clone();
    bad[11] = "Middle";
    byte[] xls = workbook(new HSSFWorkbook(), "List 1. X, MARCH 2020", bad);
    assertThrows(IOException.class,
        () -> CbsaDelineationProvider.parse(new ByteArrayInputStream(xls), 2020));
  }

  @Test void titleWithoutDateFails() throws Exception {
    byte[] xls = workbook(new HSSFWorkbook(), "List 1. no date here", ABERDEEN);
    assertThrows(IOException.class,
        () -> CbsaDelineationProvider.parse(new ByteArrayInputStream(xls), 2020));
  }
}
