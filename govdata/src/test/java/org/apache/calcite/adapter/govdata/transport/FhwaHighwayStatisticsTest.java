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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FhwaHighwayStatisticsTest {

  private static String link(String href, String text) {
    return "<td><a href=\"" + href + "\" hreflang=\"en\">" + text + "</a></td>\n";
  }

  private static final String LISTING =
      link("/p/hs-2024", "Highway Statistics 2024")
          + link("/p/hs-2023", " Highway Statistics 2023 ")
          + link("/p/guide", "A Guide to Reporting Highway Statistics");

  private static final String EDITION =
      "<a href=\"/sites/fhwa.dot.gov/files/highway_statistics_2024_hm71.pdf\">PDF</a>"
          + "<a href=\"/sites/fhwa.dot.gov/files/highway_statistics_2024_hm71.xlsx\">Excel</a>"
          + "<a href=\"/sites/fhwa.dot.gov/files/highway_statistics_2024_hm72.xlsx\">Excel</a>";

  @Test void readsEditionsFromTheListing() {
    TreeMap<Integer, String> editions = FhwaHighwayStatistics.parseListing(LISTING);
    assertEquals(2, editions.size());
    assertEquals("/p/hs-2023", editions.get(2023));
    assertEquals("/p/hs-2024", FhwaHighwayStatistics.editionPage(editions, 2024));
  }

  @Test void editionNewerThanTheListingIsNotPublished() {
    TreeMap<Integer, String> editions = FhwaHighwayStatistics.parseListing(LISTING);
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> FhwaHighwayStatistics.editionPage(editions, 2025));
    assertTrue(e.getMessage().contains("HTTP 404"), e.getMessage());
  }

  @Test void unlistedOlderEditionFails() {
    TreeMap<Integer, String> editions = FhwaHighwayStatistics.parseListing(LISTING);
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> FhwaHighwayStatistics.editionPage(editions, 2022));
    assertTrue(!e.getMessage().contains("HTTP 404"), e.getMessage());
  }

  @Test void changedListingLayoutFails() {
    assertThrows(IllegalStateException.class,
        () -> FhwaHighwayStatistics.parseListing("<html><body>moved</body></html>"));
  }

  @Test void picksTheLinkedWorkbookOfTheTable() {
    assertEquals("/sites/fhwa.dot.gov/files/highway_statistics_2024_hm71.xlsx",
        FhwaHighwayStatistics.workbookUrl(EDITION, "hm71", 2024, true));
  }

  @Test void tableNotYetInTheNewestEditionIsNotPublished() {
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> FhwaHighwayStatistics.workbookUrl(EDITION, "mv1", 2024, true));
    assertTrue(e.getMessage().contains("HTTP 404"), e.getMessage());
  }

  @Test void tableMissingFromAnOlderEditionFails() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> FhwaHighwayStatistics.workbookUrl(EDITION, "mv1", 2024, false));
    assertTrue(!e.getMessage().contains("HTTP 404"), e.getMessage());
  }

  @Test void editionPageWithoutRecognisedLinksFails() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> FhwaHighwayStatistics.workbookUrl("<a href=\"/other.xlsx\">x</a>", "mv1", 2025, true));
    assertTrue(!e.getMessage().contains("HTTP 404"), e.getMessage());
  }

  @Test void archivePathUsesTheTableExtension() {
    assertEquals("/sites/fhwa.dot.gov/files/policy/policyinformation/statistics/2017/xls/hm71.xls",
        FhwaHighwayStatistics.archiveUrl("hm71", "xls", 2017));
  }

  /** One state row in MV-1 layout: a formula automobile count, a blank bus count. */
  private static byte[] mv1(boolean textTrucks) throws IOException {
    XSSFWorkbook wb = new XSSFWorkbook();
    try {
      Sheet sheet = wb.createSheet("CY MV-1 Publish");
      Row row = sheet.createRow(9);
      row.createCell(0).setCellValue("Alabama (2)");
      row.createCell(3).setCellFormula("1000+234");
      wb.getCreationHelper().createFormulaEvaluator().evaluateFormulaCell(row.getCell(3));
      if (textTrucks) {
        row.createCell(9).setCellValue("n/a");
      } else {
        row.createCell(9).setCellValue(4);
      }
      row.createCell(12).setCellValue(5);
      row.createCell(15).setCellValue(6);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      wb.write(out);
      return out.toByteArray();
    } finally {
      wb.close();
    }
  }

  @Test void formulaCellsReadTheirCachedValue() throws IOException {
    List<Map<String, Object>> rows =
        FhwaVehicleRegistrationsTransformer.parseWorkbook(mv1(false), "u");
    assertEquals(1, rows.size());
    assertEquals("01", rows.get(0).get("state_fips"));
    assertEquals(Long.valueOf(1234), rows.get(0).get("automobiles"));
    assertNull(rows.get(0).get("buses"));
    assertEquals(Long.valueOf(4), rows.get(0).get("trucks"));
  }

  @Test void nonNumericMeasureFails() throws IOException {
    byte[] bytes = mv1(true);
    assertThrows(IOException.class,
        () -> FhwaVehicleRegistrationsTransformer.parseWorkbook(bytes, "u"));
  }
}
