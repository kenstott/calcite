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
package org.apache.calcite.adapter.govdata.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link SsaOasdiCountyTransformer}. */
@Tag("unit")
class SsaOasdiCountyTransformerTest {

  /** Writes one row: a String cell, a numeric cell, or none for each value. */
  private static void row(Sheet sheet, int index, Object... values) {
    Row row = sheet.createRow(index);
    for (int i = 0; i < values.length; i++) {
      if (values[i] instanceof Number) {
        row.createCell(i).setCellValue(((Number) values[i]).doubleValue());
      } else if (values[i] != null) {
        row.createCell(i).setCellValue((String) values[i]);
      }
    }
  }

  private static Object[] area(Object nameA, Object nameB, Object code, int total) {
    return new Object[] {nameA, nameB, code, total, 1, 2, 3, 4, 5, 6, 7, 8};
  }

  /** A state workbook laid out as SSA's, with Tables 4 and 5 on the 4th and 5th sheets. */
  private static XSSFWorkbook stateWorkbook(Object[]... dataRows) {
    XSSFWorkbook wb = new XSSFWorkbook();
    for (int s = 1; s <= 5; s++) {
      Sheet sheet = wb.createSheet("Table " + s);
      for (int r = 0; r < dataRows.length; r++) {
        row(sheet, 4 + r, dataRows[r]);
      }
      row(sheet, 6 + dataRows.length, "SOURCES: Social Security Administration.");
    }
    return wb;
  }

  private static JsonNode byFips(ArrayNode rows, String fips) {
    for (JsonNode n : rows) {
      if (fips == null ? n.get("county_fips").isNull()
          : fips.equals(n.get("county_fips").asText())) {
        return n;
      }
    }
    throw new AssertionError("no row with county_fips " + fips);
  }

  @Test void readsCountiesAndIndependentCitiesPastSubHeaders() throws IOException {
    try (XSSFWorkbook wb = stateWorkbook(
        area(null, "Total, Maryland", 24, 100),
        new Object[] {null, null, "Counties"},
        area("Allegany", null, 24001, 60),
        new Object[] {},
        new Object[] {null, null, "Independent city"},
        area("Baltimore city", null, 24510, 40))) {
      ArrayNode rows = new SsaOasdiCountyTransformer().stateRows(wb, "MD", 2024);
      assertEquals(3, rows.size());
      assertEquals("Total, Maryland", byFips(rows, null).get("county_name").asText());
      assertEquals(60.0, byFips(rows, "24001").get("total_count").asDouble());
      assertEquals(40.0, byFips(rows, "24510").get("total_amount").asDouble());
      assertEquals(8.0, byFips(rows, "24510").get("children_disability_count").asDouble());
    }
  }

  @Test void readsTextCodesBlankNameCellsAndFootnotedTotalLabel() throws IOException {
    try (XSSFWorkbook wb = stateWorkbook(
        area("", "Total, Alabama b", "01000", 100),
        area("Autauga", null, "01001", 100))) {
      ArrayNode rows = new SsaOasdiCountyTransformer().stateRows(wb, "AL", 2008);
      assertEquals("Total, Alabama", byFips(rows, null).get("county_name").asText());
      assertEquals("Autauga", byFips(rows, "01001").get("county_name").asText());
    }
  }

  @Test void cleansLabelsAndSkipsAreasWithoutFigures() throws IOException {
    try (XSSFWorkbook wb = stateWorkbook(
        area(null, "Total,\nVirginia", 51, 100),
        area("Bedford\u00a0a", null, 51019, 60),
        area("Prince of Wales-\n    Outer Ketchikan", null, 51021, 20),
        area("Lower Connecticut\n  River Valley", null, 51023, 20),
        new Object[] {"Bedford", null, 51515, "a", "a", "a", "a", "a", "a", "a", "a", "a"})) {
      ArrayNode rows = new SsaOasdiCountyTransformer().stateRows(wb, "VA", 2014);
      assertEquals(4, rows.size());
      assertEquals("Total, Virginia", byFips(rows, null).get("county_name").asText());
      assertEquals("Bedford", byFips(rows, "51019").get("county_name").asText());
      assertEquals("Prince of Wales-Outer Ketchikan",
          byFips(rows, "51021").get("county_name").asText());
      assertEquals("Lower Connecticut River Valley",
          byFips(rows, "51023").get("county_name").asText());
    }
  }

  @Test void failsWhenCodeColumnDoesNotHoldStateCodes() throws IOException {
    try (XSSFWorkbook wb = stateWorkbook(
        area(null, "Total, Alabama", 952511, 100),
        area("Autauga", null, 9155, 100))) {
      assertThrows(IllegalStateException.class,
          () -> new SsaOasdiCountyTransformer().stateRows(wb, "AL", 2007));
    }
  }

  @Test void failsOnFiguresWithoutACode() throws IOException {
    try (XSSFWorkbook wb = stateWorkbook(
        area(null, "Total, Alabama", 1, 100),
        area("Autauga", null, 1001, 50),
        area("Baldwin", null, null, 50))) {
      assertThrows(IllegalStateException.class,
          () -> new SsaOasdiCountyTransformer().stateRows(wb, "AL", 2024));
    }
  }

  private static XSSFWorkbook nationalWorkbook(int dcTotal) {
    XSSFWorkbook wb = new XSSFWorkbook();
    Sheet sheet = wb.createSheet("Table");
    row(sheet, 4, area("Alabama", null, null, 100));
    row(sheet, 12, area("District of Columbia", null, null, dcTotal));
    return wb;
  }

  @Test void readsDistrictOfColumbiaFromNationalTables() throws IOException {
    try (XSSFWorkbook counts = nationalWorkbook(85682);
         XSSFWorkbook amounts = nationalWorkbook(156808)) {
      ArrayNode rows = new SsaOasdiCountyTransformer().dcRows(counts, amounts, 2024);
      assertEquals(2, rows.size());
      for (JsonNode n : rows) {
        assertEquals("DC", n.get("state_abbr").asText());
        assertEquals(85682.0, n.get("total_count").asDouble());
        assertEquals(156808.0, n.get("total_amount").asDouble());
      }
      assertTrue(byFips(rows, null).get("county_name").asText().startsWith("Total, "));
      assertEquals("District of Columbia",
          byFips(rows, "11001").get("county_name").asText());
    }
  }
}
