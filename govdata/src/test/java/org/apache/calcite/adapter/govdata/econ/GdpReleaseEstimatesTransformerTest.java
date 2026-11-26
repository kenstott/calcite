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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link GdpReleaseEstimatesTransformer}. */
@Tag("unit")
class GdpReleaseEstimatesTransformerTest {

  /** Builds a workbook shaped like {@code routput_first_second_third.xlsx}. */
  private static InputStream workbook(Object[]... dataRows) throws IOException {
    try (XSSFWorkbook wb = new XSSFWorkbook();
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      Sheet notes = wb.createSheet("NOTES");
      notes.createRow(0).createCell(0).setCellValue("Real GNP/GDP");
      notes.createRow(8).createCell(0).setCellValue("Last updated on: October 30, 2026");
      Sheet data = wb.createSheet("DATA");
      data.createRow(0).createCell(0).setCellValue("Real GNP/GDP (ROUTPUT)");
      write(data.createRow(4), new Object[] {"Date", "First", "Second", "Third", "Most_Recent"});
      int r = 5;
      for (Object[] values : dataRows) {
        write(data.createRow(r++), values);
      }
      wb.write(out);
      return new ByteArrayInputStream(out.toByteArray());
    }
  }

  private static void write(Row row, Object[] values) {
    for (int i = 0; i < values.length; i++) {
      if (values[i] instanceof Number) {
        row.createCell(i).setCellValue(((Number) values[i]).doubleValue());
      } else if (values[i] != null) {
        row.createCell(i).setCellValue((String) values[i]);
      }
    }
  }

  @Test void emitsOneRowPerQuarterWithNullForMissingReleases() throws IOException {
    List<Map<String, Object>> rows = GdpReleaseEstimatesTransformer.parse(
        workbook(
            new Object[] {"2008:Q4", -3.8, -6.2, -6.3, -8.5},
            new Object[] {"2025:Q3", "#N/A", "#N/A", "#N/A", 3.88},
            new Object[] {"2026:Q4", "#N/A", "#N/A", "#N/A", "#N/A"}),
        "routput");

    assertEquals(2, rows.size());
    Map<String, Object> first = rows.get(0);
    assertEquals("Real GNP/GDP (ROUTPUT)", first.get("measure"));
    assertEquals("2008-10-01", first.get("date"));
    assertEquals(Integer.valueOf(2008), first.get("year"));
    assertEquals(Integer.valueOf(4), first.get("quarter"));
    assertEquals("2008Q4", first.get("period"));
    assertEquals(Double.valueOf(-3.8), first.get("first_release"));
    assertEquals(Double.valueOf(-6.2), first.get("second_release"));
    assertEquals("2026-10-30", first.get("source_updated"));
    assertNull(rows.get(1).get("first_release"));
    assertEquals(Double.valueOf(3.88), rows.get(1).get("most_recent"));
  }

  @Test void rejectsUnexpectedQuarterLabel() {
    assertThrows(IOException.class, () -> GdpReleaseEstimatesTransformer.parse(
        workbook(new Object[] {"2008M10", 1.0, 1.0, 1.0, 1.0}), "routput"));
  }

  @Test void rejectsNonNumericValue() {
    assertThrows(IOException.class, () -> GdpReleaseEstimatesTransformer.parse(
        workbook(new Object[] {"2008:Q4", "n.a.", 1.0, 1.0, 1.0}), "routput"));
  }
}
