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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parsing of the VBA Monday Morning Workload Report "Transformation" sheet. */
@Tag("unit")
class VbaClaimsWorkloadTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Builds the sheet with the label column at {@code labelCol} (B in FY2018-25, D in FY2026). */
  private static Sheet sheet(XSSFWorkbook wb, int labelCol, String reporting) {
    Sheet s = wb.createSheet("Transformation");
    int pend = labelCol + 6;
    s.createRow(1).createCell(labelCol).setCellValue("Compensation and Pension Rating Bundle Metrics");
    Row h = s.createRow(2);
    h.createCell(labelCol).setCellValue("National View");
    h.createCell(pend).setCellValue("# Pending");
    h.createCell(pend + 1).setCellValue("# Pending > 125");
    h.createCell(pend + 2).setCellValue("% Pending > 125 days");
    s.createRow(3).createCell(labelCol).setCellValue(reporting);
    line(s, 4, labelCol, "Compensation and Pension Rating Bundle", "Total", 670845, 74448);
    line(s, 5, labelCol, "Original Entitlement", "Total", 164025, 32937);
    line(s, 6, labelCol, "Initial entitlement for service-connected disability", "EP 010",
        59655, 15433);
    s.createRow(7);
    s.createRow(8).createCell(labelCol).setCellValue("Education Metrics");
    line(s, 9, labelCol, "Total Chapter 33 Original Claims", "Total", 2477, 595);
    return s;
  }

  private static void line(Sheet s, int r, int labelCol, String label, String key, long pending,
      long over) {
    Row row = s.createRow(r);
    row.createCell(labelCol).setCellValue(label);
    row.createCell(labelCol + 5).setCellValue(key);
    row.createCell(labelCol + 6).setCellValue(pending);
    row.createCell(labelCol + 7).setCellValue(over);
  }

  private static ArrayNode parse(int labelCol, String reporting) throws IOException {
    try (XSSFWorkbook wb = new XSSFWorkbook()) {
      ArrayNode out = MAPPER.createArrayNode();
      VbaClaimsWorkloadTransformer.parseSheet(sheet(wb, labelCol, reporting), "test", out);
      return out;
    }
  }

  @Test void parsesTotalsAndEpRowsAndStopsAtEducation() throws IOException {
    ArrayNode out = parse(1, "Reporting through May 07, 2022");
    assertEquals(3, out.size());
    JsonNode headline = out.get(0);
    assertEquals("2022-05-07", headline.get("week_ending").asText());
    assertEquals("total", headline.get("line_type").asText());
    assertTrue(headline.get("ep_code").isNull());
    assertEquals(670845L, headline.get("pending_claims").asLong());
    assertEquals(74448L, headline.get("pending_over_125_days").asLong());
    JsonNode ep = out.get(2);
    assertEquals("ep", ep.get("line_type").asText());
    assertEquals("010", ep.get("ep_code").asText());
  }

  @Test void handlesFy2026TwoColumnShift() throws IOException {
    ArrayNode out = parse(3, "Reporting through September 26, 2026");
    assertEquals(3, out.size());
    assertEquals("2026-09-26", out.get(0).get("week_ending").asText());
    assertEquals(15433L, out.get(2).get("pending_over_125_days").asLong());
  }

  @Test void readsFormulaCellsByCachedResult() throws IOException {
    try (XSSFWorkbook wb = new XSSFWorkbook()) {
      Sheet s = sheet(wb, 1, "x");
      s.getRow(3).getCell(1).setCellFormula("\"Reporting through \"&\"January 03, 2026\"");
      wb.getCreationHelper().createFormulaEvaluator().evaluateAll();
      ArrayNode out = MAPPER.createArrayNode();
      VbaClaimsWorkloadTransformer.parseSheet(s, "test", out);
      assertEquals("2026-01-03", out.get(0).get("week_ending").asText());
    }
  }

  @Test void failsWhenLayoutChanges() throws IOException {
    try (XSSFWorkbook wb = new XSSFWorkbook()) {
      Sheet s = wb.createSheet("Transformation");
      s.createRow(1).createCell(1).setCellValue("Reporting through May 07, 2022");
      assertThrows(IllegalStateException.class,
          () -> VbaClaimsWorkloadTransformer.parseSheet(s, "test", MAPPER.createArrayNode()));
    }
  }

  @Test void listsYearWorkbookLinksOnce() {
    String html = "<a href=\"/REPORTS/mmwr/2026/MMWR-09-26-2026.xlsx#\">x</a>"
        + "<a href=\"/REPORTS/mmwr/2026/MMWR-09-26-2026.xlsx\">x</a>"
        + "<a href=\"/REPORTS/mmwr/2026/MMWR-01-03-2026.xlsm\">x</a>"
        + "<a href=\"/REPORTS/mmwr/2025/MMWR-01-04-2025.xlsm\">x</a>";
    Set<String> paths = VbaClaimsWorkloadTransformer.workbookPaths(html, "2026");
    assertEquals(2, paths.size());
  }
}
