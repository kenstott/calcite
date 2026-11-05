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
package org.apache.calcite.adapter.govdata.census;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link UscisI765Transformer}. */
@Tag("unit")
class UscisI765TransformerTest {

  @Test void buildsUrlsForBothFolders() {
    assertEquals("https://www.uscis.gov/sites/default/files/document/reports/"
            + "i765_application_for_employment_fy2026_q1_v1.xlsx",
        UscisI765Transformer.workbookUrl("reports", 2026, 1, "_v1"));
    assertEquals("https://www.uscis.gov/sites/default/files/document/data/"
            + "i765_application_for_employment_fy2025_q4.xlsx",
        UscisI765Transformer.workbookUrl("data", 2025, 4, ""));
  }

  @Test void parsesFy2026Q1Workbook() throws Exception {
    ArrayNode out = new ArrayNode(JsonNodeFactory.instance);
    try (InputStream in = getClass().getResourceAsStream(
            "/census/i765_application_for_employment_fy2026_q1_v1.xlsx");
         XSSFWorkbook wb = new XSSFWorkbook(in)) {
      UscisI765Transformer.parseSheet(wb.getSheetAt(0), 2026, 1, out);
    }
    assertTrue(out.size() > 0 && out.size() % 4 == 0, "rows: " + out.size());
    assertEquals(2026, out.get(0).get("fiscal_year").asInt());
    assertEquals(1, out.get(0).get("fiscal_quarter").asInt());
    assertEquals("Initial", out.get(0).get("filing_type").asText());
    assertEquals("Renewal", out.get(1).get("filing_type").asText());
  }

  @Test void openLocalReadsStagedWorkbook() throws Exception {
    java.io.File dir = java.nio.file.Files.createTempDirectory("i765").toFile();
    try (InputStream src = getClass().getResourceAsStream("/census/"
        + "i765_application_for_employment_fy2026_q1_v1.xlsx")) {
      java.nio.file.Files.copy(src, new java.io.File(dir,
          "i765_application_for_employment_fy2026_q1_v1.xlsx").toPath());
    }
    try (InputStream in = UscisI765Transformer.openLocal(dir.getPath(), 2026, 1)) {
      assertTrue(in != null);
    }
    assertEquals(null, UscisI765Transformer.openLocal(dir.getPath(), 2026, 2));
    assertEquals(null, UscisI765Transformer.openLocal(null, 2026, 1));
    assertEquals(null, UscisI765Transformer.openLocal("", 2026, 1));
  }
}
