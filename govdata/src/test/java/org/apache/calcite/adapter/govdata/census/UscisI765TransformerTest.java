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

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  private static final String FIXTURE = "i765_application_for_employment_fy2026_q1_v1.xlsx";

  private Path stageFixture(Path dir) throws Exception {
    Path target = dir.resolve(FIXTURE);
    try (InputStream in = getClass().getResourceAsStream("/census/" + FIXTURE)) {
      Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
    }
    return target;
  }

  private static RequestContext year(int fy) {
    Map<String, String> dims = new HashMap<>();
    dims.put("year", Integer.toString(fy));
    return RequestContext.builder().url("unused").parameters(settings).dimensionValues(dims)
        .build();
  }

  private static int count(Iterator<Map<String, Object>> it) {
    int n = 0;
    while (it.hasNext()) {
      it.next();
      n++;
    }
    return n;
  }

  private static Map<String, String> settings = new HashMap<>();

  @org.junit.jupiter.api.BeforeEach void resetSettings() {
    settings = new HashMap<>();
  }

  private static void operand(String localDir, String script, String node) {
    Map<String, String> m = new HashMap<>();
    m.put("uscisLocalDir", localDir);
    m.put("uscisBrowserScript", script);
    m.put("uscisNodeCommand", node);
    m.put("uscisDataPageUrl", "https://www.uscis.gov/data");
    m.put("uscisRequestDelayMs", "0");
    settings = m;
  }

  /** A stand-in for the browser helper: serves one workbook, answers 404 for the rest. */
  private static Path stubHelper(Path dir, String servedUrlPart, Path file) throws Exception {
    Path script = dir.resolve("stub-helper.sh");
    String body = "while IFS= read -r line; do\n"
        + "  case \"$line\" in\n"
        + "    *" + servedUrlPart + "*) echo '{\"status\":200,\"file\":\"" + file + "\"}' ;;\n"
        + "    *) echo '{\"status\":404}' ;;\n"
        + "  esac\n"
        + "done\n";
    Files.write(script, body.getBytes(StandardCharsets.UTF_8));
    return script;
  }

  @Test void localDirectoryIsTheFirstSource(@TempDir Path dir) throws Exception {
    stageFixture(dir);
    // No browser helper exists: a quarter found locally must not need one.
    Path sub = Files.createDirectory(dir.resolve("local"));
    stageFixture(sub);
    Map<String, String> m = new HashMap<>();
    m.put("uscisLocalDir", sub.toString());
    settings = m;
    // FY2026 Q2-Q4 are not local, so the browser is required for them and its absence is named.
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new UscisI765Transformer().fetchAndTransform(year(2026)));
    assertTrue(e.getMessage().contains("browser"), e.getMessage());
  }

  @Test void quartersComeFromLocalDirectoryThenBrowser(@TempDir Path dir) throws Exception {
    Path q1 = stageFixture(dir);
    Path q2 = dir.resolve("served-by-browser.xlsx");
    Files.copy(q1, q2);
    Path script = stubHelper(dir, "fy2026_q2_v1", q2);
    operand(dir.toString(), script.toString(), "/bin/bash");
    int rows = count(new UscisI765Transformer().fetchAndTransform(year(2026)));
    int perQuarter = rows / 2;
    assertTrue(rows > 0 && rows % 2 == 0 && perQuarter % 4 == 0, "rows: " + rows);
  }

  @Test void failsNamingTheCauseWhenBrowserIsUnavailable(@TempDir Path dir) {
    operand("", new File(dir.toFile(), "missing.js").getPath(), "node");
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new UscisI765Transformer().fetchAndTransform(year(2025)));
    assertTrue(e.getMessage().contains("browser helper script not found"), e.getMessage());
  }

  @Test void failsNamingTheCauseWhenBrowserIsNotConfigured() {
    operand("", "", "node");
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new UscisI765Transformer().fetchAndTransform(year(2025)));
    assertTrue(e.getMessage().contains("headed browser not configured"), e.getMessage());
  }
}
