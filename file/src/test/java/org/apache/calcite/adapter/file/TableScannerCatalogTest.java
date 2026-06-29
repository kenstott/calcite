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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.converters.HtmlTableScanner;
import org.apache.calcite.adapter.file.converters.XmlTableScanner;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-103 / FILE-104 — exact table-catalog goldens for the HTML and XML scanners (recode of the
 * contains()-based tests), pinning the gaps: HTML duplicate-name suffixing + identifier
 * sanitization, and XML XXE/DOCTYPE rejection + repeating-element detection.
 */
@Tag("unit")
public class TableScannerCatalogTest {

  private static Path write(Path dir, String name, String content) throws IOException {
    Path f = dir.resolve(name);
    Files.write(f, content.getBytes(StandardCharsets.UTF_8));
    return f;
  }

  // -------------------------------------------------- HTML (FILE-103) --------------------------

  @Test @Tag("FILE-103") void htmlDuplicateNamesGetSuffixed(@TempDir Path dir) throws Exception {
    String html = "<html><body>"
        + "<table><caption>Report</caption><tr><th>a</th></tr><tr><td>1</td></tr></table>"
        + "<table><caption>Report</caption><tr><th>a</th></tr><tr><td>2</td></tr></table>"
        + "</body></html>";
    List<HtmlTableScanner.TableInfo> tables =
        HtmlTableScanner.scanTables(Sources.of(write(dir, "dup.html", html).toFile()));
    assertEquals(2, tables.size(), "two tables detected");
    assertEquals("report", tables.get(0).name);
    assertEquals("report_2", tables.get(1).name, "duplicate name disambiguated with _2");
  }

  @Test @Tag("FILE-103") void htmlTableNameIsSanitized(@TempDir Path dir) throws Exception {
    String html = "<html><body>"
        + "<table id=\"9 Bad-Name!\"><tr><th>a</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";
    List<HtmlTableScanner.TableInfo> tables =
        HtmlTableScanner.scanTables(Sources.of(write(dir, "san.html", html).toFile()));
    assertEquals(1, tables.size());
    String name = tables.get(0).name;
    assertTrue(name.matches("[A-Za-z_][A-Za-z0-9_]*"),
        "name must be a sanitized identifier (got '" + name + "')");
    assertTrue(name.startsWith("_"), "a leading digit must be prefixed with '_' (got '" + name + "')");
  }

  // -------------------------------------------------- XML (FILE-104) ---------------------------

  @Test @Tag("FILE-104") void xmlRepeatingElementsAreDetected(@TempDir Path dir) throws Exception {
    String xml = "<root><item><n>a</n></item><item><n>b</n></item><item><n>c</n></item></root>";
    List<XmlTableScanner.TableInfo> tables =
        XmlTableScanner.scanTables(Sources.of(write(dir, "rep.xml", xml).toFile()));
    assertEquals(1, tables.size(), "one repeating-element table");
    assertEquals(3, tables.get(0).count, "three repeating elements");
  }

  @Test @Tag("FILE-104") void xmlDoctypeIsRejectedXxeHardening(@TempDir Path dir) throws Exception {
    String xml = "<?xml version=\"1.0\"?>"
        + "<!DOCTYPE root [<!ENTITY x \"expanded\">]>"
        + "<root><item>1</item><item>2</item></root>";
    Path f = write(dir, "xxe.xml", xml);
    assertThrows(IOException.class, () -> XmlTableScanner.scanTables(Sources.of(f.toFile())),
        "a DOCTYPE-bearing document must be rejected (XXE hardening)");
  }
}
