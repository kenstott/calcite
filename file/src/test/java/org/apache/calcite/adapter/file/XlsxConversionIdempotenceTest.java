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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * FILE-015 — re-converting an unchanged xlsx must not rewrite the emitted table JSON (the conversion
 * is skipped via the ConversionMetadata mtime/etag gate). The xlsx decomposes to one JSON array file
 * per table; we compare only those array files (isolating them from the metadata object files).
 */
@Tag("unit")
public class XlsxConversionIdempotenceTest {

  @Test @Tag("FILE-015") void reConvertingUnchangedXlsxDoesNotRewriteJson(@TempDir Path root) throws Exception {
    Path src = Files.createDirectories(root.resolve("src"));
    writeXlsx(src.resolve("people.xlsx"));
    Path cache = Files.createDirectories(root.resolve("cache"));

    discover(src, cache);
    Map<String, Long> before = tableJsonMtimes(root);
    assertFalse(before.isEmpty(), "xlsx conversion should have emitted at least one table JSON");

    Thread.sleep(1100); // mtime resolves at ~1s granularity
    discover(src, cache); // unchanged workbook

    assertEquals(before, tableJsonMtimes(root),
        "re-converting an unchanged xlsx must not rewrite the table JSON (gate must skip)");
  }

  // ---------------------------------------------------------------------------------------------

  private static void writeXlsx(Path file) throws Exception {
    try (XSSFWorkbook wb = new XSSFWorkbook(); OutputStream out = Files.newOutputStream(file)) {
      Sheet sheet = wb.createSheet("people");
      Row h = sheet.createRow(0);
      h.createCell(0).setCellValue("id");
      h.createCell(1).setCellValue("name");
      Row r1 = sheet.createRow(1);
      r1.createCell(0).setCellValue(1);
      r1.createCell(1).setCellValue("alice");
      Row r2 = sheet.createRow(2);
      r2.createCell(0).setCellValue(2);
      r2.createCell(1).setCellValue("bob");
      wb.write(out);
    }
  }

  /** Builds a fresh schema over the workbook, forcing the xlsx->JSON conversion via table discovery. */
  private static void discover(Path src, Path cache) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(src, cache));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
      while (rs.next()) {
        rs.getString("TABLE_NAME");
      }
    }
  }

  /** mtimes of emitted table-data JSON (content begins with '[') — excludes metadata object files. */
  private static Map<String, Long> tableJsonMtimes(Path dir) throws Exception {
    Map<String, Long> m = new TreeMap<>();
    try (Stream<Path> s = Files.walk(dir)) {
      for (Path p : (Iterable<Path>) s.filter(x -> x.toString().endsWith(".json"))::iterator) {
        String content = new String(Files.readAllBytes(p), StandardCharsets.UTF_8).trim();
        if (content.startsWith("[")) {
          m.put(dir.relativize(p).toString(), p.toFile().lastModified());
        }
      }
    }
    return m;
  }

  private static String model(Path src, Path cache) {
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"s\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"directory\": \"" + src.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"baseDirectory\": \"" + cache.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"ephemeralCache\": false,\n"
        + "      \"executionEngine\": \"linq4j\"\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }
}
