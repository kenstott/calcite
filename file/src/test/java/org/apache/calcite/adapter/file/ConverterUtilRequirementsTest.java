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

import org.apache.calcite.adapter.file.converters.ConverterUtils;
import org.apache.calcite.adapter.file.converters.FileConversionManager;
import org.apache.calcite.adapter.file.converters.MarkdownTableScanner;
import org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-109 / FILE-105 / FILE-156 — exact-assertion recodes of weak file-adapter converter-utility
 * tests.
 *
 * <ul>
 *   <li>FILE-109: {@link ConverterUtils#setJsonValueWithTypeInference} type coercion and
 *       {@link ConverterUtils#isNumeric} acceptance/rejection.</li>
 *   <li>FILE-105: {@link MarkdownTableScanner} table-detection contract (>=3 lines, separator not
 *       at index 0), escaped-pipe preservation, and PascalCase on-disk JSON naming.</li>
 *   <li>FILE-156: {@link CsvEnumerator#parseDecimal} HALF_UP symmetric rounding and precision
 *       overflow.</li>
 *   <li>FILE-130: {@link ParquetConversionUtil#needsConversion(File, File)} staleness decision —
 *       convert iff the parquet cache is missing OR {@code source.lastModified() > parquet} (strict;
 *       equal mtimes reuse), and any exception reading timestamps forces a convert. No tolerance
 *       window. (RECODE of {@code ParquetConversionUtilDeepCoverageTest2}'s single newer-branch.)</li>
 *   <li>FILE-166: {@link FileConversionManager#requiresConversion}/{@code isDirectlyUsable} format
 *       routing — the requires-conversion set (xlsx/xls/html/htm/xml/md/docx/pptx) and the
 *       directly-usable set (csv/tsv/json/parquet/arrow/yaml + .gz) pinned exhaustively and disjoint.</li>
 * </ul>
 */
@Tag("unit")
public class ConverterUtilRequirementsTest {

  private final ObjectMapper mapper = new ObjectMapper();

  // ============================================================ FILE-109 ======================

  @Test @Tag("FILE-109") void jsonValueNullAndEmptyBecomeJsonNull() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "n", null);
    ConverterUtils.setJsonValueWithTypeInference(node, "e", "");
    assertTrue(node.get("n").isNull(), "null -> JSON null");
    assertTrue(node.get("e").isNull(), "empty -> JSON null");
  }

  @Test @Tag("FILE-109") void jsonValueIntegerStringBecomesLong() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "42");
    assertTrue(node.get("k").isNumber());
    assertTrue(node.get("k").isIntegralNumber(), "integer string stored as integral (long)");
    assertEquals(42L, node.get("k").longValue());
  }

  @Test @Tag("FILE-109") void jsonValueNegativeIntegerStringBecomesLong() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "-100");
    assertTrue(node.get("k").isIntegralNumber());
    assertEquals(-100L, node.get("k").longValue());
  }

  @Test @Tag("FILE-109") void jsonValueDecimalStringBecomesDouble() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "3.14");
    assertTrue(node.get("k").isNumber());
    assertFalse(node.get("k").isIntegralNumber(), "decimal stored as floating point (double)");
    assertEquals(3.14, node.get("k").doubleValue(), 0.0);
  }

  @Test @Tag("FILE-109") void jsonValueTrueLowerCaseBecomesBoolean() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "true");
    assertTrue(node.get("k").isBoolean());
    assertTrue(node.get("k").booleanValue());
  }

  @Test @Tag("FILE-109") void jsonValueFalseUpperCaseBecomesBoolean() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "FALSE");
    assertTrue(node.get("k").isBoolean(), "boolean parse is case-insensitive");
    assertFalse(node.get("k").booleanValue());
  }

  @Test @Tag("FILE-109") void jsonValuePlainStringStaysText() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "hello world");
    assertTrue(node.get("k").isTextual());
    assertEquals("hello world", node.get("k").textValue());
  }

  @Test @Tag("FILE-109") void jsonValueDateStringStaysText() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "2024-01-15");
    assertTrue(node.get("k").isTextual(), "date inferred to ISO text, not numeric/boolean");
    assertEquals("2024-01-15", node.get("k").textValue());
  }

  @Test @Tag("FILE-109") void jsonValueDateTimeStringStaysText() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "k", "2024-01-15 10:30:00");
    assertTrue(node.get("k").isTextual());
    // Stored as ISO_LOCAL_DATE_TIME (no configured timezone): "2024-01-15T10:30:00".
    assertEquals("2024-01-15T10:30:00", node.get("k").textValue());
  }

  @Test @Tag("FILE-109") void isNumericAcceptsIntegerNegativeDecimalScientific() {
    assertTrue(ConverterUtils.isNumeric("42"), "integer");
    assertTrue(ConverterUtils.isNumeric("-3.14"), "negative decimal");
    assertTrue(ConverterUtils.isNumeric("0.001"), "decimal");
    assertTrue(ConverterUtils.isNumeric("1.5E10"), "scientific");
  }

  @Test @Tag("FILE-109") void isNumericRejectsNullEmptyMixedText() {
    assertFalse(ConverterUtils.isNumeric(null), "null");
    assertFalse(ConverterUtils.isNumeric(""), "empty");
    assertFalse(ConverterUtils.isNumeric("12abc"), "mixed");
    assertFalse(ConverterUtils.isNumeric("abc"), "text");
  }

  // ============================================================ FILE-105 ======================

  /** Writes {@code content} to {@code dir/name} and returns the file. */
  private static File writeFile(Path dir, String name, String content) throws Exception {
    Path f = dir.resolve(name);
    Files.write(f, content.getBytes(StandardCharsets.UTF_8));
    return f.toFile();
  }

  @Test @Tag("FILE-105") void markdownWellFormedTableEmitsPascalCaseJson(@TempDir Path dir)
      throws Exception {
    // A single, valid 3-line table: header, separator (index 1, not 0), one data row.
    String md = "| Name | Age |\n"
        + "|------|-----|\n"
        + "| Alice | 30 |\n";
    File input = writeFile(dir, "people_list.md", md);
    File outDir = Files.createDirectory(dir.resolve("out")).toFile();

    MarkdownTableScanner.scanAndConvertTables(input, outDir);

    // baseName = toPascalCase("people_list") = "PeopleList"; single table -> "<base>.json".
    File expected = new File(outDir, "PeopleList.json");
    assertTrue(expected.exists(), "PascalCase JSON catalog file emitted: " + expected.getName());

    String json = new String(Files.readAllBytes(expected.toPath()), StandardCharsets.UTF_8);
    assertTrue(json.contains("\"Name\""), "header column 'Name' present");
    assertTrue(json.contains("\"Alice\""), "data value 'Alice' present");
    assertTrue(json.contains("\"30\""), "data value '30' present");
  }

  @Test @Tag("FILE-105") void markdownEscapedPipeIsPreserved(@TempDir Path dir) throws Exception {
    // An escaped pipe (\|) inside a cell must survive as a literal '|', not split the cell.
    String md = "| Expr |\n"
        + "|------|\n"
        + "| a \\| b |\n";
    File input = writeFile(dir, "exprs.md", md);
    File outDir = Files.createDirectory(dir.resolve("out")).toFile();

    MarkdownTableScanner.scanAndConvertTables(input, outDir);

    File expected = new File(outDir, "Exprs.json");
    assertTrue(expected.exists(), "JSON catalog file emitted");
    String json = new String(Files.readAllBytes(expected.toPath()), StandardCharsets.UTF_8);
    // The single data cell value is "a | b" (escaped pipe unescaped to a literal pipe).
    assertTrue(json.contains("a | b"),
        "escaped pipe preserved as a literal '|' in the cell value; got: " + json);
  }

  @Test @Tag("FILE-105") void markdownSeparatorAtIndexZeroYieldsNoTableJson(@TempDir Path dir)
      throws Exception {
    // Separator is the FIRST table row (index 0) -> parseTable returns null -> no output.
    String md = "|------|-----|\n"
        + "| Name | Age |\n"
        + "| Alice | 30 |\n";
    File input = writeFile(dir, "bad_sep.md", md);
    File outDir = Files.createDirectory(dir.resolve("out")).toFile();

    MarkdownTableScanner.scanAndConvertTables(input, outDir);

    File[] emitted = outDir.listFiles();
    assertNotNull(emitted, "output dir listing");
    assertEquals(0, emitted.length,
        "separator at index 0 yields no table JSON; got: " + Arrays.toString(emitted));
  }

  @Test @Tag("FILE-105") void markdownFewerThanThreeLinesYieldsNoTableJson(@TempDir Path dir)
      throws Exception {
    // Only header + separator (2 lines, no data row) -> below the 3-line minimum -> no output.
    String md = "| Name | Age |\n"
        + "|------|-----|\n";
    File input = writeFile(dir, "too_short.md", md);
    File outDir = Files.createDirectory(dir.resolve("out")).toFile();

    MarkdownTableScanner.scanAndConvertTables(input, outDir);

    File[] emitted = outDir.listFiles();
    assertNotNull(emitted, "output dir listing");
    assertEquals(0, emitted.length,
        "a sub-3-line table yields no JSON; got: " + Arrays.toString(emitted));
  }

  // ============================================================ FILE-156 ======================

  @Test @Tag("FILE-156") void parseDecimalHalfUpSymmetricRounding() {
    assertEquals(new BigDecimal("123.46"), CsvEnumerator.parseDecimal(5, 2, "123.455"),
        "positive tie rounds away from zero (HALF_UP)");
    assertEquals(new BigDecimal("-123.46"), CsvEnumerator.parseDecimal(5, 2, "-123.455"),
        "negative tie rounds away from zero (symmetric HALF_UP)");
    assertEquals(new BigDecimal("123.45"), CsvEnumerator.parseDecimal(5, 2, "123.454"),
        "below tie rounds down");
    assertEquals(new BigDecimal("-123.45"), CsvEnumerator.parseDecimal(5, 2, "-123.454"),
        "below tie rounds down (negative)");
  }

  @Test @Tag("FILE-156") void parseDecimalPrecisionOverflowThrows() {
    // Integer-part digits exceed (precision - scale) -> IllegalArgumentException.
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(4, 2, "123.45"));
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(4, 0, "12345"));
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(4, 0, "-12345"));
  }

  // ============================================================ FILE-130 ======================
  // ParquetConversionUtil.needsConversion(File source, File parquet): the parquet file IS the cache.
  // Mtimes are set to fixed round-second literals so the >/== boundary is asserted exactly. Hermetic
  // @TempDir; the exception branch uses a File whose lastModified() throws.

  @Test @Tag("FILE-130") void needsConversionWhenParquetCacheMissing(@TempDir Path dir)
      throws Exception {
    File source = dir.resolve("data.csv").toFile();
    Files.write(source.toPath(), "x\n1\n".getBytes(StandardCharsets.UTF_8));
    File parquet = dir.resolve("data.parquet").toFile(); // never created
    assertFalse(parquet.exists());
    assertTrue(ParquetConversionUtil.needsConversion(source, parquet),
        "a missing parquet cache must force conversion");
  }

  @Test @Tag("FILE-130") void needsConversionWhenSourceStrictlyNewer(@TempDir Path dir)
      throws Exception {
    File source = dir.resolve("data.csv").toFile();
    File parquet = dir.resolve("data.parquet").toFile();
    Files.write(source.toPath(), "x\n1\n".getBytes(StandardCharsets.UTF_8));
    Files.write(parquet.toPath(), new byte[] {0});
    assertTrue(source.setLastModified(2_000_000_000_000L));
    assertTrue(parquet.setLastModified(1_000_000_000_000L));
    assertTrue(ParquetConversionUtil.needsConversion(source, parquet),
        "source strictly newer than parquet must force conversion");
  }

  @Test @Tag("FILE-130") void noConversionWhenMtimesEqual(@TempDir Path dir) throws Exception {
    File source = dir.resolve("data.csv").toFile();
    File parquet = dir.resolve("data.parquet").toFile();
    Files.write(source.toPath(), "x\n1\n".getBytes(StandardCharsets.UTF_8));
    Files.write(parquet.toPath(), new byte[] {0});
    long t = 1_500_000_000_000L;
    assertTrue(source.setLastModified(t));
    assertTrue(parquet.setLastModified(t));
    assertFalse(ParquetConversionUtil.needsConversion(source, parquet),
        "equal mtimes reuse the cache (decision is strict >, not >=)");
  }

  @Test @Tag("FILE-130") void noConversionWhenParquetStrictlyNewer(@TempDir Path dir)
      throws Exception {
    File source = dir.resolve("data.csv").toFile();
    File parquet = dir.resolve("data.parquet").toFile();
    Files.write(source.toPath(), "x\n1\n".getBytes(StandardCharsets.UTF_8));
    Files.write(parquet.toPath(), new byte[] {0});
    assertTrue(source.setLastModified(1_000_000_000_000L));
    assertTrue(parquet.setLastModified(2_000_000_000_000L));
    assertFalse(ParquetConversionUtil.needsConversion(source, parquet),
        "parquet newer than source reuses the cache");
  }

  @Test @Tag("FILE-130") void needsConversionWhenTimestampReadThrows(@TempDir Path dir)
      throws Exception {
    File source = dir.resolve("data.csv").toFile();
    Files.write(source.toPath(), "x\n1\n".getBytes(StandardCharsets.UTF_8));
    // A parquet "file" that exists but whose mtime read throws -> exception path -> convert.
    File parquet = new File(dir.resolve("boom.parquet").toString()) {
      @Override public boolean exists() {
        return true;
      }

      @Override public long lastModified() {
        throw new RuntimeException("mtime read failed");
      }
    };
    assertTrue(ParquetConversionUtil.needsConversion(source, parquet),
        "any exception reading timestamps must force conversion (no silent reuse)");
  }

  // ============================================================ FILE-166 ======================
  // FileConversionManager.requiresConversion / isDirectlyUsable — pure static format routing.
  // The requires-conversion set and the directly-usable set are pinned exhaustively, proven disjoint,
  // and an unknown extension falls into neither.

  @Test @Tag("FILE-166") void requiresConversionSetIsExactlyDocumentFormats() {
    for (String f : new String[] {
        "a.xlsx", "a.xls", "a.html", "a.htm", "a.xml", "a.md", "a.docx", "a.pptx",
        "A.DOCX", "deck.PPTX"}) {  // case-insensitive
      assertTrue(FileConversionManager.requiresConversion(f), f + " must require conversion");
      assertFalse(FileConversionManager.isDirectlyUsable(f),
          f + " is a conversion format, not directly usable");
    }
  }

  @Test @Tag("FILE-166") void directlyUsableSetIsExactlyDataFormats() {
    for (String f : new String[] {
        "a.csv", "a.csv.gz", "a.tsv", "a.tsv.gz", "a.json", "a.json.gz",
        "a.parquet", "a.arrow", "a.yaml", "a.yml", "DATA.PARQUET"}) {  // case-insensitive
      assertTrue(FileConversionManager.isDirectlyUsable(f), f + " must be directly usable");
      assertFalse(FileConversionManager.requiresConversion(f),
          f + " is directly usable, must not require conversion");
    }
  }

  @Test @Tag("FILE-166") void csvJsonParquetAreDirectlyUsableNotConverted() {
    // FILE-166 names csv/json/parquet specifically as the directly-usable half.
    assertTrue(FileConversionManager.isDirectlyUsable("t.csv"));
    assertTrue(FileConversionManager.isDirectlyUsable("t.json"));
    assertTrue(FileConversionManager.isDirectlyUsable("t.parquet"));
    assertFalse(FileConversionManager.requiresConversion("t.csv"));
    assertFalse(FileConversionManager.requiresConversion("t.json"));
    assertFalse(FileConversionManager.requiresConversion("t.parquet"));
  }

  @Test @Tag("FILE-166") void unknownExtensionIsNeitherConvertedNorDirectlyUsable() {
    for (String f : new String[] {"notes.txt", "blob.bin", "archive.zip", "noext"}) {
      assertFalse(FileConversionManager.requiresConversion(f), f + " is not a conversion format");
      assertFalse(FileConversionManager.isDirectlyUsable(f), f + " is not a directly-usable format");
    }
  }
}
