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
import org.apache.calcite.adapter.file.converters.MarkdownTableScanner;
import org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator;

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
}
