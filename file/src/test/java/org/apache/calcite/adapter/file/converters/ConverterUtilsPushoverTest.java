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
package org.apache.calcite.adapter.file.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ConverterUtils} to push converters package coverage past 75%.
 */
@Tag("unit")
public class ConverterUtilsPushoverTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConverterUtilsPushoverTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // --- getBaseFileName(File) tests ---

  @Test @DisplayName("getBaseFileName removes .xlsx extension")
  void testGetBaseFileNameXlsx() {
    assertEquals("report", ConverterUtils.getBaseFileName(new File("report.xlsx")));
  }

  @Test @DisplayName("getBaseFileName removes .html extension")
  void testGetBaseFileNameHtml() {
    assertEquals("page", ConverterUtils.getBaseFileName(new File("page.html")));
  }

  @Test @DisplayName("getBaseFileName returns name when no extension")
  void testGetBaseFileNameNoExtension() {
    assertEquals("README", ConverterUtils.getBaseFileName(new File("README")));
  }

  @Test @DisplayName("getBaseFileName handles multiple dots")
  void testGetBaseFileNameMultipleDots() {
    assertEquals("data.backup", ConverterUtils.getBaseFileName(new File("data.backup.csv")));
  }

  // --- getBaseFileName(String, String...) tests ---

  @Test @DisplayName("getBaseFileName with extensions removes matching extension")
  void testGetBaseFileNameWithExtensions() {
    assertEquals("page", ConverterUtils.getBaseFileName("page.html", ".html", ".htm"));
    assertEquals("page", ConverterUtils.getBaseFileName("page.htm", ".html", ".htm"));
  }

  @Test @DisplayName("getBaseFileName with extensions falls back to dot removal")
  void testGetBaseFileNameWithExtensionsFallback() {
    assertEquals("data", ConverterUtils.getBaseFileName("data.csv", ".html", ".htm"));
  }

  @Test @DisplayName("getBaseFileName with extensions and no match returns name")
  void testGetBaseFileNameWithExtensionsNoMatch() {
    assertEquals("README", ConverterUtils.getBaseFileName("README", ".html", ".htm"));
  }

  // --- isNumeric tests ---

  @Test @DisplayName("isNumeric returns true for integers")
  void testIsNumericInteger() {
    assertTrue(ConverterUtils.isNumeric("42"));
    assertTrue(ConverterUtils.isNumeric("-17"));
    assertTrue(ConverterUtils.isNumeric("0"));
  }

  @Test @DisplayName("isNumeric returns true for decimals")
  void testIsNumericDecimal() {
    assertTrue(ConverterUtils.isNumeric("3.14"));
    assertTrue(ConverterUtils.isNumeric("-0.5"));
  }

  @Test @DisplayName("isNumeric returns false for non-numeric")
  void testIsNumericNonNumeric() {
    assertFalse(ConverterUtils.isNumeric("hello"));
    assertFalse(ConverterUtils.isNumeric(""));
    assertFalse(ConverterUtils.isNumeric(null));
  }

  // --- sanitizeIdentifier tests ---

  @Test @DisplayName("sanitizeIdentifier replaces special chars with underscore")
  void testSanitizeIdentifierSpecialChars() {
    assertEquals("hello_world", ConverterUtils.sanitizeIdentifier("hello world"));
    assertEquals("col_1", ConverterUtils.sanitizeIdentifier("col#1"));
  }

  @Test @DisplayName("sanitizeIdentifier handles null/empty")
  void testSanitizeIdentifierNullEmpty() {
    assertEquals("column", ConverterUtils.sanitizeIdentifier(null));
    assertEquals("column", ConverterUtils.sanitizeIdentifier(""));
  }

  @Test @DisplayName("sanitizeIdentifier collapses multiple underscores")
  void testSanitizeIdentifierCollapseUnderscores() {
    String result = ConverterUtils.sanitizeIdentifier("a!!!b");
    // 3+ underscores collapsed to double
    LOGGER.debug("Sanitized 'a!!!b': {}", result);
    assertNotNull(result);
    assertFalse(result.contains("___"),
        "Should collapse 3+ underscores: " + result);
  }

  @Test @DisplayName("sanitizeIdentifier prepends underscore for digit start")
  void testSanitizeIdentifierDigitStart() {
    String result = ConverterUtils.sanitizeIdentifier("123abc");
    assertTrue(result.startsWith("_"),
        "Should prepend underscore for digit-start: " + result);
  }

  @Test @DisplayName("sanitizeIdentifier removes leading/trailing underscores")
  void testSanitizeIdentifierTrimUnderscores() {
    String result = ConverterUtils.sanitizeIdentifier("_hello_");
    assertEquals("hello", result);
  }

  // --- toPascalCase tests ---

  @Test @DisplayName("toPascalCase converts space-separated words")
  void testToPascalCaseSpaces() {
    assertEquals("HelloWorld", ConverterUtils.toPascalCase("hello world"));
  }

  @Test @DisplayName("toPascalCase converts underscore-separated words")
  void testToPascalCaseUnderscores() {
    assertEquals("MyVariable", ConverterUtils.toPascalCase("my_variable"));
  }

  @Test @DisplayName("toPascalCase converts hyphen-separated words")
  void testToPascalCaseHyphens() {
    assertEquals("SomeValue", ConverterUtils.toPascalCase("some-value"));
  }

  @Test @DisplayName("toPascalCase handles null/empty")
  void testToPascalCaseNullEmpty() {
    assertEquals(null, ConverterUtils.toPascalCase(null));
    assertEquals("", ConverterUtils.toPascalCase(""));
  }

  @Test @DisplayName("toPascalCase with single word capitalizes first letter")
  void testToPascalCaseSingleWord() {
    assertEquals("Hello", ConverterUtils.toPascalCase("hello"));
  }

  // --- setJsonValueWithTypeInference tests ---

  @Test @DisplayName("setJsonValueWithTypeInference sets null for empty value")
  void testSetJsonValueNull() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "");
    assertTrue(node.get("key").isNull());
  }

  @Test @DisplayName("setJsonValueWithTypeInference sets null for null value")
  void testSetJsonValueNullValue() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", null);
    assertTrue(node.get("key").isNull());
  }

  @Test @DisplayName("setJsonValueWithTypeInference parses integer")
  void testSetJsonValueInteger() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "count", "42");
    assertEquals(42L, node.get("count").longValue());
  }

  @Test @DisplayName("setJsonValueWithTypeInference parses double")
  void testSetJsonValueDouble() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "price", "3.14");
    assertEquals(3.14, node.get("price").doubleValue(), 0.001);
  }

  @Test @DisplayName("setJsonValueWithTypeInference parses boolean")
  void testSetJsonValueBoolean() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "active", "true");
    assertTrue(node.get("active").booleanValue());

    ConverterUtils.setJsonValueWithTypeInference(node, "deleted", "false");
    assertFalse(node.get("deleted").booleanValue());
  }

  @Test @DisplayName("setJsonValueWithTypeInference parses date")
  void testSetJsonValueDate() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "date", "2024-01-15");
    assertNotNull(node.get("date"));
    assertTrue(node.get("date").isTextual(), "Date should be stored as ISO string");
    LOGGER.debug("Date value: {}", node.get("date").textValue());
  }

  @Test @DisplayName("setJsonValueWithTypeInference stores plain string")
  void testSetJsonValueString() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "name", "Alice");
    assertEquals("Alice", node.get("name").textValue());
  }

  @Test @DisplayName("setJsonValueWithTypeInference parses datetime")
  void testSetJsonValueDateTime() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "ts", "2024-01-15 10:30:00");
    assertNotNull(node.get("ts"));
    assertTrue(node.get("ts").isTextual());
    LOGGER.debug("DateTime value: {}", node.get("ts").textValue());
  }

  @Test @DisplayName("setJsonValueWithTypeInference parses time")
  void testSetJsonValueTime() {
    ObjectNode node = MAPPER.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "time", "14:30:00");
    assertNotNull(node.get("time"));
    assertTrue(node.get("time").isTextual());
    LOGGER.debug("Time value: {}", node.get("time").textValue());
  }
}
