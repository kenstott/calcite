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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ConverterUtils}.
 */
@Tag("unit")
class ConverterUtilsTest {

  private final ObjectMapper mapper = new ObjectMapper();

  // --- getBaseFileName(File) ---

  @Test void testGetBaseFileNameWithExtension() {
    File file = new File("data.csv");
    assertEquals("data", ConverterUtils.getBaseFileName(file));
  }

  @Test void testGetBaseFileNameWithMultipleDots() {
    File file = new File("my.data.file.xlsx");
    assertEquals("my.data.file", ConverterUtils.getBaseFileName(file));
  }

  @Test void testGetBaseFileNameWithoutExtension() {
    File file = new File("README");
    assertEquals("README", ConverterUtils.getBaseFileName(file));
  }

  @Test void testGetBaseFileNameStartingWithDot() {
    File file = new File(".hidden");
    assertEquals(".hidden", ConverterUtils.getBaseFileName(file));
  }

  @Test void testGetBaseFileNameDotAndExtension() {
    File file = new File(".hidden.txt");
    assertEquals(".hidden", ConverterUtils.getBaseFileName(file));
  }

  // --- getBaseFileName(String, String...) ---

  @Test void testGetBaseFileNameStringWithMatchingExtension() {
    assertEquals("report", ConverterUtils.getBaseFileName("report.html", ".html", ".htm"));
  }

  @Test void testGetBaseFileNameStringWithHtmExtension() {
    assertEquals("report", ConverterUtils.getBaseFileName("report.htm", ".html", ".htm"));
  }

  @Test void testGetBaseFileNameStringCaseInsensitive() {
    assertEquals("Report", ConverterUtils.getBaseFileName("Report.HTML", ".html", ".htm"));
  }

  @Test void testGetBaseFileNameStringNoMatchFallsBack() {
    assertEquals("data", ConverterUtils.getBaseFileName("data.csv", ".html", ".htm"));
  }

  @Test void testGetBaseFileNameStringNoExtension() {
    assertEquals("README", ConverterUtils.getBaseFileName("README", ".html", ".htm"));
  }

  // --- isNumeric ---

  @Test void testIsNumericInteger() {
    assertTrue(ConverterUtils.isNumeric("42"));
  }

  @Test void testIsNumericNegative() {
    assertTrue(ConverterUtils.isNumeric("-3.14"));
  }

  @Test void testIsNumericDecimal() {
    assertTrue(ConverterUtils.isNumeric("0.001"));
  }

  @Test void testIsNumericScientific() {
    assertTrue(ConverterUtils.isNumeric("1.5E10"));
  }

  @Test void testIsNumericNull() {
    assertFalse(ConverterUtils.isNumeric(null));
  }

  @Test void testIsNumericEmpty() {
    assertFalse(ConverterUtils.isNumeric(""));
  }

  @Test void testIsNumericText() {
    assertFalse(ConverterUtils.isNumeric("abc"));
  }

  @Test void testIsNumericMixed() {
    assertFalse(ConverterUtils.isNumeric("12abc"));
  }

  // --- sanitizeIdentifier ---

  @Test void testSanitizeIdentifierSimple() {
    assertEquals("hello_world", ConverterUtils.sanitizeIdentifier("hello world"));
  }

  @Test void testSanitizeIdentifierNull() {
    assertEquals("column", ConverterUtils.sanitizeIdentifier(null));
  }

  @Test void testSanitizeIdentifierEmpty() {
    assertEquals("column", ConverterUtils.sanitizeIdentifier(""));
  }

  @Test void testSanitizeIdentifierSpecialChars() {
    String result = ConverterUtils.sanitizeIdentifier("price ($)");
    assertNotNull(result);
    assertFalse(result.contains("$"));
    assertFalse(result.contains("("));
    assertFalse(result.contains(")"));
  }

  @Test void testSanitizeIdentifierStartsWithDigit() {
    String result = ConverterUtils.sanitizeIdentifier("1stColumn");
    assertTrue(result.startsWith("_"));
  }

  @Test void testSanitizeIdentifierLeadingTrailingUnderscores() {
    String result = ConverterUtils.sanitizeIdentifier("__test__");
    assertFalse(result.startsWith("_"));
    assertFalse(result.endsWith("_"));
  }

  @Test void testSanitizeIdentifierCollapseMultipleUnderscores() {
    String result = ConverterUtils.sanitizeIdentifier("a____b");
    // 3+ underscores should be collapsed to double underscore
    assertFalse(result.contains("___"));
  }

  @Test void testSanitizeIdentifierPreservesDoubleUnderscore() {
    // Double underscore is used as hierarchy separator
    String input = "group__detail";
    String result = ConverterUtils.sanitizeIdentifier(input);
    assertEquals("group__detail", result);
  }

  @Test void testSanitizeIdentifierAllSpecialChars() {
    assertEquals("column", ConverterUtils.sanitizeIdentifier("@#$%"));
  }

  // --- toPascalCase ---

  @Test void testToPascalCaseSimple() {
    assertEquals("HelloWorld", ConverterUtils.toPascalCase("hello world"));
  }

  @Test void testToPascalCaseWithUnderscores() {
    assertEquals("MyVariable", ConverterUtils.toPascalCase("my_variable"));
  }

  @Test void testToPascalCaseWithHyphens() {
    assertEquals("DataFile", ConverterUtils.toPascalCase("data-file"));
  }

  @Test void testToPascalCaseNull() {
    assertNull(ConverterUtils.toPascalCase(null));
  }

  @Test void testToPascalCaseEmpty() {
    assertEquals("", ConverterUtils.toPascalCase(""));
  }

  @Test void testToPascalCaseSingleWord() {
    assertEquals("Hello", ConverterUtils.toPascalCase("hello"));
  }

  @Test void testToPascalCaseAlreadyUpperCase() {
    assertEquals("Hello", ConverterUtils.toPascalCase("HELLO"));
  }

  @Test void testToPascalCaseMixedSeparators() {
    assertEquals("OneTwoThree", ConverterUtils.toPascalCase("one_two-three"));
  }

  // --- setJsonValueWithTypeInference ---

  @Test void testSetJsonValueNull() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", null);
    assertTrue(node.get("key").isNull());
  }

  @Test void testSetJsonValueEmpty() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "");
    assertTrue(node.get("key").isNull());
  }

  @Test void testSetJsonValueInteger() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "42");
    assertTrue(node.get("key").isNumber());
    assertEquals(42L, node.get("key").longValue());
  }

  @Test void testSetJsonValueDecimal() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "3.14");
    assertTrue(node.get("key").isNumber());
    assertEquals(3.14, node.get("key").doubleValue(), 0.001);
  }

  @Test void testSetJsonValueBooleanTrue() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "true");
    assertTrue(node.get("key").isBoolean());
    assertTrue(node.get("key").booleanValue());
  }

  @Test void testSetJsonValueBooleanFalse() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "FALSE");
    assertTrue(node.get("key").isBoolean());
    assertFalse(node.get("key").booleanValue());
  }

  @Test void testSetJsonValuePlainString() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "hello world");
    assertTrue(node.get("key").isTextual());
    assertEquals("hello world", node.get("key").textValue());
  }

  @Test void testSetJsonValueDateString() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "2024-01-15");
    // Should be parsed as ISO date
    assertTrue(node.get("key").isTextual());
    assertEquals("2024-01-15", node.get("key").textValue());
  }

  @Test void testSetJsonValueDateTimeString() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "2024-01-15 10:30:00");
    // Should be parsed as ISO datetime
    assertTrue(node.get("key").isTextual());
    assertTrue(node.get("key").textValue().contains("2024-01-15"));
  }

  @Test void testSetJsonValueNegativeNumber() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "-100");
    assertTrue(node.get("key").isNumber());
    assertEquals(-100L, node.get("key").longValue());
  }

  @Test void testSetJsonValueLargeNumber() {
    ObjectNode node = mapper.createObjectNode();
    ConverterUtils.setJsonValueWithTypeInference(node, "key", "9999999999");
    assertTrue(node.get("key").isNumber());
    assertEquals(9999999999L, node.get("key").longValue());
  }

  // --- Constants ---

  @Test void testTimezonePropertyConstant() {
    assertEquals("calcite.file.converter.timezone", ConverterUtils.TIMEZONE_PROPERTY);
  }

  @Test void testStrictDatePropertyConstant() {
    assertEquals("calcite.file.converter.date.strict", ConverterUtils.STRICT_DATE_PROPERTY);
  }
}
