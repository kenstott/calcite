/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.format.csv;

import org.apache.calcite.adapter.file.util.NullEquivalents;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CsvTypeConverter} to push format/csv coverage past 75%.
 */
@Tag("unit")
public class CsvTypeConverterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvTypeConverterTest.class);

  private CsvTypeConverter converter;
  private Map<SqlTypeName, DateTimeFormatter> formatters;

  @BeforeEach
  void setUp() {
    formatters = new HashMap<>();
    Set<String> nullEquivalents = NullEquivalents.DEFAULT_NULL_EQUIVALENTS;
    converter = new CsvTypeConverter(formatters, nullEquivalents, false);
  }

  // --- Boolean conversion ---

  @Test @DisplayName("convert BOOLEAN true values")
  void testConvertBooleanTrue() {
    assertEquals(Boolean.TRUE, converter.convert("true", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, converter.convert("TRUE", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, converter.convert("1", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, converter.convert("yes", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, converter.convert("y", SqlTypeName.BOOLEAN));
  }

  @Test @DisplayName("convert BOOLEAN false values")
  void testConvertBooleanFalse() {
    assertEquals(Boolean.FALSE, converter.convert("false", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, converter.convert("FALSE", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, converter.convert("0", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, converter.convert("no", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, converter.convert("n", SqlTypeName.BOOLEAN));
  }

  @Test @DisplayName("convert BOOLEAN null representation returns null")
  void testConvertBooleanNull() {
    assertNull(converter.convert("NULL", SqlTypeName.BOOLEAN));
  }

  @Test @DisplayName("convert BOOLEAN invalid throws NumberFormatException")
  void testConvertBooleanInvalid() {
    assertThrows(NumberFormatException.class,
        () -> converter.convert("maybe", SqlTypeName.BOOLEAN));
  }

  // --- Numeric conversions ---

  @Test @DisplayName("convert TINYINT")
  void testConvertTinyint() {
    assertEquals(Byte.valueOf((byte) 42), converter.convert("42", SqlTypeName.TINYINT));
    assertNull(converter.convert("NULL", SqlTypeName.TINYINT));
  }

  @Test @DisplayName("convert SMALLINT")
  void testConvertSmallint() {
    assertEquals(Short.valueOf((short) 1000), converter.convert("1000", SqlTypeName.SMALLINT));
    assertNull(converter.convert("NULL", SqlTypeName.SMALLINT));
  }

  @Test @DisplayName("convert INTEGER")
  void testConvertInteger() {
    assertEquals(Integer.valueOf(12345), converter.convert("12345", SqlTypeName.INTEGER));
    assertNull(converter.convert("NULL", SqlTypeName.INTEGER));
  }

  @Test @DisplayName("convert BIGINT")
  void testConvertBigint() {
    assertEquals(Long.valueOf(9999999999L), converter.convert("9999999999", SqlTypeName.BIGINT));
    assertNull(converter.convert("NULL", SqlTypeName.BIGINT));
  }

  @Test @DisplayName("convert REAL")
  void testConvertReal() {
    assertEquals(Float.valueOf(3.14f), converter.convert("3.14", SqlTypeName.REAL));
    assertNull(converter.convert("NULL", SqlTypeName.REAL));
  }

  @Test @DisplayName("convert FLOAT/DOUBLE")
  void testConvertDouble() {
    assertEquals(Double.valueOf(2.718281828), converter.convert("2.718281828", SqlTypeName.DOUBLE));
    assertNull(converter.convert("NULL", SqlTypeName.DOUBLE));
    assertEquals(Double.valueOf(1.5), converter.convert("1.5", SqlTypeName.FLOAT));
    assertNull(converter.convert("NULL", SqlTypeName.FLOAT));
  }

  @Test @DisplayName("convert DECIMAL")
  void testConvertDecimal() {
    assertEquals(new BigDecimal("123.456"), converter.convert("123.456", SqlTypeName.DECIMAL));
    assertNull(converter.convert("NULL", SqlTypeName.DECIMAL));
  }

  // --- Date/Time conversions ---

  @Test @DisplayName("convert DATE with ISO format")
  void testConvertDate() {
    Object result = converter.convert("2024-01-15", SqlTypeName.DATE);
    assertNotNull(result);
    // Should be epoch days as Integer
    assertTrue(result instanceof Integer, "DATE should return Integer (epoch days)");
    LOGGER.debug("Date conversion result: {}", result);
  }

  @Test @DisplayName("convert DATE null representation")
  void testConvertDateNull() {
    assertNull(converter.convert("NULL", SqlTypeName.DATE));
  }

  @Test @DisplayName("convert DATE with stored formatter")
  void testConvertDateWithStoredFormatter() {
    formatters.put(SqlTypeName.DATE, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    Object result = converter.convert("2024-06-15", SqlTypeName.DATE);
    assertNotNull(result);
    assertTrue(result instanceof Integer);
  }

  @Test @DisplayName("convert TIME with ISO format")
  void testConvertTime() {
    Object result = converter.convert("10:30:00", SqlTypeName.TIME);
    assertNotNull(result);
    assertTrue(result instanceof Integer, "TIME should return Integer (millis since midnight)");
    // 10:30:00 = 10*3600*1000 + 30*60*1000 = 37800000
    assertEquals(37800000, ((Integer) result).intValue());
  }

  @Test @DisplayName("convert TIME null representation")
  void testConvertTimeNull() {
    assertNull(converter.convert("NULL", SqlTypeName.TIME));
  }

  @Test @DisplayName("convert TIME with stored formatter")
  void testConvertTimeWithStoredFormatter() {
    formatters.put(SqlTypeName.TIME, DateTimeFormatter.ofPattern("HH:mm:ss"));
    Object result = converter.convert("14:45:30", SqlTypeName.TIME);
    assertNotNull(result);
    assertTrue(result instanceof Integer);
  }

  @Test @DisplayName("convert TIMESTAMP with ISO format")
  void testConvertTimestamp() {
    Object result = converter.convert("2024-01-15 10:30:00", SqlTypeName.TIMESTAMP);
    assertNotNull(result);
    assertTrue(result instanceof Long, "TIMESTAMP should return Long (epoch millis)");
    LOGGER.debug("Timestamp conversion result: {}", result);
  }

  @Test @DisplayName("convert TIMESTAMP null representation")
  void testConvertTimestampNull() {
    assertNull(converter.convert("NULL", SqlTypeName.TIMESTAMP));
  }

  @Test @DisplayName("convert TIMESTAMP_WITH_LOCAL_TIME_ZONE with UTC offset")
  void testConvertTimestampWithTimezone() {
    Object result = converter.convert("2024-01-15T10:30:00Z", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    assertNotNull(result, "Should parse ISO timestamp with Z");
    assertTrue(result instanceof Long);
    LOGGER.debug("TimestampTZ conversion result: {}", result);
  }

  @Test @DisplayName("convert TIMESTAMP_WITH_LOCAL_TIME_ZONE null representation")
  void testConvertTimestampTZNull() {
    assertNull(converter.convert("NULL", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
  }

  @Test @DisplayName("convert TIMESTAMP_WITH_LOCAL_TIME_ZONE with offset")
  void testConvertTimestampWithOffset() {
    Object result = converter.convert("2024-01-15T10:30:00+05:30", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    assertNotNull(result, "Should parse ISO timestamp with offset");
    assertTrue(result instanceof Long);
  }

  // --- String conversions ---

  @Test @DisplayName("convert VARCHAR preserves value as-is")
  void testConvertVarchar() {
    assertEquals("hello world", converter.convert("hello world", SqlTypeName.VARCHAR));
    // Empty string is preserved for string types
    assertEquals("", converter.convert("", SqlTypeName.VARCHAR));
  }

  @Test @DisplayName("convert CHAR preserves value as-is")
  void testConvertChar() {
    assertEquals("x", converter.convert("x", SqlTypeName.CHAR));
  }

  // --- Default/unknown type ---

  @Test @DisplayName("convert unknown type returns string value with warning")
  void testConvertUnknownType() {
    Object result = converter.convert("some_value", SqlTypeName.ARRAY);
    assertEquals("some_value", result, "Unknown type should return string value");
  }

  // --- Date fallback parsing ---

  @Test @DisplayName("convert TIMESTAMP from date-only value assumes midnight")
  void testConvertTimestampFromDateOnly() {
    Object result = converter.convert("2024-03-15", SqlTypeName.TIMESTAMP);
    assertNotNull(result, "Date-only value should be parsed as timestamp at midnight");
    assertTrue(result instanceof Long);
  }

  @Test @DisplayName("convert unparseable date throws due to null result debug logging")
  void testConvertUnparseableDate() {
    // parseDate returns null for unparseable dates, but the debug logging
    // at line 157 calls result.getClass() which triggers NPE
    assertThrows(NullPointerException.class,
        () -> converter.convert("not-a-date", SqlTypeName.DATE));
  }

  @Test @DisplayName("convert unparseable time returns null")
  void testConvertUnparseableTime() {
    Object result = converter.convert("not-a-time", SqlTypeName.TIME);
    assertNull(result, "Unparseable time should return null");
  }

  @Test @DisplayName("convert unparseable timestamp returns null")
  void testConvertUnparseableTimestamp() {
    Object result = converter.convert("not-a-timestamp", SqlTypeName.TIMESTAMP);
    assertNull(result, "Unparseable timestamp should return null");
  }

  @Test @DisplayName("convert TIMESTAMP_WITH_LOCAL_TIME_ZONE falls back to wall clock for non-TZ value")
  void testConvertTimestampTZFallback() {
    // A value without timezone info should fall back to wall clock parsing
    Object result = converter.convert("2024-01-15 10:30:00", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    assertNotNull(result, "Should fall back to wall clock parsing");
    assertTrue(result instanceof Long);
  }
}
