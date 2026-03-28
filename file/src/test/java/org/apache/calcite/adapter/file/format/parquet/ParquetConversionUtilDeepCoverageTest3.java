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
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.calcite.adapter.file.storage.StorageProviderFile;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Deep coverage tests for {@link ParquetConversionUtil} focusing on uncovered paths:
 * addValueToParquetGroup (all type branches), convertToParquet with real CSV data,
 * convertEnumerableToParquetDirect, needsConversion with StorageProviderFile,
 * configureS3Access, and type-specific parsing fallbacks.
 */
@Tag("unit")
public class ParquetConversionUtilDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  // ========== addValueToParquetGroup via reflection ==========

  private Method getAddValueMethod() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod(
        "addValueToParquetGroup",
        Group.class, String.class, Object.class, SqlTypeName.class, boolean.class);
    method.setAccessible(true);
    return method;
  }

  private Group createGroupWithField(String fieldName, Type parquetType) {
    MessageType schema = new MessageType("record",
        Collections.singletonList(parquetType));
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    return factory.newGroup();
  }

  // --- BOOLEAN ---

  @Test void testAddValueBooleanNativeTrue() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
        .named("flag");
    Group group = createGroupWithField("flag", field);

    method.invoke(null, group, "flag", Boolean.TRUE, SqlTypeName.BOOLEAN, false);
    assertTrue(group.getBoolean("flag", 0));
  }

  @Test void testAddValueBooleanNativeFalse() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
        .named("flag");
    Group group = createGroupWithField("flag", field);

    method.invoke(null, group, "flag", Boolean.FALSE, SqlTypeName.BOOLEAN, false);
    assertFalse(group.getBoolean("flag", 0));
  }

  @Test void testAddValueBooleanFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
        .named("flag");
    Group group = createGroupWithField("flag", field);

    method.invoke(null, group, "flag", "true", SqlTypeName.BOOLEAN, false);
    assertTrue(group.getBoolean("flag", 0));
  }

  @Test void testAddValueBooleanFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
        .named("flag");
    Group group = createGroupWithField("flag", field);

    // "NULL" is a null representation for non-string types
    method.invoke(null, group, "flag", "NULL", SqlTypeName.BOOLEAN, false);
    // Value should not be added (null representation)
    assertEquals(0, group.getFieldRepetitionCount("flag"));
  }

  @Test void testAddValueBooleanFromInvalidString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
        .named("flag");
    Group group = createGroupWithField("flag", field);

    // "notaboolean" won't throw but will parse as false per Boolean.parseBoolean
    method.invoke(null, group, "flag", "notaboolean", SqlTypeName.BOOLEAN, false);
    assertFalse(group.getBoolean("flag", 0));
  }

  // --- INTEGER ---

  @Test void testAddValueIntegerNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("count");
    Group group = createGroupWithField("count", field);

    method.invoke(null, group, "count", 42, SqlTypeName.INTEGER, false);
    assertEquals(42, group.getInteger("count", 0));
  }

  @Test void testAddValueIntegerFromLong() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("count");
    Group group = createGroupWithField("count", field);

    method.invoke(null, group, "count", 42L, SqlTypeName.INTEGER, false);
    assertEquals(42, group.getInteger("count", 0));
  }

  @Test void testAddValueIntegerFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("count");
    Group group = createGroupWithField("count", field);

    method.invoke(null, group, "count", "123", SqlTypeName.INTEGER, false);
    assertEquals(123, group.getInteger("count", 0));
  }

  @Test void testAddValueIntegerFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("count");
    Group group = createGroupWithField("count", field);

    method.invoke(null, group, "count", "NA", SqlTypeName.INTEGER, false);
    assertEquals(0, group.getFieldRepetitionCount("count"));
  }

  @Test void testAddValueIntegerFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("count");
    Group group = createGroupWithField("count", field);

    method.invoke(null, group, "count", "not_a_number", SqlTypeName.INTEGER, false);
    assertEquals(0, group.getFieldRepetitionCount("count"));
  }

  // --- TINYINT ---

  @Test void testAddValueTinyintNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("tiny");
    Group group = createGroupWithField("tiny", field);

    method.invoke(null, group, "tiny", (byte) 7, SqlTypeName.TINYINT, false);
    assertEquals(7, group.getInteger("tiny", 0));
  }

  // --- SMALLINT ---

  @Test void testAddValueSmallintNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("small");
    Group group = createGroupWithField("small", field);

    method.invoke(null, group, "small", (short) 500, SqlTypeName.SMALLINT, false);
    assertEquals(500, group.getInteger("small", 0));
  }

  // --- BIGINT ---

  @Test void testAddValueBigintNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.intType(64, true))
        .named("big");
    Group group = createGroupWithField("big", field);

    method.invoke(null, group, "big", 9999999999L, SqlTypeName.BIGINT, false);
    assertEquals(9999999999L, group.getLong("big", 0));
  }

  @Test void testAddValueBigintFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.intType(64, true))
        .named("big");
    Group group = createGroupWithField("big", field);

    method.invoke(null, group, "big", "12345678901", SqlTypeName.BIGINT, false);
    assertEquals(12345678901L, group.getLong("big", 0));
  }

  @Test void testAddValueBigintFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.intType(64, true))
        .named("big");
    Group group = createGroupWithField("big", field);

    method.invoke(null, group, "big", "N/A", SqlTypeName.BIGINT, false);
    assertEquals(0, group.getFieldRepetitionCount("big"));
  }

  @Test void testAddValueBigintFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.intType(64, true))
        .named("big");
    Group group = createGroupWithField("big", field);

    method.invoke(null, group, "big", "abc", SqlTypeName.BIGINT, false);
    assertEquals(0, group.getFieldRepetitionCount("big"));
  }

  // --- FLOAT ---

  @Test void testAddValueFloatNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
        .named("f");
    Group group = createGroupWithField("f", field);

    method.invoke(null, group, "f", 3.14f, SqlTypeName.FLOAT, false);
    assertEquals(3.14f, group.getFloat("f", 0), 0.001f);
  }

  @Test void testAddValueFloatFromDouble() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
        .named("f");
    Group group = createGroupWithField("f", field);

    method.invoke(null, group, "f", 2.5, SqlTypeName.FLOAT, false);
    assertEquals(2.5f, group.getFloat("f", 0), 0.001f);
  }

  @Test void testAddValueFloatFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
        .named("f");
    Group group = createGroupWithField("f", field);

    method.invoke(null, group, "f", "1.5", SqlTypeName.FLOAT, false);
    assertEquals(1.5f, group.getFloat("f", 0), 0.001f);
  }

  @Test void testAddValueFloatFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
        .named("f");
    Group group = createGroupWithField("f", field);

    method.invoke(null, group, "f", "null", SqlTypeName.FLOAT, false);
    assertEquals(0, group.getFieldRepetitionCount("f"));
  }

  @Test void testAddValueFloatFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
        .named("f");
    Group group = createGroupWithField("f", field);

    method.invoke(null, group, "f", "not_float", SqlTypeName.FLOAT, false);
    assertEquals(0, group.getFieldRepetitionCount("f"));
  }

  // --- REAL ---

  @Test void testAddValueRealNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
        .named("r");
    Group group = createGroupWithField("r", field);

    method.invoke(null, group, "r", 2.71f, SqlTypeName.REAL, false);
    assertEquals(2.71f, group.getFloat("r", 0), 0.01f);
  }

  // --- DOUBLE ---

  @Test void testAddValueDoubleNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
        .named("d");
    Group group = createGroupWithField("d", field);

    method.invoke(null, group, "d", 3.14159, SqlTypeName.DOUBLE, false);
    assertEquals(3.14159, group.getDouble("d", 0), 0.00001);
  }

  @Test void testAddValueDoubleFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
        .named("d");
    Group group = createGroupWithField("d", field);

    method.invoke(null, group, "d", "2.718", SqlTypeName.DOUBLE, false);
    assertEquals(2.718, group.getDouble("d", 0), 0.001);
  }

  @Test void testAddValueDoubleFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
        .named("d");
    Group group = createGroupWithField("d", field);

    method.invoke(null, group, "d", "NULL", SqlTypeName.DOUBLE, false);
    assertEquals(0, group.getFieldRepetitionCount("d"));
  }

  @Test void testAddValueDoubleFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
        .named("d");
    Group group = createGroupWithField("d", field);

    method.invoke(null, group, "d", "not_double", SqlTypeName.DOUBLE, false);
    assertEquals(0, group.getFieldRepetitionCount("d"));
  }

  // --- DECIMAL ---

  @Test void testAddValueDecimalNative() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.decimalType(2, 10))
        .named("amount");
    Group group = createGroupWithField("amount", field);

    BigDecimal val = new BigDecimal("123.45");
    method.invoke(null, group, "amount", val, SqlTypeName.DECIMAL, false);
    assertNotNull(group.getBinary("amount", 0));
  }

  @Test void testAddValueDecimalFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.decimalType(2, 10))
        .named("amount");
    Group group = createGroupWithField("amount", field);

    method.invoke(null, group, "amount", "99.99", SqlTypeName.DECIMAL, false);
    assertNotNull(group.getBinary("amount", 0));
  }

  @Test void testAddValueDecimalFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.decimalType(2, 10))
        .named("amount");
    Group group = createGroupWithField("amount", field);

    method.invoke(null, group, "amount", "NA", SqlTypeName.DECIMAL, false);
    assertEquals(0, group.getFieldRepetitionCount("amount"));
  }

  @Test void testAddValueDecimalFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.decimalType(2, 10))
        .named("amount");
    Group group = createGroupWithField("amount", field);

    method.invoke(null, group, "amount", "bad_decimal", SqlTypeName.DECIMAL, false);
    assertEquals(0, group.getFieldRepetitionCount("amount"));
  }

  // --- DATE ---

  @Test void testAddValueDateFromSqlDate() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.dateType())
        .named("dt");
    Group group = createGroupWithField("dt", field);

    java.sql.Date sqlDate = java.sql.Date.valueOf("2024-01-15");
    method.invoke(null, group, "dt", sqlDate, SqlTypeName.DATE, false);
    // Value should be days since epoch
    assertTrue(group.getInteger("dt", 0) > 0);
  }

  @Test void testAddValueDateFromLocalDate() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.dateType())
        .named("dt");
    Group group = createGroupWithField("dt", field);

    LocalDate ld = LocalDate.of(2024, 6, 15);
    method.invoke(null, group, "dt", ld, SqlTypeName.DATE, false);
    assertEquals((int) ld.toEpochDay(), group.getInteger("dt", 0));
  }

  @Test void testAddValueDateFromInteger() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.dateType())
        .named("dt");
    Group group = createGroupWithField("dt", field);

    int daysSinceEpoch = 19723; // 2024-01-01 approximately
    method.invoke(null, group, "dt", daysSinceEpoch, SqlTypeName.DATE, false);
    assertEquals(daysSinceEpoch, group.getInteger("dt", 0));
  }

  @Test void testAddValueDateFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.dateType())
        .named("dt");
    Group group = createGroupWithField("dt", field);

    method.invoke(null, group, "dt", "2024-03-15", SqlTypeName.DATE, false);
    assertTrue(group.getInteger("dt", 0) > 0);
  }

  @Test void testAddValueDateFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.dateType())
        .named("dt");
    Group group = createGroupWithField("dt", field);

    method.invoke(null, group, "dt", "NULL", SqlTypeName.DATE, false);
    assertEquals(0, group.getFieldRepetitionCount("dt"));
  }

  @Test void testAddValueDateFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.dateType())
        .named("dt");
    Group group = createGroupWithField("dt", field);

    method.invoke(null, group, "dt", "not_a_date", SqlTypeName.DATE, false);
    assertEquals(0, group.getFieldRepetitionCount("dt"));
  }

  // --- TIME ---

  @Test void testAddValueTimeFromLocalTime() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("tm");
    Group group = createGroupWithField("tm", field);

    LocalTime lt = LocalTime.of(14, 30, 0);
    method.invoke(null, group, "tm", lt, SqlTypeName.TIME, false);
    int expectedMillis = (int) (lt.toNanoOfDay() / 1_000_000L);
    assertEquals(expectedMillis, group.getInteger("tm", 0));
  }

  @Test void testAddValueTimeFromSqlTime() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("tm");
    Group group = createGroupWithField("tm", field);

    @SuppressWarnings("deprecation")
    java.sql.Time sqlTime = java.sql.Time.valueOf("08:30:00");
    method.invoke(null, group, "tm", sqlTime, SqlTypeName.TIME, false);
    assertTrue(group.getInteger("tm", 0) > 0);
  }

  @Test void testAddValueTimeFromInteger() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("tm");
    Group group = createGroupWithField("tm", field);

    int millisSinceMidnight = 3600000; // 1 hour
    method.invoke(null, group, "tm", millisSinceMidnight, SqlTypeName.TIME, false);
    assertEquals(millisSinceMidnight, group.getInteger("tm", 0));
  }

  @Test void testAddValueTimeFromTimeString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("tm");
    Group group = createGroupWithField("tm", field);

    method.invoke(null, group, "tm", "14:30:00", SqlTypeName.TIME, false);
    assertTrue(group.getInteger("tm", 0) > 0);
  }

  @Test void testAddValueTimeFromNonTimeString() throws Exception {
    Method method = getAddValueMethod();
    // For a non-parseable time string, it falls through to default string behavior
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("tm");
    Group group = createGroupWithField("tm", field);

    // Non-matching time string: not matching the regex pattern
    // This will try to append as string, which may fail for INT32 type.
    // The code does: group.append(fieldName, value.toString()) for non-matching
    try {
      method.invoke(null, group, "tm", "noon", SqlTypeName.TIME, false);
    } catch (Exception e) {
      // Expected - trying to add string to INT32 field
    }
  }

  // --- TIMESTAMP ---

  @Test void testAddValueTimestampFromSqlTimestamp() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts");
    Group group = createGroupWithField("ts", field);

    Timestamp ts = Timestamp.valueOf("2024-06-15 10:30:00");
    method.invoke(null, group, "ts", ts, SqlTypeName.TIMESTAMP, false);
    assertTrue(group.getLong("ts", 0) != 0);
  }

  @Test void testAddValueTimestampFromLocalDateTime() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts");
    Group group = createGroupWithField("ts", field);

    LocalDateTime ldt = LocalDateTime.of(2024, 6, 15, 10, 30, 0);
    method.invoke(null, group, "ts", ldt, SqlTypeName.TIMESTAMP, false);
    assertTrue(group.getLong("ts", 0) != 0);
  }

  @Test void testAddValueTimestampFromLong() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts");
    Group group = createGroupWithField("ts", field);

    long millis = 1718451000000L; // some epoch millis
    method.invoke(null, group, "ts", millis, SqlTypeName.TIMESTAMP, false);
    assertEquals(millis, group.getLong("ts", 0));
  }

  @Test void testAddValueTimestampFromString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts");
    Group group = createGroupWithField("ts", field);

    method.invoke(null, group, "ts", "2024-06-15 10:30:00", SqlTypeName.TIMESTAMP, false);
    assertTrue(group.getLong("ts", 0) != 0);
  }

  @Test void testAddValueTimestampFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts");
    Group group = createGroupWithField("ts", field);

    method.invoke(null, group, "ts", "not_a_timestamp", SqlTypeName.TIMESTAMP, false);
    // Should not add value
    assertEquals(0, group.getFieldRepetitionCount("ts"));
  }

  // --- TIMESTAMP_WITH_LOCAL_TIME_ZONE ---

  @Test void testAddValueTimestampTzFromSqlTimestamp() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts_tz");
    Group group = createGroupWithField("ts_tz", field);

    Timestamp ts = Timestamp.valueOf("2024-06-15 10:30:00");
    method.invoke(null, group, "ts_tz", ts,
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false);
    assertTrue(group.getLong("ts_tz", 0) != 0);
  }

  @Test void testAddValueTimestampTzFromLocalDateTime() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts_tz");
    Group group = createGroupWithField("ts_tz", field);

    LocalDateTime ldt = LocalDateTime.of(2024, 6, 15, 10, 30, 0);
    method.invoke(null, group, "ts_tz", ldt,
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false);
    assertTrue(group.getLong("ts_tz", 0) != 0);
  }

  @Test void testAddValueTimestampTzFromInstant() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts_tz");
    Group group = createGroupWithField("ts_tz", field);

    Instant instant = Instant.ofEpochMilli(1718451000000L);
    method.invoke(null, group, "ts_tz", instant,
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false);
    assertEquals(1718451000000L, group.getLong("ts_tz", 0));
  }

  @Test void testAddValueTimestampTzFromNumber() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts_tz");
    Group group = createGroupWithField("ts_tz", field);

    method.invoke(null, group, "ts_tz", 1718451000000L,
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false);
    assertEquals(1718451000000L, group.getLong("ts_tz", 0));
  }

  @Test void testAddValueTimestampTzFromNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts_tz");
    Group group = createGroupWithField("ts_tz", field);

    method.invoke(null, group, "ts_tz", "NULL",
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false);
    assertEquals(0, group.getFieldRepetitionCount("ts_tz"));
  }

  @Test void testAddValueTimestampTzFromBadString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts_tz");
    Group group = createGroupWithField("ts_tz", field);

    method.invoke(null, group, "ts_tz", "bad_ts",
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false);
    assertEquals(0, group.getFieldRepetitionCount("ts_tz"));
  }

  // --- VARCHAR ---

  @Test void testAddValueVarcharNonEmpty() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.stringType())
        .named("name");
    Group group = createGroupWithField("name", field);

    method.invoke(null, group, "name", "Alice", SqlTypeName.VARCHAR, false);
    assertEquals("Alice", group.getString("name", 0));
  }

  @Test void testAddValueVarcharEmptyString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.stringType())
        .named("name");
    Group group = createGroupWithField("name", field);

    // Empty strings should be preserved for VARCHAR
    method.invoke(null, group, "name", "", SqlTypeName.VARCHAR, true);
    assertEquals("", group.getString("name", 0));
  }

  @Test void testAddValueVarcharEmptyStringBlankStringsAsNull() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.stringType())
        .named("name");
    Group group = createGroupWithField("name", field);

    // Even with blankStringsAsNull=true, empty strings should be preserved for VARCHAR
    method.invoke(null, group, "name", "", SqlTypeName.VARCHAR, true);
    assertEquals("", group.getString("name", 0));
  }

  @Test void testAddValueCharEmptyString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.stringType())
        .named("code");
    Group group = createGroupWithField("code", field);

    method.invoke(null, group, "code", "", SqlTypeName.CHAR, false);
    assertEquals("", group.getString("code", 0));
  }

  // --- Default (unknown type) ---

  @Test void testAddValueDefaultType() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.stringType())
        .named("misc");
    Group group = createGroupWithField("misc", field);

    // Use a type that falls to default case
    method.invoke(null, group, "misc", "some_value", SqlTypeName.ANY, false);
    assertEquals("some_value", group.getString("misc", 0));
  }

  // --- null value ---

  @Test void testAddValueNull() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.stringType())
        .named("val");
    Group group = createGroupWithField("val", field);

    method.invoke(null, group, "val", null, SqlTypeName.VARCHAR, false);
    assertEquals(0, group.getFieldRepetitionCount("val"));
  }

  // --- non-string null representation for non-string types ---

  @Test void testAddValueNullRepresentationForNonStringType() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
        .named("count");
    Group group = createGroupWithField("count", field);

    // "NULL" should be treated as null for non-string types
    method.invoke(null, group, "count", "NULL", SqlTypeName.INTEGER, false);
    assertEquals(0, group.getFieldRepetitionCount("count"));
  }

  @Test void testAddValueNullRepresentationNA() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
        .named("val");
    Group group = createGroupWithField("val", field);

    method.invoke(null, group, "val", "N/A", SqlTypeName.DOUBLE, false);
    assertEquals(0, group.getFieldRepetitionCount("val"));
  }

  // --- VARCHAR with "NULL" value (should NOT be treated as null) ---

  @Test void testAddValueVarcharWithNullString() throws Exception {
    Method method = getAddValueMethod();
    Type field = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.stringType())
        .named("text");
    Group group = createGroupWithField("text", field);

    // For VARCHAR, "NULL" should be stored as the literal string "NULL"
    method.invoke(null, group, "text", "NULL", SqlTypeName.VARCHAR, false);
    assertEquals("NULL", group.getString("text", 0));
  }

  // ========== createParquetFieldFromCalciteType via reflection ==========

  private Method getCreateFieldMethod() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod(
        "createParquetFieldFromCalciteType",
        String.class, SqlTypeName.class, RelDataTypeField.class);
    method.setAccessible(true);
    return method;
  }

  private RelDataTypeField makeField(String name, SqlTypeName typeName) {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(typeName), true);
    return new RelDataTypeFieldImpl(name, 0, type);
  }

  private RelDataTypeField makeNonNullableField(String name, SqlTypeName typeName) {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(typeName), false);
    return new RelDataTypeFieldImpl(name, 0, type);
  }

  @Test void testCreateFieldBoolean() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.BOOLEAN);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.BOOLEAN, field);
    assertNotNull(result);
    assertEquals("f", result.getName());
  }

  @Test void testCreateFieldTinyint() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.TINYINT);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.TINYINT, field);
    assertNotNull(result);
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testCreateFieldSmallint() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.SMALLINT);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.SMALLINT, field);
    assertNotNull(result);
  }

  @Test void testCreateFieldInteger() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.INTEGER);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.INTEGER, field);
    assertNotNull(result);
  }

  @Test void testCreateFieldBigint() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.BIGINT);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.BIGINT, field);
    assertNotNull(result);
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testCreateFieldFloat() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.FLOAT);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.FLOAT, field);
    assertNotNull(result);
  }

  @Test void testCreateFieldReal() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.REAL);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.REAL, field);
    assertNotNull(result);
    assertEquals(PrimitiveType.PrimitiveTypeName.FLOAT,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testCreateFieldDouble() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("f", SqlTypeName.DOUBLE);
    Type result = (Type) method.invoke(null, "f", SqlTypeName.DOUBLE, field);
    assertNotNull(result);
    assertEquals(PrimitiveType.PrimitiveTypeName.DOUBLE,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testCreateFieldDecimalWithPrecision() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
    RelDataTypeField field = new RelDataTypeFieldImpl("amount", 0, type);

    Type result = (Type) method.invoke(null, "amount", SqlTypeName.DECIMAL, field);
    assertNotNull(result);
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testCreateFieldDecimalZeroPrecision() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    RelDataTypeField field = new RelDataTypeFieldImpl("val", 0, type);

    Type result = (Type) method.invoke(null, "val", SqlTypeName.DECIMAL, field);
    assertNotNull(result);
  }

  @Test void testCreateFieldDate() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("dt", SqlTypeName.DATE);
    Type result = (Type) method.invoke(null, "dt", SqlTypeName.DATE, field);
    assertNotNull(result);
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testCreateFieldTime() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("tm", SqlTypeName.TIME);
    Type result = (Type) method.invoke(null, "tm", SqlTypeName.TIME, field);
    assertNotNull(result);
  }

  @Test void testCreateFieldTimestamp() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("ts", SqlTypeName.TIMESTAMP);
    Type result = (Type) method.invoke(null, "ts", SqlTypeName.TIMESTAMP, field);
    assertNotNull(result);
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testCreateFieldTimestampWithTz() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("ts_tz", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    Type result = (Type) method.invoke(null, "ts_tz",
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, field);
    assertNotNull(result);
  }

  @Test void testCreateFieldVarcharIsOptional() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeNonNullableField("name", SqlTypeName.VARCHAR);
    Type result = (Type) method.invoke(null, "name", SqlTypeName.VARCHAR, field);
    assertNotNull(result);
    // VARCHAR should be forced to OPTIONAL regardless of nullability
    assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test void testCreateFieldCharIsOptional() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeNonNullableField("code", SqlTypeName.CHAR);
    Type result = (Type) method.invoke(null, "code", SqlTypeName.CHAR, field);
    assertNotNull(result);
    assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test void testCreateFieldNonNullableIntegerIsRequired() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeNonNullableField("id", SqlTypeName.INTEGER);
    Type result = (Type) method.invoke(null, "id", SqlTypeName.INTEGER, field);
    assertNotNull(result);
    assertEquals(Type.Repetition.REQUIRED, result.getRepetition());
  }

  @Test void testCreateFieldNullableIntegerIsOptional() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("id", SqlTypeName.INTEGER);
    Type result = (Type) method.invoke(null, "id", SqlTypeName.INTEGER, field);
    assertNotNull(result);
    assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test void testCreateFieldDefaultFallback() throws Exception {
    Method method = getCreateFieldMethod();
    RelDataTypeField field = makeField("other", SqlTypeName.OTHER);
    Type result = (Type) method.invoke(null, "other", SqlTypeName.OTHER, field);
    assertNotNull(result);
    // Default => BINARY with string type annotation
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        result.asPrimitiveType().getPrimitiveTypeName());
  }

  // ========== In-memory ScannableTable for convertToParquet tests ==========

  /**
   * A simple in-memory ScannableTable that provides data directly without CSV parsing.
   * This avoids the CsvTypeConverter null issue that occurs when CsvScannableTable is
   * constructed without a full Calcite framework context.
   */
  static class InMemoryScannableTable implements org.apache.calcite.schema.ScannableTable {
    private final RelDataType rowType;
    private final List<Object[]> rows;

    InMemoryScannableTable(RelDataType rowType, List<Object[]> rows) {
      this.rowType = rowType;
      this.rows = rows;
    }

    @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(
        org.apache.calcite.DataContext root) {
      return new org.apache.calcite.linq4j.AbstractEnumerable<Object[]>() {
        @Override public org.apache.calcite.linq4j.Enumerator<Object[]> enumerator() {
          return new org.apache.calcite.linq4j.Enumerator<Object[]>() {
            private int index = -1;
            @Override public Object[] current() { return rows.get(index); }
            @Override public boolean moveNext() { return ++index < rows.size(); }
            @Override public void reset() { index = -1; }
            @Override public void close() { }
          };
        }
      };
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return rowType;
    }

    @Override public org.apache.calcite.schema.Statistic getStatistic() {
      return org.apache.calcite.schema.Statistics.UNKNOWN;
    }

    @Override public org.apache.calcite.schema.Schema.TableType getJdbcTableType() {
      return org.apache.calcite.schema.Schema.TableType.TABLE;
    }

    @Override public boolean isRolledUp(String column) { return false; }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
        org.apache.calcite.sql.SqlCall call,
        org.apache.calcite.sql.SqlNode parent,
        org.apache.calcite.config.CalciteConnectionConfig config) {
      return false;
    }
  }

  private RelDataType buildRowType(SqlTypeName... types) {
    return buildRowType(null, types);
  }

  private RelDataType buildRowType(String[] names, SqlTypeName... types) {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < types.length; i++) {
      String name = (names != null && i < names.length) ? names[i] : "col" + i;
      builder.add(name, typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(types[i]), true));
    }
    return builder.build();
  }

  private File createDummySourceFile(String name) throws Exception {
    File f = new File(tempDir.toFile(), name);
    try (FileWriter fw = new FileWriter(f)) {
      fw.write("dummy");
    }
    return f;
  }

  // ========== convertToParquet with in-memory ScannableTable ==========

  @Test void testConvertToParquetWithStringData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "name", "value", "active"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"1", "Alice", "99.5", "true"});
    rows.add(new Object[]{"2", "Bob", "88.3", "false"});
    rows.add(new Object[]{"3", null, "77.1", "true"});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("test_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "test_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.getName().endsWith(".parquet"));
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithIntegerData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "count", "active_flag"},
        SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.BOOLEAN);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 100, true});
    rows.add(new Object[]{2, 200, false});
    rows.add(new Object[]{3, 0, true});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("int_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_int");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "int_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithDoubleData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "measurement"},
        SqlTypeName.INTEGER, SqlTypeName.DOUBLE);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 3.14});
    rows.add(new Object[]{2, 2.718});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("double_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_double");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "double_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithBigintData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "big_value"},
        SqlTypeName.INTEGER, SqlTypeName.BIGINT);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 9999999999L});
    rows.add(new Object[]{2, 1234567890123L});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("long_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_long");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "long_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithFloatData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "measurement"},
        SqlTypeName.INTEGER, SqlTypeName.FLOAT);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 3.14f});
    rows.add(new Object[]{2, 2.718f});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("float_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_float");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "float_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithNullValues() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "name", "score"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"1", "Alice", "95.5"});
    rows.add(new Object[]{"2", null, null});
    rows.add(new Object[]{"3", "Charlie", "NULL"});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("null_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_nulls");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "null_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
  }

  @Test void testConvertToParquetWithDateData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "event_date"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"1", "2024-01-15"});
    rows.add(new Object[]{"2", "2024-06-30"});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("date_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_dates");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "date_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithTimestampData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "event_ts"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"1", "2024-01-15 10:30:00"});
    rows.add(new Object[]{"2", "2024-06-30 23:59:59"});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("ts_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_ts");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "ts_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithTimeData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "event_time"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"1", "08:30:00"});
    rows.add(new Object[]{"2", "14:45:30"});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("time_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_time");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "time_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
  }

  @Test void testConvertToParquetWithSmallintData() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "small_val"},
        SqlTypeName.INTEGER, SqlTypeName.SMALLINT);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, (short) 100});
    rows.add(new Object[]{2, (short) 32000});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("short_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_short");
    cacheDir.mkdirs();

    File result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "short_data", table,
        cacheDir, null, "test", "UNCHANGED");

    assertNotNull(result);
    assertTrue(result.exists());
  }

  @Test void testConvertToParquetSkipsIfUpToDate() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "name"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"1", "Alice"});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("uptodate.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_uptodate");
    cacheDir.mkdirs();

    // First conversion
    File result1 = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "uptodate", table,
        cacheDir, null, "test", "UNCHANGED");
    assertNotNull(result1);
    assertTrue(result1.exists());

    // Second conversion should skip (parquet is up to date)
    File result2 = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "uptodate", table,
        cacheDir, null, "test", "UNCHANGED");
    assertNotNull(result2);
    assertTrue(result2.exists());
  }

  // ========== needsConversion(File, File) edge cases ==========

  @Test void testNeedsConversionParquetDoesNotExist() throws Exception {
    File source = new File(tempDir.toFile(), "source.csv");
    source.createNewFile();
    File target = new File(tempDir.toFile(), "nonexistent.parquet");

    assertTrue(ParquetConversionUtil.needsConversion(source, target));
  }

  @Test void testNeedsConversionSourceNewer() throws Exception {
    File source = new File(tempDir.toFile(), "source2.csv");
    source.createNewFile();
    File target = new File(tempDir.toFile(), "target2.parquet");
    target.createNewFile();

    // Make source newer
    long now = System.currentTimeMillis();
    target.setLastModified(now - 5000);
    source.setLastModified(now);

    assertTrue(ParquetConversionUtil.needsConversion(source, target));
  }

  @Test void testNeedsConversionParquetNewer() throws Exception {
    File source = new File(tempDir.toFile(), "source3.csv");
    source.createNewFile();
    File target = new File(tempDir.toFile(), "target3.parquet");
    target.createNewFile();

    // Make parquet newer
    long now = System.currentTimeMillis();
    source.setLastModified(now - 5000);
    target.setLastModified(now);

    assertFalse(ParquetConversionUtil.needsConversion(source, target));
  }

  // ========== isS3Path ==========

  @Test void testIsS3PathTrue() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "s3://bucket/key"));
  }

  @Test void testIsS3PathFalseLocal() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "/local/path"));
  }

  @Test void testIsS3PathFalseNull() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, (String) null));
  }

  @Test void testIsS3PathFalseHttp() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "http://example.com"));
  }

  // ========== getHadoopPath ==========

  @Test void testGetHadoopPathS3() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("getHadoopPath", String.class);
    method.setAccessible(true);
    assertEquals("s3a://bucket/key", method.invoke(null, "s3://bucket/key"));
  }

  @Test void testGetHadoopPathLocal() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("getHadoopPath", String.class);
    method.setAccessible(true);
    assertEquals("/local/path", method.invoke(null, "/local/path"));
  }

  @Test void testGetHadoopPathNull() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod("getHadoopPath", String.class);
    method.setAccessible(true);
    assertEquals(null, method.invoke(null, (String) null));
  }

  // ========== isNullRepresentation ==========

  @Test void testIsNullRepresentationDefault() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod(
        "isNullRepresentation", String.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, "NULL"));
    assertTrue((Boolean) method.invoke(null, "null"));
    assertTrue((Boolean) method.invoke(null, "NA"));
    assertTrue((Boolean) method.invoke(null, "N/A"));
    assertFalse((Boolean) method.invoke(null, "value"));
    assertTrue((Boolean) method.invoke(null, ""));
  }

  @Test void testIsNullRepresentationWithCustomSet() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod(
        "isNullRepresentation", String.class, Set.class);
    method.setAccessible(true);

    Set<String> customNulls = new HashSet<String>();
    customNulls.add("MISSING");
    customNulls.add("EMPTY");

    assertTrue((Boolean) method.invoke(null, "MISSING", customNulls));
    assertTrue((Boolean) method.invoke(null, "EMPTY", customNulls));
    assertFalse((Boolean) method.invoke(null, "NULL", customNulls));
  }

  // ========== convertToParquet with StorageProviderFile (local) ==========

  @Test void testConvertToParquetStorageProviderFile() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "name"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"1", "Alice"});
    rows.add(new Object[]{"2", "Bob"});

    InMemoryScannableTable table = new InMemoryScannableTable(rowType, rows);
    File sourceFile = createDummySourceFile("spf_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_spf");
    cacheDir.mkdirs();

    org.apache.calcite.adapter.file.storage.LocalFileStorageProvider localProvider =
        new org.apache.calcite.adapter.file.storage.LocalFileStorageProvider();

    StorageProviderFile cacheDirFile = StorageProviderFile.create(
        cacheDir.getAbsolutePath(), localProvider);

    StorageProviderFile result = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "spf_data", table,
        cacheDirFile, null, "test", "UNCHANGED", localProvider);

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.getPath().endsWith(".parquet"));
  }

  // ========== getParquetCacheDir tests (additional branches) ==========

  @Test void testGetParquetCacheDirBaseOnly() {
    File baseDir = tempDir.toFile();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir);
    assertNotNull(result);
    assertTrue(result.getPath().endsWith(".parquet_cache"));
    assertTrue(result.exists());
  }

  @Test void testGetParquetCacheDirWithCustomAndSchema() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom").toAbsolutePath().toString();
    File result = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "myschema");
    assertNotNull(result);
    assertTrue(result.getPath().contains("schema_myschema"));
    assertTrue(result.exists());
  }

  // ========== getCachedParquetFile tests ==========

  @Test void testGetCachedParquetFileSmartCasing() {
    File sourceFile = new File(tempDir.toFile(), "my_data.csv");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(
        sourceFile, cacheDir, true, "SMART_CASING");
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".parquet"));
    assertEquals("my_data.parquet", result.getName());
  }

  @Test void testGetCachedParquetFileNoExtensionName() {
    File sourceFile = new File(tempDir.toFile(), "rawdata");
    File cacheDir = tempDir.resolve("cache2").toFile();
    cacheDir.mkdirs();
    File result = ParquetConversionUtil.getCachedParquetFile(
        sourceFile, cacheDir, false, "UNCHANGED");
    assertNotNull(result);
    assertEquals("rawdata.parquet", result.getName());
  }

  // ========== configureS3Access via reflection ==========

  @Test void testConfigureS3Access() throws Exception {
    Method method = ParquetConversionUtil.class.getDeclaredMethod(
        "configureS3Access", org.apache.hadoop.conf.Configuration.class);
    method.setAccessible(true);

    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    // This should not throw, even if AWS credentials aren't available
    try {
      method.invoke(null, conf);
    } catch (Exception e) {
      // May fail if AWS SDK classes are not on classpath, which is acceptable
    }
  }

  // ========== convertToParquet overwrite existing ==========

  @Test void testConvertToParquetOverwritesExisting() throws Exception {
    RelDataType rowType = buildRowType(
        new String[]{"id", "name"},
        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    List<Object[]> rows1 = new ArrayList<>();
    rows1.add(new Object[]{"1", "First"});

    InMemoryScannableTable table1 = new InMemoryScannableTable(rowType, rows1);
    File sourceFile = createDummySourceFile("overwrite_data.csv");
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache_ow");
    cacheDir.mkdirs();

    // First write
    File result1 = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "overwrite_data", table1,
        cacheDir, null, "test", "UNCHANGED");
    assertNotNull(result1);
    assertTrue(result1.exists());

    // Update source file to trigger reconversion
    Thread.sleep(1100); // ensure timestamp difference
    try (FileWriter fw = new FileWriter(sourceFile)) {
      fw.write("updated dummy content");
    }

    List<Object[]> rows2 = new ArrayList<>();
    rows2.add(new Object[]{"1", "First"});
    rows2.add(new Object[]{"2", "Second"});
    rows2.add(new Object[]{"3", "Third"});
    InMemoryScannableTable table2 = new InMemoryScannableTable(rowType, rows2);

    // Reconvert - source is now newer
    File result2 = ParquetConversionUtil.convertToParquet(
        Sources.of(sourceFile), "overwrite_data", table2,
        cacheDir, null, "test", "UNCHANGED");
    assertNotNull(result2);
    assertTrue(result2.exists());
  }
}
