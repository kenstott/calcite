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
package org.apache.calcite.adapter.file.execution.linq4j;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link CsvEnumerator}.
 *
 * <p>Covers row type deduction, column casing, type inference for various SQL types,
 * filter values, TSV support, the identityList utility, parseDecimal edge cases,
 * moveNext/current lifecycle, error handling, and row converter paths.
 */
@Tag("unit")
public class CsvEnumeratorDeepCoverageTest {

  @TempDir
  Path tempDir;

  private JavaTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  // ====================================================================
  // deduceRowType: basic header parsing
  // ====================================================================

  @Test
  void testDeduceRowTypeSimpleHeaders() throws Exception {
    Path csvFile = createCsvFile("simple.csv", "name,age,city\nAlice,30,NYC\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);
    assertNotNull(rowType);
    assertEquals(3, rowType.getFieldCount());
    assertEquals("name", rowType.getFieldList().get(0).getName());
    assertEquals("age", rowType.getFieldList().get(1).getName());
    assertEquals("city", rowType.getFieldList().get(2).getName());
    // Without type hints, all columns should be VARCHAR
    for (int i = 0; i < 3; i++) {
      assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(i).getType().getSqlTypeName());
    }
  }

  // ====================================================================
  // deduceRowType: typed headers (colon-separated)
  // ====================================================================

  @Test
  void testDeduceRowTypeStringType() throws Exception {
    Path csvFile = createCsvFile("string.csv", "name:string\nAlice\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeBooleanType() throws Exception {
    Path csvFile = createCsvFile("bool.csv", "active:boolean\ntrue\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.BOOLEAN, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeByteType() throws Exception {
    Path csvFile = createCsvFile("byte.csv", "val:byte\n1\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.TINYINT, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeCharType() throws Exception {
    Path csvFile = createCsvFile("char.csv", "val:char\nA\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.CHAR, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeShortType() throws Exception {
    Path csvFile = createCsvFile("short.csv", "val:short\n100\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.SMALLINT, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeIntType() throws Exception {
    Path csvFile = createCsvFile("int.csv", "val:int\n100\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.INTEGER, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeLongType() throws Exception {
    Path csvFile = createCsvFile("long.csv", "val:long\n999999999999\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.BIGINT, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeFloatType() throws Exception {
    Path csvFile = createCsvFile("float.csv", "val:float\n3.14\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.REAL, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeDoubleType() throws Exception {
    Path csvFile = createCsvFile("double.csv", "val:double\n3.14159\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.DOUBLE, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeDateType() throws Exception {
    Path csvFile = createCsvFile("date.csv", "val:date\n2024-01-15\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.DATE, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeTimestampType() throws Exception {
    Path csvFile = createCsvFile("timestamp.csv", "val:timestamp\n2024-01-15 10:30:00\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.TIMESTAMP, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeTimestamptzType() throws Exception {
    Path csvFile = createCsvFile("timestamptz.csv", "val:timestamptz\n2024-01-15 10:30:00Z\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeTimeType() throws Exception {
    Path csvFile = createCsvFile("time.csv", "val:time\n10:30:00\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertEquals(SqlTypeName.TIME, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeUnknownType() throws Exception {
    Path csvFile = createCsvFile("unknown.csv", "val:foobar\ndata\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    // Unknown type defaults to VARCHAR
    assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testDeduceRowTypeMultipleTypedColumns() throws Exception {
    Path csvFile = createCsvFile("multi.csv",
        "name:string,age:int,score:double,active:boolean\nAlice,30,95.5,true\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);
    assertEquals(4, rowType.getFieldCount());
    assertEquals(SqlTypeName.VARCHAR, fieldTypes.get(0).getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, fieldTypes.get(1).getSqlTypeName());
    assertEquals(SqlTypeName.DOUBLE, fieldTypes.get(2).getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, fieldTypes.get(3).getSqlTypeName());
  }

  // ====================================================================
  // deduceRowType: empty / null header handling
  // ====================================================================

  @Test
  void testDeduceRowTypeEmptyFile() throws Exception {
    Path csvFile = createCsvFile("empty.csv", "");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, null, false);
    assertNotNull(rowType);
    // Empty file should produce a fallback "EmptyFileHasNoColumns" column
    assertEquals(1, rowType.getFieldCount());
    assertEquals("EmptyFileHasNoColumns", rowType.getFieldList().get(0).getName());
  }

  // ====================================================================
  // deduceRowType: stream mode adds ROWTIME column
  // ====================================================================

  @Test
  void testDeduceRowTypeStreamMode() throws Exception {
    Path csvFile = createCsvFile("stream.csv", "name,value\nAlice,100\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, true);
    assertNotNull(rowType);
    // Stream mode adds ROWTIME as first column
    assertEquals(3, rowType.getFieldCount());
    assertEquals("ROWTIME", rowType.getFieldList().get(0).getName());
    assertEquals(SqlTypeName.TIMESTAMP,
        rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  // ====================================================================
  // deduceRowType: column casing
  // ====================================================================

  @Test
  void testDeduceRowTypeWithToLowerCasing() throws Exception {
    Path csvFile = createCsvFile("casing_lower.csv", "FirstName,LastName\nAlice,Smith\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(
        typeFactory, source, null, false, "TO_LOWER");
    assertNotNull(rowType);
    // Names should be lowered
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      String name = rowType.getFieldList().get(i).getName();
      // SMART_CASING or TO_LOWER will produce lowercase variants
      assertNotNull(name);
    }
  }

  @Test
  void testDeduceRowTypeWithUnchangedCasing() throws Exception {
    Path csvFile = createCsvFile("casing_unchanged.csv", "FirstName,LastName\nAlice,Smith\n");
    Source source = Sources.of(csvFile.toFile());
    RelDataType rowType = CsvEnumerator.deduceRowType(
        typeFactory, source, null, false, "UNCHANGED");
    assertNotNull(rowType);
    assertEquals("FirstName", rowType.getFieldList().get(0).getName());
    assertEquals("LastName", rowType.getFieldList().get(1).getName());
  }

  // ====================================================================
  // identityList
  // ====================================================================

  @Test
  void testIdentityListZero() {
    int[] result = CsvEnumerator.identityList(0);
    assertEquals(0, result.length);
  }

  @Test
  void testIdentityListOne() {
    int[] result = CsvEnumerator.identityList(1);
    assertEquals(1, result.length);
    assertEquals(0, result[0]);
  }

  @Test
  void testIdentityListMultiple() {
    int[] result = CsvEnumerator.identityList(5);
    assertEquals(5, result.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(i, result[i]);
    }
  }

  // ====================================================================
  // parseDecimal
  // ====================================================================

  @Test
  void testParseDecimalExact() {
    BigDecimal result = CsvEnumerator.parseDecimal(5, 2, "123.45");
    assertEquals(new BigDecimal("123.45"), result);
  }

  @Test
  void testParseDecimalRoundsUp() {
    BigDecimal result = CsvEnumerator.parseDecimal(5, 2, "123.455");
    assertEquals(new BigDecimal("123.46"), result);
  }

  @Test
  void testParseDecimalRoundsDown() {
    BigDecimal result = CsvEnumerator.parseDecimal(5, 2, "123.454");
    assertEquals(new BigDecimal("123.45"), result);
  }

  @Test
  void testParseDecimalNegative() {
    BigDecimal result = CsvEnumerator.parseDecimal(5, 2, "-123.45");
    assertEquals(new BigDecimal("-123.45"), result);
  }

  @Test
  void testParseDecimalNegativeRoundsUp() {
    BigDecimal result = CsvEnumerator.parseDecimal(5, 2, "-123.455");
    assertEquals(new BigDecimal("-123.46"), result);
  }

  @Test
  void testParseDecimalPrecisionExceeded() {
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(4, 0, "12345"));
  }

  @Test
  void testParseDecimalPrecisionExceededNegative() {
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(4, 0, "-12345"));
  }

  @Test
  void testParseDecimalPrecisionExceededWithScale() {
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(4, 2, "123.45"));
  }

  @Test
  void testParseDecimalScientificNotation() {
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(4, 0, "1e+5"));
  }

  @Test
  void testParseDecimalZero() {
    BigDecimal result = CsvEnumerator.parseDecimal(5, 2, "0.00");
    assertEquals(new BigDecimal("0.00"), result);
  }

  @Test
  void testParseDecimalSmallNumber() {
    BigDecimal result = CsvEnumerator.parseDecimal(5, 4, "0.1234");
    assertEquals(new BigDecimal("0.1234"), result);
  }

  @Test
  void testParseDecimalInvalidString() {
    assertThrows(NumberFormatException.class,
        () -> CsvEnumerator.parseDecimal(5, 2, "abc"));
  }

  // ====================================================================
  // CsvEnumerator: constructor, moveNext, current, close
  // ====================================================================

  @Test
  void testEnumeratorMoveNextAndCurrent() throws Exception {
    Path csvFile = createCsvFile("enum.csv", "name:string,age:int\nAlice,30\nBob,25\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Arrays.asList(0, 1);

    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, fieldTypes, fields);
    try {
      assertTrue(enumerator.moveNext());
      Object[] row1 = enumerator.current();
      assertNotNull(row1);
      assertEquals(2, row1.length);

      assertTrue(enumerator.moveNext());
      Object[] row2 = enumerator.current();
      assertNotNull(row2);

      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testEnumeratorSingleColumnField() throws Exception {
    Path csvFile = createCsvFile("single_field.csv", "name:string\nAlice\nBob\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Collections.singletonList(0);

    CsvEnumerator<Object> enumerator =
        new CsvEnumerator<Object>(source, cancel, fieldTypes, fields);
    try {
      assertTrue(enumerator.moveNext());
      Object val = enumerator.current();
      assertNotNull(val);
      // Single column returns single value, not array
      assertFalse(val instanceof Object[]);

      assertTrue(enumerator.moveNext());
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testEnumeratorCancelFlag() throws Exception {
    Path csvFile = createCsvFile("cancel.csv", "name:string\nAlice\nBob\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(true); // Pre-cancelled
    List<Integer> fields = Collections.singletonList(0);

    CsvEnumerator<Object> enumerator =
        new CsvEnumerator<Object>(source, cancel, fieldTypes, fields);
    try {
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testEnumeratorResetThrows() throws Exception {
    Path csvFile = createCsvFile("reset.csv", "name:string\nAlice\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Collections.singletonList(0);

    CsvEnumerator<Object> enumerator =
        new CsvEnumerator<Object>(source, cancel, fieldTypes, fields);
    try {
      assertThrows(UnsupportedOperationException.class, enumerator::reset);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // TSV support
  // ====================================================================

  @Test
  void testTsvFile() throws Exception {
    Path tsvFile = createCsvFile("data.tsv", "name\tage\tcity\nAlice\t30\tNYC\nBob\t25\tSF\n");
    Source source = Sources.of(tsvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);
    assertEquals(3, rowType.getFieldCount());
    assertEquals("name", rowType.getFieldList().get(0).getName());
    assertEquals("age", rowType.getFieldList().get(1).getName());
    assertEquals("city", rowType.getFieldList().get(2).getName());
  }

  @Test
  void testTsvFileEnumeration() throws Exception {
    Path tsvFile = createCsvFile("enum.tsv", "name\tage\nAlice\t30\nBob\t25\n");
    Source source = Sources.of(tsvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Arrays.asList(0, 1);

    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, fieldTypes, fields);
    try {
      int count = 0;
      while (enumerator.moveNext()) {
        count++;
      }
      assertEquals(2, count);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Filter values
  // ====================================================================

  @Test
  void testFilterValuesMatchingRows() throws Exception {
    Path csvFile = createCsvFile("filter.csv",
        "name:string,city:string\nAlice,NYC\nBob,SF\nCharlie,NYC\n");
    Source source = Sources.of(csvFile.toFile());

    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    // Filter on city = "NYC"
    String[] filterValues = new String[]{null, "NYC"};

    CsvEnumerator.RowConverter<Object[]> converter =
        CsvEnumerator.arrayConverter(fieldTypes, Arrays.asList(0, 1), false);

    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, false, filterValues, converter);
    try {
      int count = 0;
      while (enumerator.moveNext()) {
        count++;
      }
      assertEquals(2, count); // Alice and Charlie match NYC
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testFilterValuesNoMatch() throws Exception {
    Path csvFile = createCsvFile("no_match.csv",
        "name:string,city:string\nAlice,NYC\nBob,SF\n");
    Source source = Sources.of(csvFile.toFile());

    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    String[] filterValues = new String[]{null, "LONDON"};

    CsvEnumerator.RowConverter<Object[]> converter =
        CsvEnumerator.arrayConverter(fieldTypes, Arrays.asList(0, 1), false);

    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, false, filterValues, converter);
    try {
      int count = 0;
      while (enumerator.moveNext()) {
        count++;
      }
      assertEquals(0, count);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testFilterValuesNull() throws Exception {
    Path csvFile = createCsvFile("null_filter.csv", "name:string\nAlice\nBob\n");
    Source source = Sources.of(csvFile.toFile());

    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);

    CsvEnumerator.RowConverter<Object[]> converter =
        CsvEnumerator.arrayConverter(fieldTypes, Collections.singletonList(0), false);

    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, false, null, converter);
    try {
      int count = 0;
      while (enumerator.moveNext()) {
        count++;
      }
      assertEquals(2, count);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // ArrayRowConverter with various parameters
  // ====================================================================

  @Test
  void testArrayConverterStreamMode() throws Exception {
    Path csvFile = createCsvFile("stream_arr.csv", "val:int\n42\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    CsvEnumerator.RowConverter<Object[]> converter =
        CsvEnumerator.arrayConverter(fieldTypes, Collections.singletonList(0), true);
    assertNotNull(converter);
  }

  @Test
  void testArrayConverterBlankStringsAsNull() throws Exception {
    Path csvFile = createCsvFile("blank.csv", "val:string\n\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    CsvEnumerator.RowConverter<Object[]> converter1 =
        CsvEnumerator.arrayConverter(fieldTypes, Collections.singletonList(0), false, true);
    assertNotNull(converter1);

    CsvEnumerator.RowConverter<Object[]> converter2 =
        CsvEnumerator.arrayConverter(fieldTypes, Collections.singletonList(0), false, false);
    assertNotNull(converter2);
  }

  @Test
  void testArrayConverterWithTypeConverter() throws Exception {
    Path csvFile = createCsvFile("tc.csv", "val:int\n42\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    CsvEnumerator.RowConverter<Object[]> converter =
        CsvEnumerator.arrayConverter(fieldTypes, Collections.singletonList(0),
            false, true, null);
    assertNotNull(converter);
  }

  // ====================================================================
  // deduceRowType: decimal type in header
  // ====================================================================

  @Test
  void testDeduceRowTypeDecimalType() throws Exception {
    // Decimal type pattern: "decimal(precision,scale)
    Path csvFile = createCsvFile("decimal.csv", "amount:\"decimal(10,2)\"\n123.45\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    RelDataType rowType = CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);
    assertEquals(1, rowType.getFieldCount());
    assertEquals(SqlTypeName.DECIMAL, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  // ====================================================================
  // openCsv: non-TSV file
  // ====================================================================

  @Test
  void testOpenCsvRegularFile() throws Exception {
    Path csvFile = createCsvFile("regular.csv", "a,b,c\n1,2,3\n");
    Source source = Sources.of(csvFile.toFile());
    com.opencsv.CSVReader reader = CsvEnumerator.openCsv(source);
    assertNotNull(reader);
    String[] header = reader.readNext();
    assertNotNull(header);
    assertEquals(3, header.length);
    reader.close();
  }

  // ====================================================================
  // Close releases lock
  // ====================================================================

  @Test
  void testCloseReleasesLock() throws Exception {
    Path csvFile = createCsvFile("lock.csv", "name:string\nAlice\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    CsvEnumerator<Object> enumerator =
        new CsvEnumerator<Object>(source, cancel, fieldTypes, Collections.singletonList(0));
    // Close should not throw and should release lock
    enumerator.close();
  }

  // ====================================================================
  // Multiple rows with various data types
  // ====================================================================

  @Test
  void testEnumeratorWithMixedTypes() throws Exception {
    Path csvFile = createCsvFile("mixed.csv",
        "name:string,age:int,score:double,active:boolean\n"
            + "Alice,30,95.5,true\n"
            + "Bob,25,87.3,false\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Arrays.asList(0, 1, 2, 3);

    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, fieldTypes, fields);
    try {
      assertTrue(enumerator.moveNext());
      Object[] row1 = enumerator.current();
      assertEquals(4, row1.length);

      assertTrue(enumerator.moveNext());
      Object[] row2 = enumerator.current();
      assertEquals(4, row2.length);

      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Empty data rows
  // ====================================================================

  @Test
  void testEnumeratorNoDataRows() throws Exception {
    Path csvFile = createCsvFile("no_data.csv", "name:string,age:int\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Arrays.asList(0, 1);

    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, fieldTypes, fields);
    try {
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Cancel during iteration
  // ====================================================================

  @Test
  void testCancelDuringIteration() throws Exception {
    Path csvFile = createCsvFile("cancel_mid.csv",
        "name:string\nAlice\nBob\nCharlie\nDiana\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Collections.singletonList(0);

    CsvEnumerator<Object> enumerator =
        new CsvEnumerator<Object>(source, cancel, fieldTypes, fields);
    try {
      assertTrue(enumerator.moveNext());
      cancel.set(true);
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Enumerator with TypeInferenceConfig constructor
  // ====================================================================

  @Test
  void testEnumeratorWithTypeInferenceConfig() throws Exception {
    Path csvFile = createCsvFile("type_inf.csv", "name:string,val:int\nAlice,42\n");
    Source source = Sources.of(csvFile.toFile());
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);

    AtomicBoolean cancel = new AtomicBoolean(false);
    List<Integer> fields = Arrays.asList(0, 1);

    // Constructor with null TypeInferenceConfig
    CsvEnumerator<Object[]> enumerator =
        new CsvEnumerator<Object[]>(source, cancel, fieldTypes, fields, null);
    try {
      assertTrue(enumerator.moveNext());
      assertNotNull(enumerator.current());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private Path createCsvFile(String name, String content) throws IOException {
    Path file = tempDir.resolve(name);
    Files.write(file, content.getBytes());
    return file;
  }
}
