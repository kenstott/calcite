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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.execution.arrow.UniversalDataBatchAdapter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.arrow.vector.VectorSchemaRoot;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Deep coverage tests for {@link UniversalDataBatchAdapter}.
 * Covers type conversion, null handling, round-trip conversion, and edge cases.
 */
@Tag("unit")
public class UniversalDataBatchAdapterDeepTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UniversalDataBatchAdapterDeepTest.class);

  private final RelDataTypeFactory typeFactory =
      new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

  private RelDataType createRowType(SqlTypeName... types) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < types.length; i++) {
      builder.add("col" + i, types[i]);
    }
    return builder.build();
  }

  @Test public void testConvertIntegerBatch() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1});
    data.add(new Object[]{2});
    data.add(new Object[]{3});

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());
    assertEquals(1, batch.getFieldVectors().size());

    // Convert back and verify
    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(3, resultList.size());
    assertEquals(1, resultList.get(0)[0]);
    assertEquals(2, resultList.get(1)[0]);
    assertEquals(3, resultList.get(2)[0]);

    batch.close();
  }

  @Test public void testConvertDoubleBatch() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1.5});
    data.add(new Object[]{2.7});
    data.add(new Object[]{3.14});

    RelDataType rowType = createRowType(SqlTypeName.DOUBLE);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(3, resultList.size());
    assertEquals(1.5, (Double) resultList.get(0)[0], 0.001);
    assertEquals(2.7, (Double) resultList.get(1)[0], 0.001);

    batch.close();
  }

  @Test public void testConvertBooleanBatch() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{true});
    data.add(new Object[]{false});
    data.add(new Object[]{true});

    RelDataType rowType = createRowType(SqlTypeName.BOOLEAN);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(3, resultList.size());
    assertEquals(true, resultList.get(0)[0]);
    assertEquals(false, resultList.get(1)[0]);

    batch.close();
  }

  @Test public void testConvertVarcharBatch() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{"hello"});
    data.add(new Object[]{"world"});

    RelDataType rowType = createRowType(SqlTypeName.VARCHAR);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(2, resultList.size());
    assertEquals("hello", resultList.get(0)[0]);
    assertEquals("world", resultList.get(1)[0]);

    batch.close();
  }

  @Test public void testConvertMixedTypesBatch() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1, 2.5, true, "text"});
    data.add(new Object[]{2, 3.7, false, "more"});

    RelDataType rowType =
        createRowType(SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());
    assertEquals(4, batch.getFieldVectors().size());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(2, resultList.size());
    assertEquals(4, resultList.get(0).length);
    assertEquals(1, resultList.get(0)[0]);
    assertEquals(2.5, (Double) resultList.get(0)[1], 0.001);
    assertEquals(true, resultList.get(0)[2]);
    assertEquals("text", resultList.get(0)[3]);

    batch.close();
  }

  @Test public void testConvertNullValues() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{null, null, null});
    data.add(new Object[]{1, 2.0, "test"});

    RelDataType rowType =
        createRowType(SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.VARCHAR);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(2, resultList.size());
    assertNull(resultList.get(0)[0]);
    assertNull(resultList.get(0)[1]);
    assertNull(resultList.get(0)[2]);
    assertEquals(1, resultList.get(1)[0]);

    batch.close();
  }

  @Test public void testConvertEmptyBatch() {
    List<Object[]> data = new ArrayList<>();

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(0, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    assertFalse(result.hasNext());

    batch.close();
  }

  @Test public void testBatchSizeLimiting() {
    List<Object[]> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add(new Object[]{i});
    }

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    // Batch size of 10 - should only take 10 rows
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(10, batch.getRowCount());

    batch.close();
  }

  @Test public void testStringConversionFromNumber() {
    // Integer value passed to a VARCHAR column
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{42});
    data.add(new Object[]{3.14});
    data.add(new Object[]{true});

    RelDataType rowType = createRowType(SqlTypeName.VARCHAR);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(3, resultList.size());
    assertEquals("42", resultList.get(0)[0]);
    assertEquals("3.14", resultList.get(1)[0]);
    assertEquals("true", resultList.get(2)[0]);

    batch.close();
  }

  @Test public void testNumberFromStringConversion() {
    // String values passed to INTEGER column
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{"42"});
    data.add(new Object[]{"99"});

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(2, resultList.size());
    assertEquals(42, resultList.get(0)[0]);

    batch.close();
  }

  @Test public void testDecimalMapsToDouble() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1.5});
    data.add(new Object[]{2.7});

    RelDataType rowType = createRowType(SqlTypeName.DECIMAL);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());

    batch.close();
  }

  @Test public void testShortRowsHandled() {
    // Rows with fewer columns than the schema
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1}); // Missing second column

    RelDataType rowType = createRowType(SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(1, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(1, resultList.size());
    assertEquals(1, resultList.get(0)[0]);
    // Second column should be null since row was short
    assertNull(resultList.get(0)[1]);

    batch.close();
  }

  @Test public void testBigIntType() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{Long.MAX_VALUE});
    data.add(new Object[]{Long.MIN_VALUE});

    RelDataType rowType = createRowType(SqlTypeName.BIGINT);
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(data.iterator(), rowType, 1024);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());

    Iterator<Object[]> result = UniversalDataBatchAdapter.convertFromArrowBatch(batch);
    List<Object[]> resultList = new ArrayList<>();
    while (result.hasNext()) {
      resultList.add(result.next());
    }

    assertEquals(2, resultList.size());
    assertEquals(Long.MAX_VALUE, resultList.get(0)[0]);
    assertEquals(Long.MIN_VALUE, resultList.get(1)[0]);

    batch.close();
  }
}
