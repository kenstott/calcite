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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link UniversalDataBatchAdapter}.
 */
@Tag("unit")
public class UniversalDataBatchAdapterTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UniversalDataBatchAdapterTest.class);

  private RelDataTypeFactory typeFactory =
      new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

  @Test public void testConvertIntegerRows() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{1},
        new Object[]{2},
        new Object[]{3});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());
    assertEquals(1, batch.getFieldVectors().size());
    assertEquals("id", batch.getSchema().getFields().get(0).getName());
    LOGGER.debug("Converted {} integer rows to Arrow", batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertDoubleRows() {
    RelDataType rowType = typeFactory.builder()
        .add("amount", SqlTypeName.DOUBLE)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{1.5},
        new Object[]{2.7},
        new Object[]{3.9});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertBooleanRows() {
    RelDataType rowType = typeFactory.builder()
        .add("flag", SqlTypeName.BOOLEAN)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{true},
        new Object[]{false},
        new Object[]{true});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertVarcharRows() {
    RelDataType rowType = typeFactory.builder()
        .add("name", SqlTypeName.VARCHAR, 100)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{"Alice"},
        new Object[]{"Bob"},
        new Object[]{"Charlie"});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertMixedTypeRows() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR, 100)
        .add("amount", SqlTypeName.DOUBLE)
        .add("active", SqlTypeName.BOOLEAN)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{1, "Alice", 100.5, true},
        new Object[]{2, "Bob", 200.0, false});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());
    assertEquals(4, batch.getFieldVectors().size());
    batch.close();
  }

  @Test public void testConvertWithNullValues() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR, 100)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{1, "Alice"},
        new Object[]{null, null},
        new Object[]{3, "Charlie"});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(3, batch.getRowCount());

    // Verify nulls
    assertTrue(batch.getVector(0).isNull(1), "Second row id should be null");
    assertTrue(batch.getVector(1).isNull(1), "Second row name should be null");
    batch.close();
  }

  @Test public void testConvertWithBatchSizeLimit() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .build();

    List<Object[]> rows = new ArrayList<Object[]>();
    for (int i = 0; i < 100; i++) {
      rows.add(new Object[]{i});
    }

    // Only take first 10
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(10, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertEmptyIterator() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .build();

    List<Object[]> rows = new ArrayList<Object[]>();
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(0, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertFromArrowBatch() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR, 100)
        .add("amount", SqlTypeName.DOUBLE)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{1, "Alice", 100.5},
        new Object[]{2, "Bob", 200.0});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    // Convert back
    Iterator<Object[]> resultIterator =
        UniversalDataBatchAdapter.convertFromArrowBatch(batch);

    assertTrue(resultIterator.hasNext());
    Object[] row1 = resultIterator.next();
    assertEquals(3, row1.length);
    assertEquals(1, row1[0]);
    assertEquals("Alice", row1[1]);
    assertEquals(100.5, (Double) row1[2], 0.01);

    assertTrue(resultIterator.hasNext());
    Object[] row2 = resultIterator.next();
    assertEquals(2, row2[0]);
    assertEquals("Bob", row2[1]);
    assertEquals(200.0, (Double) row2[2], 0.01);

    assertFalse(resultIterator.hasNext());
    batch.close();
  }

  @Test public void testConvertFromArrowBatchWithNulls() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR, 100)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{1, "Alice"},
        new Object[]{null, null});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    Iterator<Object[]> resultIterator =
        UniversalDataBatchAdapter.convertFromArrowBatch(batch);

    Object[] row1 = resultIterator.next();
    assertEquals(1, row1[0]);
    assertEquals("Alice", row1[1]);

    Object[] row2 = resultIterator.next();
    assertNull(row2[0]);
    assertNull(row2[1]);

    batch.close();
  }

  @Test public void testRoundTripConversion() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("amount", SqlTypeName.DOUBLE)
        .add("active", SqlTypeName.BOOLEAN)
        .build();

    List<Object[]> originalRows =
        Arrays.asList(new Object[]{1, 10.5, true},
        new Object[]{2, 20.0, false},
        new Object[]{3, 30.5, true});

    // Convert to Arrow
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            originalRows.iterator(), rowType, 10);

    // Convert back
    Iterator<Object[]> resultIterator =
        UniversalDataBatchAdapter.convertFromArrowBatch(batch);

    List<Object[]> resultRows = new ArrayList<Object[]>();
    while (resultIterator.hasNext()) {
      resultRows.add(resultIterator.next());
    }

    assertEquals(3, resultRows.size());
    assertEquals(1, resultRows.get(0)[0]);
    assertEquals(10.5, (Double) resultRows.get(0)[1], 0.01);
    assertEquals(true, resultRows.get(0)[2]);

    batch.close();
  }

  @Test public void testConvertWithNumericStringValues() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .build();

    // Pass a string that can be parsed as integer
    List<Object[]> rows = new ArrayList<Object[]>();
    rows.add(new Object[]{"42"});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(1, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertWithNumberSubtypes() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("amount", SqlTypeName.DOUBLE)
        .build();

    // Use Number subtypes: Long for INTEGER column, Float for DOUBLE column
    List<Object[]> rows = new ArrayList<Object[]>();
    rows.add(new Object[]{Long.valueOf(1), Float.valueOf(1.5f)});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(1, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertWithDecimalType() {
    RelDataType rowType = typeFactory.builder()
        .add("amount", SqlTypeName.DECIMAL, 10, 2)
        .build();

    List<Object[]> rows =
        Arrays.asList(new Object[]{99.99},
        new Object[]{100.50});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(2, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertWithShortRow() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR, 100)
        .add("amount", SqlTypeName.DOUBLE)
        .build();

    // Row shorter than schema
    List<Object[]> rows = new ArrayList<Object[]>();
    rows.add(new Object[]{1});

    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    assertNotNull(batch);
    assertEquals(1, batch.getRowCount());
    batch.close();
  }

  @Test public void testConvertFromArrowBatchEmpty() {
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .build();

    List<Object[]> rows = new ArrayList<Object[]>();
    VectorSchemaRoot batch =
        UniversalDataBatchAdapter.convertToArrowBatch(
            rows.iterator(), rowType, 10);

    Iterator<Object[]> resultIterator =
        UniversalDataBatchAdapter.convertFromArrowBatch(batch);

    assertFalse(resultIterator.hasNext());
    batch.close();
  }
}
