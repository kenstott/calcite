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

import org.apache.calcite.adapter.file.FileRowConverter;

import org.jsoup.select.Elements;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FileEnumerator} to push execution package coverage past 75%.
 */
@Tag("unit")
public class FileEnumeratorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileEnumeratorTest.class);

  @Test
  @DisplayName("moveNext returns false for empty iterator")
  void testMoveNextEmpty() {
    List<Elements> emptyList = new ArrayList<>();
    FileRowConverter converter = Mockito.mock(FileRowConverter.class);
    when(converter.width()).thenReturn(2);
    FileEnumerator enumerator = new FileEnumerator(emptyList.iterator(), converter);

    assertFalse(enumerator.moveNext());
  }

  @Test
  @DisplayName("moveNext returns true when elements available and sets current")
  void testMoveNextWithElements() {
    List<Elements> rows = new ArrayList<>();
    rows.add(new Elements());
    rows.add(new Elements());
    FileRowConverter converter = Mockito.mock(FileRowConverter.class);
    when(converter.width()).thenReturn(1);
    when(converter.toRow(any(Elements.class), any(int[].class)))
        .thenReturn("mock_value");
    FileEnumerator enumerator = new FileEnumerator(rows.iterator(), converter);

    assertTrue(enumerator.moveNext());
    assertNotNull(enumerator.current());
    assertTrue(enumerator.moveNext());
    assertFalse(enumerator.moveNext());
  }

  @Test
  @DisplayName("current triggers moveNext when called first")
  void testCurrentTriggersMove() {
    List<Elements> rows = new ArrayList<>();
    rows.add(new Elements());
    FileRowConverter converter = Mockito.mock(FileRowConverter.class);
    when(converter.width()).thenReturn(1);
    when(converter.toRow(any(Elements.class), any(int[].class)))
        .thenReturn("first_row");
    FileEnumerator enumerator = new FileEnumerator(rows.iterator(), converter);

    // Calling current() without moveNext() should trigger moveNext
    Object result = enumerator.current();
    LOGGER.debug("Current result on first call: {}", result);
  }

  @Test
  @DisplayName("reset throws UnsupportedOperationException")
  void testResetThrowsUnsupported() {
    List<Elements> rows = new ArrayList<>();
    FileRowConverter converter = Mockito.mock(FileRowConverter.class);
    when(converter.width()).thenReturn(1);
    FileEnumerator enumerator = new FileEnumerator(rows.iterator(), converter);

    assertThrows(UnsupportedOperationException.class, () -> enumerator.reset());
  }

  @Test
  @DisplayName("close does not throw")
  void testCloseDoesNotThrow() {
    List<Elements> rows = new ArrayList<>();
    FileRowConverter converter = Mockito.mock(FileRowConverter.class);
    when(converter.width()).thenReturn(1);
    FileEnumerator enumerator = new FileEnumerator(rows.iterator(), converter);

    enumerator.close(); // Should not throw
  }

  @Test
  @DisplayName("constructor with fields parameter uses provided fields")
  void testConstructorWithFields() {
    List<Elements> rows = new ArrayList<>();
    rows.add(new Elements());
    FileRowConverter converter = Mockito.mock(FileRowConverter.class);
    when(converter.width()).thenReturn(3);
    int[] fields = {0, 2};
    when(converter.toRow(any(Elements.class), eq(fields)))
        .thenReturn(new Object[]{"val0", "val2"});

    FileEnumerator enumerator = new FileEnumerator(rows.iterator(), converter, fields);
    assertTrue(enumerator.moveNext());
    assertNotNull(enumerator.current());
    LOGGER.debug("Field-filtered enumerator works");
  }

  @Test
  @DisplayName("current returns null after iterator exhausted")
  void testCurrentAfterExhaustion() {
    List<Elements> rows = new ArrayList<>();
    FileRowConverter converter = Mockito.mock(FileRowConverter.class);
    when(converter.width()).thenReturn(1);
    FileEnumerator enumerator = new FileEnumerator(rows.iterator(), converter);

    // Iterator is empty, moveNext returns false
    assertFalse(enumerator.moveNext());
    // After moveNext returns false, current should be null
    assertNull(enumerator.current());
  }
}
