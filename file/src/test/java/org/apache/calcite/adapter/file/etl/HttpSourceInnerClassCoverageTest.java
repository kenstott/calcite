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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HttpSource.LazyCSVIterator inner class.
 * Uses reflection to create instances of the private inner class.
 */
@Tag("unit")
public class HttpSourceInnerClassCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  /**
   * Creates a minimal HttpSource for constructing LazyCSVIterator.
   */
  private HttpSource createMinimalHttpSource() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    return new HttpSource(config);
  }

  /**
   * Creates a LazyCSVIterator via reflection.
   */
  @SuppressWarnings("unchecked")
  private Iterator<Map<String, Object>> createLazyCSVIterator(
      HttpSource source, InputStream inputStream, String cachePath,
      char delimiter, HttpSourceConfig.RowFilterConfig filter,
      HttpSourceConfig.WideToNarrowConfig wideToNarrow,
      boolean hasHeader, String columnNames) throws Exception {
    // Get the inner class
    Class<?> innerClass = null;
    for (Class<?> cls : HttpSource.class.getDeclaredClasses()) {
      if (cls.getSimpleName().equals("LazyCSVIterator")) {
        innerClass = cls;
        break;
      }
    }
    assertNotNull(innerClass, "LazyCSVIterator class should exist");

    // Get the constructor
    Constructor<?> constructor = innerClass.getDeclaredConstructor(
        HttpSource.class,
        InputStream.class,
        String.class,
        char.class,
        HttpSourceConfig.RowFilterConfig.class,
        HttpSourceConfig.WideToNarrowConfig.class,
        boolean.class,
        String.class);
    constructor.setAccessible(true);

    return (Iterator<Map<String, Object>>) constructor.newInstance(
        source, inputStream, cachePath, delimiter, filter, wideToNarrow,
        hasHeader, columnNames);
  }

  @Test void testBasicCSVParsing() throws Exception {
    String csv = "name,age,city\nAlice,30,NY\nBob,25,LA\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    assertTrue(iter.hasNext());
    Map<String, Object> row1 = iter.next();
    assertEquals("Alice", row1.get("name"));
    assertEquals(30L, row1.get("age"));
    assertEquals("NY", row1.get("city"));

    assertTrue(iter.hasNext());
    Map<String, Object> row2 = iter.next();
    assertEquals("Bob", row2.get("name"));
    assertEquals(25L, row2.get("age"));
    assertEquals("LA", row2.get("city"));

    assertFalse(iter.hasNext());
  }

  @Test void testEmptyCSV() throws Exception {
    String csv = "";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    assertFalse(iter.hasNext());
  }

  @Test void testHeaderOnlyCSV() throws Exception {
    String csv = "name,age,city\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    assertFalse(iter.hasNext());
  }

  @Test void testExplicitColumnNames() throws Exception {
    String csv = "Alice,30,NY\nBob,25,LA\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, false, "name,age,city");

    assertTrue(iter.hasNext());
    Map<String, Object> row1 = iter.next();
    assertEquals("Alice", row1.get("name"));
    assertEquals(30L, row1.get("age"));

    assertTrue(iter.hasNext());
    Map<String, Object> row2 = iter.next();
    assertEquals("Bob", row2.get("name"));
    assertFalse(iter.hasNext());
  }

  @Test void testHeaderlessWithoutColumnNames() throws Exception {
    // When no header and no column names, positional names should be generated
    String csv = "Alice,30,NY\nBob,25,LA\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, false, null);

    assertTrue(iter.hasNext());
    Map<String, Object> row1 = iter.next();
    // First row becomes both: used for determining column count and queued as data
    assertNotNull(row1.get("field_0"));

    // Second row should also be available
    assertTrue(iter.hasNext());
    Map<String, Object> row2 = iter.next();
    assertNotNull(row2.get("field_0"));
  }

  @Test void testTabDelimited() throws Exception {
    String tsv = "name\tage\tcity\nAlice\t30\tNY\n";
    InputStream is = new ByteArrayInputStream(tsv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", '\t', null, null, true, null);

    assertTrue(iter.hasNext());
    Map<String, Object> row = iter.next();
    assertEquals("Alice", row.get("name"));
    assertEquals(30L, row.get("age"));
    assertEquals("NY", row.get("city"));
    assertFalse(iter.hasNext());
  }

  @Test void testQuotedValues() throws Exception {
    String csv = "name,age,city\n\"Alice\",\"30\",\"New York\"\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    assertTrue(iter.hasNext());
    Map<String, Object> row = iter.next();
    assertEquals("Alice", row.get("name"));
    assertFalse(iter.hasNext());
  }

  @Test void testEmptyLines() throws Exception {
    String csv = "name,age\n\nAlice,30\n\nBob,25\n\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    assertTrue(iter.hasNext());
    Map<String, Object> row1 = iter.next();
    assertEquals("Alice", row1.get("name"));

    assertTrue(iter.hasNext());
    Map<String, Object> row2 = iter.next();
    assertEquals("Bob", row2.get("name"));

    assertFalse(iter.hasNext());
  }

  @Test void testNoSuchElementException() throws Exception {
    String csv = "name\nAlice\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    assertTrue(iter.hasNext());
    iter.next(); // consume Alice
    assertFalse(iter.hasNext());
    assertThrows(NoSuchElementException.class, () -> iter.next());
  }

  @Test void testMultipleColumns() throws Exception {
    String csv = "a,b,c,d,e\n1,2,3,4,5\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    assertTrue(iter.hasNext());
    Map<String, Object> row = iter.next();
    assertEquals(5, row.size());
    assertEquals(1L, row.get("a"));
    assertEquals(5L, row.get("e"));
  }

  @Test void testLargeCSV() throws Exception {
    StringBuilder sb = new StringBuilder("id,value\n");
    for (int i = 0; i < 100; i++) {
      sb.append(i).append(",val_").append(i).append("\n");
    }
    InputStream is = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Iterator<Map<String, Object>> iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    int count = 0;
    while (iter.hasNext()) {
      Map<String, Object> row = iter.next();
      assertNotNull(row);
      count++;
    }
    assertEquals(100, count);
  }

  @Test void testCloseableIterator() throws Exception {
    String csv = "name,age\nAlice,30\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalHttpSource();

    Object iter = createLazyCSVIterator(
        source, is, "/test", ',', null, null, true, null);

    // Verify it implements Closeable
    assertTrue(iter instanceof java.io.Closeable);

    // Close should not throw
    ((java.io.Closeable) iter).close();
  }
}
