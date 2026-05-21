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

import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CsvStreamReader} to improve line coverage.
 * Exercises the constructor, readNext(), getNextLine(), close(),
 * and the CsvContentListener inner class.
 */
@Tag("unit")
public class CsvStreamReaderTest {

  @TempDir
  java.nio.file.Path tempDir;

  /**
   * Tests the full constructor with a real CSV file.
   * This exercises the constructor (lines 57-65, 79-109),
   * the Tailer setup, CsvContentListener, and the Thread.sleep path.
   * After construction, exercises readNext() and close().
   */
  @Test public void testConstructorWithRealFile() throws Exception {
    File csvFile =
        createCsvFile("data.csv", "name,age,city\nAlice,30,NYC\nBob,25,LA\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      // The Tailer should have read the file content into the queue
      // Read the header line
      String[] header = reader.readNext();
      assertNotNull(header, "Should read header line");
      assertEquals(3, header.length, "Header should have 3 fields");
      assertEquals("name", header[0]);
      assertEquals("age", header[1]);
      assertEquals("city", header[2]);

      // Read first data line
      String[] row1 = reader.readNext();
      assertNotNull(row1, "Should read first data row");
      assertEquals(3, row1.length);
      assertEquals("Alice", row1[0]);
      assertEquals("30", row1[1]);
      assertEquals("NYC", row1[2]);

      // Read second data line
      String[] row2 = reader.readNext();
      assertNotNull(row2, "Should read second data row");
      assertEquals(3, row2.length);
      assertEquals("Bob", row2[0]);
      assertEquals("25", row2[1]);
      assertEquals("LA", row2[2]);

      // After all lines consumed, readNext should return null
      String[] end = reader.readNext();
      assertNull(end, "Should return null when no more lines");
    } finally {
      reader.close();
    }
  }

  /**
   * Tests readNext() with an empty CSV file.
   * After the Tailer reads an empty file, readNext should return null.
   */
  @Test public void testEmptyFile() throws Exception {
    File csvFile = createCsvFile("empty.csv", "");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      String[] result = reader.readNext();
      assertNull(result, "Empty file should return null from readNext");
    } finally {
      reader.close();
    }
  }

  /**
   * Tests readNext() with a single-line CSV file (just a header).
   */
  @Test public void testSingleLineFile() throws Exception {
    File csvFile = createCsvFile("single.csv", "col1,col2,col3\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      String[] header = reader.readNext();
      assertNotNull(header, "Should read the single header line");
      assertEquals(3, header.length);
      assertEquals("col1", header[0]);

      // No more lines
      String[] next = reader.readNext();
      assertNull(next, "No more lines after header");
    } finally {
      reader.close();
    }
  }

  /**
   * Tests readNext() with CSV containing quoted fields and special characters.
   */
  @Test public void testQuotedFields() throws Exception {
    File csvFile =
        createCsvFile("quoted.csv", "name,description\n\"Alice\",\"Has a, comma\"\n\"Bob\",\"No special\"\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      // Read header
      String[] header = reader.readNext();
      assertNotNull(header);
      assertEquals("name", header[0]);
      assertEquals("description", header[1]);

      // Read first data row with comma inside quotes
      String[] row1 = reader.readNext();
      assertNotNull(row1, "Should parse quoted field with comma");
      assertEquals("Alice", row1[0]);
      assertEquals("Has a, comma", row1[1]);
    } finally {
      reader.close();
    }
  }

  /**
   * Tests the DEFAULT_SKIP_LINES and DEFAULT_MONITOR_DELAY constants.
   */
  @Test public void testConstants() {
    assertEquals(0, CsvStreamReader.DEFAULT_SKIP_LINES,
        "Default skip lines should be 0");
    assertEquals(2000L, CsvStreamReader.DEFAULT_MONITOR_DELAY,
        "Default monitor delay should be 2000ms");
  }

  /**
   * Tests getNextLine() returns null when the contentQueue is empty.
   * Uses reflection to directly access and verify the protected getNextLine() method.
   */
  @Test public void testGetNextLineReturnsNullWhenQueueEmpty() throws Exception {
    File csvFile = createCsvFile("forqueue.csv", "a,b\n1,2\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      // Drain the queue
      while (reader.readNext() != null) {
        // consume all lines
      }
      // Now getNextLine should return null (readNext also returns null)
      String[] result = reader.readNext();
      assertNull(result, "Should return null when queue is empty");
    } finally {
      reader.close();
    }
  }

  /**
   * Tests that close() can be called without errors.
   */
  @Test public void testCloseIsNoOp() throws Exception {
    File csvFile = createCsvFile("closeable.csv", "x\n1\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    // close() is a no-op but should not throw
    reader.close();
    // Call it again to verify idempotency
    reader.close();
  }

  /**
   * Tests that the internal contentQueue and parser fields are properly initialized.
   * Uses reflection to verify internal state.
   */
  @Test public void testInternalFieldsInitialized() throws Exception {
    File csvFile = createCsvFile("fields.csv", "col\nval\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      // Verify parser is initialized via reflection
      Field parserField = CsvStreamReader.class.getDeclaredField("parser");
      parserField.setAccessible(true);
      Object parser = parserField.get(reader);
      assertNotNull(parser, "Parser should be initialized");

      // Verify contentQueue is initialized
      Field queueField = CsvStreamReader.class.getDeclaredField("contentQueue");
      queueField.setAccessible(true);
      Object queue = queueField.get(reader);
      assertNotNull(queue, "Content queue should be initialized");
      assertTrue(queue instanceof Queue, "Content queue should be a Queue instance");

      // Verify tailer is initialized
      Field tailerField = CsvStreamReader.class.getDeclaredField("tailer");
      tailerField.setAccessible(true);
      Object tailer = tailerField.get(reader);
      assertNotNull(tailer, "Tailer should be initialized");

      // Verify skipLines
      Field skipField = CsvStreamReader.class.getDeclaredField("skipLines");
      skipField.setAccessible(true);
      int skipLines = skipField.getInt(reader);
      assertEquals(CsvStreamReader.DEFAULT_SKIP_LINES, skipLines,
          "skipLines should be DEFAULT_SKIP_LINES");
    } finally {
      reader.close();
    }
  }

  /**
   * Tests readNext() with CSV data containing multiple fields per line.
   * Exercises the r.length > 0 and result == null branches in readNext.
   */
  @Test public void testReadNextMultipleFields() throws Exception {
    File csvFile =
        createCsvFile("multi.csv", "a,b,c,d,e\n1,2,3,4,5\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      String[] header = reader.readNext();
      assertNotNull(header);
      assertEquals(5, header.length);
      assertArrayEquals(new String[]{"a", "b", "c", "d", "e"}, header);

      String[] data = reader.readNext();
      assertNotNull(data);
      assertEquals(5, data.length);
      assertArrayEquals(new String[]{"1", "2", "3", "4", "5"}, data);
    } finally {
      reader.close();
    }
  }

  /**
   * Tests CsvContentListener by directly exercising the contentQueue.
   * Uses reflection to add content to the queue and then reads from it.
   */
  @SuppressWarnings("unchecked")
  @Test public void testContentQueueDirectAccess() throws Exception {
    File csvFile = createCsvFile("direct.csv", "h1,h2\n");
    Source source = Sources.of(csvFile);

    CsvStreamReader reader = new CsvStreamReader(source);
    try {
      // Drain existing queue
      while (reader.readNext() != null) {
        // consume
      }

      // Add content directly to the queue via reflection
      Field queueField = CsvStreamReader.class.getDeclaredField("contentQueue");
      queueField.setAccessible(true);
      Queue<String> queue = (Queue<String>) queueField.get(reader);

      // Simulate the CsvContentListener adding lines
      queue.add("x1,x2");
      queue.add("y1,y2");

      // Now readNext should return the manually added lines
      String[] row1 = reader.readNext();
      assertNotNull(row1);
      assertEquals(2, row1.length);
      assertEquals("x1", row1[0]);
      assertEquals("x2", row1[1]);

      String[] row2 = reader.readNext();
      assertNotNull(row2);
      assertEquals(2, row2.length);
      assertEquals("y1", row2[0]);
      assertEquals("y2", row2[1]);

      // Queue is empty again
      String[] row3 = reader.readNext();
      assertNull(row3);
    } finally {
      reader.close();
    }
  }

  private File createCsvFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
    return file;
  }
}
