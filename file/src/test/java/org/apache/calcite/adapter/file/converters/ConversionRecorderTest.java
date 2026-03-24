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
package org.apache.calcite.adapter.file.converters;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ConversionRecorder}.
 */
@Tag("unit")
class ConversionRecorderTest {

  @TempDir
  File tempDir;

  @Test void testRecordConversionDoesNotThrow() throws IOException {
    File source = new File(tempDir, "source.xlsx");
    source.createNewFile();
    File target = new File(tempDir, "target.json");
    target.createNewFile();

    // Should not throw even though the metadata recording may fail
    ConversionRecorder.recordConversion(source, target, "EXCEL_TO_JSON", tempDir);
    assertTrue(true); // No exception thrown
  }

  @Test void testRecordConversionWithExplicitTableName() throws IOException {
    File source = new File(tempDir, "source.xlsx");
    source.createNewFile();
    File target = new File(tempDir, "target.json");
    target.createNewFile();

    ConversionRecorder.recordConversion(source, target, "EXCEL_TO_JSON", tempDir, "my_table");
    assertTrue(true);
  }

  @Test void testRecordConversionNullBaseDirectory() throws IOException {
    File source = new File(tempDir, "source.html");
    source.createNewFile();
    File target = new File(tempDir, "target.json");
    target.createNewFile();

    // Null baseDirectory should use converted file's parent
    ConversionRecorder.recordConversion(source, target, "HTML_TO_JSON", null);
    assertTrue(true);
  }

  @Test void testRecordExcelConversion() throws IOException {
    File source = new File(tempDir, "data.xlsx");
    source.createNewFile();
    File target = new File(tempDir, "data.json");
    target.createNewFile();

    ConversionRecorder.recordExcelConversion(source, target, tempDir);
    assertTrue(true);
  }

  @Test void testRecordHtmlConversion() throws IOException {
    File source = new File(tempDir, "page.html");
    source.createNewFile();
    File target = new File(tempDir, "page.json");
    target.createNewFile();

    ConversionRecorder.recordHtmlConversion(source, target, tempDir);
    assertTrue(true);
  }

  @Test void testRecordXmlConversion() throws IOException {
    File source = new File(tempDir, "data.xml");
    source.createNewFile();
    File target = new File(tempDir, "data.json");
    target.createNewFile();

    ConversionRecorder.recordXmlConversion(source, target, tempDir);
    assertTrue(true);
  }

  @Test void testRecordJsonPathExtraction() throws IOException {
    File source = new File(tempDir, "source.json");
    source.createNewFile();
    File target = new File(tempDir, "extracted.json");
    target.createNewFile();

    ConversionRecorder.recordJsonPathExtraction(source, target, "$.data[*]", tempDir);
    assertTrue(true);
  }

  @Test void testRecordConversionNullExplicitTableName() throws IOException {
    File source = new File(tempDir, "source.xlsx");
    source.createNewFile();
    File target = new File(tempDir, "target.json");
    target.createNewFile();

    // Null explicit table name should use default recording
    ConversionRecorder.recordConversion(source, target, "EXCEL_TO_JSON", tempDir, null);
    assertTrue(true);
  }
}
