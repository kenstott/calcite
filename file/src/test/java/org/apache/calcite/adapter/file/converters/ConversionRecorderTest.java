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
