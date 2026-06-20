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
package org.apache.calcite.adapter.file.refresh;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link ConversionFileWatcher} covering singleton creation,
 * file type detection, schema base directory registration, and
 * watch/unwatch lifecycle.
 */
@Tag("unit")
class ConversionFileWatcherTest {

  @TempDir
  File tempDir;

  @Test void testGetInstanceReturnsSingleton() {
    ConversionFileWatcher watcher1 = ConversionFileWatcher.getInstance();
    ConversionFileWatcher watcher2 = ConversionFileWatcher.getInstance();
    assertNotNull(watcher1);
    assertNotNull(watcher2);
    // Singleton pattern - should be same instance
    org.junit.jupiter.api.Assertions.assertSame(watcher1, watcher2);
  }

  @Test void testRegisterSchemaBaseDirectory() {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    // Should not throw
    watcher.registerSchemaBaseDirectory("test_schema", tempDir);
  }

  @Test void testRegisterSchemaBaseDirectoryNull() {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    // Null params should be handled gracefully
    watcher.registerSchemaBaseDirectory(null, tempDir);
    watcher.registerSchemaBaseDirectory("schema", null);
    watcher.registerSchemaBaseDirectory(null, null);
  }

  @Test void testWatchFileNullFile() {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    // Null file should be handled gracefully
    watcher.watchFile(null, Duration.ofMinutes(1));
  }

  @Test void testWatchFileNonExistent() {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File nonExistent = new File(tempDir, "nonexistent.xlsx");
    // Non-existent file should be ignored
    watcher.watchFile(nonExistent, Duration.ofMinutes(1));
  }

  @Test void testWatchExcelFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File excelFile = new File(tempDir, "test.xlsx");
    Files.write(excelFile.toPath(), new byte[]{0x50, 0x4B}); // Minimal bytes
    watcher.watchFile("test_schema", excelFile, Duration.ofMinutes(1));
    // Should be accepted without error
  }

  @Test void testWatchHtmlFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File htmlFile = new File(tempDir, "test.html");
    Files.write(htmlFile.toPath(), "<html></html>".getBytes());
    watcher.watchFile("test_schema", htmlFile, Duration.ofMinutes(1));
  }

  @Test void testWatchXmlFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File xmlFile = new File(tempDir, "test.xml");
    Files.write(xmlFile.toPath(), "<root/>".getBytes());
    watcher.watchFile("test_schema", xmlFile, Duration.ofMinutes(1));
  }

  @Test void testWatchMarkdownFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File mdFile = new File(tempDir, "test.md");
    Files.write(mdFile.toPath(), "| a | b |\n|---|---|\n| 1 | 2 |".getBytes());
    watcher.watchFile("test_schema", mdFile, Duration.ofMinutes(1));
  }

  @Test void testWatchMarkdownExtensionFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File mdFile = new File(tempDir, "test.markdown");
    Files.write(mdFile.toPath(), "| a | b |\n|---|---|\n| 1 | 2 |".getBytes());
    watcher.watchFile("test_schema", mdFile, Duration.ofMinutes(1));
  }

  @Test void testWatchDocxFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File docxFile = new File(tempDir, "test.docx");
    Files.write(docxFile.toPath(), new byte[]{0x50, 0x4B}); // Minimal bytes
    watcher.watchFile("test_schema", docxFile, Duration.ofMinutes(1));
  }

  @Test void testWatchPptxFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File pptxFile = new File(tempDir, "test.pptx");
    Files.write(pptxFile.toPath(), new byte[]{0x50, 0x4B}); // Minimal bytes
    watcher.watchFile("test_schema", pptxFile, Duration.ofMinutes(1));
  }

  @Test void testWatchUnsupportedFileType() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File csvFile = new File(tempDir, "test.csv");
    Files.write(csvFile.toPath(), "a,b,c".getBytes());
    // CSV is not a convertible type - should be silently ignored
    watcher.watchFile("test_schema", csvFile, Duration.ofMinutes(1));
  }

  @Test void testWatchFileWithNullSchema() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File excelFile = new File(tempDir, "no_schema.xlsx");
    Files.write(excelFile.toPath(), new byte[]{0x50, 0x4B});
    // Null schema should use "default"
    watcher.watchFile(null, excelFile, Duration.ofMinutes(1));
  }

  @Test void testWatchFileWithNullDuration() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File excelFile = new File(tempDir, "null_dur.xlsx");
    Files.write(excelFile.toPath(), new byte[]{0x50, 0x4B});
    // Null duration should use default (60s)
    watcher.watchFile("test_schema", excelFile, null);
  }

  @Test void testUnwatchFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File excelFile = new File(tempDir, "unwatch.xlsx");
    Files.write(excelFile.toPath(), new byte[]{0x50, 0x4B});
    watcher.watchFile("test_schema", excelFile, Duration.ofMinutes(1));
    // Should not throw
    watcher.unwatchFile(excelFile);
  }

  @Test void testUnwatchFileWithSchema() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File excelFile = new File(tempDir, "unwatch_schema.xlsx");
    Files.write(excelFile.toPath(), new byte[]{0x50, 0x4B});
    watcher.watchFile("my_schema", excelFile, Duration.ofMinutes(1));
    watcher.unwatchFile("my_schema", excelFile);
  }

  @Test void testUnwatchFileWithNullSchema() {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File someFile = new File(tempDir, "some.xlsx");
    // Should use "default" schema
    watcher.unwatchFile(null, someFile);
  }

  @Test void testWatchFileHtmExtension() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File htmFile = new File(tempDir, "test.htm");
    Files.write(htmFile.toPath(), "<html></html>".getBytes());
    watcher.watchFile("test_schema", htmFile, Duration.ofMinutes(1));
  }

  @Test void testWatchXlsFile() throws IOException {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File xlsFile = new File(tempDir, "test.xls");
    Files.write(xlsFile.toPath(), new byte[]{(byte) 0xD0, (byte) 0xCF});
    watcher.watchFile("test_schema", xlsFile, Duration.ofMinutes(1));
  }
}
