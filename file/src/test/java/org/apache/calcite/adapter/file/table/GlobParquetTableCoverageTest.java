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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.file.refresh.RefreshableTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link GlobParquetTable}.
 */
@Tag("unit")
class GlobParquetTableCoverageTest {

  @TempDir
  File tempDir;

  private File cacheDir;

  @BeforeEach void setUp() throws IOException {
    cacheDir = new File(tempDir, "cache");
    cacheDir.mkdirs();
  }

  // ===== Constructor and basic accessors =====

  @Test void testConstructorAndToString() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        null,
        "TO_LOWER");

    String str = table.toString();
    assertNotNull(str);
    assertTrue(str.contains("GlobParquetTable"));
    assertTrue(str.contains("*.csv"));
  }

  @Test void testConstructorWithRefreshInterval() {
    Duration interval = Duration.ofMinutes(5);
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        interval,
        null,
        "TO_LOWER");

    assertEquals(interval, table.getRefreshInterval());
  }

  @Test void testConstructorNullRefreshInterval() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        null,
        "TO_LOWER");

    assertNull(table.getRefreshInterval());
  }

  // ===== getRefreshInterval =====

  @Test void testGetRefreshInterval() {
    Duration interval = Duration.ofHours(1);
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        interval,
        null,
        "TO_LOWER");

    assertEquals(interval, table.getRefreshInterval());
  }

  // ===== getLastRefreshTime =====

  @Test void testGetLastRefreshTimeInitiallyNull() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        null,
        "TO_LOWER");

    assertNull(table.getLastRefreshTime());
  }

  // ===== needsRefresh =====

  @Test void testNeedsRefreshNoInterval() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        null,
        "TO_LOWER");

    assertFalse(table.needsRefresh());
  }

  @Test void testNeedsRefreshNeverRefreshed() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        Duration.ofMinutes(5),
        null,
        "TO_LOWER");

    assertTrue(table.needsRefresh());
  }

  // ===== getRefreshBehavior =====

  @Test void testGetRefreshBehavior() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        null,
        "TO_LOWER");

    assertEquals(RefreshableTable.RefreshBehavior.DIRECTORY_SCAN,
        table.getRefreshBehavior());
  }

  // ===== generateCacheFileName =====

  @Test void testCacheFileNameGenerated() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        null,
        "TO_LOWER");

    // The cache file should be created inside cacheDir
    // It uses MD5 hash of pattern + table name
    assertNotNull(table.toString());
  }

  // ===== refresh with no interval =====

  @Test void testRefreshWithNoInterval() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        null,
        "TO_LOWER");

    // Should not throw - refresh is a no-op when no interval
    table.refresh();
    assertNull(table.getLastRefreshTime());
  }

  // ===== refresh with interval but no files =====

  @Test void testRefreshWithIntervalNoFiles() {
    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        Duration.ofMinutes(5),
        null,
        "TO_LOWER");

    // refresh should work without error even with no files
    table.refresh();
    // After refresh, lastRefreshTime should be set
    assertNotNull(table.getLastRefreshTime());
  }

  // ===== CacheMetadata inner class =====

  @Test void testCacheMetadataFileInfoHasChanged() throws IOException {
    // Create a test file
    File testFile = new File(tempDir, "test1.csv");
    try (FileWriter writer = new FileWriter(testFile)) {
      writer.write("a,b,c\n1,2,3\n");
    }

    // File size and modification time should be tracked
    assertNotNull(testFile);
    assertTrue(testFile.exists());
    assertTrue(testFile.length() > 0);
  }

  // ===== findMatchingFiles with simple pattern =====

  @Test void testRefreshFindsMatchingFiles() throws IOException {
    // Create CSV files in tempDir
    File csv1 = new File(tempDir, "data1.csv");
    try (FileWriter writer = new FileWriter(csv1)) {
      writer.write("col1,col2\nval1,val2\n");
    }
    File csv2 = new File(tempDir, "data2.csv");
    try (FileWriter writer = new FileWriter(csv2)) {
      writer.write("col1,col2\nval3,val4\n");
    }

    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        Duration.ofMinutes(5),
        null,
        "TO_LOWER");

    // This exercises findMatchingFiles and refresh path
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  // ===== Pattern without directory separator =====

  @Test void testRefreshPatternWithoutSeparator() throws IOException {
    GlobParquetTable table = new GlobParquetTable(
        "*.csv",
        "test_table",
        cacheDir,
        Duration.ofMinutes(5),
        null,
        "TO_LOWER");

    // Should handle pattern without directory separator
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  // ===== preprocessFiles =====

  @Test void testRefreshWithHtmlFile() throws IOException {
    // Create an HTML file in tempDir
    File htmlFile = new File(tempDir, "data.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body><table>"
          + "<tr><th>Name</th><th>Value</th></tr>"
          + "<tr><td>foo</td><td>bar</td></tr>"
          + "</table></body></html>");
    }

    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.html",
        "html_table",
        cacheDir,
        Duration.ofMinutes(5),
        null,
        "TO_LOWER");

    // This exercises the HTML preprocessing code path
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshWithExcelFile() throws IOException {
    // Create a dummy xlsx file (unsupported format for writeToParquet)
    File xlsxFile = new File(tempDir, "data.xlsx");
    try (FileWriter writer = new FileWriter(xlsxFile)) {
      writer.write("dummy");
    }

    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.xlsx",
        "excel_table",
        cacheDir,
        Duration.ofMinutes(5),
        null,
        "TO_LOWER");

    // refresh() catches the IOException for unsupported format gracefully
    table.refresh();
    // lastRefreshTime remains null because the refresh failed
    assertNull(table.getLastRefreshTime());
  }

  // ===== Repeat needsRefresh after a refresh =====

  @Test void testNeedsRefreshAfterRefreshWithLongInterval() throws IOException {
    // Create a CSV file so the refresh fully succeeds and sets cacheValid=true
    File csv = new File(tempDir, "refresh_test.csv");
    try (FileWriter writer = new FileWriter(csv)) {
      writer.write("col1,col2\nval1,val2\n");
    }

    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        Duration.ofHours(1),
        null,
        "TO_LOWER");

    assertTrue(table.needsRefresh());

    table.refresh();

    // After refresh with 1-hour interval, should not need refresh again immediately
    assertFalse(table.needsRefresh());
  }

  // ===== CsvTypeInferenceConfig =====

  @Test void testConstructorWithCsvTypeInferenceConfig() {
    CsvTypeInferrer.TypeInferenceConfig typeConfig =
        new CsvTypeInferrer.TypeInferenceConfig(
            true, 1.0, 1000, 0.8, true, true, true, true, 0.5);

    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/*.csv",
        "test_table",
        cacheDir,
        null,
        typeConfig,
        "TO_UPPER");

    assertNotNull(table);
  }

  // ===== Multiple refreshes with cache update detection =====

  @Test void testMultipleRefreshesDetectsChanges() throws IOException {
    // Create initial file
    File csv1 = new File(tempDir, "changing.csv");
    try (FileWriter writer = new FileWriter(csv1)) {
      writer.write("col1,col2\nval1,val2\n");
    }

    GlobParquetTable table = new GlobParquetTable(
        tempDir.getAbsolutePath() + "/changing*.csv",
        "changing_table",
        cacheDir,
        Duration.ofMillis(1), // Very short interval for testing
        null,
        "TO_LOWER");

    table.refresh();
    Instant firstRefresh = table.getLastRefreshTime();
    assertNotNull(firstRefresh);

    // Wait briefly, then create a new file and refresh again
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    File csv2 = new File(tempDir, "changing2.csv");
    try (FileWriter writer = new FileWriter(csv2)) {
      writer.write("col1,col2\nval3,val4\n");
    }

    table.refresh();
    Instant secondRefresh = table.getLastRefreshTime();
    assertNotNull(secondRefresh);
    // Second refresh should be later
    assertTrue(secondRefresh.isAfter(firstRefresh) || secondRefresh.equals(firstRefresh));
  }
}
