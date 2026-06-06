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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for RefreshableJsonTable covering refresh behavior,
 * row type deduction, and source conversion monitoring.
 */
@Tag("unit")
public class RefreshableJsonTableCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  private File createJsonFile(String content) throws IOException {
    File file = new File(tempDir.toFile(), "test_data.json");
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }

  @Test void testBasicConstruction() throws IOException {
    String json = "[{\"name\":\"Alice\",\"age\":30}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMinutes(5));

    assertNotNull(table);
    assertEquals("RefreshableJsonTable(test_table)", table.toString());
    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
  }

  @Test void testGetRowType() throws IOException {
    String json = "[{\"name\":\"Alice\",\"age\":30,\"active\":true}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMinutes(5));

    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertTrue(rowType.getFieldCount() >= 3);
  }

  @Test void testGetRowTypeCached() throws IOException {
    String json = "[{\"x\":1,\"y\":2}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMinutes(5));

    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType1 = table.getRowType(typeFactory);
    RelDataType rowType2 = table.getRowType(typeFactory);

    // Should return the same cached instance
    assertTrue(rowType1 == rowType2);
  }

  @Test void testRefreshBehavior() throws IOException {
    String json = "[{\"val\":1}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMinutes(5));

    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
  }

  @Test void testWithColumnNameCasing() throws IOException {
    String json = "[{\"Full Name\":\"Alice\",\"Age Score\":30}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMinutes(5), "SMART_CASING");

    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
  }

  @Test void testNullRefreshInterval() throws IOException {
    String json = "[{\"val\":1}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", null);

    assertNull(table.getRefreshInterval());
    assertFalse(table.needsRefresh());
  }

  @Test void testRefreshIntervalSet() throws IOException {
    String json = "[{\"val\":1}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    Duration interval = Duration.ofMinutes(10);
    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", interval);

    assertEquals(interval, table.getRefreshInterval());
    // First time should need refresh
    assertTrue(table.needsRefresh());
  }

  @Test void testToString() throws IOException {
    String json = "[{\"val\":1}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "my_table", null);

    assertEquals("RefreshableJsonTable(my_table)", table.toString());
  }

  @Test void testDoRefreshUpdatesTimestamp() throws IOException {
    String json = "[{\"val\":1}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMillis(1));

    // Initially no last refresh time
    assertNull(table.getLastRefreshTime());

    // Call refresh
    table.refresh();

    // After refresh, last refresh time should be set
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testDoRefreshWithModifiedFile() throws Exception {
    String json = "[{\"val\":1}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMillis(1));

    // First refresh
    table.refresh();
    assertNotNull(table.getLastRefreshTime());

    // Wait a bit and modify the file
    Thread.sleep(10);
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("[{\"val\":2}]");
    }
    // Ensure last modified is updated
    file.setLastModified(System.currentTimeMillis());

    // Wait for refresh interval
    Thread.sleep(10);

    // Second refresh should detect changes
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testConstructorWithOperatingDirectory() throws IOException {
    String json = "[{\"val\":1}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);
    File cacheDir = tempDir.toFile();

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMinutes(5), "UNCHANGED", cacheDir);

    assertNotNull(table);
    assertEquals("RefreshableJsonTable(test_table)", table.toString());
  }

  @Test void testRefreshWithNonExistentSourceFile() throws IOException {
    // Create a source pointing to a file that gets deleted
    File file = new File(tempDir.toFile(), "will_delete.json");
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("[{\"val\":1}]");
    }
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", Duration.ofMillis(1));

    // Delete the file
    file.delete();

    // Refresh should handle missing file gracefully
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testMultipleRowTypes() throws IOException {
    String json = "[{\"str\":\"hello\",\"num\":42,\"bool\":true,\"dec\":3.14}]";
    File file = createJsonFile(json);
    Source source = Sources.of(file);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "test_table", null);

    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertTrue(rowType.getFieldCount() >= 4);
  }
}
