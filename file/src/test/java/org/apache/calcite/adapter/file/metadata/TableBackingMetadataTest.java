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
package org.apache.calcite.adapter.file.metadata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TableBackingMetadata}.
 */
@Tag("unit")
class TableBackingMetadataTest {

  @Test void testConstructorAndTableName() {
    TableBackingMetadata meta = new TableBackingMetadata("my_table");
    assertEquals("my_table", meta.getTableName());
  }

  @Test void testInitialStateIsNull() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    assertNull(meta.getOriginalSource());
    assertNull(meta.getGeneratedSource());
    assertNull(meta.getCached());
  }

  @Test void testSetAndGetOriginalSource() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File original = new File("/data/source.html");
    meta.setOriginalSource(original);
    assertEquals(original, meta.getOriginalSource());
  }

  @Test void testSetAndGetGeneratedSource() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File generated = new File("/data/generated.json");
    meta.setGeneratedSource(generated);
    assertEquals(generated, meta.getGeneratedSource());
  }

  @Test void testSetAndGetCached() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File cached = new File("/cache/data.parquet");
    meta.setCached(cached);
    assertEquals(cached, meta.getCached());
  }

  @Test void testGetBackingFileReturnsCachedWhenRequired() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File original = new File("/data/source.html");
    File generated = new File("/data/generated.json");
    File cached = new File("/cache/data.parquet");

    meta.setOriginalSource(original);
    meta.setGeneratedSource(generated);
    meta.setCached(cached);

    assertEquals(cached, meta.getBackingFile(true));
  }

  @Test void testGetBackingFileReturnsGeneratedWhenCachedNotRequired() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File original = new File("/data/source.html");
    File generated = new File("/data/generated.json");
    File cached = new File("/cache/data.parquet");

    meta.setOriginalSource(original);
    meta.setGeneratedSource(generated);
    meta.setCached(cached);

    assertEquals(generated, meta.getBackingFile(false));
  }

  @Test void testGetBackingFileReturnsGeneratedWhenNoCached() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File original = new File("/data/source.html");
    File generated = new File("/data/generated.json");

    meta.setOriginalSource(original);
    meta.setGeneratedSource(generated);

    assertEquals(generated, meta.getBackingFile(true));
  }

  @Test void testGetBackingFileReturnsOriginalWhenNoGenerated() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File original = new File("/data/source.csv");

    meta.setOriginalSource(original);

    assertEquals(original, meta.getBackingFile(false));
  }

  @Test void testGetBackingFileReturnsOriginalWhenNothingElse() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    File original = new File("/data/source.csv");

    meta.setOriginalSource(original);

    assertEquals(original, meta.getBackingFile(true));
  }

  @Test void testGetBackingFileReturnsNullWhenAllNull() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    assertNull(meta.getBackingFile(false));
    assertNull(meta.getBackingFile(true));
  }

  @Test void testToString() {
    TableBackingMetadata meta = new TableBackingMetadata("my_table");
    meta.setOriginalSource(new File("/data/source.html"));
    String str = meta.toString();
    assertNotNull(str);
    assertTrue(str.contains("my_table"));
    assertTrue(str.contains("source.html"));
  }

  @Test void testToStringWithNulls() {
    TableBackingMetadata meta = new TableBackingMetadata("test");
    String str = meta.toString();
    assertNotNull(str);
    assertTrue(str.contains("test"));
  }
}
