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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link FileSourceConfig}.
 */
@Tag("unit")
class FileSourceConfigTest {

  @Test void testBuilderBasic() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("s3://bucket/data/report.xlsx")
        .format("xlsx")
        .sheet("Sheet1")
        .build();

    assertEquals("s3://bucket/data/report.xlsx", config.getPath());
    assertEquals("xlsx", config.getFormat());
    assertEquals("Sheet1", config.getSheet());
  }

  @Test void testBuilderMinimal() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/data/file.csv")
        .build();

    assertEquals("/data/file.csv", config.getPath());
    assertNull(config.getFormat());
    assertNull(config.getSheet());
  }

  @Test void testBuilderMissingPathThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        FileSourceConfig.builder().build());
  }

  @Test void testBuilderEmptyPathThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        FileSourceConfig.builder().path("").build());
  }

  @Test void testFromMapWithPath() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "/data/file.csv");
    map.put("format", "csv");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("/data/file.csv", config.getPath());
    assertEquals("csv", config.getFormat());
  }

  @Test void testFromMapWithLocation() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("location", "s3://bucket/data.parquet");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("s3://bucket/data.parquet", config.getPath());
  }

  @Test void testFromMapWithSheet() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "/data/report.xlsx");
    map.put("sheet", "DataSheet");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("DataSheet", config.getSheet());
  }

  @Test void testFromMapLocationOverridesPath() {
    // When both path and location exist, location wins
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "/data/file1.csv");
    map.put("location", "/data/file2.csv");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("/data/file2.csv", config.getPath());
  }
}
