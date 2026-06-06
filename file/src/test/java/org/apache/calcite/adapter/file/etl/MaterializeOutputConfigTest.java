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

/**
 * Tests for {@link MaterializeOutputConfig}.
 */
@Tag("unit")
class MaterializeOutputConfigTest {

  @Test void testBuilderWithAllFields() {
    MaterializeOutputConfig config = MaterializeOutputConfig.builder()
        .location("s3://bucket/data/")
        .pattern("type=sales/year=STAR/")
        .format("parquet")
        .compression("zstd")
        .build();

    assertEquals("s3://bucket/data/", config.getLocation());
    assertEquals("type=sales/year=STAR/", config.getPattern());
    assertEquals("parquet", config.getFormat());
    assertEquals("zstd", config.getCompression());
  }

  @Test void testBuilderDefaults() {
    MaterializeOutputConfig config = MaterializeOutputConfig.builder().build();

    assertNull(config.getLocation());
    assertNull(config.getPattern());
    assertEquals("parquet", config.getFormat());
    assertEquals("snappy", config.getCompression());
  }

  @Test void testFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("location", "/data/output");
    map.put("pattern", "year=STAR/region=STAR/");
    map.put("format", "parquet");
    map.put("compression", "gzip");

    MaterializeOutputConfig config = MaterializeOutputConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("/data/output", config.getLocation());
    assertEquals("year=STAR/region=STAR/", config.getPattern());
    assertEquals("parquet", config.getFormat());
    assertEquals("gzip", config.getCompression());
  }

  @Test void testFromMapDefaults() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("pattern", "data/");

    MaterializeOutputConfig config = MaterializeOutputConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("parquet", config.getFormat());
    assertEquals("snappy", config.getCompression());
  }

  @Test void testFromMapNull() {
    assertNull(MaterializeOutputConfig.fromMap(null));
  }

  @Test void testFromMapEmpty() {
    MaterializeOutputConfig config =
        MaterializeOutputConfig.fromMap(new HashMap<String, Object>());
    assertNotNull(config);
    assertEquals("parquet", config.getFormat());
    assertEquals("snappy", config.getCompression());
  }
}
