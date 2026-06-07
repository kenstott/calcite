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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BulkDownloadConfig}.
 */
@Tag("unit")
class BulkDownloadConfigTest {

  @Test void testBuilderBasic() {
    BulkDownloadConfig config = BulkDownloadConfig.builder()
        .name("qcew_annual")
        .cachePattern("bulk/qcew/year={year}/data.zip")
        .url("https://data.bls.gov/cew/{year}/data.zip")
        .comment("Annual QCEW data")
        .build();

    assertEquals("qcew_annual", config.getName());
    assertEquals("bulk/qcew/year={year}/data.zip", config.getCachePattern());
    assertEquals("https://data.bls.gov/cew/{year}/data.zip", config.getUrl());
    assertEquals("Annual QCEW data", config.getComment());
    assertTrue(config.getDimensions().isEmpty());
  }

  @Test void testBuilderWithDimensions() {
    DimensionConfig yearDim = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2023)
        .build();

    BulkDownloadConfig config = BulkDownloadConfig.builder()
        .name("data")
        .cachePattern("data/{year}/file.zip")
        .url("https://example.com/{year}/file.zip")
        .dimension("year", yearDim)
        .build();

    assertEquals(1, config.getDimensions().size());
    assertNotNull(config.getDimensions().get("year"));
  }

  @Test void testBuilderMissingNameThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        BulkDownloadConfig.builder()
            .cachePattern("pattern")
            .url("url")
            .build());
  }

  @Test void testBuilderEmptyNameThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        BulkDownloadConfig.builder()
            .name("")
            .cachePattern("pattern")
            .url("url")
            .build());
  }

  @Test void testBuilderMissingCachePatternThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        BulkDownloadConfig.builder()
            .name("test")
            .url("url")
            .build());
  }

  @Test void testBuilderMissingUrlThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        BulkDownloadConfig.builder()
            .name("test")
            .cachePattern("pattern")
            .build());
  }

  @Test void testResolveCachePath() {
    BulkDownloadConfig config = BulkDownloadConfig.builder()
        .name("data")
        .cachePattern("bulk/{year}/{frequency}_file.zip")
        .url("https://example.com")
        .build();

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    variables.put("frequency", "annual");

    String resolved = config.resolveCachePath(variables);
    assertEquals("bulk/2024/annual_file.zip", resolved);
  }

  @Test void testResolveUrl() {
    BulkDownloadConfig config = BulkDownloadConfig.builder()
        .name("data")
        .cachePattern("pattern")
        .url("https://api.example.com/{year}/data?format={format}")
        .build();

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    variables.put("format", "csv");

    String resolved = config.resolveUrl(variables);
    assertEquals("https://api.example.com/2024/data?format=csv", resolved);
  }

  @Test void testFromMapBasic() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("cachePattern", "data/{year}/file.zip");
    map.put("url", "https://example.com/{year}/file.zip");
    map.put("comment", "test data");

    BulkDownloadConfig config = BulkDownloadConfig.fromMap("test_data", map);
    assertNotNull(config);
    assertEquals("test_data", config.getName());
    assertEquals("data/{year}/file.zip", config.getCachePattern());
    assertEquals("https://example.com/{year}/file.zip", config.getUrl());
    assertEquals("test data", config.getComment());
  }

  @Test void testFromMapWithDimensions() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("cachePattern", "data/{year}/file.zip");
    map.put("url", "https://example.com/{year}/file.zip");

    Map<String, Object> dimensionsMap = new HashMap<String, Object>();
    Map<String, Object> yearDim = new HashMap<String, Object>();
    yearDim.put("type", "range");
    yearDim.put("start", 2020);
    yearDim.put("end", 2024);
    dimensionsMap.put("year", yearDim);
    map.put("dimensions", dimensionsMap);

    BulkDownloadConfig config = BulkDownloadConfig.fromMap("test", map);
    assertNotNull(config);
    assertEquals(1, config.getDimensions().size());
    assertNotNull(config.getDimensions().get("year"));
  }

  @Test void testFromMapWithLegacyVariables() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("cachePattern", "data/{year}/file.zip");
    map.put("url", "https://example.com/{year}/file.zip");
    map.put("variables", Arrays.asList("year", "frequency"));

    BulkDownloadConfig config = BulkDownloadConfig.fromMap("test", map);
    assertNotNull(config);
    assertEquals(2, config.getDimensions().size());
    assertNotNull(config.getDimensions().get("year"));
    assertNotNull(config.getDimensions().get("frequency"));
  }

  @Test void testDimensionsAreImmutable() {
    BulkDownloadConfig config = BulkDownloadConfig.builder()
        .name("test")
        .cachePattern("pattern")
        .url("url")
        .build();

    assertThrows(UnsupportedOperationException.class, () ->
        config.getDimensions().put("new", DimensionConfig.builder().name("new").build()));
  }

  @Test void testToString() {
    BulkDownloadConfig config = BulkDownloadConfig.builder()
        .name("test")
        .cachePattern("pattern/{year}")
        .url("https://example.com/{year}")
        .build();

    String str = config.toString();
    assertTrue(str.contains("test"));
    assertTrue(str.contains("pattern/{year}"));
  }
}
