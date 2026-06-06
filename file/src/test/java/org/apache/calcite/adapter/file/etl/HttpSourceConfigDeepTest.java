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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep tests for {@link HttpSourceConfig} inner configuration classes.
 *
 * <p>Covers BatchConfig, RowFilterConfig, AuthConfig, ResponseConfig,
 * PaginationConfig, RateLimitConfig, CacheConfig, RawCacheConfig,
 * ResponsePartitioningConfig, WideToNarrowConfig, DocumentSourceConfig,
 * EmbeddingConfig, and UrlRule.
 */
@Tag("unit")
class HttpSourceConfigDeepTest {

  // --- BatchConfig tests ---

  @Test void testBatchConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("field", "seriesid");
    map.put("source", "/catalog/bls-series.json");
    map.put("path", "series");
    map.put("size", 50);
    map.put("delayMs", 1000L);

    HttpSourceConfig.BatchConfig config = HttpSourceConfig.BatchConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("seriesid", config.getField());
    assertEquals("/catalog/bls-series.json", config.getSource());
    assertEquals("series", config.getPath());
    assertEquals(50, config.getSize());
    assertEquals(1000, config.getDelayMs());
    assertTrue(config.isEnabled());
  }

  @Test void testBatchConfigFromMapNull() {
    assertNull(HttpSourceConfig.BatchConfig.fromMap(null));
  }

  @Test void testBatchConfigFromMapDefaults() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("field", "id");
    map.put("source", "/data.json");

    HttpSourceConfig.BatchConfig config = HttpSourceConfig.BatchConfig.fromMap(map);

    assertEquals(50, config.getSize());
    assertEquals(0, config.getDelayMs());
    assertNull(config.getPath());
  }

  @Test void testBatchConfigNotEnabled() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "items");

    HttpSourceConfig.BatchConfig config = HttpSourceConfig.BatchConfig.fromMap(map);
    assertFalse(config.isEnabled());
  }

  @Test void testBatchConfigToString() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("field", "seriesid");
    map.put("source", "/catalog.json");
    map.put("path", "items");
    map.put("size", 25);
    map.put("delayMs", 500L);

    HttpSourceConfig.BatchConfig config = HttpSourceConfig.BatchConfig.fromMap(map);
    String str = config.toString();
    assertTrue(str.contains("seriesid"));
    assertTrue(str.contains("/catalog.json"));
    assertTrue(str.contains("25"));
  }

  @Test void testBatchConfigNegativeSize() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("field", "id");
    map.put("source", "/data.json");
    map.put("size", -1);

    HttpSourceConfig.BatchConfig config = HttpSourceConfig.BatchConfig.fromMap(map);
    assertEquals(50, config.getSize()); // Defaults to 50 for invalid values
  }

  // --- RowFilterConfig tests ---

  @Test void testRowFilterConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("column", "area_fips");
    map.put("pattern", "^C.*");
    map.put("maxRows", 100000);

    HttpSourceConfig.RowFilterConfig config = HttpSourceConfig.RowFilterConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("area_fips", config.getColumn());
    assertEquals("^C.*", config.getPattern());
    assertEquals(100000, config.getMaxRows());
    assertTrue(config.isEnabled());
  }

  @Test void testRowFilterConfigFromMapNull() {
    assertNull(HttpSourceConfig.RowFilterConfig.fromMap(null));
  }

  @Test void testRowFilterConfigNotEnabled() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("column", "area_fips");
    // Missing pattern

    HttpSourceConfig.RowFilterConfig config = HttpSourceConfig.RowFilterConfig.fromMap(map);
    assertFalse(config.isEnabled());
  }

  @Test void testRowFilterConfigDefaultMaxRows() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("column", "state");
    map.put("pattern", "CA");

    HttpSourceConfig.RowFilterConfig config = HttpSourceConfig.RowFilterConfig.fromMap(map);
    assertEquals(0, config.getMaxRows());
  }

  @Test void testRowFilterConfigToString() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("column", "state");
    map.put("pattern", "^A.*");
    map.put("maxRows", 500);

    HttpSourceConfig.RowFilterConfig config = HttpSourceConfig.RowFilterConfig.fromMap(map);
    String str = config.toString();
    assertTrue(str.contains("state"));
    assertTrue(str.contains("^A.*"));
    assertTrue(str.contains("500"));
  }

  // --- AuthConfig tests ---

  @Test void testAuthConfigNone() {
    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.none();
    assertEquals(HttpSourceConfig.AuthType.NONE, config.getType());
    assertNull(config.getLocation());
    assertNull(config.getName());
    assertNull(config.getValue());
    assertNull(config.getUsername());
    assertNull(config.getPassword());
  }

  @Test void testAuthConfigApiKey() {
    HttpSourceConfig.AuthConfig config =
        HttpSourceConfig.AuthConfig.apiKey(HttpSourceConfig.AuthLocation.HEADER, "X-Api-Key", "secret123");
    assertEquals(HttpSourceConfig.AuthType.API_KEY, config.getType());
    assertEquals(HttpSourceConfig.AuthLocation.HEADER, config.getLocation());
    assertEquals("X-Api-Key", config.getName());
    assertEquals("secret123", config.getValue());
  }

  @Test void testAuthConfigBasic() {
    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.basic("user", "pass");
    assertEquals(HttpSourceConfig.AuthType.BASIC, config.getType());
    assertEquals("user", config.getUsername());
    assertEquals("pass", config.getPassword());
  }

  @Test void testAuthConfigBearer() {
    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.bearer("mytoken");
    assertEquals(HttpSourceConfig.AuthType.BEARER, config.getType());
    assertEquals(HttpSourceConfig.AuthLocation.HEADER, config.getLocation());
    assertEquals("Authorization", config.getName());
    assertEquals("Bearer mytoken", config.getValue());
  }

  @Test void testAuthConfigFromMapNull() {
    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(null);
    assertEquals(HttpSourceConfig.AuthType.NONE, config.getType());
  }

  @Test void testAuthConfigFromMapNoType() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("username", "admin");

    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(map);
    assertEquals(HttpSourceConfig.AuthType.NONE, config.getType());
  }

  @Test void testAuthConfigFromMapApiKey() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "apiKey");
    map.put("location", "query");
    map.put("name", "api_key");
    map.put("value", "{env:API_KEY}");

    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(map);
    assertEquals(HttpSourceConfig.AuthType.API_KEY, config.getType());
    assertEquals(HttpSourceConfig.AuthLocation.QUERY, config.getLocation());
    assertEquals("api_key", config.getName());
    assertEquals("{env:API_KEY}", config.getValue());
  }

  @Test void testAuthConfigFromMapBasic() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "basic");
    map.put("username", "admin");
    map.put("password", "secret");

    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(map);
    assertEquals(HttpSourceConfig.AuthType.BASIC, config.getType());
    assertEquals("admin", config.getUsername());
    assertEquals("secret", config.getPassword());
  }

  @Test void testAuthConfigFromMapBearer() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "bearer");

    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(map);
    assertEquals(HttpSourceConfig.AuthType.BEARER, config.getType());
  }

  @Test void testAuthConfigFromMapOauth2() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "oauth2");

    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(map);
    assertEquals(HttpSourceConfig.AuthType.OAUTH2, config.getType());
  }

  @Test void testAuthConfigFromMapUnknownType() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "unknown_auth");

    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(map);
    assertEquals(HttpSourceConfig.AuthType.NONE, config.getType());
  }

  @Test void testAuthConfigFromMapDefaultLocation() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "apiKey");
    // No location specified - should default to HEADER

    HttpSourceConfig.AuthConfig config = HttpSourceConfig.AuthConfig.fromMap(map);
    assertEquals(HttpSourceConfig.AuthLocation.HEADER, config.getLocation());
  }

  // --- ResponseConfig tests ---

  @Test void testResponseConfigDefaults() {
    HttpSourceConfig.ResponseConfig config = HttpSourceConfig.ResponseConfig.defaults();
    assertEquals(HttpSourceConfig.ResponseFormat.JSON, config.getFormat());
    assertNull(config.getDataPath());
    assertNull(config.getErrorPath());
    assertNotNull(config.getPagination());
    assertTrue(config.isHasHeader());
    assertNull(config.getColumnNames());
    assertNull(config.getDelimiter());
  }

  @Test void testResponseConfigFromMapNull() {
    HttpSourceConfig.ResponseConfig config = HttpSourceConfig.ResponseConfig.fromMap(null);
    assertEquals(HttpSourceConfig.ResponseFormat.JSON, config.getFormat());
  }

  @Test void testResponseConfigFromMapFull() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "csv");
    map.put("dataPath", "$.results");
    map.put("errorPath", "$.error");
    map.put("hasHeader", false);
    map.put("columnNames", "COL1|COL2|COL3");
    map.put("delimiter", "|");

    HttpSourceConfig.ResponseConfig config = HttpSourceConfig.ResponseConfig.fromMap(map);
    assertEquals(HttpSourceConfig.ResponseFormat.CSV, config.getFormat());
    assertEquals("$.results", config.getDataPath());
    assertEquals("$.error", config.getErrorPath());
    assertFalse(config.isHasHeader());
    assertEquals("COL1|COL2|COL3", config.getColumnNames());
    assertEquals("|", config.getDelimiter());
  }

  @Test void testResponseConfigFromMapWithPagination() {
    Map<String, Object> paginationMap = new HashMap<String, Object>();
    paginationMap.put("type", "offset");
    paginationMap.put("limitParam", "limit");
    paginationMap.put("offsetParam", "offset");
    paginationMap.put("pageSize", 500);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "json");
    map.put("pagination", paginationMap);

    HttpSourceConfig.ResponseConfig config = HttpSourceConfig.ResponseConfig.fromMap(map);
    assertNotNull(config.getPagination());
    assertEquals(HttpSourceConfig.PaginationType.OFFSET, config.getPagination().getType());
    assertEquals("limit", config.getPagination().getLimitParam());
    assertEquals("offset", config.getPagination().getOffsetParam());
    assertEquals(500, config.getPagination().getPageSize());
  }

  @Test void testResponseConfigFromMapTsv() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "tsv");

    HttpSourceConfig.ResponseConfig config = HttpSourceConfig.ResponseConfig.fromMap(map);
    assertEquals(HttpSourceConfig.ResponseFormat.TSV, config.getFormat());
  }

  @Test void testResponseConfigFromMapXml() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "xml");

    HttpSourceConfig.ResponseConfig config = HttpSourceConfig.ResponseConfig.fromMap(map);
    assertEquals(HttpSourceConfig.ResponseFormat.XML, config.getFormat());
  }

  // --- PaginationConfig tests ---

  @Test void testPaginationConfigNone() {
    HttpSourceConfig.PaginationConfig config = HttpSourceConfig.PaginationConfig.none();
    assertEquals(HttpSourceConfig.PaginationType.NONE, config.getType());
    assertNull(config.getLimitParam());
    assertNull(config.getOffsetParam());
    assertNull(config.getCursorParam());
    assertNull(config.getPageParam());
    assertEquals(0, config.getPageSize());
  }

  @Test void testPaginationConfigOffset() {
    HttpSourceConfig.PaginationConfig config =
        HttpSourceConfig.PaginationConfig.offset("limit", "offset", 100);
    assertEquals(HttpSourceConfig.PaginationType.OFFSET, config.getType());
    assertEquals("limit", config.getLimitParam());
    assertEquals("offset", config.getOffsetParam());
    assertEquals(100, config.getPageSize());
  }

  @Test void testPaginationConfigFromMapNull() {
    HttpSourceConfig.PaginationConfig config = HttpSourceConfig.PaginationConfig.fromMap(null);
    assertEquals(HttpSourceConfig.PaginationType.NONE, config.getType());
  }

  @Test void testPaginationConfigFromMapCursor() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "cursor");
    map.put("cursorParam", "next_cursor");
    map.put("pageSize", 200);

    HttpSourceConfig.PaginationConfig config = HttpSourceConfig.PaginationConfig.fromMap(map);
    assertEquals(HttpSourceConfig.PaginationType.CURSOR, config.getType());
    assertEquals("next_cursor", config.getCursorParam());
    assertEquals(200, config.getPageSize());
  }

  @Test void testPaginationConfigFromMapPage() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "page");
    map.put("pageParam", "page");

    HttpSourceConfig.PaginationConfig config = HttpSourceConfig.PaginationConfig.fromMap(map);
    assertEquals(HttpSourceConfig.PaginationType.PAGE, config.getType());
    assertEquals("page", config.getPageParam());
    assertEquals(1000, config.getPageSize()); // Default
  }

  @Test void testPaginationConfigFromMapDefaultType() {
    Map<String, Object> map = new HashMap<String, Object>();

    HttpSourceConfig.PaginationConfig config = HttpSourceConfig.PaginationConfig.fromMap(map);
    assertEquals(HttpSourceConfig.PaginationType.NONE, config.getType());
  }

  // --- RateLimitConfig tests ---

  @Test void testRateLimitConfigDefaults() {
    HttpSourceConfig.RateLimitConfig config = HttpSourceConfig.RateLimitConfig.defaults();
    assertEquals(10, config.getRequestsPerSecond());
    assertEquals(2, config.getRetryOn().length);
    assertEquals(429, config.getRetryOn()[0]);
    assertEquals(503, config.getRetryOn()[1]);
    assertEquals(3, config.getMaxRetries());
    assertEquals(1000, config.getRetryBackoffMs());
  }

  @Test void testRateLimitConfigFromMapNull() {
    HttpSourceConfig.RateLimitConfig config = HttpSourceConfig.RateLimitConfig.fromMap(null);
    assertEquals(10, config.getRequestsPerSecond());
  }

  @Test void testRateLimitConfigFromMapFull() {
    List<Integer> retryOn = new ArrayList<Integer>();
    retryOn.add(429);
    retryOn.add(500);
    retryOn.add(502);
    retryOn.add(503);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("requestsPerSecond", 5);
    map.put("retryOn", retryOn);
    map.put("maxRetries", 5);
    map.put("retryBackoffMs", 2000L);

    HttpSourceConfig.RateLimitConfig config = HttpSourceConfig.RateLimitConfig.fromMap(map);
    assertEquals(5, config.getRequestsPerSecond());
    assertEquals(4, config.getRetryOn().length);
    assertEquals(429, config.getRetryOn()[0]);
    assertEquals(500, config.getRetryOn()[1]);
    assertEquals(502, config.getRetryOn()[2]);
    assertEquals(503, config.getRetryOn()[3]);
    assertEquals(5, config.getMaxRetries());
    assertEquals(2000, config.getRetryBackoffMs());
  }

  // --- CacheConfig tests ---

  @Test void testCacheConfigDefaults() {
    HttpSourceConfig.CacheConfig config = HttpSourceConfig.CacheConfig.defaults();
    assertFalse(config.isEnabled());
    assertEquals(86400, config.getTtlSeconds());
  }

  @Test void testCacheConfigFromMapNull() {
    HttpSourceConfig.CacheConfig config = HttpSourceConfig.CacheConfig.fromMap(null);
    assertFalse(config.isEnabled());
    assertEquals(86400, config.getTtlSeconds());
  }

  @Test void testCacheConfigFromMapEnabled() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", true);
    map.put("ttlSeconds", 3600L);

    HttpSourceConfig.CacheConfig config = HttpSourceConfig.CacheConfig.fromMap(map);
    assertTrue(config.isEnabled());
    assertEquals(3600, config.getTtlSeconds());
  }

  // --- RawCacheConfig tests ---

  @Test void testRawCacheConfigDefaults() {
    HttpSourceConfig.RawCacheConfig config = HttpSourceConfig.RawCacheConfig.defaults();
    assertFalse(config.isEnabled());
  }

  @Test void testRawCacheConfigEnabled() {
    HttpSourceConfig.RawCacheConfig config = HttpSourceConfig.RawCacheConfig.enabled();
    assertTrue(config.isEnabled());
  }

  @Test void testRawCacheConfigFromMapNull() {
    HttpSourceConfig.RawCacheConfig config = HttpSourceConfig.RawCacheConfig.fromMap(null);
    assertFalse(config.isEnabled());
  }

  @Test void testRawCacheConfigFromMapBoolean() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", true);

    HttpSourceConfig.RawCacheConfig config = HttpSourceConfig.RawCacheConfig.fromMap(map);
    assertTrue(config.isEnabled());
  }

  @Test void testRawCacheConfigFromMapString() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", "true");

    HttpSourceConfig.RawCacheConfig config = HttpSourceConfig.RawCacheConfig.fromMap(map);
    assertTrue(config.isEnabled());
  }

  @Test void testRawCacheConfigFromMapStringFalse() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", "false");

    HttpSourceConfig.RawCacheConfig config = HttpSourceConfig.RawCacheConfig.fromMap(map);
    assertFalse(config.isEnabled());
  }

  // --- ResponsePartitioningConfig tests ---

  @Test void testResponsePartitioningConfigFromMapNull() {
    assertNull(HttpSourceConfig.ResponsePartitioningConfig.fromMap(null));
  }

  @Test void testResponsePartitioningConfigFromMapEmptyFields() {
    Map<String, Object> map = new HashMap<String, Object>();
    // No "fields" key

    assertNull(HttpSourceConfig.ResponsePartitioningConfig.fromMap(map));
  }

  @Test void testResponsePartitioningConfigFromMapWithFields() {
    Map<String, Object> fieldsMap = new HashMap<String, Object>();
    fieldsMap.put("country_code", "countryiso3code");
    fieldsMap.put("year", "date");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("fields", fieldsMap);

    HttpSourceConfig.ResponsePartitioningConfig config =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertEquals(2, config.getFields().size());
    assertEquals("countryiso3code", config.getFields().get("country_code"));
    assertEquals("date", config.getFields().get("year"));
    assertFalse(config.hasYearFilter());
  }

  @Test void testResponsePartitioningConfigWithYearFilter() {
    Map<String, Object> fieldsMap = new HashMap<String, Object>();
    fieldsMap.put("country_code", "countryiso3code");

    Map<String, Object> yearFilterMap = new HashMap<String, Object>();
    yearFilterMap.put("field", "date");
    yearFilterMap.put("start", 2000);
    yearFilterMap.put("end", 2025);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("fields", fieldsMap);
    map.put("yearFilter", yearFilterMap);

    HttpSourceConfig.ResponsePartitioningConfig config =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.hasYearFilter());
    assertEquals("date", config.getYearField());
    assertEquals(2000, config.getYearStart());
    assertEquals(2025, config.getYearEnd());
  }

  @Test void testResponsePartitioningIsYearInRange() {
    Map<String, Object> fieldsMap = new HashMap<String, Object>();
    fieldsMap.put("year", "date");

    Map<String, Object> yearFilterMap = new HashMap<String, Object>();
    yearFilterMap.put("field", "date");
    yearFilterMap.put("start", 2000);
    yearFilterMap.put("end", 2020);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("fields", fieldsMap);
    map.put("yearFilter", yearFilterMap);

    HttpSourceConfig.ResponsePartitioningConfig config =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(map);

    assertTrue(config.isYearInRange(2010));
    assertTrue(config.isYearInRange(2000));
    assertTrue(config.isYearInRange(2020));
    assertFalse(config.isYearInRange(1999));
    assertFalse(config.isYearInRange(2021));
    assertTrue(config.isYearInRange(null)); // null returns true
    assertFalse(config.isYearInRange("abc")); // non-numeric returns false
    assertTrue(config.isYearInRange("2010")); // String parseable as int
  }

  @Test void testResponsePartitioningIsYearInRangeNoFilter() {
    Map<String, Object> fieldsMap = new HashMap<String, Object>();
    fieldsMap.put("country", "code");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("fields", fieldsMap);

    HttpSourceConfig.ResponsePartitioningConfig config =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(map);

    // No year filter = always in range
    assertTrue(config.isYearInRange(1900));
    assertTrue(config.isYearInRange(3000));
  }

  @Test void testResponsePartitioningFieldsImmutable() {
    Map<String, Object> fieldsMap = new HashMap<String, Object>();
    fieldsMap.put("country", "code");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("fields", fieldsMap);

    HttpSourceConfig.ResponsePartitioningConfig config =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(map);

    try {
      config.getFields().put("sneaky", "value");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testResponsePartitioningToString() {
    Map<String, Object> fieldsMap = new HashMap<String, Object>();
    fieldsMap.put("country", "code");

    Map<String, Object> yearFilterMap = new HashMap<String, Object>();
    yearFilterMap.put("field", "date");
    yearFilterMap.put("start", 2000);
    yearFilterMap.put("end", 2025);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("fields", fieldsMap);
    map.put("yearFilter", yearFilterMap);

    HttpSourceConfig.ResponsePartitioningConfig config =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(map);

    String str = config.toString();
    assertTrue(str.contains("country"));
    assertTrue(str.contains("yearFilter"));
    assertTrue(str.contains("date"));
    assertTrue(str.contains("2000"));
    assertTrue(str.contains("2025"));
  }

  // --- WideToNarrowConfig tests ---

  @Test void testWideToNarrowConfigFromMapNull() {
    assertNull(HttpSourceConfig.WideToNarrowConfig.fromMap(null));
  }

  @Test void testWideToNarrowConfigFromMapNoKeyColumns() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("valueColumnPattern", "^\\d{4}$");

    assertNull(HttpSourceConfig.WideToNarrowConfig.fromMap(map));
  }

  @Test void testWideToNarrowConfigFromMapEmptyKeyColumns() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", new ArrayList<String>());

    assertNull(HttpSourceConfig.WideToNarrowConfig.fromMap(map));
  }

  @Test void testWideToNarrowConfigFromMapFull() {
    List<String> keyColumns = Arrays.asList("GeoFIPS", "GeoName", "Unit");
    List<String> skipValues = Arrays.asList("(NA)", "(D)", "");

    Map<String, Object> columnMapping = new HashMap<String, Object>();
    columnMapping.put("GeoFIPS", "geo_fips");
    columnMapping.put("GeoName", "geo_name");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", keyColumns);
    map.put("valueColumnPattern", "^\\d{4}$");
    map.put("keyColumnName", "Year");
    map.put("valueColumnName", "DataValue");
    map.put("skipValues", skipValues);
    map.put("columnMapping", columnMapping);

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertEquals(3, config.getKeyColumns().size());
    assertEquals("GeoFIPS", config.getKeyColumns().get(0));
    assertEquals("^\\d{4}$", config.getValueColumnPattern());
    assertEquals("Year", config.getKeyColumnName());
    assertEquals("DataValue", config.getValueColumnName());
    assertEquals(3, config.getSkipValues().size());
    assertTrue(config.getSkipValues().contains("(NA)"));
    assertTrue(config.getSkipValues().contains("(D)"));
    assertTrue(config.hasColumnMapping());
    assertEquals("geo_fips", config.getOutputColumnName("GeoFIPS"));
    assertEquals("geo_name", config.getOutputColumnName("GeoName"));
    assertEquals("Unit", config.getOutputColumnName("Unit")); // No mapping, returns source
  }

  @Test void testWideToNarrowConfigDefaults() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("Col1"));

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertEquals("Key", config.getKeyColumnName());
    assertEquals("Value", config.getValueColumnName());
    assertNull(config.getValueColumnPattern());
    assertTrue(config.getSkipValues().isEmpty());
    assertFalse(config.hasColumnMapping());
  }

  @Test void testWideToNarrowShouldSkipValue() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("Col1"));
    map.put("skipValues", Arrays.asList("(NA)", "(D)"));

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertTrue(config.shouldSkipValue(null));
    assertTrue(config.shouldSkipValue(""));
    assertTrue(config.shouldSkipValue("(NA)"));
    assertTrue(config.shouldSkipValue("(D)"));
    assertFalse(config.shouldSkipValue("12345.0"));
  }

  @Test void testWideToNarrowIsValueColumn() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("GeoFIPS", "GeoName"));
    map.put("valueColumnPattern", "^\\d{4}$");

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertTrue(config.isValueColumn("2020"));
    assertTrue(config.isValueColumn("1999"));
    assertFalse(config.isValueColumn("GeoFIPS"));
    assertFalse(config.isValueColumn("Name"));
  }

  @Test void testWideToNarrowIsValueColumnNoPattern() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("GeoFIPS", "GeoName"));

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    // No pattern = all non-key columns are value columns
    assertFalse(config.isValueColumn("GeoFIPS"));
    assertFalse(config.isValueColumn("GeoName"));
    assertTrue(config.isValueColumn("2020"));
    assertTrue(config.isValueColumn("AnyOtherCol"));
  }

  @Test void testWideToNarrowKeyColumnsImmutable() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("Col1"));

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    try {
      config.getKeyColumns().add("sneaky");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testWideToNarrowToString() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("GeoFIPS"));
    map.put("keyColumnName", "Year");
    map.put("valueColumnName", "Value");

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    String str = config.toString();
    assertTrue(str.contains("GeoFIPS"));
    assertTrue(str.contains("Year"));
    assertTrue(str.contains("Value"));
  }

  // --- DocumentSourceConfig tests ---

  @Test void testDocumentSourceConfigFromMapNull() {
    assertNull(HttpSourceConfig.DocumentSourceConfig.fromMap(null));
  }

  @Test void testDocumentSourceConfigFromMapFull() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("metadataUrl", "https://data.sec.gov/submissions/CIK{cik}.json");
    map.put("documentUrl", "https://www.sec.gov/Archives/{cik}/{accession}");
    map.put("extractionType", "xbrl_facts");
    map.put("documentConverter", "com.example.XbrlConverter");
    map.put("responseTransformer", "com.example.EdgarTransformer");
    map.put("documentTypes", Arrays.asList("*.xml", "*.htm"));
    map.put("extractionStrategies", Arrays.asList("regex_item7", "direct_search"));
    map.put("itemFilter", Arrays.asList("2.02", "7.01"));
    map.put("startYear", 2015);
    map.put("endYear", 2025);

    HttpSourceConfig.DocumentSourceConfig config =
        HttpSourceConfig.DocumentSourceConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertEquals("https://data.sec.gov/submissions/CIK{cik}.json", config.getMetadataUrl());
    assertEquals("https://www.sec.gov/Archives/{cik}/{accession}", config.getDocumentUrl());
    assertEquals("xbrl_facts", config.getExtractionType());
    assertEquals("com.example.XbrlConverter", config.getDocumentConverter());
    assertEquals("com.example.EdgarTransformer", config.getResponseTransformer());
    assertEquals(2, config.getDocumentTypes().size());
    assertEquals("*.xml", config.getDocumentTypes().get(0));
    assertEquals(2, config.getExtractionStrategies().size());
    assertEquals("regex_item7", config.getExtractionStrategies().get(0));
    assertEquals(2, config.getItemFilter().size());
    assertEquals("2.02", config.getItemFilter().get(0));
    assertEquals(Integer.valueOf(2015), config.getStartYear());
    assertEquals(Integer.valueOf(2025), config.getEndYear());
  }

  @Test void testDocumentSourceConfigNotEnabled() {
    Map<String, Object> map = new HashMap<String, Object>();
    // No metadataUrl or documentUrl

    HttpSourceConfig.DocumentSourceConfig config =
        HttpSourceConfig.DocumentSourceConfig.fromMap(map);

    assertNotNull(config);
    assertFalse(config.isEnabled());
  }

  @Test void testDocumentSourceConfigMinimal() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("metadataUrl", "https://api.example.com/meta");

    HttpSourceConfig.DocumentSourceConfig config =
        HttpSourceConfig.DocumentSourceConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertNull(config.getDocumentUrl());
    assertNull(config.getExtractionType());
    assertNull(config.getDocumentConverter());
    assertNull(config.getResponseTransformer());
    assertTrue(config.getDocumentTypes().isEmpty());
    assertTrue(config.getExtractionStrategies().isEmpty());
    assertTrue(config.getItemFilter().isEmpty());
    assertNull(config.getEmbeddingConfig());
    assertNull(config.getStartYear());
    assertNull(config.getEndYear());
  }

  @Test void testDocumentSourceConfigWithEmbedding() {
    Map<String, Object> embeddingMap = new HashMap<String, Object>();
    embeddingMap.put("provider", "openai");
    embeddingMap.put("model", "text-embedding-3-small");
    embeddingMap.put("dimension", 512);
    embeddingMap.put("maxTextLength", 4096);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("metadataUrl", "https://api.example.com/meta");
    map.put("embeddingConfig", embeddingMap);

    HttpSourceConfig.DocumentSourceConfig config =
        HttpSourceConfig.DocumentSourceConfig.fromMap(map);

    assertNotNull(config.getEmbeddingConfig());
    assertEquals("openai", config.getEmbeddingConfig().getProvider());
    assertEquals("text-embedding-3-small", config.getEmbeddingConfig().getModel());
    assertEquals(512, config.getEmbeddingConfig().getDimension());
    assertEquals(4096, config.getEmbeddingConfig().getMaxTextLength());
  }

  @Test void testDocumentSourceConfigToString() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("metadataUrl", "https://api.example.com/meta");
    map.put("documentUrl", "https://api.example.com/doc");
    map.put("extractionType", "xbrl");
    map.put("documentTypes", Arrays.asList("*.xml"));

    HttpSourceConfig.DocumentSourceConfig config =
        HttpSourceConfig.DocumentSourceConfig.fromMap(map);

    String str = config.toString();
    assertTrue(str.contains("metadataUrl"));
    assertTrue(str.contains("documentUrl"));
    assertTrue(str.contains("xbrl"));
  }

  // --- EmbeddingConfig tests ---

  @Test void testEmbeddingConfigFromMapNull() {
    assertNull(HttpSourceConfig.EmbeddingConfig.fromMap(null));
  }

  @Test void testEmbeddingConfigDefaults() {
    Map<String, Object> map = new HashMap<String, Object>();

    HttpSourceConfig.EmbeddingConfig config = HttpSourceConfig.EmbeddingConfig.fromMap(map);

    assertEquals("duckdb_quackformers", config.getProvider());
    assertEquals("sentence-transformers/all-MiniLM-L6-v2", config.getModel());
    assertEquals(256, config.getDimension());
    assertEquals(8192, config.getMaxTextLength());
  }

  @Test void testEmbeddingConfigCustom() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("provider", "openai");
    map.put("model", "text-embedding-3-large");
    map.put("dimension", 1024);
    map.put("maxTextLength", 16384);

    HttpSourceConfig.EmbeddingConfig config = HttpSourceConfig.EmbeddingConfig.fromMap(map);

    assertEquals("openai", config.getProvider());
    assertEquals("text-embedding-3-large", config.getModel());
    assertEquals(1024, config.getDimension());
    assertEquals(16384, config.getMaxTextLength());
  }

  // --- UrlRule tests ---

  @Test void testUrlRuleConstructor() {
    HttpSourceConfig.UrlRule rule =
        new HttpSourceConfig.UrlRule(2000, 2019, "https://api.example.com/v1");
    assertEquals(2000, rule.getYearMin());
    assertEquals(2019, rule.getYearMax());
    assertEquals("https://api.example.com/v1", rule.getUrl());
  }

  @Test void testUrlRuleFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("yearRange", Arrays.asList(2000, 2019));
    map.put("url", "https://api.example.com/v1");

    HttpSourceConfig.UrlRule rule = HttpSourceConfig.UrlRule.fromMap(map);

    assertNotNull(rule);
    assertEquals(2000, rule.getYearMin());
    assertEquals(2019, rule.getYearMax());
    assertEquals("https://api.example.com/v1", rule.getUrl());
  }

  @Test void testUrlRuleFromMapMissingUrl() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("yearRange", Arrays.asList(2000, 2019));

    HttpSourceConfig.UrlRule rule = HttpSourceConfig.UrlRule.fromMap(map);
    assertNull(rule);
  }

  @Test void testUrlRuleFromMapInvalidRange() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("yearRange", Arrays.asList(2000)); // Only one element
    map.put("url", "https://api.example.com");

    HttpSourceConfig.UrlRule rule = HttpSourceConfig.UrlRule.fromMap(map);
    assertNull(rule);
  }

  @Test void testUrlRuleFromMapNoRange() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com");

    HttpSourceConfig.UrlRule rule = HttpSourceConfig.UrlRule.fromMap(map);
    assertNull(rule);
  }

  // --- Effective URL with URL rules ---

  @Test void testGetEffectiveUrlWithRules() {
    List<HttpSourceConfig.UrlRule> rules = new ArrayList<HttpSourceConfig.UrlRule>();
    rules.add(new HttpSourceConfig.UrlRule(2000, 2019, "https://api.old.com"));
    rules.add(new HttpSourceConfig.UrlRule(2020, 2030, "https://api.new.com"));

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.default.com")
        .urlRules(rules)
        .build();

    Map<String, String> vars2015 = new HashMap<String, String>();
    vars2015.put("year", "2015");
    assertEquals("https://api.old.com", config.getEffectiveUrl(vars2015));

    Map<String, String> vars2025 = new HashMap<String, String>();
    vars2025.put("year", "2025");
    assertEquals("https://api.new.com", config.getEffectiveUrl(vars2025));

    Map<String, String> vars1990 = new HashMap<String, String>();
    vars1990.put("year", "1990");
    assertEquals("https://api.default.com", config.getEffectiveUrl(vars1990));
  }

  // --- Full fromMap pipeline test ---

  @SuppressWarnings("unchecked")
  @Test void testFromMapFullPipeline() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("url", "https://api.bls.gov/publicAPI/v2/timeseries/data/");
    map.put("method", "POST");
    map.put("parallel", 4);
    map.put("type", "http");

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("apiKey", "{env:BLS_API_KEY}");
    map.put("parameters", params);

    Map<String, Object> headers = new HashMap<String, Object>();
    headers.put("Content-Type", "application/json");
    map.put("headers", headers);

    Map<String, Object> body = new HashMap<String, Object>();
    body.put("seriesid", "{seriesList}");
    body.put("startyear", "{year}");
    map.put("body", body);

    map.put("bodyFormat", "json");

    Map<String, Object> batching = new HashMap<String, Object>();
    batching.put("field", "seriesid");
    batching.put("source", "/catalog/bls.json");
    batching.put("path", "series");
    batching.put("size", 50);
    map.put("batching", batching);

    Map<String, Object> auth = new HashMap<String, Object>();
    auth.put("type", "apiKey");
    auth.put("location", "query");
    auth.put("name", "registrationkey");
    auth.put("value", "{env:BLS_API_KEY}");
    map.put("auth", auth);

    Map<String, Object> response = new HashMap<String, Object>();
    response.put("format", "json");
    response.put("dataPath", "Results.series");
    response.put("errorPath", "Results.error");
    map.put("response", response);

    Map<String, Object> rateLimit = new HashMap<String, Object>();
    rateLimit.put("requestsPerSecond", 5);
    rateLimit.put("maxRetries", 3);
    map.put("rateLimit", rateLimit);

    Map<String, Object> cache = new HashMap<String, Object>();
    cache.put("enabled", true);
    cache.put("ttlSeconds", 3600L);
    map.put("cache", cache);

    Map<String, Object> rawCache = new HashMap<String, Object>();
    rawCache.put("enabled", true);
    map.put("rawCache", rawCache);

    // Parse the full config
    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("https://api.bls.gov/publicAPI/v2/timeseries/data/", config.getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.POST, config.getMethod());
    assertEquals(4, config.getParallel());
    assertEquals("http", config.getSourceType());

    // Parameters
    assertEquals("{env:BLS_API_KEY}", config.getParameters().get("apiKey"));

    // Headers
    assertEquals("application/json", config.getHeaders().get("Content-Type"));

    // Body
    assertTrue(config.hasBody());
    assertEquals("{seriesList}", config.getBody().get("seriesid"));
    assertEquals(HttpSourceConfig.BodyFormat.JSON, config.getBodyFormat());

    // Batching
    assertTrue(config.hasBatching());
    assertEquals("seriesid", config.getBatching().getField());
    assertEquals(50, config.getBatching().getSize());

    // Auth
    assertEquals(HttpSourceConfig.AuthType.API_KEY, config.getAuth().getType());

    // Response
    assertEquals(HttpSourceConfig.ResponseFormat.JSON, config.getResponse().getFormat());
    assertEquals("Results.series", config.getResponse().getDataPath());
    assertEquals("Results.error", config.getResponse().getErrorPath());

    // Rate limit
    assertEquals(5, config.getRateLimit().getRequestsPerSecond());

    // Cache
    assertTrue(config.getCache().isEnabled());
    assertEquals(3600, config.getCache().getTtlSeconds());

    // Raw cache
    assertTrue(config.getRawCache().isEnabled());
  }

  @Test void testFromMapWithDocumentSourceType() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "document");
    map.put("metadataUrl", "https://data.sec.gov/submissions/CIK{cik}.json");
    map.put("documentUrl", "https://www.sec.gov/Archives/{cik}/{accession}");
    map.put("documentConverter", "com.example.XbrlConverter");

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("document", config.getSourceType());
    assertTrue(config.isDocumentSource());
    assertNotNull(config.getDocumentSource());
    assertTrue(config.getDocumentSource().isEnabled());
  }

  @Test void testFromMapWithNestedDocumentSource() {
    Map<String, Object> docSourceMap = new HashMap<String, Object>();
    docSourceMap.put("metadataUrl", "https://api.example.com/meta");
    docSourceMap.put("documentUrl", "https://api.example.com/doc");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com");
    map.put("documentSource", docSourceMap);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertNotNull(config.getDocumentSource());
    assertTrue(config.getDocumentSource().isEnabled());
  }

  @Test void testFromMapWithUrlRules() {
    List<Map<String, Object>> urlRules = new ArrayList<Map<String, Object>>();

    Map<String, Object> rule1 = new HashMap<String, Object>();
    rule1.put("yearRange", Arrays.asList(2000, 2019));
    rule1.put("url", "https://old.api.com");
    urlRules.add(rule1);

    Map<String, Object> rule2 = new HashMap<String, Object>();
    rule2.put("yearRange", Arrays.asList(2020, 2030));
    rule2.put("url", "https://new.api.com");
    urlRules.add(rule2);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://default.api.com");
    map.put("urlRules", urlRules);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertEquals(2, config.getUrlRules().size());
    assertEquals("https://old.api.com", config.getUrlRules().get(0).getUrl());
  }

  @Test void testFromMapWithRowFilter() {
    Map<String, Object> rowFilterMap = new HashMap<String, Object>();
    rowFilterMap.put("column", "state_fips");
    rowFilterMap.put("pattern", "^06$");
    rowFilterMap.put("maxRows", 50000);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com");
    map.put("rowFilter", rowFilterMap);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.hasRowFilter());
    assertEquals("state_fips", config.getRowFilter().getColumn());
    assertEquals("^06$", config.getRowFilter().getPattern());
    assertEquals(50000, config.getRowFilter().getMaxRows());
  }

  @Test void testFromMapWithResponsePartitioning() {
    Map<String, Object> fieldsMap = new HashMap<String, Object>();
    fieldsMap.put("country", "countryiso3code");

    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", fieldsMap);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com");
    map.put("responsePartitioning", rpMap);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.hasResponsePartitioning());
  }

  @Test void testFromMapWithWideToNarrow() {
    Map<String, Object> wtnMap = new HashMap<String, Object>();
    wtnMap.put("keyColumns", Arrays.asList("GeoFIPS", "Unit"));
    wtnMap.put("keyColumnName", "Year");
    wtnMap.put("valueColumnName", "DataValue");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com");
    map.put("wideToNarrow", wtnMap);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.hasWideToNarrow());
    assertEquals("Year", config.getWideToNarrow().getKeyColumnName());
  }

  @Test void testFromMapWithExtractPattern() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("bulkDownload", "census_shapes");
    map.put("extractPattern", "*.shp");

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("census_shapes", config.getBulkDownload());
    assertEquals("*.shp", config.getExtractPattern());
    assertTrue(config.isBulkDownloadSource());
  }

  // --- Builder validation tests ---

  @Test void testBuilderRequiresUrlOrBulkDownloadOrDocumentSource() {
    assertThrows(IllegalArgumentException.class, () ->
        HttpSourceConfig.builder().build());
  }

  @Test void testBuilderWithAllSubConfigs() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com")
        .method(HttpSourceConfig.HttpMethod.PUT)
        .auth(HttpSourceConfig.AuthConfig.bearer("token123"))
        .response(HttpSourceConfig.ResponseConfig.defaults())
        .rateLimit(HttpSourceConfig.RateLimitConfig.defaults())
        .cache(HttpSourceConfig.CacheConfig.defaults())
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .parallel(8)
        .sourceType("http")
        .build();

    assertNotNull(config);
    assertEquals(HttpSourceConfig.HttpMethod.PUT, config.getMethod());
    assertEquals(HttpSourceConfig.AuthType.BEARER, config.getAuth().getType());
    assertEquals(8, config.getParallel());
    assertTrue(config.getRawCache().isEnabled());
  }

  // --- hasXxx convenience methods ---

  @Test void testHasMethodsWithNoConfig() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com")
        .build();

    assertFalse(config.hasBatching());
    assertFalse(config.hasBody());
    assertFalse(config.hasRowFilter());
    assertFalse(config.hasResponsePartitioning());
    assertFalse(config.hasWideToNarrow());
    assertFalse(config.isDocumentSource());
    assertFalse(config.isBulkDownloadSource());
    assertNull(config.getRowFilter());
    assertNull(config.getResponsePartitioning());
    assertNull(config.getWideToNarrow());
    assertNull(config.getDocumentSource());
    assertNull(config.getExtractPattern());
  }
}
