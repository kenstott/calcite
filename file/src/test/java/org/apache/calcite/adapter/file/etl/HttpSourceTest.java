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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HttpSource and HttpSourceConfig.
 */
@Tag("unit")
public class HttpSourceTest {

  @Test void testHttpSourceConfigBuilder() {
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("apiKey", "{env:API_KEY}");
    params.put("year", "{year}");

    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Accept", "application/json");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .method(HttpSourceConfig.HttpMethod.GET)
        .parameters(params)
        .headers(headers)
        .build();

    assertEquals("https://api.example.com/data", config.getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.GET, config.getMethod());
    assertEquals(2, config.getParameters().size());
    assertEquals("{env:API_KEY}", config.getParameters().get("apiKey"));
    assertEquals(1, config.getHeaders().size());
  }

  @Test void testHttpSourceConfigDefaults() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    assertEquals(HttpSourceConfig.HttpMethod.GET, config.getMethod());
    assertTrue(config.getParameters().isEmpty());
    assertTrue(config.getHeaders().isEmpty());
    assertEquals(HttpSourceConfig.AuthType.NONE, config.getAuth().getType());
    assertEquals(HttpSourceConfig.ResponseFormat.JSON, config.getResponse().getFormat());
    assertEquals(10, config.getRateLimit().getRequestsPerSecond());
    assertFalse(config.getCache().isEnabled());
  }

  @Test void testHttpSourceConfigRequiresUrl() {
    assertThrows(IllegalArgumentException.class, () -> {
      HttpSourceConfig.builder().build();
    });
  }

  @Test void testHttpSourceConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com/data");
    map.put("method", "POST");

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("year", "2024");
    map.put("parameters", params);

    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Content-Type", "application/json");
    map.put("headers", headers);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertEquals("https://api.example.com/data", config.getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.POST, config.getMethod());
    assertEquals("2024", config.getParameters().get("year"));
    assertEquals("application/json", config.getHeaders().get("Content-Type"));
  }

  @Test void testAuthConfigApiKey() {
    HttpSourceConfig.AuthConfig auth =
        HttpSourceConfig.AuthConfig.apiKey(HttpSourceConfig.AuthLocation.HEADER,
        "X-Api-Key",
        "{env:API_KEY}");

    assertEquals(HttpSourceConfig.AuthType.API_KEY, auth.getType());
    assertEquals(HttpSourceConfig.AuthLocation.HEADER, auth.getLocation());
    assertEquals("X-Api-Key", auth.getName());
    assertEquals("{env:API_KEY}", auth.getValue());
  }

  @Test void testAuthConfigBasic() {
    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.basic("user", "pass");

    assertEquals(HttpSourceConfig.AuthType.BASIC, auth.getType());
    assertEquals("user", auth.getUsername());
    assertEquals("pass", auth.getPassword());
  }

  @Test void testAuthConfigBearer() {
    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.bearer("token123");

    assertEquals(HttpSourceConfig.AuthType.BEARER, auth.getType());
    assertEquals("Authorization", auth.getName());
    assertEquals("Bearer token123", auth.getValue());
  }

  @Test void testAuthConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "apiKey");
    map.put("location", "header");
    map.put("name", "X-Api-Key");
    map.put("value", "secret");

    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.fromMap(map);

    assertEquals(HttpSourceConfig.AuthType.API_KEY, auth.getType());
    assertEquals(HttpSourceConfig.AuthLocation.HEADER, auth.getLocation());
    assertEquals("X-Api-Key", auth.getName());
    assertEquals("secret", auth.getValue());
  }

  @Test void testResponseConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "json");
    map.put("dataPath", "$.results.data");

    Map<String, Object> paginationMap = new HashMap<String, Object>();
    paginationMap.put("type", "offset");
    paginationMap.put("limitParam", "limit");
    paginationMap.put("offsetParam", "offset");
    paginationMap.put("pageSize", 500);
    map.put("pagination", paginationMap);

    HttpSourceConfig.ResponseConfig response = HttpSourceConfig.ResponseConfig.fromMap(map);

    assertEquals(HttpSourceConfig.ResponseFormat.JSON, response.getFormat());
    assertEquals("$.results.data", response.getDataPath());
    assertEquals(HttpSourceConfig.PaginationType.OFFSET, response.getPagination().getType());
    assertEquals("limit", response.getPagination().getLimitParam());
    assertEquals("offset", response.getPagination().getOffsetParam());
    assertEquals(500, response.getPagination().getPageSize());
  }

  @Test void testPaginationConfigOffset() {
    HttpSourceConfig.PaginationConfig pagination =
        HttpSourceConfig.PaginationConfig.offset("limit", "offset", 1000);

    assertEquals(HttpSourceConfig.PaginationType.OFFSET, pagination.getType());
    assertEquals("limit", pagination.getLimitParam());
    assertEquals("offset", pagination.getOffsetParam());
    assertEquals(1000, pagination.getPageSize());
  }

  @Test void testRateLimitConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("requestsPerSecond", 5);
    map.put("retryOn", Arrays.asList(429, 500, 503));
    map.put("maxRetries", 5);
    map.put("retryBackoffMs", 2000);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.fromMap(map);

    assertEquals(5, rateLimit.getRequestsPerSecond());
    assertEquals(3, rateLimit.getRetryOn().length);
    assertEquals(429, rateLimit.getRetryOn()[0]);
    assertEquals(5, rateLimit.getMaxRetries());
    assertEquals(2000, rateLimit.getRetryBackoffMs());
  }

  @Test void testRateLimitConfigDefaults() {
    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();

    assertEquals(10, rateLimit.getRequestsPerSecond());
    assertEquals(2, rateLimit.getRetryOn().length);
    assertEquals(429, rateLimit.getRetryOn()[0]);
    assertEquals(503, rateLimit.getRetryOn()[1]);
    assertEquals(3, rateLimit.getMaxRetries());
    assertEquals(1000, rateLimit.getRetryBackoffMs());
  }

  @Test void testCacheConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", true);
    map.put("ttlSeconds", 3600);

    HttpSourceConfig.CacheConfig cache = HttpSourceConfig.CacheConfig.fromMap(map);

    assertTrue(cache.isEnabled());
    assertEquals(3600, cache.getTtlSeconds());
  }

  @Test void testCacheConfigDefaults() {
    HttpSourceConfig.CacheConfig cache = HttpSourceConfig.CacheConfig.defaults();

    assertFalse(cache.isEnabled());
    assertEquals(86400, cache.getTtlSeconds());
  }

  @Test void testHttpSourceType() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
  }

  @Test void testHttpSourceConfigFromMapComplete() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com/v1/data");
    map.put("method", "GET");

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("year", "{year}");
    params.put("region", "{region}");
    map.put("parameters", params);

    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Accept", "application/json");
    headers.put("User-Agent", "CalciteETL/1.0");
    map.put("headers", headers);

    Map<String, Object> authMap = new HashMap<String, Object>();
    authMap.put("type", "bearer");
    authMap.put("value", "{env:AUTH_TOKEN}");
    map.put("auth", authMap);

    Map<String, Object> responseMap = new HashMap<String, Object>();
    responseMap.put("format", "json");
    responseMap.put("dataPath", "$.data.records");
    Map<String, Object> paginationMap = new HashMap<String, Object>();
    paginationMap.put("type", "page");
    paginationMap.put("pageParam", "page");
    paginationMap.put("pageSize", 100);
    responseMap.put("pagination", paginationMap);
    map.put("response", responseMap);

    Map<String, Object> rateLimitMap = new HashMap<String, Object>();
    rateLimitMap.put("requestsPerSecond", 2);
    rateLimitMap.put("maxRetries", 5);
    map.put("rateLimit", rateLimitMap);

    Map<String, Object> cacheMap = new HashMap<String, Object>();
    cacheMap.put("enabled", true);
    cacheMap.put("ttlSeconds", 7200);
    map.put("cache", cacheMap);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertEquals("https://api.example.com/v1/data", config.getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.GET, config.getMethod());
    assertEquals(2, config.getParameters().size());
    assertEquals(2, config.getHeaders().size());
    assertEquals(HttpSourceConfig.AuthType.BEARER, config.getAuth().getType());
    assertEquals(HttpSourceConfig.ResponseFormat.JSON, config.getResponse().getFormat());
    assertEquals("$.data.records", config.getResponse().getDataPath());
    assertEquals(HttpSourceConfig.PaginationType.PAGE, config.getResponse().getPagination().getType());
    assertEquals(100, config.getResponse().getPagination().getPageSize());
    assertEquals(2, config.getRateLimit().getRequestsPerSecond());
    assertEquals(5, config.getRateLimit().getMaxRetries());
    assertTrue(config.getCache().isEnabled());
    assertEquals(7200, config.getCache().getTtlSeconds());
  }

  @Test void testHttpSourceClose() {
    Map<String, Object> cacheMap = new HashMap<String, Object>();
    cacheMap.put("enabled", true);
    cacheMap.put("ttlSeconds", 3600);

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .cache(HttpSourceConfig.CacheConfig.fromMap(cacheMap))
        .build();

    HttpSource source = new HttpSource(config);
    // Should not throw
    source.close();
  }

  @Test void testWideToNarrowConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("GeoFIPS", "GeoName", "TableName", "LineCode"));
    map.put("valueColumnPattern", "^\\d{4}$");
    map.put("keyColumnName", "Year");
    map.put("valueColumnName", "DataValue");
    map.put("skipValues", Arrays.asList("(NA)", "(D)", ""));

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertEquals(4, config.getKeyColumns().size());
    assertEquals("GeoFIPS", config.getKeyColumns().get(0));
    assertEquals("^\\d{4}$", config.getValueColumnPattern());
    assertEquals("Year", config.getKeyColumnName());
    assertEquals("DataValue", config.getValueColumnName());
    assertTrue(config.isEnabled());

    // Test isValueColumn
    assertTrue(config.isValueColumn("2020"));
    assertTrue(config.isValueColumn("1929"));
    assertFalse(config.isValueColumn("GeoFIPS"));
    assertFalse(config.isValueColumn("Description"));

    // Test shouldSkipValue
    assertTrue(config.shouldSkipValue(""));
    assertTrue(config.shouldSkipValue("(NA)"));
    assertTrue(config.shouldSkipValue("(D)"));
    assertTrue(config.shouldSkipValue(null));
    assertFalse(config.shouldSkipValue("12345.0"));
    assertFalse(config.shouldSkipValue("100"));
  }

  @Test void testWideToNarrowConfigDefaults() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("GeoFIPS"));

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertEquals("Key", config.getKeyColumnName());
    assertEquals("Value", config.getValueColumnName());
    assertTrue(config.getSkipValues().isEmpty());
    assertTrue(config.isEnabled());
  }

  @Test void testWideToNarrowConfigRequiresKeyColumns() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("valueColumnPattern", "^\\d{4}$");

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    // Should return null when keyColumns is missing
    assertEquals(null, config);
  }

  // --- New tests for expanded code path coverage ---

  @Test void testPaginationConfigCursorBased() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "cursor");
    map.put("cursorParam", "next_cursor");
    map.put("pageSize", 200);

    HttpSourceConfig.PaginationConfig pagination =
        HttpSourceConfig.PaginationConfig.fromMap(map);

    assertEquals(HttpSourceConfig.PaginationType.CURSOR, pagination.getType());
    assertEquals("next_cursor", pagination.getCursorParam());
    assertEquals(200, pagination.getPageSize());
    assertNull(pagination.getLimitParam());
    assertNull(pagination.getOffsetParam());
    assertNull(pagination.getPageParam());
  }

  @Test void testPaginationConfigPageBased() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "page");
    map.put("pageParam", "page");
    map.put("pageSize", 50);

    HttpSourceConfig.PaginationConfig pagination =
        HttpSourceConfig.PaginationConfig.fromMap(map);

    assertEquals(HttpSourceConfig.PaginationType.PAGE, pagination.getType());
    assertEquals("page", pagination.getPageParam());
    assertEquals(50, pagination.getPageSize());
    assertNull(pagination.getLimitParam());
    assertNull(pagination.getOffsetParam());
    assertNull(pagination.getCursorParam());
  }

  @Test void testPaginationConfigFromMapNone() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "none");

    HttpSourceConfig.PaginationConfig pagination =
        HttpSourceConfig.PaginationConfig.fromMap(map);

    assertEquals(HttpSourceConfig.PaginationType.NONE, pagination.getType());
  }

  @Test void testPaginationConfigFromMapNull() {
    HttpSourceConfig.PaginationConfig pagination =
        HttpSourceConfig.PaginationConfig.fromMap(null);

    assertEquals(HttpSourceConfig.PaginationType.NONE, pagination.getType());
  }

  @Test void testPaginationConfigFromMapDefaultPageSize() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "offset");
    map.put("limitParam", "limit");
    map.put("offsetParam", "offset");
    // No pageSize specified - should default to 1000

    HttpSourceConfig.PaginationConfig pagination =
        HttpSourceConfig.PaginationConfig.fromMap(map);

    assertEquals(HttpSourceConfig.PaginationType.OFFSET, pagination.getType());
    assertEquals(1000, pagination.getPageSize());
  }

  @Test void testAuthConfigFromMapBasicAuth() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "basic");
    map.put("username", "admin");
    map.put("password", "secret123");

    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.fromMap(map);

    assertEquals(HttpSourceConfig.AuthType.BASIC, auth.getType());
    assertEquals("admin", auth.getUsername());
    assertEquals("secret123", auth.getPassword());
  }

  @Test void testAuthConfigFromMapBearerAuth() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "bearer");
    map.put("name", "Authorization");
    map.put("value", "Bearer eyJhbGciOiJIUzI1NiJ9.test");

    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.fromMap(map);

    assertEquals(HttpSourceConfig.AuthType.BEARER, auth.getType());
    assertEquals("Authorization", auth.getName());
    assertEquals("Bearer eyJhbGciOiJIUzI1NiJ9.test", auth.getValue());
  }

  @Test void testAuthConfigFromMapNone() {
    Map<String, Object> map = new HashMap<String, Object>();
    // No type specified

    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.fromMap(map);

    assertEquals(HttpSourceConfig.AuthType.NONE, auth.getType());
  }

  @Test void testAuthConfigFromMapNull() {
    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.fromMap(null);

    assertEquals(HttpSourceConfig.AuthType.NONE, auth.getType());
  }

  @Test void testAuthConfigFromMapInvalidType() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "nonexistent_auth");

    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.fromMap(map);

    assertEquals(HttpSourceConfig.AuthType.NONE, auth.getType());
  }

  @Test void testAuthConfigFromMapOAuth2() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "oauth2");
    map.put("name", "Authorization");
    map.put("value", "Bearer oauth_token_here");

    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.fromMap(map);

    assertEquals(HttpSourceConfig.AuthType.OAUTH2, auth.getType());
    assertEquals("Authorization", auth.getName());
  }

  @Test void testResponseConfigWithCsvFormat() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "csv");
    map.put("hasHeader", true);
    map.put("delimiter", "|");

    HttpSourceConfig.ResponseConfig response = HttpSourceConfig.ResponseConfig.fromMap(map);

    assertEquals(HttpSourceConfig.ResponseFormat.CSV, response.getFormat());
    assertTrue(response.isHasHeader());
    assertEquals("|", response.getDelimiter());
    assertNull(response.getDataPath());
  }

  @Test void testResponseConfigWithTsvFormat() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "tsv");
    map.put("hasHeader", false);
    map.put("columnNames", "COL1|COL2|COL3");

    HttpSourceConfig.ResponseConfig response = HttpSourceConfig.ResponseConfig.fromMap(map);

    assertEquals(HttpSourceConfig.ResponseFormat.TSV, response.getFormat());
    assertFalse(response.isHasHeader());
    assertEquals("COL1|COL2|COL3", response.getColumnNames());
  }

  @Test void testResponseConfigWithErrorPath() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "json");
    map.put("dataPath", "$.data.results");
    map.put("errorPath", "BEAAPI.Results.Error");

    HttpSourceConfig.ResponseConfig response = HttpSourceConfig.ResponseConfig.fromMap(map);

    assertEquals(HttpSourceConfig.ResponseFormat.JSON, response.getFormat());
    assertEquals("$.data.results", response.getDataPath());
    assertEquals("BEAAPI.Results.Error", response.getErrorPath());
  }

  @Test void testResponseConfigDefaults() {
    HttpSourceConfig.ResponseConfig response = HttpSourceConfig.ResponseConfig.defaults();

    assertEquals(HttpSourceConfig.ResponseFormat.JSON, response.getFormat());
    assertNull(response.getDataPath());
    assertNull(response.getErrorPath());
    assertTrue(response.isHasHeader());
    assertNull(response.getColumnNames());
    assertNull(response.getDelimiter());
    assertEquals(HttpSourceConfig.PaginationType.NONE, response.getPagination().getType());
  }

  @Test void testHttpSourceConfigPostMethodWithBodyTemplate() {
    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("seriesid", Arrays.asList("CUUR0000SA0", "CUUR0000AA0"));
    body.put("startyear", "{year}");
    body.put("endyear", "{year}");
    body.put("registrationkey", "{env:BLS_API_KEY}");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.bls.gov/publicAPI/v2/timeseries/data/")
        .method(HttpSourceConfig.HttpMethod.POST)
        .body(body)
        .bodyFormat(HttpSourceConfig.BodyFormat.JSON)
        .build();

    assertEquals("https://api.bls.gov/publicAPI/v2/timeseries/data/", config.getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.POST, config.getMethod());
    assertTrue(config.hasBody());
    assertEquals(4, config.getBody().size());
    assertEquals("{year}", config.getBody().get("startyear"));
    assertEquals(HttpSourceConfig.BodyFormat.JSON, config.getBodyFormat());
  }

  @Test void testHttpSourceConfigPostMethodFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "https://api.example.com/graphql");
    map.put("method", "POST");

    Map<String, Object> bodyMap = new LinkedHashMap<String, Object>();
    bodyMap.put("query", "{ users { id name } }");
    map.put("body", bodyMap);
    map.put("bodyFormat", "JSON");

    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Content-Type", "application/json");
    map.put("headers", headers);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);

    assertEquals(HttpSourceConfig.HttpMethod.POST, config.getMethod());
    assertTrue(config.hasBody());
    assertEquals("{ users { id name } }", config.getBody().get("query"));
    assertEquals(HttpSourceConfig.BodyFormat.JSON, config.getBodyFormat());
    assertEquals("application/json", config.getHeaders().get("Content-Type"));
  }

  @Test void testHttpSourceConfigFormUrlEncodedBody() {
    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("grant_type", "client_credentials");
    body.put("client_id", "{env:CLIENT_ID}");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://auth.example.com/token")
        .method(HttpSourceConfig.HttpMethod.POST)
        .body(body)
        .bodyFormat(HttpSourceConfig.BodyFormat.FORM_URLENCODED)
        .build();

    assertEquals(HttpSourceConfig.BodyFormat.FORM_URLENCODED, config.getBodyFormat());
    assertTrue(config.hasBody());
  }

  @Test void testWideToNarrowConfigNoPattern() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("GeoFIPS", "GeoName", "Description"));
    // No valueColumnPattern - all non-key columns should be value columns
    map.put("keyColumnName", "Period");
    map.put("valueColumnName", "Amount");

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertNotNull(config);
    assertEquals(3, config.getKeyColumns().size());
    assertNull(config.getValueColumnPattern());
    assertEquals("Period", config.getKeyColumnName());
    assertEquals("Amount", config.getValueColumnName());
    assertTrue(config.isEnabled());

    // Without pattern, all non-key columns are value columns
    assertTrue(config.isValueColumn("2020"));
    assertTrue(config.isValueColumn("SomeRandomColumn"));
    assertFalse(config.isValueColumn("GeoFIPS"));
    assertFalse(config.isValueColumn("GeoName"));
    assertFalse(config.isValueColumn("Description"));
  }

  @Test void testWideToNarrowConfigColumnMapping() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("FIPS", "Name"));
    map.put("keyColumnName", "Year");
    map.put("valueColumnName", "Value");

    Map<String, String> columnMapping = new LinkedHashMap<String, String>();
    columnMapping.put("FIPS", "geo_fips");
    columnMapping.put("Name", "geo_name");
    map.put("columnMapping", columnMapping);

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.hasColumnMapping());
    assertEquals("geo_fips", config.getOutputColumnName("FIPS"));
    assertEquals("geo_name", config.getOutputColumnName("Name"));
    assertEquals("UnmappedCol", config.getOutputColumnName("UnmappedCol"));
  }

  @Test void testWideToNarrowConfigToString() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("keyColumns", Arrays.asList("ID", "Name"));
    map.put("valueColumnPattern", "^\\d{4}$");
    map.put("keyColumnName", "Year");
    map.put("valueColumnName", "Amount");

    HttpSourceConfig.WideToNarrowConfig config =
        HttpSourceConfig.WideToNarrowConfig.fromMap(map);

    String str = config.toString();
    assertTrue(str.contains("keyColumns="));
    assertTrue(str.contains("valueColumnPattern="));
    assertTrue(str.contains("Year"));
    assertTrue(str.contains("Amount"));
  }

  @Test void testHttpSourceConstructorAndGetType() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/v1/data")
        .method(HttpSourceConfig.HttpMethod.GET)
        .auth(HttpSourceConfig.AuthConfig.bearer("my_token"))
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            new HashMap<String, Object>() {{
              put("format", "json");
              put("dataPath", "$.results");
            }}))
        .build();

    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
  }

  @Test void testHttpSourceWithCacheEnabled() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .cache(
            HttpSourceConfig.CacheConfig.fromMap(
            new HashMap<String, Object>() {{
              put("enabled", true);
              put("ttlSeconds", 1800);
            }}))
        .build();

    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    // Close should not throw
    source.close();
  }

  @Test void testHttpSourceConfigBulkDownload() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://example.com/data.zip")
        .bulkDownload("annual_data")
        .extractPattern("*.csv")
        .build();

    assertTrue(config.isBulkDownloadSource());
    assertEquals("annual_data", config.getBulkDownload());
    assertEquals("*.csv", config.getExtractPattern());
  }

  @Test void testHttpSourceConfigParallel() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .parallel(4)
        .build();

    assertEquals(4, config.getParallel());
  }

  @Test void testHttpSourceConfigDefaultParallel() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    assertEquals(1, config.getParallel());
  }

  @Test void testHttpSourceConfigSourceType() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    assertEquals("http", config.getSourceType());
    assertFalse(config.isDocumentSource());
    assertFalse(config.isBulkDownloadSource());
    assertFalse(config.hasBody());
    assertFalse(config.hasBatching());
    assertFalse(config.hasRowFilter());
    assertFalse(config.hasResponsePartitioning());
    assertFalse(config.hasWideToNarrow());
  }

  @Test void testHttpSourceConfigNoBody() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    assertFalse(config.hasBody());
    assertTrue(config.getBody().isEmpty());
    assertEquals(HttpSourceConfig.BodyFormat.JSON, config.getBodyFormat());
  }

  @Test void testRateLimitConfigFromMapPartial() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("requestsPerSecond", 20);
    // Other fields use defaults

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.fromMap(map);

    assertEquals(20, rateLimit.getRequestsPerSecond());
    // Defaults for unspecified
    assertEquals(2, rateLimit.getRetryOn().length);
    assertEquals(3, rateLimit.getMaxRetries());
    assertEquals(1000, rateLimit.getRetryBackoffMs());
  }

  @Test void testResponseConfigFromMapWithCsvPagination() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("format", "csv");
    map.put("hasHeader", true);

    Map<String, Object> paginationMap = new HashMap<String, Object>();
    paginationMap.put("type", "cursor");
    paginationMap.put("cursorParam", "cursor");
    paginationMap.put("pageSize", 250);
    map.put("pagination", paginationMap);

    HttpSourceConfig.ResponseConfig response = HttpSourceConfig.ResponseConfig.fromMap(map);

    assertEquals(HttpSourceConfig.ResponseFormat.CSV, response.getFormat());
    assertEquals(HttpSourceConfig.PaginationType.CURSOR, response.getPagination().getType());
    assertEquals("cursor", response.getPagination().getCursorParam());
    assertEquals(250, response.getPagination().getPageSize());
  }
}
