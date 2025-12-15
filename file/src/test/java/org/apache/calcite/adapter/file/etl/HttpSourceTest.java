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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
    HttpSourceConfig.AuthConfig auth = HttpSourceConfig.AuthConfig.apiKey(
        HttpSourceConfig.AuthLocation.HEADER,
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
}
