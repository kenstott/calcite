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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HttpSourceConfig} covering builder defaults,
 * fromMap parsing, and URL rule selection.
 */
@Tag("unit")
class HttpSourceConfigTest {

  @Test void testBuilderDefaults() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com/api")
        .build();
    assertEquals("http://example.com/api", config.getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.GET, config.getMethod());
    assertTrue(config.getParameters().isEmpty());
    assertTrue(config.getHeaders().isEmpty());
    assertFalse(config.hasBody());
    assertEquals(HttpSourceConfig.BodyFormat.JSON, config.getBodyFormat());
    assertNotNull(config.getAuth());
    assertNotNull(config.getResponse());
    assertNotNull(config.getRateLimit());
    assertNotNull(config.getCache());
    assertFalse(config.hasBatching());
    assertNull(config.getBatching());
  }

  @Test void testBuilderWithMethod() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .method(HttpSourceConfig.HttpMethod.POST)
        .build();
    assertEquals(HttpSourceConfig.HttpMethod.POST, config.getMethod());
  }

  @Test void testBuilderWithParameters() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("apiKey", "12345");
    params.put("format", "json");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .parameters(params)
        .build();
    assertEquals(2, config.getParameters().size());
    assertEquals("12345", config.getParameters().get("apiKey"));
  }

  @Test void testParametersAreImmutable() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .build();
    try {
      config.getParameters().put("key", "value");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testBuilderWithHeaders() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Content-Type", "application/json");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .headers(headers)
        .build();
    assertEquals(1, config.getHeaders().size());
  }

  @Test void testHeadersAreImmutable() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .build();
    try {
      config.getHeaders().put("key", "value");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testBuilderWithBody() {
    Map<String, Object> body = new HashMap<String, Object>();
    body.put("seriesid", "{series}");
    body.put("startyear", "{year}");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .method(HttpSourceConfig.HttpMethod.POST)
        .body(body)
        .build();
    assertTrue(config.hasBody());
    assertEquals(2, config.getBody().size());
  }

  @Test void testBodyAreImmutable() {
    Map<String, Object> body = new HashMap<String, Object>();
    body.put("key", "value");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .body(body)
        .build();
    try {
      config.getBody().put("sneaky", "val");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testBuilderWithBodyFormat() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .bodyFormat(HttpSourceConfig.BodyFormat.FORM_URLENCODED)
        .build();
    assertEquals(HttpSourceConfig.BodyFormat.FORM_URLENCODED, config.getBodyFormat());
  }

  @Test void testBulkDownloadSource() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .bulkDownload("big_file")
        .build();
    assertTrue(config.isBulkDownloadSource());
    assertEquals("big_file", config.getBulkDownload());
  }

  @Test void testNotBulkDownloadSource() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .build();
    assertFalse(config.isBulkDownloadSource());
    assertNull(config.getBulkDownload());
  }

  @Test void testGetEffectiveUrlNoRules() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://default.com")
        .build();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2020");
    assertEquals("http://default.com", config.getEffectiveUrl(vars));
  }

  @Test void testGetEffectiveUrlNullVariables() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://default.com")
        .build();
    assertEquals("http://default.com", config.getEffectiveUrl(null));
  }

  @Test void testGetEffectiveUrlNonNumericYear() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://default.com")
        .build();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "abc");
    assertEquals("http://default.com", config.getEffectiveUrl(vars));
  }

  @Test void testFromMapNull() {
    assertNull(HttpSourceConfig.fromMap(null));
  }

  @Test void testFromMapBasic() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "http://api.example.com/data");
    map.put("method", "POST");

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("http://api.example.com/data", config.getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.POST, config.getMethod());
  }

  @Test void testFromMapWithParameters() {
    Map<String, Object> paramsMap = new HashMap<String, Object>();
    paramsMap.put("key", "value");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("url", "http://example.com");
    map.put("parameters", paramsMap);

    HttpSourceConfig config = HttpSourceConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("value", config.getParameters().get("key"));
  }

  @Test void testHttpMethodEnum() {
    HttpSourceConfig.HttpMethod[] values = HttpSourceConfig.HttpMethod.values();
    assertEquals(4, values.length);
    assertNotNull(HttpSourceConfig.HttpMethod.valueOf("GET"));
    assertNotNull(HttpSourceConfig.HttpMethod.valueOf("POST"));
    assertNotNull(HttpSourceConfig.HttpMethod.valueOf("PUT"));
    assertNotNull(HttpSourceConfig.HttpMethod.valueOf("DELETE"));
  }

  @Test void testResponseFormatEnum() {
    HttpSourceConfig.ResponseFormat[] values = HttpSourceConfig.ResponseFormat.values();
    assertEquals(5, values.length);
    assertNotNull(HttpSourceConfig.ResponseFormat.valueOf("JSON"));
    assertNotNull(HttpSourceConfig.ResponseFormat.valueOf("CSV"));
    assertNotNull(HttpSourceConfig.ResponseFormat.valueOf("XML"));
    assertNotNull(HttpSourceConfig.ResponseFormat.valueOf("TSV"));
    assertNotNull(HttpSourceConfig.ResponseFormat.valueOf("TEXT"));
  }

  @Test void testAuthTypeEnum() {
    HttpSourceConfig.AuthType[] values = HttpSourceConfig.AuthType.values();
    assertEquals(5, values.length);
  }

  @Test void testAuthLocationEnum() {
    HttpSourceConfig.AuthLocation[] values = HttpSourceConfig.AuthLocation.values();
    assertEquals(2, values.length);
  }

  @Test void testPaginationTypeEnum() {
    HttpSourceConfig.PaginationType[] values = HttpSourceConfig.PaginationType.values();
    assertEquals(5, values.length);
    assertNotNull(HttpSourceConfig.PaginationType.valueOf("CSV_STREAM"));
  }

  @Test void testBodyFormatEnum() {
    HttpSourceConfig.BodyFormat[] values = HttpSourceConfig.BodyFormat.values();
    assertEquals(2, values.length);
  }

  @Test void testSourceTypeHttp() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .build();
    assertEquals("http", config.getSourceType());
  }

  @Test void testSourceTypeBulkDownload() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .bulkDownload("mydownload")
        .build();
    assertEquals("bulkDownload", config.getSourceType());
  }

  @Test void testUrlRulesAreImmutable() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .build();
    assertTrue(config.getUrlRules().isEmpty());
    try {
      config.getUrlRules().add(null);
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testParallelDefault() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .build();
    assertEquals(1, config.getParallel());
  }

  @Test void testParallelCustom() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://example.com")
        .parallel(4)
        .build();
    assertEquals(4, config.getParallel());
  }
}
