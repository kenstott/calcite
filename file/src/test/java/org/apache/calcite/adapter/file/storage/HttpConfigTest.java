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
package org.apache.calcite.adapter.file.storage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HttpConfig}.
 */
@Tag("unit")
public class HttpConfigTest {

  @Test void testDefaultConstructor() {
    HttpConfig config = new HttpConfig();

    assertEquals("GET", config.getMethod());
    assertNull(config.getBody());
    assertNotNull(config.getHeaders());
    assertTrue(config.getHeaders().isEmpty());
    assertNull(config.getMimeType());
    assertTrue(config.isCacheEnabled());
    assertEquals(300000, config.getCacheTtl());
  }

  @Test void testParameterizedConstructor() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Accept", "application/json");

    HttpConfig config = new HttpConfig("POST", "body", headers, "application/json");

    assertEquals("POST", config.getMethod());
    assertEquals("body", config.getBody());
    assertEquals("application/json", config.getHeaders().get("Accept"));
    assertEquals("application/json", config.getMimeType());
  }

  @Test void testConstructorNullMethodDefaultsToGet() {
    HttpConfig config = new HttpConfig(null, null, null, null);

    assertEquals("GET", config.getMethod());
    assertNotNull(config.getHeaders());
    assertTrue(config.getHeaders().isEmpty());
  }

  @Test void testFromMapDefaultMethod() {
    Map<String, Object> map = new HashMap<String, Object>();

    HttpConfig config = HttpConfig.fromMap(map);

    assertEquals("GET", config.getMethod());
  }

  @Test void testFromMapWithMethod() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("method", "DELETE");

    HttpConfig config = HttpConfig.fromMap(map);

    assertEquals("DELETE", config.getMethod());
  }

  @Test void testFromMapWithHeaders() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Custom", "value");
    headers.put("Accept", "text/csv");
    map.put("headers", headers);

    HttpConfig config = HttpConfig.fromMap(map);

    assertEquals("value", config.getHeaders().get("X-Custom"));
    assertEquals("text/csv", config.getHeaders().get("Accept"));
  }

  @Test void testFromMapWithNonMapHeaders() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("headers", "not a map");

    HttpConfig config = HttpConfig.fromMap(map);

    // Headers should be empty when invalid type
    assertTrue(config.getHeaders().isEmpty());
  }

  @Test void testFromMapCompleteAuth() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> auth = new HashMap<String, Object>();
    auth.put("bearerToken", "my-token");
    auth.put("apiKey", "my-key");
    auth.put("username", "user");
    auth.put("password", "pass");
    auth.put("tokenCommand", "get-token");
    auth.put("tokenEnv", "TOKEN_VAR");
    auth.put("tokenFile", "/path/token");
    auth.put("tokenEndpoint", "http://token.svc");
    auth.put("proxyEndpoint", "http://proxy.svc");
    auth.put("cacheEnabled", Boolean.FALSE);
    auth.put("cacheTtl", 60000L);
    map.put("authConfig", auth);

    HttpConfig config = HttpConfig.fromMap(map);

    assertEquals("my-token", config.getBearerToken());
    assertEquals("my-key", config.getApiKey());
    assertEquals("user", config.getUsername());
    assertEquals("pass", config.getPassword());
    assertEquals("get-token", config.getTokenCommand());
    assertEquals("TOKEN_VAR", config.getTokenEnv());
    assertEquals("/path/token", config.getTokenFile());
    assertEquals("http://token.svc", config.getTokenEndpoint());
    assertEquals("http://proxy.svc", config.getProxyEndpoint());
    assertEquals(false, config.isCacheEnabled());
    assertEquals(60000L, config.getCacheTtl());
  }

  @Test void testFromMapAuthHeaders() {
    Map<String, Object> map = new HashMap<String, Object>();
    Map<String, Object> auth = new HashMap<String, Object>();
    Map<String, String> authHeaders = new HashMap<String, String>();
    authHeaders.put("X-Auth", "secret");
    auth.put("authHeaders", authHeaders);
    map.put("authConfig", auth);

    HttpConfig config = HttpConfig.fromMap(map);

    assertNotNull(config.getAuthHeaders());
    assertEquals("secret", config.getAuthHeaders().get("X-Auth"));
  }

  @Test void testBuilderFull() {
    Map<String, String> authHeaders = new HashMap<String, String>();
    authHeaders.put("X-Auth", "hdr-val");

    HttpConfig config = new HttpConfig.Builder()
        .bearerToken("token")
        .apiKey("key")
        .basicAuth("user", "pass")
        .tokenCommand("cmd")
        .tokenEnv("env")
        .tokenFile("file")
        .tokenEndpoint("endpoint")
        .authHeaders(authHeaders)
        .proxyEndpoint("proxy")
        .cacheEnabled(false)
        .cacheTtl(10000)
        .build();

    assertEquals("token", config.getBearerToken());
    assertEquals("key", config.getApiKey());
    assertEquals("user", config.getUsername());
    assertEquals("pass", config.getPassword());
    assertEquals("cmd", config.getTokenCommand());
    assertEquals("env", config.getTokenEnv());
    assertEquals("file", config.getTokenFile());
    assertEquals("endpoint", config.getTokenEndpoint());
    assertEquals("hdr-val", config.getAuthHeaders().get("X-Auth"));
    assertEquals("proxy", config.getProxyEndpoint());
    assertEquals(false, config.isCacheEnabled());
    assertEquals(10000, config.getCacheTtl());
  }

  @Test void testCreateStorageProvider() {
    HttpConfig config = new HttpConfig("GET", null, new HashMap<String, String>(), null);

    HttpStorageProvider provider = config.createStorageProvider();

    assertNotNull(provider);
    assertEquals("http", provider.getStorageType());
  }
}
