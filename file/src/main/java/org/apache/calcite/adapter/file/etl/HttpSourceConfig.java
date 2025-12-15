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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration for HTTP data source.
 *
 * <p>HttpSourceConfig defines how to fetch data from HTTP/REST APIs,
 * including URL, method, parameters, headers, authentication, response
 * parsing, pagination, rate limiting, and caching.
 *
 * <h3>YAML Configuration Example</h3>
 * <pre>{@code
 * source:
 *   type: http
 *   url: "https://api.example.com/data"
 *   method: GET
 *   parameters:
 *     apiKey: "{env:API_KEY}"
 *     year: "{year}"
 *     region: "{region}"
 *   headers:
 *     Accept: "application/json"
 *     Authorization: "Bearer {env:AUTH_TOKEN}"
 *   auth:
 *     type: apiKey
 *     location: header
 *     name: "X-Api-Key"
 *     value: "{env:API_KEY}"
 *   response:
 *     format: json
 *     dataPath: "$.results.data"
 *     pagination:
 *       type: offset
 *       limitParam: "limit"
 *       offsetParam: "offset"
 *       pageSize: 1000
 *   rateLimit:
 *     requestsPerSecond: 10
 *     retryOn: [429, 503]
 *     maxRetries: 3
 *   cache:
 *     enabled: true
 *     ttlSeconds: 86400
 * }</pre>
 *
 * @see HttpSource
 */
public class HttpSourceConfig {

  /**
   * HTTP methods supported.
   */
  public enum HttpMethod {
    GET, POST, PUT, DELETE
  }

  /**
   * Response format types.
   */
  public enum ResponseFormat {
    JSON, CSV, XML
  }

  /**
   * Authentication types.
   */
  public enum AuthType {
    NONE, API_KEY, BASIC, OAUTH2, BEARER
  }

  /**
   * Authentication location.
   */
  public enum AuthLocation {
    HEADER, QUERY
  }

  /**
   * Pagination types.
   */
  public enum PaginationType {
    NONE, OFFSET, CURSOR, PAGE
  }

  private final String url;
  private final HttpMethod method;
  private final Map<String, String> parameters;
  private final Map<String, String> headers;
  private final AuthConfig auth;
  private final ResponseConfig response;
  private final RateLimitConfig rateLimit;
  private final CacheConfig cache;

  private HttpSourceConfig(Builder builder) {
    this.url = builder.url;
    this.method = builder.method != null ? builder.method : HttpMethod.GET;
    this.parameters = builder.parameters != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.parameters))
        : Collections.<String, String>emptyMap();
    this.headers = builder.headers != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.headers))
        : Collections.<String, String>emptyMap();
    this.auth = builder.auth != null ? builder.auth : AuthConfig.none();
    this.response = builder.response != null ? builder.response : ResponseConfig.defaults();
    this.rateLimit = builder.rateLimit != null ? builder.rateLimit : RateLimitConfig.defaults();
    this.cache = builder.cache != null ? builder.cache : CacheConfig.defaults();
  }

  public String getUrl() {
    return url;
  }

  public HttpMethod getMethod() {
    return method;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public AuthConfig getAuth() {
    return auth;
  }

  public ResponseConfig getResponse() {
    return response;
  }

  public RateLimitConfig getRateLimit() {
    return rateLimit;
  }

  public CacheConfig getCache() {
    return cache;
  }

  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings("unchecked")
  public static HttpSourceConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();
    builder.url((String) map.get("url"));

    Object methodObj = map.get("method");
    if (methodObj instanceof String) {
      builder.method(HttpMethod.valueOf(((String) methodObj).toUpperCase()));
    }

    Object paramsObj = map.get("parameters");
    if (paramsObj instanceof Map) {
      Map<String, String> params = new LinkedHashMap<String, String>();
      for (Map.Entry<?, ?> e : ((Map<?, ?>) paramsObj).entrySet()) {
        params.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
      }
      builder.parameters(params);
    }

    Object headersObj = map.get("headers");
    if (headersObj instanceof Map) {
      Map<String, String> hdrs = new LinkedHashMap<String, String>();
      for (Map.Entry<?, ?> e : ((Map<?, ?>) headersObj).entrySet()) {
        hdrs.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
      }
      builder.headers(hdrs);
    }

    Object authObj = map.get("auth");
    if (authObj instanceof Map) {
      builder.auth(AuthConfig.fromMap((Map<String, Object>) authObj));
    }

    Object responseObj = map.get("response");
    if (responseObj instanceof Map) {
      builder.response(ResponseConfig.fromMap((Map<String, Object>) responseObj));
    }

    Object rateLimitObj = map.get("rateLimit");
    if (rateLimitObj instanceof Map) {
      builder.rateLimit(RateLimitConfig.fromMap((Map<String, Object>) rateLimitObj));
    }

    Object cacheObj = map.get("cache");
    if (cacheObj instanceof Map) {
      builder.cache(CacheConfig.fromMap((Map<String, Object>) cacheObj));
    }

    return builder.build();
  }

  /**
   * Authentication configuration.
   */
  public static class AuthConfig {
    private final AuthType type;
    private final AuthLocation location;
    private final String name;
    private final String value;
    private final String username;
    private final String password;

    private AuthConfig(AuthType type, AuthLocation location, String name, String value,
        String username, String password) {
      this.type = type;
      this.location = location;
      this.name = name;
      this.value = value;
      this.username = username;
      this.password = password;
    }

    public static AuthConfig none() {
      return new AuthConfig(AuthType.NONE, null, null, null, null, null);
    }

    public static AuthConfig apiKey(AuthLocation location, String name, String value) {
      return new AuthConfig(AuthType.API_KEY, location, name, value, null, null);
    }

    public static AuthConfig basic(String username, String password) {
      return new AuthConfig(AuthType.BASIC, null, null, null, username, password);
    }

    public static AuthConfig bearer(String token) {
      return new AuthConfig(AuthType.BEARER, AuthLocation.HEADER, "Authorization",
          "Bearer " + token, null, null);
    }

    public AuthType getType() {
      return type;
    }

    public AuthLocation getLocation() {
      return location;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @SuppressWarnings("unchecked")
    public static AuthConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return none();
      }
      String typeStr = (String) map.get("type");
      if (typeStr == null) {
        return none();
      }

      AuthType type;
      try {
        // Convert camelCase or kebab-case to UPPER_SNAKE_CASE
        String normalized = typeStr
            .replaceAll("([a-z])([A-Z])", "$1_$2")  // camelCase -> camel_Case
            .replace("-", "_")                       // kebab-case -> kebab_case
            .toUpperCase();
        type = AuthType.valueOf(normalized);
      } catch (IllegalArgumentException e) {
        return none();
      }

      String locationStr = (String) map.get("location");
      AuthLocation location = locationStr != null
          ? AuthLocation.valueOf(locationStr.toUpperCase())
          : AuthLocation.HEADER;

      return new AuthConfig(
          type,
          location,
          (String) map.get("name"),
          (String) map.get("value"),
          (String) map.get("username"),
          (String) map.get("password")
      );
    }
  }

  /**
   * Response parsing configuration.
   */
  public static class ResponseConfig {
    private final ResponseFormat format;
    private final String dataPath;
    private final PaginationConfig pagination;

    private ResponseConfig(ResponseFormat format, String dataPath, PaginationConfig pagination) {
      this.format = format;
      this.dataPath = dataPath;
      this.pagination = pagination;
    }

    public static ResponseConfig defaults() {
      return new ResponseConfig(ResponseFormat.JSON, null, PaginationConfig.none());
    }

    public ResponseFormat getFormat() {
      return format;
    }

    public String getDataPath() {
      return dataPath;
    }

    public PaginationConfig getPagination() {
      return pagination;
    }

    @SuppressWarnings("unchecked")
    public static ResponseConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return defaults();
      }

      String formatStr = (String) map.get("format");
      ResponseFormat format = formatStr != null
          ? ResponseFormat.valueOf(formatStr.toUpperCase())
          : ResponseFormat.JSON;

      String dataPath = (String) map.get("dataPath");

      Object paginationObj = map.get("pagination");
      PaginationConfig pagination = paginationObj instanceof Map
          ? PaginationConfig.fromMap((Map<String, Object>) paginationObj)
          : PaginationConfig.none();

      return new ResponseConfig(format, dataPath, pagination);
    }
  }

  /**
   * Pagination configuration.
   */
  public static class PaginationConfig {
    private final PaginationType type;
    private final String limitParam;
    private final String offsetParam;
    private final String cursorParam;
    private final String pageParam;
    private final int pageSize;

    private PaginationConfig(PaginationType type, String limitParam, String offsetParam,
        String cursorParam, String pageParam, int pageSize) {
      this.type = type;
      this.limitParam = limitParam;
      this.offsetParam = offsetParam;
      this.cursorParam = cursorParam;
      this.pageParam = pageParam;
      this.pageSize = pageSize;
    }

    public static PaginationConfig none() {
      return new PaginationConfig(PaginationType.NONE, null, null, null, null, 0);
    }

    public static PaginationConfig offset(String limitParam, String offsetParam, int pageSize) {
      return new PaginationConfig(PaginationType.OFFSET, limitParam, offsetParam,
          null, null, pageSize);
    }

    public PaginationType getType() {
      return type;
    }

    public String getLimitParam() {
      return limitParam;
    }

    public String getOffsetParam() {
      return offsetParam;
    }

    public String getCursorParam() {
      return cursorParam;
    }

    public String getPageParam() {
      return pageParam;
    }

    public int getPageSize() {
      return pageSize;
    }

    public static PaginationConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return none();
      }

      String typeStr = (String) map.get("type");
      PaginationType type = typeStr != null
          ? PaginationType.valueOf(typeStr.toUpperCase())
          : PaginationType.NONE;

      int pageSize = 1000;
      Object pageSizeObj = map.get("pageSize");
      if (pageSizeObj instanceof Number) {
        pageSize = ((Number) pageSizeObj).intValue();
      }

      return new PaginationConfig(
          type,
          (String) map.get("limitParam"),
          (String) map.get("offsetParam"),
          (String) map.get("cursorParam"),
          (String) map.get("pageParam"),
          pageSize
      );
    }
  }

  /**
   * Rate limit configuration.
   */
  public static class RateLimitConfig {
    private final int requestsPerSecond;
    private final int[] retryOn;
    private final int maxRetries;
    private final long retryBackoffMs;

    private RateLimitConfig(int requestsPerSecond, int[] retryOn, int maxRetries,
        long retryBackoffMs) {
      this.requestsPerSecond = requestsPerSecond;
      this.retryOn = retryOn;
      this.maxRetries = maxRetries;
      this.retryBackoffMs = retryBackoffMs;
    }

    public static RateLimitConfig defaults() {
      return new RateLimitConfig(10, new int[]{429, 503}, 3, 1000);
    }

    public int getRequestsPerSecond() {
      return requestsPerSecond;
    }

    public int[] getRetryOn() {
      return retryOn;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public long getRetryBackoffMs() {
      return retryBackoffMs;
    }

    @SuppressWarnings("unchecked")
    public static RateLimitConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return defaults();
      }

      int rps = 10;
      Object rpsObj = map.get("requestsPerSecond");
      if (rpsObj instanceof Number) {
        rps = ((Number) rpsObj).intValue();
      }

      int[] retryOn = new int[]{429, 503};
      Object retryOnObj = map.get("retryOn");
      if (retryOnObj instanceof java.util.List) {
        java.util.List<?> list = (java.util.List<?>) retryOnObj;
        retryOn = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
          retryOn[i] = ((Number) list.get(i)).intValue();
        }
      }

      int maxRetries = 3;
      Object maxRetriesObj = map.get("maxRetries");
      if (maxRetriesObj instanceof Number) {
        maxRetries = ((Number) maxRetriesObj).intValue();
      }

      long backoff = 1000;
      Object backoffObj = map.get("retryBackoffMs");
      if (backoffObj instanceof Number) {
        backoff = ((Number) backoffObj).longValue();
      }

      return new RateLimitConfig(rps, retryOn, maxRetries, backoff);
    }
  }

  /**
   * Cache configuration.
   */
  public static class CacheConfig {
    private final boolean enabled;
    private final long ttlSeconds;

    private CacheConfig(boolean enabled, long ttlSeconds) {
      this.enabled = enabled;
      this.ttlSeconds = ttlSeconds;
    }

    public static CacheConfig defaults() {
      return new CacheConfig(false, 86400);
    }

    public boolean isEnabled() {
      return enabled;
    }

    public long getTtlSeconds() {
      return ttlSeconds;
    }

    public static CacheConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return defaults();
      }

      boolean enabled = false;
      Object enabledObj = map.get("enabled");
      if (enabledObj instanceof Boolean) {
        enabled = (Boolean) enabledObj;
      }

      long ttl = 86400;
      Object ttlObj = map.get("ttlSeconds");
      if (ttlObj instanceof Number) {
        ttl = ((Number) ttlObj).longValue();
      }

      return new CacheConfig(enabled, ttl);
    }
  }

  /**
   * Builder for HttpSourceConfig.
   */
  public static class Builder {
    private String url;
    private HttpMethod method;
    private Map<String, String> parameters;
    private Map<String, String> headers;
    private AuthConfig auth;
    private ResponseConfig response;
    private RateLimitConfig rateLimit;
    private CacheConfig cache;

    public Builder url(String url) {
      this.url = url;
      return this;
    }

    public Builder method(HttpMethod method) {
      this.method = method;
      return this;
    }

    public Builder parameters(Map<String, String> parameters) {
      this.parameters = parameters;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    public Builder auth(AuthConfig auth) {
      this.auth = auth;
      return this;
    }

    public Builder response(ResponseConfig response) {
      this.response = response;
      return this;
    }

    public Builder rateLimit(RateLimitConfig rateLimit) {
      this.rateLimit = rateLimit;
      return this;
    }

    public Builder cache(CacheConfig cache) {
      this.cache = cache;
      return this;
    }

    public HttpSourceConfig build() {
      if (url == null || url.isEmpty()) {
        throw new IllegalArgumentException("URL is required");
      }
      return new HttpSourceConfig(this);
    }
  }
}
