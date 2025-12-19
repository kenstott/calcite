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
 * including URL, method, parameters, headers, request body, authentication,
 * response parsing, pagination, rate limiting, and caching.
 *
 * <h3>GET Request Example</h3>
 * <pre>{@code
 * source:
 *   type: http
 *   url: "https://api.example.com/data"
 *   method: GET
 *   parameters:
 *     apiKey: "{env:API_KEY}"
 *     year: "{year}"
 *   response:
 *     format: json
 *     dataPath: "$.results.data"
 * }</pre>
 *
 * <h3>POST Request with JSON Body</h3>
 * <pre>{@code
 * source:
 *   type: http
 *   url: "https://api.example.com/data"
 *   method: POST
 *   headers:
 *     Content-Type: "application/json"
 *   body:
 *     seriesid: "{seriesList}"
 *     startyear: "{year}"
 *     endyear: "{year}"
 *     registrationkey: "{env:API_KEY}"
 *   bodyFormat: json  # optional, defaults to json
 *   response:
 *     format: json
 *     dataPath: "Results.series"
 * }</pre>
 *
 * <p>Body values support variable substitution using {@code {varName}} syntax
 * for dimension variables and {@code {env:VAR_NAME}} for environment variables.
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
    JSON, CSV, XML, TSV
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

  /**
   * Request body format types.
   */
  public enum BodyFormat {
    JSON, FORM_URLENCODED
  }

  private final String url;
  private final HttpMethod method;
  private final Map<String, String> parameters;
  private final Map<String, String> headers;
  private final Map<String, Object> body;
  private final BodyFormat bodyFormat;
  private final BatchConfig batching;
  private final AuthConfig auth;
  private final ResponseConfig response;
  private final RateLimitConfig rateLimit;
  private final CacheConfig cache;

  // Bulk download reference (alternative to direct HTTP)
  private final String bulkDownload;
  private final String extractPattern;

  // Row filtering for CSV parsing (to avoid loading entire file into memory)
  private final RowFilterConfig rowFilter;

  private HttpSourceConfig(Builder builder) {
    this.url = builder.url;
    this.method = builder.method != null ? builder.method : HttpMethod.GET;
    this.bulkDownload = builder.bulkDownload;
    this.extractPattern = builder.extractPattern;
    this.parameters = builder.parameters != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.parameters))
        : Collections.<String, String>emptyMap();
    this.headers = builder.headers != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.headers))
        : Collections.<String, String>emptyMap();
    this.body = builder.body != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, Object>(builder.body))
        : Collections.<String, Object>emptyMap();
    this.bodyFormat = builder.bodyFormat != null ? builder.bodyFormat : BodyFormat.JSON;
    this.batching = builder.batching;
    this.auth = builder.auth != null ? builder.auth : AuthConfig.none();
    this.response = builder.response != null ? builder.response : ResponseConfig.defaults();
    this.rateLimit = builder.rateLimit != null ? builder.rateLimit : RateLimitConfig.defaults();
    this.cache = builder.cache != null ? builder.cache : CacheConfig.defaults();
    this.rowFilter = builder.rowFilter;
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

  /**
   * Returns the request body template.
   *
   * <p>The body is a map of key-value pairs that will be serialized according
   * to the {@link #getBodyFormat()} setting. Values may contain variable
   * placeholders like {@code {year}} or {@code {env:API_KEY}} that are
   * substituted at request time.
   *
   * @return Body template map, never null (may be empty)
   */
  public Map<String, Object> getBody() {
    return body;
  }

  /**
   * Returns the format for serializing the request body.
   *
   * @return Body format (JSON or FORM_URLENCODED), defaults to JSON
   */
  public BodyFormat getBodyFormat() {
    return bodyFormat;
  }

  /**
   * Returns true if this source has a request body configured.
   */
  public boolean hasBody() {
    return body != null && !body.isEmpty();
  }

  /**
   * Returns the batching configuration for splitting large requests.
   *
   * <p>When configured, the HTTP source will:
   * <ol>
   *   <li>Load values from the specified JSON catalog</li>
   *   <li>Split them into batches of the specified size</li>
   *   <li>Make multiple requests, one per batch</li>
   *   <li>Aggregate all results</li>
   * </ol>
   *
   * @return Batching config, or null if not configured
   */
  public BatchConfig getBatching() {
    return batching;
  }

  /**
   * Returns true if batching is configured.
   */
  public boolean hasBatching() {
    return batching != null && batching.isEnabled();
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

  /**
   * Returns the bulk download reference name.
   *
   * <p>When set, this source references a pre-downloaded bulk file
   * instead of making HTTP requests. The bulk file is downloaded
   * during the bulk download phase of the ETL lifecycle.
   *
   * @return Bulk download name, or null for direct HTTP source
   */
  public String getBulkDownload() {
    return bulkDownload;
  }

  /**
   * Returns true if this source references a bulk download.
   */
  public boolean isBulkDownloadSource() {
    return bulkDownload != null && !bulkDownload.isEmpty();
  }

  /**
   * Returns the pattern for extracting files from a bulk download.
   *
   * <p>For ZIP files, this is a glob pattern to match files inside the archive.
   * For example, "*.csv" extracts all CSV files.
   *
   * @return Extract pattern, or null to use the entire bulk file
   */
  public String getExtractPattern() {
    return extractPattern;
  }

  /**
   * Returns the row filter configuration for CSV parsing.
   *
   * <p>When set, only rows matching the filter are kept during parsing.
   * This allows processing large CSV files without loading everything into memory.
   *
   * @return Row filter config, or null if no filtering
   */
  public RowFilterConfig getRowFilter() {
    return rowFilter;
  }

  /**
   * Returns true if row filtering is configured.
   */
  public boolean hasRowFilter() {
    return rowFilter != null && rowFilter.isEnabled();
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

    // Bulk download reference
    Object bulkDownloadObj = map.get("bulkDownload");
    if (bulkDownloadObj instanceof String) {
      builder.bulkDownload((String) bulkDownloadObj);
    }
    Object extractPatternObj = map.get("extractPattern");
    if (extractPatternObj instanceof String) {
      builder.extractPattern((String) extractPatternObj);
    }

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

    // Parse request body (preserves nested structure for JSON serialization)
    Object bodyObj = map.get("body");
    if (bodyObj instanceof Map) {
      Map<String, Object> bodyMap = new LinkedHashMap<String, Object>();
      for (Map.Entry<?, ?> e : ((Map<?, ?>) bodyObj).entrySet()) {
        bodyMap.put(String.valueOf(e.getKey()), e.getValue());
      }
      builder.body(bodyMap);
    }

    // Parse body format (defaults to JSON)
    Object bodyFormatObj = map.get("bodyFormat");
    if (bodyFormatObj instanceof String) {
      builder.bodyFormat(BodyFormat.valueOf(((String) bodyFormatObj).toUpperCase()));
    }

    // Parse batching configuration
    Object batchingObj = map.get("batching");
    if (batchingObj instanceof Map) {
      builder.batching(BatchConfig.fromMap((Map<String, Object>) batchingObj));
    }

    // Parse row filter configuration
    Object rowFilterObj = map.get("rowFilter");
    if (rowFilterObj instanceof Map) {
      builder.rowFilter(RowFilterConfig.fromMap((Map<String, Object>) rowFilterObj));
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
   * Batching configuration for splitting large requests into smaller batches.
   *
   * <p>Use this when an API has limits on array sizes (e.g., BLS allows max 50 series).
   * The HTTP source will automatically:
   * <ol>
   *   <li>Load all values from a JSON catalog file</li>
   *   <li>Split into batches of the configured size</li>
   *   <li>Make sequential API calls for each batch</li>
   *   <li>Aggregate results from all batches</li>
   * </ol>
   *
   * <h3>YAML Configuration Example</h3>
   * <pre>{@code
   * source:
   *   type: http
   *   url: "https://api.example.com/data"
   *   method: POST
   *   batching:
   *     field: seriesid          # Body field to populate with batch values
   *     source: "/catalog.json"  # JSON catalog resource path
   *     path: "items"            # JSONPath to array of values
   *     size: 50                 # Max items per batch
   *     delayMs: 1000            # Delay between batch requests (rate limiting)
   *   body:
   *     seriesid: []             # Will be populated by batching
   *     year: "{year}"
   * }</pre>
   */
  public static class BatchConfig {
    private final String field;
    private final String source;
    private final String path;
    private final int size;
    private final long delayMs;

    private BatchConfig(String field, String source, String path, int size, long delayMs) {
      this.field = field;
      this.source = source;
      this.path = path;
      this.size = size > 0 ? size : 50;
      this.delayMs = delayMs > 0 ? delayMs : 0;
    }

    /**
     * Creates a BatchConfig from a map.
     */
    @SuppressWarnings("unchecked")
    public static BatchConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }
      String field = (String) map.get("field");
      String source = (String) map.get("source");
      String path = (String) map.get("path");

      int size = 50;
      Object sizeObj = map.get("size");
      if (sizeObj instanceof Number) {
        size = ((Number) sizeObj).intValue();
      }

      long delayMs = 0;
      Object delayObj = map.get("delayMs");
      if (delayObj instanceof Number) {
        delayMs = ((Number) delayObj).longValue();
      }

      return new BatchConfig(field, source, path, size, delayMs);
    }

    /**
     * Returns the body field name to populate with batch values.
     */
    public String getField() {
      return field;
    }

    /**
     * Returns the JSON catalog resource path.
     */
    public String getSource() {
      return source;
    }

    /**
     * Returns the JSONPath expression to extract values from the catalog.
     */
    public String getPath() {
      return path;
    }

    /**
     * Returns the maximum number of items per batch.
     */
    public int getSize() {
      return size;
    }

    /**
     * Returns the delay in milliseconds between batch requests.
     */
    public long getDelayMs() {
      return delayMs;
    }

    /**
     * Returns true if this batching config is valid and enabled.
     */
    public boolean isEnabled() {
      return field != null && !field.isEmpty()
          && source != null && !source.isEmpty();
    }

    @Override
    public String toString() {
      return "BatchConfig{field='" + field + "', source='" + source
          + "', path='" + path + "', size=" + size + ", delayMs=" + delayMs + "}";
    }
  }

  /**
   * Row filter configuration for CSV parsing.
   *
   * <p>Filters rows during CSV parsing to avoid loading entire large files into memory.
   * Only rows where the specified column matches the pattern are kept.
   *
   * <h3>YAML Configuration Example</h3>
   * <pre>{@code
   * source:
   *   type: http
   *   url: "https://example.com/data.zip"
   *   extractPattern: "*.csv"
   *   rowFilter:
   *     column: area_fips       # Column name to filter on
   *     pattern: "^C.*"         # Regex pattern (metro areas start with C)
   *     maxRows: 100000         # Optional: stop after this many matching rows
   * }</pre>
   */
  public static class RowFilterConfig {
    private final String column;
    private final String pattern;
    private final int maxRows;

    private RowFilterConfig(String column, String pattern, int maxRows) {
      this.column = column;
      this.pattern = pattern;
      this.maxRows = maxRows;
    }

    /**
     * Creates a RowFilterConfig from a map.
     */
    public static RowFilterConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }
      String column = (String) map.get("column");
      String pattern = (String) map.get("pattern");

      int maxRows = 0;
      Object maxRowsObj = map.get("maxRows");
      if (maxRowsObj instanceof Number) {
        maxRows = ((Number) maxRowsObj).intValue();
      }

      return new RowFilterConfig(column, pattern, maxRows);
    }

    /**
     * Returns the column name to filter on.
     */
    public String getColumn() {
      return column;
    }

    /**
     * Returns the regex pattern to match column values.
     */
    public String getPattern() {
      return pattern;
    }

    /**
     * Returns the maximum number of matching rows to keep (0 = unlimited).
     */
    public int getMaxRows() {
      return maxRows;
    }

    /**
     * Returns true if this filter is valid and enabled.
     */
    public boolean isEnabled() {
      return column != null && !column.isEmpty()
          && pattern != null && !pattern.isEmpty();
    }

    @Override
    public String toString() {
      return "RowFilterConfig{column='" + column + "', pattern='" + pattern
          + "', maxRows=" + maxRows + "}";
    }
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
          (String) map.get("password"));
    }
  }

  /**
   * Response parsing configuration.
   */
  public static class ResponseConfig {
    private final ResponseFormat format;
    private final String dataPath;
    private final String errorPath;
    private final PaginationConfig pagination;

    private ResponseConfig(ResponseFormat format, String dataPath, String errorPath,
        PaginationConfig pagination) {
      this.format = format;
      this.dataPath = dataPath;
      this.errorPath = errorPath;
      this.pagination = pagination;
    }

    public static ResponseConfig defaults() {
      return new ResponseConfig(ResponseFormat.JSON, null, null, PaginationConfig.none());
    }

    public ResponseFormat getFormat() {
      return format;
    }

    public String getDataPath() {
      return dataPath;
    }

    /**
     * Returns the JSON path to check for API errors.
     * If this path exists and is non-null in the response, it indicates an error.
     *
     * @return Error path (e.g., "BEAAPI.Results.Error"), or null if not configured
     */
    public String getErrorPath() {
      return errorPath;
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
      String errorPath = (String) map.get("errorPath");

      Object paginationObj = map.get("pagination");
      PaginationConfig pagination = paginationObj instanceof Map
          ? PaginationConfig.fromMap((Map<String, Object>) paginationObj)
          : PaginationConfig.none();

      return new ResponseConfig(format, dataPath, errorPath, pagination);
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
          pageSize);
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
    private Map<String, Object> body;
    private BodyFormat bodyFormat;
    private BatchConfig batching;
    private AuthConfig auth;
    private ResponseConfig response;
    private RateLimitConfig rateLimit;
    private CacheConfig cache;
    private String bulkDownload;
    private String extractPattern;
    private RowFilterConfig rowFilter;

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

    public Builder body(Map<String, Object> body) {
      this.body = body;
      return this;
    }

    public Builder bodyFormat(BodyFormat bodyFormat) {
      this.bodyFormat = bodyFormat;
      return this;
    }

    public Builder batching(BatchConfig batching) {
      this.batching = batching;
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

    public Builder bulkDownload(String bulkDownload) {
      this.bulkDownload = bulkDownload;
      return this;
    }

    public Builder extractPattern(String extractPattern) {
      this.extractPattern = extractPattern;
      return this;
    }

    public Builder rowFilter(RowFilterConfig rowFilter) {
      this.rowFilter = rowFilter;
      return this;
    }

    public HttpSourceConfig build() {
      // Either url or bulkDownload must be set
      boolean hasUrl = url != null && !url.isEmpty();
      boolean hasBulkDownload = bulkDownload != null && !bulkDownload.isEmpty();
      if (!hasUrl && !hasBulkDownload) {
        throw new IllegalArgumentException("Either URL or bulkDownload is required");
      }
      return new HttpSourceConfig(this);
    }
  }
}
