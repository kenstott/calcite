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
  private final RawCacheConfig rawCache;

  // Bulk download reference (alternative to direct HTTP)
  private final String bulkDownload;
  private final String extractPattern;

  // Row filtering for CSV parsing (to avoid loading entire file into memory)
  private final RowFilterConfig rowFilter;

  // Response partitioning for bulk API responses
  private final ResponsePartitioningConfig responsePartitioning;

  // Wide-to-narrow transformation for CSV files with years as columns
  private final WideToNarrowConfig wideToNarrow;

  // Document source configuration for document-based ETL (SEC filings, etc.)
  private final DocumentSourceConfig documentSource;

  // Source type: "http", "document", or "bulkDownload"
  private final String sourceType;

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
    this.rawCache = builder.rawCache != null ? builder.rawCache : RawCacheConfig.defaults();
    this.rowFilter = builder.rowFilter;
    this.responsePartitioning = builder.responsePartitioning;
    this.wideToNarrow = builder.wideToNarrow;
    this.documentSource = builder.documentSource;
    this.sourceType = builder.sourceType != null ? builder.sourceType : determineSourceType(builder);
  }

  private static String determineSourceType(Builder builder) {
    if (builder.documentSource != null && builder.documentSource.isEnabled()) {
      return "document";
    } else if (builder.bulkDownload != null && !builder.bulkDownload.isEmpty()) {
      return "bulkDownload";
    } else {
      return "http";
    }
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

  public RawCacheConfig getRawCache() {
    return rawCache;
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
   * Returns the response partitioning configuration.
   *
   * <p>When enabled, rows from the response are grouped by the specified fields
   * and written to separate partitions. This allows a single bulk API call to
   * produce multiple output partitions.
   *
   * @return Response partitioning config, or null if not configured
   */
  public ResponsePartitioningConfig getResponsePartitioning() {
    return responsePartitioning;
  }

  /**
   * Returns true if response partitioning is configured.
   */
  public boolean hasResponsePartitioning() {
    return responsePartitioning != null && responsePartitioning.isEnabled();
  }

  /**
   * Returns true if row filtering is configured.
   */
  public boolean hasRowFilter() {
    return rowFilter != null && rowFilter.isEnabled();
  }

  /**
   * Returns the wide-to-narrow transformation configuration.
   *
   * <p>When configured, CSV data in wide format (time periods as columns) is
   * transformed to narrow format (time period as a row value) during parsing.
   *
   * @return Wide-to-narrow config, or null if not configured
   */
  public WideToNarrowConfig getWideToNarrow() {
    return wideToNarrow;
  }

  /**
   * Returns true if wide-to-narrow transformation is configured.
   */
  public boolean hasWideToNarrow() {
    return wideToNarrow != null && wideToNarrow.isEnabled();
  }

  /**
   * Returns the document source configuration for document-based ETL.
   *
   * <p>Document sources are used for SEC EDGAR filings where documents
   * (XBRL, HTML) are downloaded and multiple tables are extracted from
   * each document.
   *
   * @return Document source config, or null if not configured
   */
  public DocumentSourceConfig getDocumentSource() {
    return documentSource;
  }

  /**
   * Returns true if this is a document source configuration.
   */
  public boolean isDocumentSource() {
    return "document".equals(sourceType)
        || (documentSource != null && documentSource.isEnabled());
  }

  /**
   * Returns the source type: "http", "document", or "bulkDownload".
   */
  public String getSourceType() {
    return sourceType;
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

    Object rawCacheObj = map.get("rawCache");
    if (rawCacheObj instanceof Map) {
      builder.rawCache(RawCacheConfig.fromMap((Map<String, Object>) rawCacheObj));
    }

    Object responsePartitioningObj = map.get("responsePartitioning");
    if (responsePartitioningObj instanceof Map) {
      builder.responsePartitioning(
          ResponsePartitioningConfig.fromMap((Map<String, Object>) responsePartitioningObj));
    }

    Object wideToNarrowObj = map.get("wideToNarrow");
    if (wideToNarrowObj instanceof Map) {
      builder.wideToNarrow(WideToNarrowConfig.fromMap((Map<String, Object>) wideToNarrowObj));
    }

    // Parse source type
    Object typeObj = map.get("type");
    if (typeObj instanceof String) {
      builder.sourceType((String) typeObj);
    }

    // Parse document source configuration
    // Document source can be specified inline (metadataUrl, documentUrl, etc.)
    // or as a nested "documentSource" block
    if ("document".equals(typeObj)) {
      // Build DocumentSourceConfig from top-level fields
      builder.documentSource(DocumentSourceConfig.fromMap(map));
    } else {
      Object documentSourceObj = map.get("documentSource");
      if (documentSourceObj instanceof Map) {
        builder.documentSource(DocumentSourceConfig.fromMap((Map<String, Object>) documentSourceObj));
      }
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
   * Raw response cache configuration for persistent storage.
   *
   * <p>Unlike the in-memory cache ({@link CacheConfig}), raw cache stores
   * API responses in the storage provider (S3, local filesystem) for:
   * <ul>
   *   <li>Persistence across runs - no re-download if cache exists</li>
   *   <li>Self-healing - re-process from raw data without API calls</li>
   *   <li>Data lineage - preserve original API responses</li>
   * </ul>
   *
   * <h3>Cache Path Structure</h3>
   * <pre>
   * {basePath}/.raw/{tableName}/{partitionKey}/response.json
   * </pre>
   */
  public static class RawCacheConfig {
    private final boolean enabled;

    private RawCacheConfig(boolean enabled) {
      this.enabled = enabled;
    }

    public static RawCacheConfig defaults() {
      return new RawCacheConfig(false);
    }

    public static RawCacheConfig enabled() {
      return new RawCacheConfig(true);
    }

    public boolean isEnabled() {
      return enabled;
    }

    @SuppressWarnings("unchecked")
    public static RawCacheConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return defaults();
      }

      boolean enabled = false;
      Object enabledObj = map.get("enabled");
      if (enabledObj instanceof Boolean) {
        enabled = (Boolean) enabledObj;
      } else if (enabledObj instanceof String) {
        enabled = "true".equalsIgnoreCase((String) enabledObj);
      }

      return new RawCacheConfig(enabled);
    }
  }

  /**
   * Configuration for partitioning response data by fields extracted from the response.
   *
   * <p>Response partitioning allows a single bulk API call to be split into multiple
   * output partitions based on values in the response data. This is useful when an API
   * returns data for multiple dimensions (e.g., all countries, all years) in a single
   * response, but you want to partition the output by those dimensions.
   *
   * <h3>Example: World Bank API</h3>
   * <pre>{@code
   * source:
   *   url: "https://api.worldbank.org/v2/country/all/indicator/{indicator}"
   *   parameters:
   *     per_page: "20000"
   *     date: "{startYear}:{endYear}"
   *   responsePartitioning:
   *     fields:
   *       country_code: "countryiso3code"   # Extract country from response
   *       year: "date"                       # Extract year from response
   * }</pre>
   *
   * <p>When response partitioning is enabled:
   * <ol>
   *   <li>The API is called once with URL dimensions (e.g., just indicator)</li>
   *   <li>Response rows are grouped by the extracted partition field values</li>
   *   <li>Each group is written separately with partition variables set</li>
   * </ol>
   */
  public static class ResponsePartitioningConfig {
    private final Map<String, String> fields;
    private final String yearField;
    private final int yearStart;
    private final int yearEnd;

    private ResponsePartitioningConfig(Map<String, String> fields,
        String yearField, int yearStart, int yearEnd) {
      this.fields = fields != null
          ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(fields))
          : Collections.<String, String>emptyMap();
      this.yearField = yearField;
      this.yearStart = yearStart;
      this.yearEnd = yearEnd;
    }

    /**
     * Creates a ResponsePartitioningConfig from a map.
     *
     * <p>Expected structure:
     * <pre>{@code
     * responsePartitioning:
     *   fields:
     *     partition_column: "source_field"
     *   yearFilter:
     *     field: "date"
     *     start: 2000
     *     end: 2025
     * }</pre>
     */
    @SuppressWarnings("unchecked")
    public static ResponsePartitioningConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }

      Map<String, String> fields = new LinkedHashMap<String, String>();
      Object fieldsObj = map.get("fields");
      if (fieldsObj instanceof Map) {
        for (Map.Entry<?, ?> e : ((Map<?, ?>) fieldsObj).entrySet()) {
          fields.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
        }
      }

      if (fields.isEmpty()) {
        return null;
      }

      // Parse year filter if present
      String yearField = null;
      int yearStart = 0;
      int yearEnd = Integer.MAX_VALUE;

      Object yearFilterObj = map.get("yearFilter");
      if (yearFilterObj instanceof Map) {
        Map<?, ?> yearFilter = (Map<?, ?>) yearFilterObj;
        yearField = (String) yearFilter.get("field");
        Object startObj = yearFilter.get("start");
        Object endObj = yearFilter.get("end");

        if (startObj instanceof Number) {
          yearStart = ((Number) startObj).intValue();
        } else if (startObj instanceof String) {
          yearStart = VariableResolver.resolveInteger((String) startObj);
        }

        if (endObj instanceof Number) {
          yearEnd = ((Number) endObj).intValue();
        } else if (endObj instanceof String) {
          String endStr = (String) endObj;
          if ("current".equalsIgnoreCase(endStr)) {
            yearEnd = java.time.Year.now().getValue();
          } else {
            yearEnd = VariableResolver.resolveInteger(endStr);
          }
        }
      }

      return new ResponsePartitioningConfig(fields, yearField, yearStart, yearEnd);
    }

    /**
     * Returns the mapping from partition column names to source field names.
     *
     * <p>Keys are the output partition column names (e.g., "country_code").
     * Values are the source field names in the response data (e.g., "countryiso3code").
     *
     * @return Partition field mappings, never null
     */
    public Map<String, String> getFields() {
      return fields;
    }

    /**
     * Returns true if response partitioning is enabled.
     */
    public boolean isEnabled() {
      return !fields.isEmpty();
    }

    /**
     * Returns true if year filtering is enabled.
     */
    public boolean hasYearFilter() {
      return yearField != null && !yearField.isEmpty();
    }

    /**
     * Returns the source field name for year filtering.
     */
    public String getYearField() {
      return yearField;
    }

    /**
     * Returns the minimum year to include (inclusive).
     */
    public int getYearStart() {
      return yearStart;
    }

    /**
     * Returns the maximum year to include (inclusive).
     */
    public int getYearEnd() {
      return yearEnd;
    }

    /**
     * Checks if a year value is within the configured range.
     *
     * @param yearValue The year value from the response (as string)
     * @return true if within range or no filter configured
     */
    public boolean isYearInRange(Object yearValue) {
      if (!hasYearFilter() || yearValue == null) {
        return true;
      }
      try {
        int year;
        if (yearValue instanceof Number) {
          year = ((Number) yearValue).intValue();
        } else {
          year = Integer.parseInt(String.valueOf(yearValue).trim());
        }
        return year >= yearStart && year <= yearEnd;
      } catch (NumberFormatException e) {
        return false;  // Skip rows with invalid year values
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ResponsePartitioningConfig{fields=");
      sb.append(fields);
      if (hasYearFilter()) {
        sb.append(", yearFilter={field=").append(yearField)
            .append(", range=").append(yearStart).append("-").append(yearEnd).append("}");
      }
      sb.append("}");
      return sb.toString();
    }
  }

  /**
   * Configuration for transforming wide-format CSV to narrow format.
   *
   * <p>Many bulk data files (like BEA Regional data) use wide format where time periods
   * (years, quarters) are stored as column headers. This config transforms such data
   * into a normalized narrow format with separate rows for each time period.
   *
   * <h3>BEA Example</h3>
   * <pre>
   * Wide format (input):
   * GeoFIPS,GeoName,TableName,LineCode,Description,Unit,1929,1930,...,2024
   * 00000,United States,SAINC1,1,Personal income,Millions,85151.0,76394.0,...,12345.0
   *
   * Narrow format (output):
   * GeoFIPS,GeoName,TableName,LineCode,Description,Unit,Year,DataValue
   * 00000,United States,SAINC1,1,Personal income,Millions,1929,85151.0
   * 00000,United States,SAINC1,1,Personal income,Millions,1930,76394.0
   * ...
   * </pre>
   *
   * <h3>YAML Configuration</h3>
   * <pre>{@code
   * wideToNarrow:
   *   keyColumns: [GeoFIPS, GeoName, TableName, LineCode, Description, Unit]
   *   valueColumnPattern: "^\\d{4}$"  # Match 4-digit year columns
   *   keyColumnName: Year             # Name for unpivoted key column
   *   valueColumnName: DataValue      # Name for unpivoted value column
   *   skipValues: ["(NA)", "(D)", ""]  # Values to skip during unpivot
   * }</pre>
   */
  public static class WideToNarrowConfig {
    private final java.util.List<String> keyColumns;
    private final String valueColumnPattern;
    private final String keyColumnName;
    private final String valueColumnName;
    private final java.util.Set<String> skipValues;
    private final java.util.Map<String, String> columnMapping;

    private WideToNarrowConfig(java.util.List<String> keyColumns, String valueColumnPattern,
        String keyColumnName, String valueColumnName, java.util.Set<String> skipValues,
        java.util.Map<String, String> columnMapping) {
      this.keyColumns = keyColumns != null
          ? Collections.unmodifiableList(new java.util.ArrayList<String>(keyColumns))
          : Collections.<String>emptyList();
      this.valueColumnPattern = valueColumnPattern;
      this.keyColumnName = keyColumnName != null ? keyColumnName : "Key";
      this.valueColumnName = valueColumnName != null ? valueColumnName : "Value";
      this.skipValues = skipValues != null
          ? Collections.unmodifiableSet(new java.util.HashSet<String>(skipValues))
          : Collections.<String>emptySet();
      this.columnMapping = columnMapping != null
          ? Collections.unmodifiableMap(new java.util.LinkedHashMap<String, String>(columnMapping))
          : Collections.<String, String>emptyMap();
    }

    /**
     * Creates a WideToNarrowConfig from a map.
     */
    @SuppressWarnings("unchecked")
    public static WideToNarrowConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }

      java.util.List<String> keyColumns = new java.util.ArrayList<String>();
      Object keyColumnsObj = map.get("keyColumns");
      if (keyColumnsObj instanceof java.util.List) {
        for (Object item : (java.util.List<?>) keyColumnsObj) {
          keyColumns.add(String.valueOf(item));
        }
      }

      if (keyColumns.isEmpty()) {
        return null;  // keyColumns is required
      }

      String valueColumnPattern = (String) map.get("valueColumnPattern");
      String keyColumnName = (String) map.get("keyColumnName");
      String valueColumnName = (String) map.get("valueColumnName");

      java.util.Set<String> skipValues = new java.util.HashSet<String>();
      Object skipValuesObj = map.get("skipValues");
      if (skipValuesObj instanceof java.util.List) {
        for (Object item : (java.util.List<?>) skipValuesObj) {
          skipValues.add(String.valueOf(item));
        }
      }

      // Parse columnMapping: source column name -> output column name
      java.util.Map<String, String> columnMapping = new java.util.LinkedHashMap<String, String>();
      Object columnMappingObj = map.get("columnMapping");
      if (columnMappingObj instanceof Map) {
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) columnMappingObj).entrySet()) {
          columnMapping.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
      }

      return new WideToNarrowConfig(keyColumns, valueColumnPattern, keyColumnName,
          valueColumnName, skipValues, columnMapping);
    }

    /**
     * Returns the list of columns to keep as-is (not unpivoted).
     */
    public java.util.List<String> getKeyColumns() {
      return keyColumns;
    }

    /**
     * Returns the regex pattern to identify value columns (columns to unpivot).
     * If null, all columns not in keyColumns are treated as value columns.
     */
    public String getValueColumnPattern() {
      return valueColumnPattern;
    }

    /**
     * Returns the name for the new key column (e.g., "Year").
     */
    public String getKeyColumnName() {
      return keyColumnName;
    }

    /**
     * Returns the name for the new value column (e.g., "DataValue").
     */
    public String getValueColumnName() {
      return valueColumnName;
    }

    /**
     * Returns true if this config is valid and enabled.
     */
    public boolean isEnabled() {
      return !keyColumns.isEmpty();
    }

    /**
     * Returns the set of values to skip during unpivot.
     * Empty values are always skipped regardless of this setting.
     */
    public java.util.Set<String> getSkipValues() {
      return skipValues;
    }

    /**
     * Checks if a value should be skipped during unpivot.
     *
     * @param value The value to check
     * @return true if the value is empty or in the skipValues set
     */
    public boolean shouldSkipValue(String value) {
      return value == null || value.isEmpty() || skipValues.contains(value);
    }

    /**
     * Checks if a column name matches the value column pattern.
     *
     * @param columnName The column name to check
     * @return true if this column should be unpivoted as a value column
     */
    public boolean isValueColumn(String columnName) {
      if (valueColumnPattern == null || valueColumnPattern.isEmpty()) {
        // If no pattern, all non-key columns are value columns
        return !keyColumns.contains(columnName);
      }
      return columnName.matches(valueColumnPattern);
    }

    /**
     * Returns the column mapping (source name -> output name).
     * Used to rename columns during the wide-to-narrow transformation.
     *
     * @return Immutable map of source column names to output column names
     */
    public java.util.Map<String, String> getColumnMapping() {
      return columnMapping;
    }

    /**
     * Returns the output column name for a given source column.
     * If no mapping exists, returns the source column name unchanged.
     *
     * @param sourceColumn The source column name from the CSV
     * @return The output column name to use in the transformed data
     */
    public String getOutputColumnName(String sourceColumn) {
      return columnMapping.getOrDefault(sourceColumn, sourceColumn);
    }

    /**
     * Returns true if column mapping is configured.
     */
    public boolean hasColumnMapping() {
      return !columnMapping.isEmpty();
    }

    @Override
    public String toString() {
      return "WideToNarrowConfig{keyColumns=" + keyColumns
          + ", valueColumnPattern='" + valueColumnPattern + "'"
          + ", keyColumnName='" + keyColumnName + "'"
          + ", valueColumnName='" + valueColumnName + "'"
          + ", columnMapping=" + columnMapping + "}";
    }
  }

  /**
   * Document source configuration for document-based ETL.
   *
   * <p>Document sources download filing documents (XBRL, HTML, XML) and extract
   * multiple tables from each document. This is used for SEC EDGAR filings where
   * a single 10-K document produces financial_line_items, filing_contexts,
   * mda_sections, and other tables.
   *
   * <h3>YAML Configuration Example</h3>
   * <pre>{@code
   * source:
   *   type: document
   *   metadataUrl: "https://data.sec.gov/submissions/CIK{cik}.json"
   *   documentUrl: "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"
   *   documentTypes:
   *     - "*.xml"
   *     - "*.htm"
   *   extractionType: xbrl_facts
   *   documentConverter: "org.apache.calcite.adapter.govdata.sec.XbrlToParquetConverter"
   *   responseTransformer: "org.apache.calcite.adapter.govdata.sec.EdgarResponseTransformer"
   * }</pre>
   */
  public static class DocumentSourceConfig {
    private final String metadataUrl;
    private final String documentUrl;
    private final java.util.List<String> documentTypes;
    private final String extractionType;
    private final String documentConverter;
    private final String responseTransformer;
    private final java.util.List<String> extractionStrategies;
    private final java.util.List<String> itemFilter;
    private final EmbeddingConfig embeddingConfig;
    private final Integer startYear;
    private final Integer endYear;

    private DocumentSourceConfig(String metadataUrl, String documentUrl,
        java.util.List<String> documentTypes, String extractionType,
        String documentConverter, String responseTransformer,
        java.util.List<String> extractionStrategies, java.util.List<String> itemFilter,
        EmbeddingConfig embeddingConfig, Integer startYear, Integer endYear) {
      this.metadataUrl = metadataUrl;
      this.documentUrl = documentUrl;
      this.documentTypes = documentTypes != null
          ? Collections.unmodifiableList(new java.util.ArrayList<String>(documentTypes))
          : Collections.<String>emptyList();
      this.extractionType = extractionType;
      this.documentConverter = documentConverter;
      this.responseTransformer = responseTransformer;
      this.extractionStrategies = extractionStrategies != null
          ? Collections.unmodifiableList(new java.util.ArrayList<String>(extractionStrategies))
          : Collections.<String>emptyList();
      this.itemFilter = itemFilter != null
          ? Collections.unmodifiableList(new java.util.ArrayList<String>(itemFilter))
          : Collections.<String>emptyList();
      this.embeddingConfig = embeddingConfig;
      this.startYear = startYear;
      this.endYear = endYear;
    }

    @SuppressWarnings("unchecked")
    public static DocumentSourceConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }
      String metadataUrl = (String) map.get("metadataUrl");
      String documentUrl = (String) map.get("documentUrl");
      String extractionType = (String) map.get("extractionType");
      String documentConverter = (String) map.get("documentConverter");
      String responseTransformer = (String) map.get("responseTransformer");

      java.util.List<String> documentTypes = null;
      Object docTypesObj = map.get("documentTypes");
      if (docTypesObj instanceof java.util.List) {
        documentTypes = new java.util.ArrayList<String>();
        for (Object item : (java.util.List<?>) docTypesObj) {
          documentTypes.add(String.valueOf(item));
        }
      }

      java.util.List<String> extractionStrategies = null;
      Object strategiesObj = map.get("extractionStrategies");
      if (strategiesObj instanceof java.util.List) {
        extractionStrategies = new java.util.ArrayList<String>();
        for (Object item : (java.util.List<?>) strategiesObj) {
          extractionStrategies.add(String.valueOf(item));
        }
      }

      java.util.List<String> itemFilter = null;
      Object filterObj = map.get("itemFilter");
      if (filterObj instanceof java.util.List) {
        itemFilter = new java.util.ArrayList<String>();
        for (Object item : (java.util.List<?>) filterObj) {
          itemFilter.add(String.valueOf(item));
        }
      }

      EmbeddingConfig embeddingConfig = null;
      Object embeddingObj = map.get("embeddingConfig");
      if (embeddingObj instanceof Map) {
        embeddingConfig = EmbeddingConfig.fromMap((Map<String, Object>) embeddingObj);
      }

      Integer startYear = null;
      Object startYearObj = map.get("startYear");
      if (startYearObj instanceof Number) {
        startYear = ((Number) startYearObj).intValue();
      }

      Integer endYear = null;
      Object endYearObj = map.get("endYear");
      if (endYearObj instanceof Number) {
        endYear = ((Number) endYearObj).intValue();
      }

      return new DocumentSourceConfig(metadataUrl, documentUrl, documentTypes,
          extractionType, documentConverter, responseTransformer,
          extractionStrategies, itemFilter, embeddingConfig, startYear, endYear);
    }

    /**
     * Returns the URL pattern for fetching filing metadata.
     * Supports variable substitution like {cik} for dimension values.
     */
    public String getMetadataUrl() {
      return metadataUrl;
    }

    /**
     * Returns the URL pattern for fetching document files.
     * Supports variables like {cik}, {accession}, {document}.
     */
    public String getDocumentUrl() {
      return documentUrl;
    }

    /**
     * Returns the list of document file patterns to process.
     * For example: ["*.xml", "*.htm", "*_htm.xml"]
     */
    public java.util.List<String> getDocumentTypes() {
      return documentTypes;
    }

    /**
     * Returns the type of extraction to perform.
     * Values: metadata, xbrl_facts, xbrl_contexts, xbrl_linkbases,
     *         mda_text, insider_html_tables, earnings_text, vectorization
     */
    public String getExtractionType() {
      return extractionType;
    }

    /**
     * Returns the fully qualified class name of the document converter.
     */
    public String getDocumentConverter() {
      return documentConverter;
    }

    /**
     * Returns the fully qualified class name of the response transformer
     * for metadata API responses.
     */
    public String getResponseTransformer() {
      return responseTransformer;
    }

    /**
     * Returns the list of extraction strategies for text extraction.
     * For MD&A: ["regex_item7", "direct_search", "html_element_specific", "aggressive_fallback"]
     */
    public java.util.List<String> getExtractionStrategies() {
      return extractionStrategies;
    }

    /**
     * Returns the list of item filters for 8-K filings.
     * For example: ["2.02"] to filter for Item 2.02 (Results of Operations).
     */
    public java.util.List<String> getItemFilter() {
      return itemFilter;
    }

    /**
     * Returns the embedding configuration for vectorization.
     */
    public EmbeddingConfig getEmbeddingConfig() {
      return embeddingConfig;
    }

    /**
     * Returns the start year for filtering documents (inclusive).
     * Returns null if no year filtering is configured.
     */
    public Integer getStartYear() {
      return startYear;
    }

    /**
     * Returns the end year for filtering documents (inclusive).
     * Returns null if no year filtering is configured.
     */
    public Integer getEndYear() {
      return endYear;
    }

    /**
     * Returns true if this is a valid document source configuration.
     */
    public boolean isEnabled() {
      return (metadataUrl != null && !metadataUrl.isEmpty())
          || (documentUrl != null && !documentUrl.isEmpty());
    }

    @Override
    public String toString() {
      return "DocumentSourceConfig{metadataUrl='" + metadataUrl
          + "', documentUrl='" + documentUrl
          + "', extractionType='" + extractionType
          + "', documentTypes=" + documentTypes + "}";
    }
  }

  /**
   * Embedding configuration for text vectorization.
   */
  public static class EmbeddingConfig {
    private final String provider;
    private final String model;
    private final int dimension;
    private final int maxTextLength;

    private EmbeddingConfig(String provider, String model, int dimension, int maxTextLength) {
      this.provider = provider != null ? provider : "duckdb_quackformers";
      this.model = model != null ? model : "sentence-transformers/all-MiniLM-L6-v2";
      this.dimension = dimension > 0 ? dimension : 256;
      this.maxTextLength = maxTextLength > 0 ? maxTextLength : 8192;
    }

    public static EmbeddingConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }
      String provider = (String) map.get("provider");
      String model = (String) map.get("model");
      int dimension = 256;
      Object dimObj = map.get("dimension");
      if (dimObj instanceof Number) {
        dimension = ((Number) dimObj).intValue();
      }
      int maxTextLength = 8192;
      Object maxTextObj = map.get("maxTextLength");
      if (maxTextObj instanceof Number) {
        maxTextLength = ((Number) maxTextObj).intValue();
      }
      return new EmbeddingConfig(provider, model, dimension, maxTextLength);
    }

    public String getProvider() {
      return provider;
    }

    public String getModel() {
      return model;
    }

    public int getDimension() {
      return dimension;
    }

    public int getMaxTextLength() {
      return maxTextLength;
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
    private RawCacheConfig rawCache;
    private String bulkDownload;
    private String extractPattern;
    private RowFilterConfig rowFilter;
    private ResponsePartitioningConfig responsePartitioning;
    private WideToNarrowConfig wideToNarrow;
    private DocumentSourceConfig documentSource;
    private String sourceType;

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

    public Builder rawCache(RawCacheConfig rawCache) {
      this.rawCache = rawCache;
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

    public Builder responsePartitioning(ResponsePartitioningConfig responsePartitioning) {
      this.responsePartitioning = responsePartitioning;
      return this;
    }

    public Builder wideToNarrow(WideToNarrowConfig wideToNarrow) {
      this.wideToNarrow = wideToNarrow;
      return this;
    }

    public Builder documentSource(DocumentSourceConfig documentSource) {
      this.documentSource = documentSource;
      return this;
    }

    public Builder sourceType(String sourceType) {
      this.sourceType = sourceType;
      return this;
    }

    public HttpSourceConfig build() {
      // Either url, bulkDownload, or documentSource must be set
      boolean hasUrl = url != null && !url.isEmpty();
      boolean hasBulkDownload = bulkDownload != null && !bulkDownload.isEmpty();
      boolean hasDocumentSource = documentSource != null && documentSource.isEnabled();
      if (!hasUrl && !hasBulkDownload && !hasDocumentSource) {
        throw new IllegalArgumentException(
            "Either URL, bulkDownload, or documentSource is required");
      }
      return new HttpSourceConfig(this);
    }
  }
}
