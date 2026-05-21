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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * HTTP data source that fetches data from REST APIs.
 *
 * <p>HttpSource implements the {@link DataSource} interface to fetch data
 * from HTTP/REST APIs with support for:
 * <ul>
 *   <li>Variable substitution in URL, parameters, headers, and request body</li>
 *   <li>Environment variable references ({@code {env:VAR_NAME}})</li>
 *   <li>POST/PUT request bodies (JSON or form-urlencoded)</li>
 *   <li>Pagination (offset, cursor, page-based)</li>
 *   <li>Rate limiting with exponential backoff</li>
 *   <li>Response caching</li>
 *   <li>JSONPath data extraction</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * HttpSourceConfig config = HttpSourceConfig.builder()
 *     .url("https://api.example.com/data")
 *     .method(HttpMethod.GET)
 *     .parameters(Collections.singletonMap("year", "{year}"))
 *     .build();
 *
 * HttpSource source = new HttpSource(config);
 * Iterator<Map<String, Object>> data = source.fetch(Collections.singletonMap("year", "2024"));
 * }</pre>
 *
 * @see HttpSourceConfig
 * @see DataSource
 */
public class HttpSource implements DataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Pattern VAR_PATTERN = Pattern.compile("\\{([^}]+)\\}");
  private static final Pattern ENV_PATTERN = Pattern.compile("env:(.+)");

  private final HttpSourceConfig config;
  private final Map<String, CacheEntry> cache;
  private final ResponseTransformer responseTransformer;
  private final VariableNormalizer variableNormalizer;
  private final StorageProvider storageProvider;
  private final String rawCachePath;
  /** Local filesystem path for raw cache, used instead of S3 to reduce API calls. */
  private final String localRawCachePath;
  /** Operating directory from model operands (e.g., .aperio/<schema>), used for local cache base. */
  private final String operatingDirectory;
  /** CAS-based slot reservation for lock-free rate limiting across parallel threads. */
  private final AtomicLong nextAllowedNanos = new AtomicLong();

  /**
   * Creates a new HttpSource with the given configuration.
   *
   * @param config HTTP source configuration
   */
  public HttpSource(HttpSourceConfig config) {
    this(config, (HooksConfig) null, null, null, null);
  }

  /**
   * Creates a new HttpSource with configuration and hooks.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig) {
    this(config, hooksConfig, null, null, null);
  }

  /**
   * Creates a new HttpSource with configuration, hooks, and storage provider for raw caching.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   * @param storageProvider Storage provider for raw response caching (S3, local, etc.)
   * @param rawCachePath Base path for raw response cache (e.g., s3://bucket/.raw)
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig,
      StorageProvider storageProvider, String rawCachePath) {
    this(config, hooksConfig, storageProvider, rawCachePath, null);
  }

  /**
   * Creates a new HttpSource with configuration, hooks, storage provider, and operating directory.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   * @param storageProvider Storage provider for raw response caching (S3, local, etc.)
   * @param rawCachePath Base path for raw response cache (e.g., s3://bucket/.raw)
   * @param operatingDirectory Operating directory for local cache (e.g., .aperio/schema); may be null
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig,
      StorageProvider storageProvider, String rawCachePath, String operatingDirectory) {
    this.config = config;
    this.cache = config.getCache().isEnabled()
        ? new ConcurrentHashMap<String, CacheEntry>()
        : null;
    // Rate limiting uses AtomicLong nextAllowedNanos (CAS-based, no init needed)
    this.responseTransformer = loadResponseTransformer(hooksConfig);
    this.variableNormalizer = loadVariableNormalizer(hooksConfig);
    this.storageProvider = storageProvider;
    this.rawCachePath = rawCachePath;
    this.operatingDirectory = operatingDirectory;
    this.localRawCachePath = computeLocalRawCachePath(rawCachePath);
  }

  /**
   * Creates a new HttpSource with configuration and explicit response transformer.
   *
   * @param config HTTP source configuration
   * @param responseTransformer Response transformer instance
   */
  public HttpSource(HttpSourceConfig config, ResponseTransformer responseTransformer) {
    this.config = config;
    this.cache = config.getCache().isEnabled()
        ? new ConcurrentHashMap<String, CacheEntry>()
        : null;
    // Rate limiting uses AtomicLong nextAllowedNanos (CAS-based, no init needed)
    this.responseTransformer = responseTransformer;
    this.variableNormalizer = null;
    this.storageProvider = null;
    this.rawCachePath = null;
    this.operatingDirectory = null;
    this.localRawCachePath = null;
  }

  /**
   * Loads a ResponseTransformer from HooksConfig.
   */
  private ResponseTransformer loadResponseTransformer(HooksConfig hooksConfig) {
    if (hooksConfig == null || hooksConfig.getResponseTransformerClass() == null) {
      return null;
    }

    String className = hooksConfig.getResponseTransformerClass();
    try {
      Class<?> clazz = Class.forName(className);
      if (!ResponseTransformer.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + className + " does not implement ResponseTransformer");
      }
      return (ResponseTransformer) clazz.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("ResponseTransformer class not found: " + className, e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to instantiate ResponseTransformer: " + className, e);
    }
  }

  /**
   * Loads a VariableNormalizer from HooksConfig.
   *
   * <p>Tries to instantiate using a Map constructor if config is provided,
   * otherwise falls back to the default constructor.
   */
  private VariableNormalizer loadVariableNormalizer(HooksConfig hooksConfig) {
    if (hooksConfig == null || hooksConfig.getVariableNormalizerClass() == null) {
      return null;
    }

    String className = hooksConfig.getVariableNormalizerClass();
    Map<String, Object> config = hooksConfig.getVariableNormalizerConfig();

    try {
      Class<?> clazz = Class.forName(className);
      if (!VariableNormalizer.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + className + " does not implement VariableNormalizer");
      }

      // Try constructor with Map config first if config is provided
      if (config != null && !config.isEmpty()) {
        try {
          return (VariableNormalizer) clazz
              .getDeclaredConstructor(Map.class)
              .newInstance(config);
        } catch (NoSuchMethodException e) {
          // Fall through to default constructor
          LOGGER.debug("No Map constructor for {}, using default", className);
        }
      }

      // Fall back to default constructor
      return (VariableNormalizer) clazz.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("VariableNormalizer class not found: " + className, e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to instantiate VariableNormalizer: " + className, e);
    }
  }

  /**
   * Normalizes field names in parsed records using the configured VariableNormalizer.
   *
   * <p>This method is called after parsing the API response but before returning
   * records. It enables schema evolution by mapping API-specific field names
   * to consistent conceptual names.
   *
   * @param records Parsed records with original field names
   * @param context Dimension values providing context for normalization
   * @return Records with normalized field names
   */
  private List<Map<String, Object>> normalizeRecords(
      List<Map<String, Object>> records, Map<String, String> context) {
    if (variableNormalizer == null || records.isEmpty()) {
      return records;
    }

    List<Map<String, Object>> normalized = new ArrayList<Map<String, Object>>(records.size());
    for (Map<String, Object> record : records) {
      Map<String, Object> normalizedRecord = new LinkedHashMap<String, Object>();
      for (Map.Entry<String, Object> entry : record.entrySet()) {
        String fieldName = entry.getKey();
        String normalizedName;

        if (variableNormalizer.shouldPreserve(fieldName)) {
          normalizedName = fieldName;
        } else {
          normalizedName = variableNormalizer.normalize(fieldName, context);
          if (normalizedName == null) {
            normalizedName = fieldName; // Fall back to original if no mapping
          }
        }

        normalizedRecord.put(normalizedName, entry.getValue());
      }
      normalized.add(normalizedRecord);
    }

    if (LOGGER.isDebugEnabled() && !records.isEmpty()) {
      LOGGER.debug("Normalized {} records using {}", records.size(),
          variableNormalizer.getClass().getSimpleName());
    }

    return normalized;
  }

  @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables) throws IOException {
    // Check if batching is configured - if so, use batched fetching
    if (config.hasBatching()) {
      return fetchWithBatching(variables);
    }

    // Make a mutable copy of variables so incremental bounds can be injected for transformers
    variables =
        new LinkedHashMap<String, String>(variables != null ? variables : Collections.<String, String>emptyMap());

    // Build the URL with variables substituted (check urlRules for year-dependent URLs)
    String url = substituteVariables(config.getEffectiveUrl(variables), variables);

    // Build query parameters
    Map<String, String> params = new LinkedHashMap<String, String>();
    for (Map.Entry<String, String> e : config.getParameters().entrySet()) {
      params.put(e.getKey(), substituteVariables(e.getValue(), variables));
    }

    // Apply incremental filter when configured and a bound is active
    HttpSourceConfig.IncrementalConfig incr = config.getIncremental();
    if (incr != null && incr.getFilterParam() != null) {
      String resolvedDate      =
          substituteVariables(incr.getSinceDate()    != null ? incr.getSinceDate()    : "", variables);
      String resolvedYear      =
          substituteVariables(incr.getSinceYear()    != null ? incr.getSinceYear()    : "", variables);
      String resolvedQuarter   =
          substituteVariables(incr.getSinceQuarter() != null ? incr.getSinceQuarter() : "", variables);
      String resolvedUntilDate =
          substituteVariables(incr.getUntilDate()    != null ? incr.getUntilDate()    : "", variables);
      String resolvedUntilYear =
          substituteVariables(incr.getUntilYear()    != null ? incr.getUntilYear()    : "", variables);
      String filterValue =
          incr.buildFilterValue(resolvedDate, resolvedYear, resolvedQuarter, resolvedUntilDate, resolvedUntilYear);
      if (filterValue != null) {
        params.put(incr.getFilterParam(), filterValue);
        // Expose bounds to transformers via RequestContext.dimensionValues
        if (!resolvedDate.isEmpty())      { variables.put("sinceDate", resolvedDate); }
        if (!resolvedYear.isEmpty())      { variables.put("sinceYear", resolvedYear); }
        if (!resolvedQuarter.isEmpty())   { variables.put("sinceQuarter", resolvedQuarter); }
        if (!resolvedUntilDate.isEmpty()) { variables.put("untilDate", resolvedUntilDate); }
        if (!resolvedUntilYear.isEmpty()) { variables.put("untilYear", resolvedUntilYear); }
        LOGGER.info("Incremental filter active: {}={}", incr.getFilterParam(), filterValue);
      }
    }

    // Streaming short-circuit: bypasses StringWriter pipeline entirely
    if (responseTransformer instanceof StreamingResponseTransformer) {
      RequestContext ctx = RequestContext.builder()
          .url(url)
          .parameters(params)
          .headers(config.getHeaders())
          .dimensionValues(variables)
          .build();
      return ((StreamingResponseTransformer) responseTransformer).fetchAndTransform(ctx);
    }

    // Check raw cache first (persistent storage-based)
    String rawCacheFilePath = null;
    if (isRawCacheEnabled()) {
      rawCacheFilePath = buildRawCachePath(variables);
      if (hasValidRawCache(rawCacheFilePath)) {
        HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
        if ((respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV
            || respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV)
            && responseTransformer == null) {
          char delimiter = resolveDelimiter(respConfig);
          LOGGER.info("Streaming CSV from raw cache: {}", rawCacheFilePath);
          return parseDelimitedResponseStreaming(rawCacheFilePath, delimiter);
        }
        // For CSV/TSV with a per-record transformer, stream rows and apply transformer per-row
        if ((respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV
            || respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV)
            && responseTransformer instanceof PerRecordResponseTransformer) {
          char delimiter = resolveDelimiter(respConfig);
          LOGGER.info("Streaming CSV with per-record transformer from raw cache: {}", rawCacheFilePath);
          return streamDelimitedFromRawCache(rawCacheFilePath, delimiter, url, params, variables,
              (PerRecordResponseTransformer) responseTransformer);
        }
        // For JSON with a per-record transformer, stream directly from cache file
        if (responseTransformer instanceof PerRecordResponseTransformer
            && respConfig.getFormat() == HttpSourceConfig.ResponseFormat.JSON) {
          return streamFromRawCache(rawCacheFilePath, url, params, variables,
              (PerRecordResponseTransformer) responseTransformer);
        }
        // For JSON, or CSV/TSV with a responseTransformer, read into memory and transform
        String cachedResponse = readRawCache(rawCacheFilePath);
        cachedResponse = transformResponse(cachedResponse, url, params, variables);
        List<Map<String, Object>> data = parseResponse(cachedResponse);
        data = normalizeRecords(data, variables);
        LOGGER.info("Fetched {} records from raw cache", data.size());
        return data.iterator();
      }
    }

    // Check in-memory cache if enabled
    String cacheKey = buildCacheKey(url, params);
    if (cache != null) {
      CacheEntry cached = cache.get(cacheKey);
      if (cached != null && !cached.isExpired()) {
        LOGGER.debug("Cache hit for {}", cacheKey);
        return cached.getData().iterator();
      }
    }

    HttpSourceConfig.PaginationConfig pagination = config.getResponse().getPagination();

    if (pagination.getType() == HttpSourceConfig.PaginationType.NONE) {
      // Single request - response is cached in doRequest, returns cache path
      String cachePath = executeRequest(url, params, variables, rawCacheFilePath);

      // For CSV/TSV without a transformer, stream directly from cache
      HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
      if ((respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV
          || respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV)
          && responseTransformer == null) {
        char delimiter = resolveDelimiter(respConfig);
        return parseDelimitedResponseStreaming(cachePath, delimiter);
      }
      // For CSV/TSV with a per-record transformer, stream rows and apply transformer per-row
      if ((respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV
          || respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV)
          && responseTransformer instanceof PerRecordResponseTransformer) {
        char delimiter = resolveDelimiter(respConfig);
        return streamDelimitedFromRawCache(cachePath, delimiter, url, params, variables,
            (PerRecordResponseTransformer) responseTransformer);
      }

      // For JSON, read from cache, transform, and parse
      // cachePath is "" when skipResponseBody=true; skip readFromCache in that case
      String content = cachePath.isEmpty() ? "" : readFromCache(cachePath);
      content = transformResponse(content, url, params, variables);
      List<Map<String, Object>> data = parseResponse(content);
      data = normalizeRecords(data, variables);

      if (cache != null) {
        long ttlMs = config.getCache().getTtlSeconds() * 1000;
        cache.put(cacheKey, new CacheEntry(data, System.currentTimeMillis() + ttlMs));
        LOGGER.debug("Cached {} records for {}", data.size(), cacheKey);
      }

      LOGGER.info("Fetched {} records from {}", data.size(), url);
      return data.iterator();
    } else {
      // Paginated requests - use streaming iterator to avoid buffering all pages in memory
      return new PaginatedIterator(url, params, variables, pagination, cacheKey, rawCacheFilePath);
    }
  }

  private class PaginatedIterator implements Iterator<Map<String, Object>> {
    private final String url;
    private final Map<String, String> baseParams;
    private final Map<String, String> variables;
    private final HttpSourceConfig.PaginationConfig pagination;
    private final String cacheKey;
    private final String rawCacheFilePath;

    private int offset = 0;
    private int pageSize;
    private String cursor = null;
    private boolean hasMore = true;
    private int totalCount = -1; // populated from API "count" field on first page

    private Iterator<Map<String, Object>> currentPageIterator = null;
    private long totalYielded = 0;

    // CSV_STREAM state
    private BufferedReader csvReader = null;
    private String csvHeaderLine = null;

    // Raw-cache accumulation: stream result records from each page to a temp file so
    // we never hold more than one page of Jackson nodes in heap at a time.
    private File tempCacheFile = null;
    private FileOutputStream tempCacheStream = null;
    private com.fasterxml.jackson.core.JsonGenerator cacheGenerator = null;
    private long cachedRecordCount = 0;
    private boolean mergedCacheWritten = false;

    PaginatedIterator(String url, Map<String, String> baseParams, Map<String, String> variables,
        HttpSourceConfig.PaginationConfig pagination, String cacheKey, String rawCacheFilePath) {
      this.url = url;
      this.baseParams = baseParams;
      this.variables = variables;
      this.pagination = pagination;
      this.pageSize = pagination.getPageSize();
      this.cacheKey = cacheKey;
      this.rawCacheFilePath = rawCacheFilePath;
    }

    @Override public boolean hasNext() {
      // If current page has more records, we have next
      if (currentPageIterator != null && currentPageIterator.hasNext()) {
        return true;
      }

      // If no more pages to fetch, we're done
      if (!hasMore) {
        return false;
      }

      // Try to fetch next page
      return fetchNextPage();
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Map<String, Object> record = currentPageIterator.next();
      totalYielded++;
      return record;
    }

    private boolean fetchNextPage() {
      if (!hasMore) {
        return false;
      }

      try {
        Map<String, String> pageParams = new LinkedHashMap<String, String>(baseParams);

        switch (pagination.getType()) {
          case OFFSET:
            pageParams.put(pagination.getLimitParam(), String.valueOf(pageSize));
            pageParams.put(pagination.getOffsetParam(), String.valueOf(offset));
            break;
          case PAGE:
            int page = (offset / pageSize) + 1;
            pageParams.put(pagination.getPageParam(), String.valueOf(page));
            if (pagination.getLimitParam() != null) {
              pageParams.put(pagination.getLimitParam(), String.valueOf(pageSize));
            }
            break;
          case PAGE_ZERO:
            int pageZero = offset / pageSize;
            pageParams.put(pagination.getPageParam(), String.valueOf(pageZero));
            if (pagination.getLimitParam() != null) {
              pageParams.put(pagination.getLimitParam(), String.valueOf(pageSize));
            }
            break;
          case CURSOR:
            if (cursor != null) {
              pageParams.put(pagination.getCursorParam(), cursor);
            }
            if (pagination.getLimitParam() != null) {
              pageParams.put(pagination.getLimitParam(), String.valueOf(pageSize));
            }
            break;
          case CSV_STREAM:
            return fetchNextCsvBatch();
          default:
            hasMore = false;
            return false;
        }

        String response;
        try {
          response = executeRequest(url, pageParams, variables, null);
        } catch (IOException e) {
          // HTTP 400 during pagination means skip/offset limit exceeded
          if (e.getMessage() != null && e.getMessage().startsWith("HTTP 400")) {
            LOGGER.info("Pagination stopped at offset={}: results window limit reached",
                offset);
            hasMore = false;
            writeMergedCache();
            return false;
          }
          throw e;
        }

        // Extract total record count from API response before transformation.
        // Required for APIs (e.g. Urban Institute) that wrap around and return page 1 data
        // for out-of-bounds page numbers — pageData.size() < pageSize never triggers without this.
        if (totalCount < 0) {
          try {
            String countBody = response;
            JsonNode countRoot = OBJECT_MAPPER.readTree(countBody);
            String countPath = pagination.getCountPath();
            JsonNode countNode;
            if (countPath != null && !countPath.isEmpty()) {
              countNode = countRoot;
              for (String segment : countPath.split("\\.")) {
                countNode = countNode.path(segment);
              }
            } else {
              countNode = countRoot.path("count");
            }
            if (!countNode.isMissingNode() && countNode.isNumber()) {
              totalCount = countNode.intValue();
              LOGGER.debug("Total record count from API: {}", totalCount);
            }
          } catch (Exception e) {
            LOGGER.debug("Could not extract count field from response: {}", e.getMessage());
          }
        }

        accumulateRawPage(response);
        String rawResponse = response;
        response = transformResponse(response, url, pageParams, variables);
        List<Map<String, Object>> pageData = parseResponse(response);

        if (pageData.isEmpty()) {
          hasMore = false;
          writeMergedCache();
          return false;
        }

        pageData = normalizeRecords(pageData, variables);
        currentPageIterator = pageData.iterator();

        // Handle pagination state for next fetch
        if (pagination.getType() == HttpSourceConfig.PaginationType.CURSOR) {
          try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(rawResponse);
            String cursorPath = pagination.getCursorPath();
            if (cursorPath != null && !cursorPath.isEmpty()) {
              JsonNode cursorNode = root.path(cursorPath);
              String nextCursor = cursorNode.asText(null);
              if (nextCursor == null || nextCursor.isEmpty()) {
                hasMore = false;
                writeMergedCache();
              } else {
                cursor = nextCursor;
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to extract cursor from response: {}", e.getMessage());
            hasMore = false;
          }
        } else {
          offset += pageSize;
          // When the API reports a total count, use it as the sole termination signal.
          // When no count is available, fall back to partial-page detection.
          boolean reachedEnd = totalCount >= 0
              ? offset >= totalCount
              : pageData.size() < pageSize;
          if (reachedEnd) {
            hasMore = false;
            writeMergedCache();
          }
        }

        LOGGER.debug("Fetched page with {} records (total yielded: {})", pageData.size(), totalYielded);
        return true;

      } catch (IOException e) {
        LOGGER.error("Error fetching paginated data: {}", e.getMessage());
        hasMore = false;
        return false;
      }
    }

    private boolean fetchNextCsvBatch() {
      try {
        if (csvReader == null) {
          // Open the streaming connection on first call
          enforceRateLimit();
          String fullUrl = buildUrlWithParams(url, baseParams);
          URL connUrl = java.net.URI.create(fullUrl).toURL();
          HttpURLConnection conn = (HttpURLConnection) connUrl.openConnection();
          conn.setRequestMethod("GET");
          conn.setConnectTimeout(30000);
          conn.setReadTimeout(300000); // 5 min for large CSV files
          conn.setRequestProperty("User-Agent",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36");
          for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
            conn.setRequestProperty(e.getKey(), e.getValue());
          }
          applyAuth(conn, variables);
          int status = conn.getResponseCode();
          if (status >= 400) {
            throw new IOException("HTTP " + status + " for CSV_STREAM: " + fullUrl);
          }
          InputStream is = conn.getInputStream();
          csvReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
          csvHeaderLine = csvReader.readLine();
          if (csvHeaderLine == null) {
            hasMore = false;
            return false;
          }
          LOGGER.info("CSV_STREAM opened: {}", fullUrl);
        }

        int batchSize = pageSize > 0 ? pageSize : 1000;
        StringBuilder batchSb = new StringBuilder();
        batchSb.append(csvHeaderLine).append("\n");
        int linesRead = 0;
        String line;
        while (linesRead < batchSize && (line = csvReader.readLine()) != null) {
          batchSb.append(line).append("\n");
          linesRead++;
        }

        if (linesRead == 0) {
          hasMore = false;
          csvReader.close();
          csvReader = null;
          return false;
        }

        String csvBatch = batchSb.toString();
        String jsonResponse = transformResponse(csvBatch, url, baseParams, variables);
        List<Map<String, Object>> pageData = parseResponse(jsonResponse);

        if (pageData.isEmpty()) {
          hasMore = false;
          return false;
        }

        pageData = normalizeRecords(pageData, variables);
        currentPageIterator = pageData.iterator();

        if (linesRead < batchSize) {
          hasMore = false;
          csvReader.close();
          csvReader = null;
        }

        LOGGER.debug("CSV_STREAM batch: {} records (total yielded: {})", pageData.size(), totalYielded);
        return true;

      } catch (IOException e) {
        LOGGER.error("Error in CSV_STREAM batch: {}", e.getMessage());
        hasMore = false;
        if (csvReader != null) {
          try { csvReader.close(); } catch (IOException closeEx) { LOGGER.debug("Failed to close CSV reader: {}", closeEx.getMessage()); }
          csvReader = null;
        }
        return false;
      }
    }

    private void accumulateRawPage(String rawResponse) {
      if (rawCacheFilePath == null) {
        return;
      }
      try {
        if (cacheGenerator == null) {
          File destParent = new File(rawCacheFilePath).getParentFile();
          if (destParent != null) {
            destParent.mkdirs();
          }
          tempCacheFile = File.createTempFile("http-raw-cache-", ".json", destParent);
          tempCacheFile.deleteOnExit();
          tempCacheStream = new FileOutputStream(tempCacheFile);
          cacheGenerator =
              OBJECT_MAPPER.getFactory().createGenerator(tempCacheStream, com.fasterxml.jackson.core.JsonEncoding.UTF8);
          cacheGenerator.writeStartObject();
          cacheGenerator.writeArrayFieldStart("results");
        }
        com.fasterxml.jackson.databind.JsonNode root = OBJECT_MAPPER.readTree(rawResponse);
        com.fasterxml.jackson.databind.JsonNode resultsNode = root.has("results")
            ? root.get("results") : root;
        if (resultsNode != null && resultsNode.isArray()) {
          for (com.fasterxml.jackson.databind.JsonNode record : resultsNode) {
            OBJECT_MAPPER.writeTree(cacheGenerator, record);
            cachedRecordCount++;
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to stream raw page to cache: {}", e.getMessage());
      }
    }

    private void writeMergedCache() {
      if (rawCacheFilePath == null || mergedCacheWritten || cacheGenerator == null) {
        return;
      }
      try {
        cacheGenerator.writeEndArray();
        cacheGenerator.writeEndObject();
        cacheGenerator.close();
        tempCacheStream.close();
        String parentPath = rawCacheFilePath.substring(0, rawCacheFilePath.lastIndexOf('/'));
        if (isLocalPath(rawCacheFilePath)) {
          File dest = new File(rawCacheFilePath);
          dest.getParentFile().mkdirs();
          java.nio.file.Files.copy(tempCacheFile.toPath(), dest.toPath(),
              java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } else {
          storageProvider.createDirectories(parentPath);
          try (java.io.InputStream in = new FileInputStream(tempCacheFile)) {
            storageProvider.writeFile(rawCacheFilePath, in);
          }
        }
        mergedCacheWritten = true;
        LOGGER.info("Wrote streaming merged cache: {} ({} records)",
            rawCacheFilePath, cachedRecordCount);
      } catch (Exception e) {
        LOGGER.warn("Failed to write merged cache: {}", e.getMessage());
      } finally {
        if (tempCacheFile != null && tempCacheFile.exists()) {
          tempCacheFile.delete();
        }
      }
    }
  }

  @Override public String getType() {
    return "http";
  }

  @Override public void close() {
    if (cache != null) {
      cache.clear();
    }
  }

  /**
   * Fetches data using batching - loads values from a catalog and makes
   * multiple requests, one per batch.
   *
   * @param variables Dimension variables for this batch
   * @return Iterator over all records from all batches
   */
  private Iterator<Map<String, Object>> fetchWithBatching(Map<String, String> variables)
      throws IOException {
    HttpSourceConfig.BatchConfig batching = config.getBatching();
    LOGGER.info("Fetching with batching: field={}, size={}", batching.getField(), batching.getSize());

    // Load all values from the JSON catalog
    List<String> allValues = loadBatchValues(batching.getSource(), batching.getPath());
    LOGGER.info("Loaded {} values from catalog {}", allValues.size(), batching.getSource());

    // Split into batches
    List<List<String>> batches = createBatches(allValues, batching.getSize());
    LOGGER.info("Split into {} batches of up to {} items", batches.size(), batching.getSize());

    // Fetch each batch
    List<Map<String, Object>> allData = new ArrayList<Map<String, Object>>();
    String url = substituteVariables(config.getEffectiveUrl(variables), variables);

    for (int i = 0; i < batches.size(); i++) {
      List<String> batch = batches.get(i);
      LOGGER.info("Processing batch {}/{} ({} items)", i + 1, batches.size(), batch.size());

      try {
        // Create a modified body with this batch's values
        Map<String, Object> batchBody = new LinkedHashMap<String, Object>(config.getBody());
        batchBody.put(batching.getField(), batch);

        // Build query parameters
        Map<String, String> params = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> e : config.getParameters().entrySet()) {
          params.put(e.getKey(), substituteVariables(e.getValue(), variables));
        }

        // Execute request with batch body
        String response = executeRequestWithBody(url, params, variables, batchBody);
        response = transformResponse(response, url, params, variables);
        List<Map<String, Object>> batchData = parseResponse(response);

        allData.addAll(batchData);
        LOGGER.debug("Batch {}/{} returned {} records", i + 1, batches.size(), batchData.size());

        // Rate limiting between batches
        if (i < batches.size() - 1 && batching.getDelayMs() > 0) {
          try {
            Thread.sleep(batching.getDelayMs());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during batch delay", e);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Batch {}/{} failed: {}", i + 1, batches.size(), e.getMessage());
        // Continue with remaining batches
      }
    }

    // Normalize field names for schema evolution
    allData = normalizeRecords(allData, variables);

    LOGGER.info("Batched fetch complete: {} total records from {} batches",
        allData.size(), batches.size());
    return allData.iterator();
  }

  /**
   * Loads batch values from a JSON catalog resource.
   */
  private List<String> loadBatchValues(String resourcePath, String path) throws IOException {
    return JsonCatalogResolver.resolve(getClass(), resourcePath, path);
  }

  /**
   * Splits a list into batches of the specified size.
   */
  private static <T> List<List<T>> createBatches(List<T> list, int batchSize) {
    List<List<T>> batches = new ArrayList<List<T>>();
    for (int i = 0; i < list.size(); i += batchSize) {
      batches.add(new ArrayList<T>(list.subList(i, Math.min(i + batchSize, list.size()))));
    }
    return batches;
  }

  /**
   * Executes an HTTP request with a specific body (for batching).
   */
  private String executeRequestWithBody(String baseUrl, Map<String, String> params,
      Map<String, String> variables, Map<String, Object> body) throws IOException {
    // Apply rate limiting
    enforceRateLimit();

    // Build URL with query parameters
    StringBuilder urlBuilder = new StringBuilder(baseUrl);
    if (!params.isEmpty()) {
      urlBuilder.append(baseUrl.contains("?") ? "&" : "?");
      boolean first = true;
      for (Map.Entry<String, String> e : params.entrySet()) {
        if (!first) {
          urlBuilder.append("&");
        }
        first = false;
        try {
          urlBuilder.append(URLEncoder.encode(e.getKey(), "UTF-8"));
          urlBuilder.append("=");
          urlBuilder.append(URLEncoder.encode(e.getValue(), "UTF-8"));
        } catch (Exception ex) {
          urlBuilder.append(e.getKey()).append("=").append(e.getValue());
        }
      }
    }

    String urlString = urlBuilder.toString();

    // Retry logic
    int maxRetries = config.getRateLimit().getMaxRetries();
    IOException lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return doRequestWithBody(urlString, variables, body);
      } catch (IOException e) {
        lastException = e;
        if (attempt < maxRetries) {
          long backoff = config.getRateLimit().getRetryBackoffMs() * (1L << attempt);
          LOGGER.warn("Request failed, retrying in {}ms: {}", backoff, e.getMessage());
          try {
            Thread.sleep(backoff);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
      }
    }

    throw lastException != null ? lastException : new IOException("Request failed after retries");
  }

  /**
   * Performs the actual HTTP request with a specific body.
   */
  private String doRequestWithBody(String urlString, Map<String, String> variables,
      Map<String, Object> body) throws IOException {
    java.net.URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod(config.getMethod().name());
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(120000);

      if (config.getHeaders().get("User-Agent") == null) {
        conn.setRequestProperty("User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36");
      }

      // Set headers (skip headers whose value is empty after substitution — avoids
      // APIs that treat an empty header as invalid, e.g. NVD's apiKey check)
      for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
        String value = substituteVariables(e.getValue(), variables);
        if (value != null && !value.isEmpty()) {
          conn.setRequestProperty(e.getKey(), value);
        }
      }

      // Apply authentication
      applyAuth(conn, variables);

      // Send body
      if (config.getMethod() == HttpSourceConfig.HttpMethod.POST
          || config.getMethod() == HttpSourceConfig.HttpMethod.PUT) {
        conn.setDoOutput(true);
        String bodyContent = serializeBody(body, config.getBodyFormat(), variables);
        String contentType = config.getBodyFormat() == HttpSourceConfig.BodyFormat.JSON
            ? "application/json"
            : "application/x-www-form-urlencoded";
        if (conn.getRequestProperty("Content-Type") == null) {
          conn.setRequestProperty("Content-Type", contentType);
        }
        LOGGER.debug("Sending batched body: {} bytes", bodyContent.length());
        try (OutputStream os = conn.getOutputStream()) {
          os.write(bodyContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          os.flush();
        }
      }

      int responseCode = conn.getResponseCode();
      LOGGER.debug("HTTP {} {} -> {}", config.getMethod(), urlString, responseCode);

      if (responseCode >= 200 && responseCode < 300) {
        return readResponse(conn.getInputStream());
      } else {
        String errorBody = readResponse(conn.getErrorStream());
        throw new IOException("HTTP " + responseCode + ": " + errorBody);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Executes an HTTP request with rate limiting and retries.
   *
   * @param baseUrl Base URL for the request
   * @param params Query parameters
   * @param variables Variable substitution map
   * @param rawCachePath Optional path to write large files directly to cache (null to use temp files)
   */
  private String executeRequest(String baseUrl, Map<String, String> params,
      Map<String, String> variables, String rawCachePath) throws IOException {

    // Rate limiting
    enforceRateLimit();

    // Build full URL with query parameters
    String fullUrl = buildUrlWithParams(baseUrl, params);

    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    int retries = 0;
    IOException lastException = null;

    while (retries <= rateLimit.getMaxRetries()) {
      try {
        String response = doRequest(fullUrl, variables, rawCachePath);
        return response;
      } catch (IOException e) {
        lastException = e;

        // Check if we should retry
        if (shouldRetry(e, rateLimit)) {
          retries++;
          if (retries <= rateLimit.getMaxRetries()) {
            long backoff = rateLimit.getRetryBackoffMs() * (1L << (retries - 1));
            LOGGER.warn("Request failed, retrying in {}ms (attempt {}/{}): {}",
                backoff, retries, rateLimit.getMaxRetries(), e.getMessage());
            try {
              Thread.sleep(backoff);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new IOException("Interrupted during retry backoff", ie);
            }
          }
        } else {
          throw e;
        }
      }
    }

    throw lastException != null ? lastException : new IOException("Request failed after retries");
  }

  /**
   * Performs the actual HTTP request.
   *
   * @param urlString Full URL to request
   * @param variables Variable substitution map
   * @param rawCachePath Optional path to write large files directly to cache (null to use temp files)
   */
  private String doRequest(String urlString, Map<String, String> variables,
      String rawCachePath) throws IOException {
    URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod(config.getMethod().name());
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(120000);

      if (config.getHeaders().get("User-Agent") == null) {
        conn.setRequestProperty("User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36");
      }

      // Set headers from config (skip empty values — avoids APIs treating an empty
      // header as invalid)
      for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
        String value = substituteVariables(e.getValue(), variables);
        if (value != null && !value.isEmpty()) {
          conn.setRequestProperty(e.getKey(), value);
        }
      }

      // Log headers being used for debugging
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("HTTP {} {} with {} custom headers",
            config.getMethod(), urlString, config.getHeaders().size());
        for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
          LOGGER.debug("  Header: {}={}", e.getKey(),
              e.getKey().toLowerCase().contains("key") ? "[REDACTED]" : e.getValue());
        }
      }

      // Apply authentication
      applyAuth(conn, variables);

      // Handle POST/PUT body if needed
      if (config.getMethod() == HttpSourceConfig.HttpMethod.POST
          || config.getMethod() == HttpSourceConfig.HttpMethod.PUT) {
        conn.setDoOutput(true);
        if (config.hasBody()) {
          String bodyContent = serializeBody(config.getBody(), config.getBodyFormat(), variables);
          // Set Content-Type if not already set
          String contentType = config.getBodyFormat() == HttpSourceConfig.BodyFormat.JSON
              ? "application/json"
              : "application/x-www-form-urlencoded";
          if (conn.getRequestProperty("Content-Type") == null) {
            conn.setRequestProperty("Content-Type", contentType);
          }
          LOGGER.debug("Sending body: {}", bodyContent);
          try (OutputStream os = conn.getOutputStream()) {
            os.write(bodyContent.getBytes(StandardCharsets.UTF_8));
            os.flush();
          }
        }
      }

      int responseCode = conn.getResponseCode();
      LOGGER.debug("HTTP {} {} -> {}", config.getMethod(), urlString, responseCode);

      if (responseCode >= 200 && responseCode < 300) {
        // Check if we need to extract from ZIP
        String extractPattern = config.getExtractPattern();
        if (extractPattern != null && !extractPattern.isEmpty()) {
          Map<String, String> zipVars = addDerivedVariables(variables);
          String resolvedPattern = substituteVariables(extractPattern, zipVars);
          String fallbackPattern = config.getExtractPatternFallback();
          if (fallbackPattern != null && !fallbackPattern.isEmpty()) {
            String resolvedFallback = substituteVariables(fallbackPattern, zipVars);
            return extractFromZipWithFallback(conn.getInputStream(), resolvedPattern,
                resolvedFallback, rawCachePath);
          }
          return extractFromZip(conn.getInputStream(), resolvedPattern, rawCachePath);
        }
        // Transformer downloads its own data — discard response body to avoid OOM
        if (config.isSkipResponseBody()) {
          try (java.io.InputStream is = conn.getInputStream()) {
            byte[] drain = new byte[8192];
            while (is.read(drain) != -1) {
              // intentionally empty
            }
          }
          return "";
        }
        // Read response into memory first to check for API-level errors before caching
        String responseBody = readResponse(conn.getInputStream());

        // Check for API-level errors in JSON responses before caching
        HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
        if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.JSON) {
          String apiError = checkForApiError(responseBody, respConfig);
          if (apiError != null) {
            throw new IOException("API error (not cached): " + apiError);
          }
        }

        // No API error - cache the response
        return cacheResponseString(responseBody, rawCachePath);
      } else {
        String errorBody = readResponse(conn.getErrorStream());
        if (config.isSkipResponseBody()) {
          // Trigger URL failed but body is skipped — transformer owns all actual fetching.
          // Log and return empty string so the transformer can still run (and decide to skip).
          LOGGER.warn("Trigger URL returned HTTP {} (skipResponseBody=true): {}",
              responseCode, urlString);
          return "";
        }
        if (shouldSkip(responseCode, config.getRateLimit())) {
          LOGGER.debug("HTTP {} from {} — skipping batch (skipOn match)", responseCode, urlString);
          throw new SkippedBatchException("HTTP " + responseCode + " (skipped): " + urlString);
        }
        throw new IOException("HTTP " + responseCode + ": " + errorBody);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Caches HTTP response to storage provider.
   *
   * @param input Response input stream
   * @param cachePath Path to write to storage provider
   * @return The cache path
   * @throws IOException if caching fails
   */
  private String cacheResponse(InputStream input, String cachePath) throws IOException {
    if (cachePath == null) {
      return readResponse(input);
    }
    if (isLocalPath(cachePath)) {
      File file = new File(cachePath);
      file.getParentFile().mkdirs();
      try (FileOutputStream fos = new FileOutputStream(file)) {
        byte[] buffer = new byte[8192];
        int len;
        while ((len = input.read(buffer)) != -1) {
          fos.write(buffer, 0, len);
        }
      }
      LOGGER.info("Cached response (local): {}", cachePath);
      return cachePath;
    }
    String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
    storageProvider.createDirectories(parentPath);
    storageProvider.writeFile(cachePath, input);
    LOGGER.info("Cached response: {}", cachePath);
    return cachePath;
  }

  /**
   * Caches a string response to storage provider.
   *
   * @param response Response content as string
   * @param cachePath Path to write to storage provider
   * @return The cache path
   * @throws IOException if caching fails
   */
  private String cacheResponseString(String response, String cachePath) throws IOException {
    if (cachePath == null) {
      return response;
    }
    if (isLocalPath(cachePath)) {
      File file = new File(cachePath);
      file.getParentFile().mkdirs();
      byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
      Files.write(Paths.get(cachePath), bytes);
      LOGGER.info("Cached response (local): {} ({} bytes)", cachePath, bytes.length);
      return cachePath;
    }
    String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
    storageProvider.createDirectories(parentPath);
    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    storageProvider.writeFile(cachePath, new ByteArrayInputStream(bytes));
    LOGGER.info("Cached response: {} ({} bytes)", cachePath, bytes.length);
    return cachePath;
  }

  /**
   * Checks for API-level errors in a JSON response before caching.
   * Returns the error message if an error is found, null otherwise.
   *
   * <p>This prevents caching error responses that would cause repeated failures.
   * Empty data responses (valid "no data" cases) return null and will be cached.
   *
   * @param responseBody The JSON response body
   * @param respConfig Response configuration with optional errorPath
   * @return Error message if API error found, null if response is valid (or empty data)
   */
  private String checkForApiError(String responseBody,
      HttpSourceConfig.ResponseConfig respConfig) {
    try {
      JsonNode root = OBJECT_MAPPER.readTree(responseBody);

      // Check for API errors using errorPath if configured
      if (respConfig.getErrorPath() != null && !respConfig.getErrorPath().isEmpty()) {
        JsonNode errorNode = navigateToPath(root, respConfig.getErrorPath());
        // Skip if error node is missing, null, or an empty array (common API pattern for "no error")
        boolean hasError = errorNode != null && !errorNode.isMissingNode() && !errorNode.isNull()
            && !(errorNode.isArray() && errorNode.size() == 0);
        if (hasError) {
          String errorMessage = errorNode.isTextual()
              ? errorNode.asText()
              : errorNode.toString();

          // Check for "no data" type errors that should be cached as empty results
          String errorLower = errorMessage.toLowerCase();
          if (errorLower.contains("no data") || errorLower.contains("not found")
              || errorLower.contains("parameter_empty") || errorLower.contains("unknown error")) {
            LOGGER.debug("API returned no-data message (will cache): {}", errorMessage);
            return null; // This is valid, cache it
          }

          return errorMessage; // Real API error - don't cache
        }
      }

      return null; // No error found (or no errorPath configured)
    } catch (Exception e) {
      LOGGER.debug("Could not check for API error (treating as valid): {}", e.getMessage());
      return null; // If we can't parse, assume it's valid
    }
  }

  /**
   * Reads content from cache.
   *
   * @param cachePath Path in storage provider
   * @return Content as string
   * @throws IOException if reading fails
   */
  private String readFromCache(String cachePath) throws IOException {
    if (isLocalPath(cachePath)) {
      byte[] bytes = Files.readAllBytes(Paths.get(cachePath));
      return new String(bytes, StandardCharsets.UTF_8);
    }
    try (InputStream is = storageProvider.openInputStream(cachePath);
         java.io.Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
      StringBuilder sb = new StringBuilder();
      char[] buffer = new char[8192];
      int len;
      while ((len = reader.read(buffer)) != -1) {
        sb.append(buffer, 0, len);
      }
      return sb.toString();
    }
  }

  /**
   * Extracts content from a ZIP archive and caches it to storage provider.
   *
   * @param input ZIP file input stream
   * @param pattern Glob pattern to match file names (e.g., "*.csv")
   * @param cachePath Path to write file to storage provider
   * @return The cache path
   * @throws IOException if extraction fails or no matching file found
   */
  private Map<String, String> addDerivedVariables(Map<String, String> variables) {
    Map<String, String> result = new LinkedHashMap<String, String>(variables);
    String formType = variables.get("form_type");
    if (formType != null) {
      result.put("form_type_lower", formType.toLowerCase(java.util.Locale.ROOT));
    }
    return result;
  }

  private String extractFromZipWithFallback(InputStream input, String pattern,
      String fallbackPattern, String cachePath) throws IOException {
    String regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");
    String fallbackRegex = fallbackPattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");

    // Write ZIP to a temp file so we can scan twice (primary then fallback) without
    // buffering the entire archive in memory.
    File tempZip = File.createTempFile("http-source-zip-", ".zip");
    tempZip.deleteOnExit();
    try {
      byte[] tmp = new byte[65536];
      int len;
      try (FileOutputStream fos = new FileOutputStream(tempZip)) {
        while ((len = input.read(tmp)) > 0) {
          fos.write(tmp, 0, len);
        }
      }

      // Try primary pattern first
      try (ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZip))) {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          String name = entry.getName();
          if (name.matches(regex) || name.endsWith(pattern.replace("*", ""))) {
            return writeZipEntry(zis, name, cachePath);
          }
          zis.closeEntry();
        }
      }

      // Primary not found — try fallback
      LOGGER.info("ZIP pattern '{}' not found, trying fallback '{}'", pattern, fallbackPattern);
      try (ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZip))) {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          String name = entry.getName();
          if (name.matches(fallbackRegex) || name.endsWith(fallbackPattern.replace("*", ""))) {
            return writeZipEntry(zis, name, cachePath);
          }
          zis.closeEntry();
        }
      }

      throw new IOException("No file matching '" + pattern + "' or '" + fallbackPattern
          + "' found in ZIP");
    } finally {
      if (!tempZip.delete()) {
        LOGGER.debug("Could not delete temp ZIP file: {}", tempZip.getAbsolutePath());
      }
    }
  }

  private String writeZipEntry(ZipInputStream zis, String name, String cachePath)
      throws IOException {
    LOGGER.info("Extracting from ZIP: {}", name);
    File tempFile = File.createTempFile("http-source-", ".tmp");
    tempFile.deleteOnExit();
    long totalBytes = 0;
    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
      byte[] buffer = new byte[65536];
      int len;
      long lastLogTime = System.currentTimeMillis();
      while ((len = zis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
        totalBytes += len;
        long now = System.currentTimeMillis();
        if (now - lastLogTime > 5000) {
          LOGGER.info("Extracting... {} MB", totalBytes / (1024 * 1024));
          lastLogTime = now;
        }
      }
    }
    if (isLocalPath(cachePath)) {
      File cacheFile = new File(cachePath);
      cacheFile.getParentFile().mkdirs();
      try (java.io.InputStream fis = new FileInputStream(tempFile);
           FileOutputStream fos = new FileOutputStream(cacheFile)) {
        byte[] copyBuf = new byte[65536];
        int copyLen;
        while ((copyLen = fis.read(copyBuf)) > 0) {
          fos.write(copyBuf, 0, copyLen);
        }
      }
    } else {
      try (java.io.InputStream fis = new FileInputStream(tempFile)) {
        String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
        storageProvider.createDirectories(parentPath);
        storageProvider.writeFile(cachePath, fis);
      }
    }
    tempFile.delete();
    LOGGER.info("Cached {} MB: {}", totalBytes / (1024 * 1024), cachePath);
    return cachePath;
  }

  private String extractFromZip(InputStream input, String pattern, String cachePath)
      throws IOException {
    String regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");

    try (ZipInputStream zis = new ZipInputStream(input)) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (name.matches(regex) || name.endsWith(pattern.replace("*", ""))) {
          LOGGER.info("Extracting from ZIP: {}", name);

          // Extract to temp file (ZIP streaming requires it)
          File tempFile = File.createTempFile("http-source-", ".tmp");
          tempFile.deleteOnExit();
          long totalBytes = 0;

          try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[65536];
            int len;
            long lastLogTime = System.currentTimeMillis();
            while ((len = zis.read(buffer)) > 0) {
              fos.write(buffer, 0, len);
              totalBytes += len;
              long now = System.currentTimeMillis();
              if (now - lastLogTime > 5000) {
                LOGGER.info("Extracting... {} MB", totalBytes / (1024 * 1024));
                lastLogTime = now;
              }
            }
          }

          // Write to cache
          if (isLocalPath(cachePath)) {
            File cacheFile = new File(cachePath);
            cacheFile.getParentFile().mkdirs();
            try (InputStream fis = new FileInputStream(tempFile);
                 FileOutputStream fos = new FileOutputStream(cacheFile)) {
              byte[] copyBuf = new byte[65536];
              int copyLen;
              while ((copyLen = fis.read(copyBuf)) > 0) {
                fos.write(copyBuf, 0, copyLen);
              }
            }
          } else {
            try (InputStream fis = new FileInputStream(tempFile)) {
              String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
              storageProvider.createDirectories(parentPath);
              storageProvider.writeFile(cachePath, fis);
            }
          }
          tempFile.delete();
          LOGGER.info("Cached {} MB: {}", totalBytes / (1024 * 1024), cachePath);
          return cachePath;
        }
        zis.closeEntry();
      }
    }
    throw new IOException("No file matching pattern '" + pattern + "' found in ZIP");
  }

  /**
   * Applies authentication to the connection.
   */
  private void applyAuth(HttpURLConnection conn, Map<String, String> variables) {
    HttpSourceConfig.AuthConfig auth = config.getAuth();
    if (auth.getType() == HttpSourceConfig.AuthType.NONE) {
      return;
    }

    switch (auth.getType()) {
      case API_KEY:
        String value = substituteVariables(auth.getValue(), variables);
        if (auth.getLocation() == HttpSourceConfig.AuthLocation.HEADER) {
          conn.setRequestProperty(auth.getName(), value);
        }
        // Query param auth is handled in URL building
        break;

      case BASIC:
        String credentials = substituteVariables(auth.getUsername(), variables)
            + ":" + substituteVariables(auth.getPassword(), variables);
        String encoded =
            Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + encoded);
        break;

      case BEARER:
        String token = substituteVariables(auth.getValue(), variables);
        conn.setRequestProperty("Authorization", "Bearer " + token);
        break;

      default:
        break;
    }
  }

  /**
   * Serializes the request body to a string format.
   *
   * @param body Body map from configuration
   * @param format Body format (JSON or FORM_URLENCODED)
   * @param variables Variables for substitution
   * @return Serialized body string
   */
  private String serializeBody(Map<String, Object> body, HttpSourceConfig.BodyFormat format,
      Map<String, String> variables) {
    // First, substitute variables in all body values
    Map<String, Object> resolvedBody = substituteBodyVariables(body, variables);

    if (format == HttpSourceConfig.BodyFormat.JSON) {
      try {
        Object toSerialize = config.isBodyWrapArray()
            ? java.util.Collections.singletonList(resolvedBody)
            : resolvedBody;
        return OBJECT_MAPPER.writeValueAsString(toSerialize);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize body to JSON: " + e.getMessage(), e);
      }
    } else {
      // FORM_URLENCODED
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Map.Entry<String, Object> e : resolvedBody.entrySet()) {
        if (!first) {
          sb.append("&");
        }
        first = false;
        try {
          sb.append(URLEncoder.encode(e.getKey(), "UTF-8"));
          sb.append("=");
          sb.append(URLEncoder.encode(String.valueOf(e.getValue()), "UTF-8"));
        } catch (Exception ex) {
          sb.append(e.getKey()).append("=").append(e.getValue());
        }
      }
      return sb.toString();
    }
  }

  /**
   * Recursively substitutes variables in body values.
   *
   * @param body Original body map
   * @param variables Variables for substitution
   * @return New map with all string values substituted
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> substituteBodyVariables(Map<String, Object> body,
      Map<String, String> variables) {
    Map<String, Object> result = new LinkedHashMap<String, Object>();

    for (Map.Entry<String, Object> e : body.entrySet()) {
      Object value = e.getValue();
      if (value instanceof String) {
        result.put(e.getKey(), substituteVariables((String) value, variables));
      } else if (value instanceof Map) {
        result.put(e.getKey(), substituteBodyVariables((Map<String, Object>) value, variables));
      } else if (value instanceof List) {
        result.put(e.getKey(), substituteListVariables((List<?>) value, variables));
      } else {
        result.put(e.getKey(), value);
      }
    }

    return result;
  }

  /**
   * Substitutes variables in list values.
   */
  @SuppressWarnings("unchecked")
  private List<Object> substituteListVariables(List<?> list, Map<String, String> variables) {
    List<Object> result = new ArrayList<Object>();
    for (Object item : list) {
      if (item instanceof String) {
        result.add(substituteVariables((String) item, variables));
      } else if (item instanceof Map) {
        result.add(substituteBodyVariables((Map<String, Object>) item, variables));
      } else if (item instanceof List) {
        result.add(substituteListVariables((List<?>) item, variables));
      } else {
        result.add(item);
      }
    }
    return result;
  }

  /**
   * Parses the response based on configured format and data path.
   * Checks for API errors using errorPath before extracting data.
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> parseResponse(String body) throws IOException {
    HttpSourceConfig.ResponseConfig respConfig = config.getResponse();

    // Handle CSV format — but only when no responseTransformer is present.
    // A transformer always returns JSON regardless of the source format config.
    if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV && responseTransformer == null) {
      return parseDelimitedResponse(body, resolveDelimiter(respConfig));
    }

    // Handle TSV format — same transformer guard as CSV.
    if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV && responseTransformer == null) {
      return parseDelimitedResponse(body, resolveDelimiter(respConfig));
    }

    if (respConfig.getFormat() != HttpSourceConfig.ResponseFormat.JSON
        && respConfig.getFormat() != HttpSourceConfig.ResponseFormat.TEXT
        && responseTransformer == null) {
      throw new IOException("Unsupported response format: " + respConfig.getFormat());
    }

    // Transformer output is always a plain JSON array. Stream-parse to avoid building
    // a full Jackson tree for large responses (100k+ records).
    if (responseTransformer != null) {
      return parseJsonArrayStreaming(body);
    }

    String rawJson = body;
    JsonNode root = OBJECT_MAPPER.readTree(rawJson);

    // Check for API errors using errorPath if configured
    if (respConfig.getErrorPath() != null && !respConfig.getErrorPath().isEmpty()) {
      JsonNode errorNode = navigateToPath(root, respConfig.getErrorPath());
      // Skip if error node is missing, null, or an empty array (common API pattern for "no error")
      boolean hasError = errorNode != null && !errorNode.isMissingNode() && !errorNode.isNull()
          && !(errorNode.isArray() && errorNode.size() == 0);
      if (hasError) {
        // API returned an error in the configured error location
        String errorMessage = errorNode.isTextual()
            ? errorNode.asText()
            : errorNode.toString();

        // Check for "no data" type errors that should return empty results
        // These indicate the parameter combination is invalid, not a real API error
        String errorLower = errorMessage.toLowerCase();
        if (errorLower.contains("no data") || errorLower.contains("not found")
            || errorLower.contains("parameter_empty") || errorLower.contains("unknown error")) {
          LOGGER.debug("API returned no-data error, returning empty result: {}", errorMessage);
          return Collections.emptyList();
        }

        LOGGER.warn("API error at {}: {}", respConfig.getErrorPath(), errorMessage);
        throw new IOException("API error: " + errorMessage);
      }
    }

    // Navigate to data path if specified — but skip when a responseTransformer was applied.
    // Transformers are responsible for their own data extraction (e.g. via extractDataArray()).
    // Applying dataPath to the transformer's already-extracted array returns empty results.
    if (responseTransformer == null
        && respConfig.getDataPath() != null
        && !respConfig.getDataPath().isEmpty()) {
      root = navigateToPath(root, respConfig.getDataPath());
    }

    // Convert to list of maps
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

    if (root.isArray()) {
      for (JsonNode item : root) {
        Map<String, Object> row = OBJECT_MAPPER.convertValue(item, Map.class);
        result.add(row);
      }
    } else if (root.isObject()) {
      // Single object - wrap in list
      Map<String, Object> row = OBJECT_MAPPER.convertValue(root, Map.class);
      result.add(row);
    }

    return result;
  }

  private List<Map<String, Object>> parseJsonArrayStreaming(String json) throws IOException {
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    try (JsonParser parser = OBJECT_MAPPER.getFactory().createParser(json)) {
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        return result;
      }
      while (parser.nextToken() == JsonToken.START_OBJECT) {
        Map<String, Object> row = OBJECT_MAPPER.readValue(parser, Map.class);
        result.add(row);
      }
    }
    return result;
  }

  /**
   * Parses cached delimited response (CSV or TSV) returning a lazy iterator.
   *
   * @param cachePath Path to cached file in storage provider
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return Iterator over rows, each row is a Map with column names as keys
   * @throws IOException if reading from cache fails
   */
  private static char resolveDelimiter(HttpSourceConfig.ResponseConfig respConfig) {
    String custom = respConfig.getDelimiter();
    if (custom != null && !custom.isEmpty()) {
      return custom.charAt(0);
    }
    return respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV ? ',' : '\t';
  }

  private Iterator<Map<String, Object>> parseDelimitedResponseStreaming(String cachePath, char delimiter)
      throws IOException {
    LOGGER.info("Streaming from cache: {}", cachePath);
    HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
    // Open input stream: local file or storage provider
    InputStream inputStream;
    if (isLocalPath(cachePath)) {
      inputStream = new FileInputStream(cachePath);
    } else {
      inputStream = storageProvider.openInputStream(cachePath);
    }
    return new LazyCSVIterator(inputStream, cachePath, delimiter,
        config.getRowFilter(), config.getWideToNarrow(),
        respConfig.isHasHeader(), respConfig.getColumnNames());
  }

  /**
   * Lazy iterator that reads CSV rows one at a time from storage provider.
   * Parses rows on-demand to avoid loading entire file into memory.
   * Supports wide-to-narrow transformation (unpivot) for bulk CSV files.
   */
  private class LazyCSVIterator implements Iterator<Map<String, Object>>, java.io.Closeable {
    private final BufferedReader reader;
    private final char delimiter;
    private final String[] headers;
    private final int filterColumnIndex;
    private final java.util.regex.Pattern filterRegex;
    private final int maxRows;

    // Wide-to-narrow transformation support
    private final HttpSourceConfig.WideToNarrowConfig wideToNarrow;
    private final List<Integer> keyColumnIndices;
    private final List<Integer> valueColumnIndices;
    private final List<String> valueColumnNames;
    private final Deque<Map<String, Object>> expandedRowQueue;

    private Map<String, Object> nextRow;
    private boolean exhausted;
    private int lineNumber;
    private int matchedRows;
    private int skippedRows;
    private long lastLogTime;

    LazyCSVIterator(InputStream inputStream, String cachePath, char delimiter,
        HttpSourceConfig.RowFilterConfig filter,
        HttpSourceConfig.WideToNarrowConfig wideToNarrow,
        boolean hasHeader, String columnNames) throws IOException {
      this.delimiter = delimiter;
      this.wideToNarrow = wideToNarrow;
      this.reader =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      this.exhausted = false;
      this.lineNumber = 0;
      this.matchedRows = 0;
      this.skippedRows = 0;
      this.lastLogTime = System.currentTimeMillis();

      // Initialize wide-to-narrow data structures
      this.keyColumnIndices = new ArrayList<Integer>();
      this.valueColumnIndices = new ArrayList<Integer>();
      this.valueColumnNames = new ArrayList<String>();
      this.expandedRowQueue = new ArrayDeque<Map<String, Object>>();

      // Determine headers: use explicit columnNames, read from file, or generate positional
      if (!hasHeader && columnNames != null && !columnNames.isEmpty()) {
        // Headerless file with explicit column names from config
        this.headers = parseDelimitedLine(columnNames, delimiter);
        LOGGER.info("Using {} explicit column names for headerless CSV (from cache: {})",
            headers.length, cachePath);
      } else if (hasHeader) {
        // Read header row from file
        String headerLine = reader.readLine();
        if (headerLine == null) {
          this.headers = new String[0];
          this.filterColumnIndex = -1;
          this.filterRegex = null;
          this.maxRows = 0;
          exhausted = true;
          return;
        }
        this.headers = parseDelimitedLine(headerLine, delimiter);
        LOGGER.debug("Parsed {} columns from header (from cache: {})", headers.length, cachePath);
      } else {
        // Headerless file without explicit names — peek first line for column count
        String firstLine = reader.readLine();
        if (firstLine == null) {
          this.headers = new String[0];
          this.filterColumnIndex = -1;
          this.filterRegex = null;
          this.maxRows = 0;
          exhausted = true;
          return;
        }
        String[] firstFields = parseDelimitedLine(firstLine, delimiter);
        this.headers = new String[firstFields.length];
        for (int i = 0; i < firstFields.length; i++) {
          this.headers[i] = "field_" + i;
        }
        LOGGER.info("Generated {} positional column names for headerless CSV", headers.length);
        // Push the first line back as data by pre-parsing it
        Map<String, Object> firstRow = new LinkedHashMap<String, Object>();
        for (int i = 0; i < headers.length && i < firstFields.length; i++) {
          firstRow.put(headers[i], firstFields[i]);
        }
        expandedRowQueue.add(firstRow);
      }

      // Setup wide-to-narrow column indices
      if (wideToNarrow != null && wideToNarrow.isEnabled()) {
        for (int i = 0; i < headers.length; i++) {
          String header = headers[i].trim();
          if (wideToNarrow.getKeyColumns().contains(header)) {
            keyColumnIndices.add(i);
          } else if (wideToNarrow.isValueColumn(header)) {
            valueColumnIndices.add(i);
            valueColumnNames.add(header);
          }
        }
        LOGGER.info("Wide-to-narrow streaming: {} key columns, {} value columns to unpivot",
            keyColumnIndices.size(), valueColumnIndices.size());
      }

      // Setup filter
      String filterColumn = filter != null ? filter.getColumn() : null;
      String filterPattern = filter != null ? filter.getPattern() : null;
      this.maxRows = filter != null ? filter.getMaxRows() : 0;
      this.filterRegex = filterPattern != null
          ? java.util.regex.Pattern.compile(filterPattern)
          : null;

      // Find filter column index
      int foundIndex = -1;
      if (filterColumn != null) {
        for (int i = 0; i < headers.length; i++) {
          if (headers[i].trim().equals(filterColumn)) {
            foundIndex = i;
            break;
          }
        }
        if (foundIndex < 0) {
          LOGGER.warn("Filter column '{}' not found in CSV headers", filterColumn);
        }
      }
      this.filterColumnIndex = foundIndex;

      if (filter != null && filter.isEnabled()) {
        LOGGER.info("CSV filter: column={}, pattern={}, maxRows={}",
            filterColumn, filterPattern, maxRows > 0 ? maxRows : "unlimited");
      }

      // Pre-fetch first matching row
      advance();
    }

    private void advance() {
      if (exhausted) {
        return;
      }

      // Check if we have expanded rows from wide-to-narrow transformation
      if (!expandedRowQueue.isEmpty()) {
        nextRow = expandedRowQueue.poll();
        return;
      }

      try {
        String line;
        while ((line = reader.readLine()) != null) {
          lineNumber++;
          line = line.trim();
          if (line.isEmpty()) {
            continue;
          }

          String[] values = parseDelimitedLine(line, delimiter);

          // Apply filter if configured
          if (filterColumnIndex >= 0 && filterRegex != null) {
            if (filterColumnIndex >= values.length) {
              skippedRows++;
              continue;
            }
            String filterValue = values[filterColumnIndex].trim();
            if (filterValue.startsWith("\"") && filterValue.endsWith("\"")) {
              filterValue = filterValue.substring(1, filterValue.length() - 1);
            }
            if (!filterRegex.matcher(filterValue).find()) {
              skippedRows++;
              continue;
            }
          }

          // Wide-to-narrow transformation: one input row -> N output rows
          if (wideToNarrow != null && wideToNarrow.isEnabled()) {
            // Build base row with key columns (applying column name mapping if configured)
            Map<String, Object> baseRow = new LinkedHashMap<String, Object>();
            for (int idx : keyColumnIndices) {
              if (idx < values.length) {
                String header = headers[idx].trim();
                String value = values[idx].trim();
                if (value.startsWith("\"") && value.endsWith("\"")) {
                  value = value.substring(1, value.length() - 1);
                }
                // Apply column name mapping: source name -> output name
                String outputName = wideToNarrow.getOutputColumnName(header);
                baseRow.put(outputName, parseValue(value));
              }
            }

            // Create one output row per value column
            for (int i = 0; i < valueColumnIndices.size(); i++) {
              int idx = valueColumnIndices.get(i);
              if (idx < values.length) {
                String valueStr = values[idx].trim();
                if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
                  valueStr = valueStr.substring(1, valueStr.length() - 1);
                }

                // Skip null/empty values based on config
                if (wideToNarrow.shouldSkipValue(valueStr)) {
                  continue;
                }

                Map<String, Object> row = new LinkedHashMap<String, Object>(baseRow);
                row.put(wideToNarrow.getKeyColumnName(), valueColumnNames.get(i));  // e.g., "2020"
                row.put(wideToNarrow.getValueColumnName(), parseValue(valueStr));   // e.g., 12345.0
                expandedRowQueue.add(row);
                matchedRows++;

                // Check maxRows limit
                if (maxRows > 0 && matchedRows >= maxRows) {
                  LOGGER.info("Reached maxRows limit ({}), stopping lazy parse", maxRows);
                  exhausted = true;
                  break;
                }
              }
            }

            // Return first expanded row if we have any
            if (!expandedRowQueue.isEmpty()) {
              nextRow = expandedRowQueue.poll();

              // Log progress periodically
              long now = System.currentTimeMillis();
              if (now - lastLogTime > 10000 || lineNumber % 100000 == 0) {
                LOGGER.info("Lazy CSV (wide-to-narrow)... {} lines read, {} output rows, {} skipped",
                    lineNumber, matchedRows, skippedRows);
                lastLogTime = now;
              }

              return;
            }
            // If no rows added (all values skipped), continue to next line
            continue;

          } else {
            // Standard row parsing (no transformation)
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            for (int j = 0; j < headers.length && j < values.length; j++) {
              String header = headers[j].trim();
              String value = values[j].trim();
              if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
              }
              Object parsed = parseValue(value);
              row.put(header, parsed);
            }

            nextRow = row;
            matchedRows++;

            // Check maxRows limit
            if (maxRows > 0 && matchedRows >= maxRows) {
              LOGGER.info("Reached maxRows limit ({}), stopping lazy parse", maxRows);
              exhausted = true;
            }

            // Log progress periodically
            long now = System.currentTimeMillis();
            if (now - lastLogTime > 10000 || lineNumber % 100000 == 0) {
              LOGGER.info("Lazy CSV... {} lines read, {} matched, {} skipped",
                  lineNumber, matchedRows, skippedRows);
              lastLogTime = now;
            }

            return;
          }
        }

        // End of file
        exhausted = true;
        nextRow = null;
        LOGGER.info("Lazy CSV complete: {} lines read, {} output rows, {} skipped",
            lineNumber, matchedRows, skippedRows);
        close();

      } catch (IOException e) {
        LOGGER.error("Error reading CSV: {}", e.getMessage());
        exhausted = true;
        nextRow = null;
        try {
          close();
        } catch (IOException ignored) {
          // Already logging the original error
        }
      }
    }

    @Override public boolean hasNext() {
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> current = nextRow;
      nextRow = null;
      if (!exhausted) {
        advance();
      }
      return current;
    }

    @Override public void close() throws IOException {
      reader.close();
    }
  }

  /**
   * Parses a delimited response (CSV or TSV) into a list of maps with streaming and optional filtering.
   *
   * <p>Uses streaming to avoid loading entire file into memory.
   * When rowFilter is configured, only matching rows are kept.
   *
   * @param response Delimited content with header row
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return List of maps, one per row, with column names as keys
   * @throws IOException if response contains error content instead of tabular data
   */
  private List<Map<String, Object>> parseDelimitedResponse(String response, char delimiter)
      throws IOException {
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

    if (response == null || response.isEmpty()) {
      LOGGER.warn("Received empty response body - returning 0 records");
      return result;
    }

    // Get reader - parse in-memory content (used for paginated responses)
    Reader sourceReader = new StringReader(response);

    // Get filter config if present
    HttpSourceConfig.RowFilterConfig filter = config.getRowFilter();
    String filterColumn = filter != null ? filter.getColumn() : null;
    String filterPattern = filter != null ? filter.getPattern() : null;
    int maxRows = filter != null ? filter.getMaxRows() : 0;
    java.util.regex.Pattern filterRegex = filterPattern != null
        ? java.util.regex.Pattern.compile(filterPattern)
        : null;

    if (filter != null && filter.isEnabled()) {
      LOGGER.info("CSV filter: column={}, pattern={}, maxRows={}",
          filterColumn, filterPattern, maxRows > 0 ? maxRows : "unlimited");
    }

    // Stream through the CSV line by line
    try (BufferedReader reader = new BufferedReader(sourceReader)) {
      // Determine headers
      HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
      String[] headers;
      if (!respConfig.isHasHeader() && respConfig.getColumnNames() != null) {
        // Headerless file with explicit column names
        headers = parseDelimitedLine(respConfig.getColumnNames(), delimiter);
        LOGGER.debug("Using {} explicit column names for headerless CSV", headers.length);
      } else {
        // Parse header row from file
        String headerLine = reader.readLine();
        if (headerLine == null) {
          return result;
        }
        headers = parseDelimitedLine(headerLine, delimiter);
        LOGGER.debug("Parsed {} columns from header", headers.length);
      }

      // Find filter column index if filtering is enabled
      int filterColumnIndex = -1;
      if (filterColumn != null) {
        for (int i = 0; i < headers.length; i++) {
          if (headers[i].trim().equals(filterColumn)) {
            filterColumnIndex = i;
            break;
          }
        }
        if (filterColumnIndex < 0) {
          LOGGER.warn("Filter column '{}' not found in CSV headers", filterColumn);
        }
      }

      // Wide-to-narrow transformation setup
      HttpSourceConfig.WideToNarrowConfig wideToNarrow = config.getWideToNarrow();
      List<Integer> keyColumnIndices = new ArrayList<Integer>();
      List<Integer> valueColumnIndices = new ArrayList<Integer>();
      List<String> valueColumnNames = new ArrayList<String>();

      if (wideToNarrow != null && wideToNarrow.isEnabled()) {
        // Build index lists for key and value columns
        for (int i = 0; i < headers.length; i++) {
          String header = headers[i].trim();
          if (wideToNarrow.getKeyColumns().contains(header)) {
            keyColumnIndices.add(i);
          } else if (wideToNarrow.isValueColumn(header)) {
            valueColumnIndices.add(i);
            valueColumnNames.add(header);
          }
          // Columns not in keyColumns and not matching valueColumnPattern are skipped
        }
        LOGGER.info("Wide-to-narrow: {} key columns, {} value columns to unpivot",
            keyColumnIndices.size(), valueColumnIndices.size());
      }

      // Parse data rows with streaming
      String line;
      int lineNumber = 0;
      int matchedRows = 0;
      int skippedRows = 0;
      long lastLogTime = System.currentTimeMillis();

      while ((line = reader.readLine()) != null) {
        lineNumber++;
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }

        String[] values = parseDelimitedLine(line, delimiter);

        // Apply filter if configured
        if (filterColumnIndex >= 0 && filterRegex != null) {
          if (filterColumnIndex >= values.length) {
            skippedRows++;
            continue;
          }
          String filterValue = values[filterColumnIndex].trim();
          // Remove quotes if present
          if (filterValue.startsWith("\"") && filterValue.endsWith("\"")) {
            filterValue = filterValue.substring(1, filterValue.length() - 1);
          }
          if (!filterRegex.matcher(filterValue).find()) {
            skippedRows++;
            continue;
          }
        }

        // Wide-to-narrow transformation: one input row -> N output rows
        if (wideToNarrow != null && wideToNarrow.isEnabled()) {
          // Build base row with key columns
          Map<String, Object> baseRow = new LinkedHashMap<String, Object>();
          for (int idx : keyColumnIndices) {
            if (idx < values.length) {
              String header = headers[idx].trim();
              String value = values[idx].trim();
              if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
              }
              baseRow.put(header, parseValue(value));
            }
          }

          // Create one output row per value column
          for (int i = 0; i < valueColumnIndices.size(); i++) {
            int idx = valueColumnIndices.get(i);
            if (idx < values.length) {
              String valueStr = values[idx].trim();
              if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
                valueStr = valueStr.substring(1, valueStr.length() - 1);
              }

              // Skip null/empty values based on config
              if (wideToNarrow.shouldSkipValue(valueStr)) {
                continue;
              }

              Map<String, Object> row = new LinkedHashMap<String, Object>(baseRow);
              row.put(wideToNarrow.getKeyColumnName(), valueColumnNames.get(i));  // e.g., "2020"
              row.put(wideToNarrow.getValueColumnName(), parseValue(valueStr));   // e.g., 12345.0
              result.add(row);
              matchedRows++;

              // Check maxRows limit
              if (maxRows > 0 && matchedRows >= maxRows) {
                LOGGER.info("Reached maxRows limit ({}), stopping CSV parse", maxRows);
                break;
              }
            }
          }
          if (maxRows > 0 && matchedRows >= maxRows) {
            break;
          }
        } else {
          // Standard row parsing (no transformation)
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          for (int j = 0; j < headers.length && j < values.length; j++) {
            String header = headers[j].trim();
            String value = values[j].trim();

            // Remove surrounding quotes if present
            if (value.startsWith("\"") && value.endsWith("\"")) {
              value = value.substring(1, value.length() - 1);
            }

            // Try to parse as number
            Object parsed = parseValue(value);
            row.put(header, parsed);
          }

          result.add(row);
          matchedRows++;

          // Check maxRows limit
          if (maxRows > 0 && matchedRows >= maxRows) {
            LOGGER.info("Reached maxRows limit ({}), stopping CSV parse", maxRows);
            break;
          }
        }

        // Log progress every 10 seconds or 100k lines
        long now = System.currentTimeMillis();
        if (now - lastLogTime > 10000 || lineNumber % 100000 == 0) {
          LOGGER.info("Parsing CSV... {} lines read, {} output rows",
              lineNumber, matchedRows);
          lastLogTime = now;
        }
      }

      LOGGER.info("CSV parse complete: {} lines read, {} output rows, {} skipped",
          lineNumber, matchedRows, skippedRows);
    }

    return result;
  }

  /**
   * Parses a single delimited line, handling quoted fields.
   *
   * @param line The line to parse
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return Array of field values
   */
  private String[] parseDelimitedLine(String line, char delimiter) {
    List<String> fields = new ArrayList<String>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (c == '"') {
        // Check for escaped quote ("")
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          current.append('"');
          i++; // Skip next quote
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == delimiter && !inQuotes) {
        fields.add(current.toString());
        current = new StringBuilder();
      } else {
        current.append(c);
      }
    }
    fields.add(current.toString());

    return fields.toArray(new String[0]);
  }

  /**
   * Attempts to parse a string value as a number.
   */
  private Object parseValue(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }

    // Treat common CSV null markers as SQL NULL
    if ("NULL".equalsIgnoreCase(value) || "NA".equals(value) || "N/A".equals(value)) {
      return null;
    }

    // Try integer first
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      // Not an integer
    }

    // Only try double for values that contain a decimal point.
    // Strings without '.' that overflow Long are identifiers (e.g. all-digit LEI codes
    // like "13250000000000000000", or alphanumeric IDs like "300300E1000345000084" that
    // look like scientific notation but are not). Calling Double.parseDouble on these
    // silently loses precision or produces "Infinity".
    if (value.indexOf('.') >= 0) {
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException e) {
        // Not a double
      }
    }

    // Return as string
    return value;
  }

  /**
   * Navigates to a JSON path (simple dot or bracket notation).
   */
  private JsonNode navigateToPath(JsonNode root, String path) {
    // Handle JSONPath-like syntax: $.results.data or results.data
    String cleanPath = path;
    if (cleanPath.startsWith("$.")) {
      cleanPath = cleanPath.substring(2);
    } else if (cleanPath.startsWith("$")) {
      cleanPath = cleanPath.substring(1);
    }

    JsonNode current = root;
    for (String part : cleanPath.split("\\.")) {
      if (current == null || current.isMissingNode()) {
        return OBJECT_MAPPER.createArrayNode();
      }

      // Handle array index: data[0]
      if (part.contains("[")) {
        int bracketIdx = part.indexOf('[');
        String fieldName = part.substring(0, bracketIdx);
        if (!fieldName.isEmpty()) {
          current = current.get(fieldName);
        }

        // Extract index
        int endBracket = part.indexOf(']');
        String indexStr = part.substring(bracketIdx + 1, endBracket);
        int index = Integer.parseInt(indexStr);
        current = current != null ? current.get(index) : null;
      } else {
        current = current.get(part);
      }
    }

    return current != null ? current : OBJECT_MAPPER.createArrayNode();
  }

  /**
   * Substitutes variables in a string.
   * Supports {varName} for variables and {env:VAR_NAME} for environment variables.
   */
  private String substituteVariables(String template, Map<String, String> variables) {
    return VariableResolver.substitute(template, variables);
  }

  /**
   * Builds URL with query parameters.
   */
  private String buildUrlWithParams(String baseUrl, Map<String, String> params) {
    if (params == null || params.isEmpty()) {
      return baseUrl;
    }

    StringBuilder url = new StringBuilder(baseUrl);
    char separator = baseUrl.contains("?") ? '&' : '?';

    for (Map.Entry<String, String> e : params.entrySet()) {
      String value = e.getValue();
      if (value == null || value.isEmpty()) {
        continue; // skip unresolved optional parameters (e.g. incremental bounds not yet set)
      }
      try {
        url.append(separator)
            .append(URLEncoder.encode(e.getKey(), "UTF-8"))
            .append('=')
            .append(URLEncoder.encode(value, "UTF-8"));
        separator = '&';
      } catch (Exception ex) {
        // Fallback without encoding
        url.append(separator).append(e.getKey()).append('=').append(value);
        separator = '&';
      }
    }

    return url.toString();
  }

  /**
   * Builds a cache key from URL and parameters.
   */
  private String buildCacheKey(String url, Map<String, String> params) {
    StringBuilder key = new StringBuilder(url);
    if (params != null && !params.isEmpty()) {
      List<String> sortedKeys = new ArrayList<String>(params.keySet());
      Collections.sort(sortedKeys);
      for (String k : sortedKeys) {
        key.append('|').append(k).append('=').append(params.get(k));
      }
    }
    return key.toString();
  }

  /**
   * Enforces rate limiting using lock-free CAS-based slot reservation.
   * Each thread reserves a time slot on a timeline, allowing multiple threads
   * to make concurrent requests while respecting the global rate limit.
   */
  private void enforceRateLimit() {
    int rps = config.getRateLimit().getRequestsPerSecond();
    if (rps <= 0) {
      return;
    }

    long intervalNanos = 1000000000L / rps;
    while (true) {
      long current = nextAllowedNanos.get();
      long now = System.nanoTime();
      long next = Math.max(now, current) + intervalNanos;
      if (nextAllowedNanos.compareAndSet(current, next)) {
        long sleepNanos = Math.max(0, current - now);
        if (sleepNanos > 0) {
          try {
            Thread.sleep(sleepNanos / 1000000, (int) (sleepNanos % 1000000));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return;
      }
      // CAS failed (another thread reserved a slot), retry
    }
  }

  /**
   * Checks if a response code matches the skipOn list — batch should be silently dropped.
   */
  private boolean shouldSkip(int responseCode, HttpSourceConfig.RateLimitConfig rateLimit) {
    for (int code : rateLimit.getSkipOn()) {
      if (responseCode == code) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if we should retry based on the error.
   */
  private boolean shouldRetry(IOException e, HttpSourceConfig.RateLimitConfig rateLimit) {
    // Socket-level timeouts (SocketTimeoutException) are always retryable — the server
    // may be temporarily slow or the connection was idle-closed between pages.
    if (e instanceof java.net.SocketTimeoutException) {
      return true;
    }

    String message = e.getMessage();
    if (message == null) {
      return false;
    }

    // Check for retryable HTTP status codes
    for (int code : rateLimit.getRetryOn()) {
      if (message.contains("HTTP " + code)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Reads response body from input stream.
   */
  private String readResponse(InputStream input) throws IOException {
    if (input == null) {
      return "";
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line).append('\n');
      }
    }
    return response.toString();
  }

  /**
   * Transforms the response using the configured ResponseTransformer.
   *
   * @param response Raw response from HTTP request
   * @param url The request URL
   * @param params The request parameters
   * @param dimensionValues The dimension values used
   * @return Transformed response, or original if no transformer configured
   */
  private String transformResponse(String response, String url, Map<String, String> params,
      Map<String, String> dimensionValues) {
    if (responseTransformer == null) {
      return response;
    }

    // Build request context for the transformer
    RequestContext context = RequestContext.builder()
        .url(url)
        .parameters(params)
        .headers(config.getHeaders())
        .dimensionValues(dimensionValues)
        .build();

    try {
      String transformed = responseTransformer.transform(response, context);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("ResponseTransformer transformed response for {}", url);
      }
      return transformed;
    } catch (RuntimeException e) {
      // ResponseTransformer threw an exception - this is how it signals API errors
      LOGGER.warn("ResponseTransformer threw exception for {}: {}", url, e.getMessage());
      throw e;
    }
  }

  /**
   * Creates an HttpSource from configuration.
   */
  public static HttpSource create(HttpSourceConfig config) {
    return new HttpSource(config);
  }

  /**
   * Creates an HttpSource from configuration with hooks.
   */
  public static HttpSource create(HttpSourceConfig config, HooksConfig hooksConfig) {
    return new HttpSource(config, hooksConfig);
  }

  // --- Raw Response Caching (StorageProvider-based, with local filesystem optimization) ---

  /**
   * Computes the local filesystem cache path to use instead of S3 for raw caching.
   *
   * <p>When rawCachePath points to S3, using S3 for per-response caching generates
   * one PUT per API call (e.g., 2.88M PUTs for crime data). This method computes
   * a local filesystem mirror path to avoid those S3 calls entirely.
   *
   * <p>The local cache base can be set via the {@code ETL_LOCAL_RAW_CACHE} environment
   * variable. If not set and the rawCachePath starts with {@code s3://}, defaults
   * to {@code /tmp/etl-raw-cache}.
   *
   * @param rawCachePath the configured raw cache path (may be S3 or local)
   * @return local filesystem path for raw cache, or null if local caching is not applicable
   */
  private String computeLocalRawCachePath(String rawCachePath) {
    if (rawCachePath == null) {
      return null;
    }

    // Determine the local cache base directory:
    // 1. ETL_LOCAL_RAW_CACHE env var (explicit override)
    // 2. operatingDirectory from model operands (e.g., .aperio/<schema>/cache/raw)
    // 3. Fallback: <workingDir>/.aperio/cache/raw
    String localCacheBase = System.getenv("ETL_LOCAL_RAW_CACHE");
    if (localCacheBase == null && operatingDirectory != null) {
      localCacheBase = operatingDirectory + "/cache/raw";
    }
    if (localCacheBase == null) {
      String workDir = System.getProperty("user.dir", System.getProperty("user.home", "/tmp"));
      localCacheBase = workDir + "/.aperio/cache/raw";
    }

    // Extract the meaningful part of the path: strip scheme + bucket for cloud URIs
    String suffix = rawCachePath;
    if (!StorageProvider.isLocalPath(suffix)) {
      // cloud URI: scheme://bucket/path → path
      int slashIdx = suffix.indexOf('/', suffix.indexOf("//") + 2);
      suffix = slashIdx >= 0 ? suffix.substring(slashIdx + 1) : "";
    }

    if (suffix.isEmpty()) {
      return localCacheBase;
    }

    return localCacheBase + "/" + suffix;
  }

  /**
   * Checks whether a cache path refers to a local file (not a cloud storage path).
   */
  private static boolean isLocalPath(String path) {
    return StorageProvider.isLocalPath(path);
  }

  /**
   * Checks if raw cache is enabled and available.
   */
  private boolean isRawCacheEnabled() {
    if (localRawCachePath != null && config.getRawCache().isEnabled()) {
      return true;
    }
    return config.getRawCache().isEnabled()
        && storageProvider != null
        && rawCachePath != null;
  }

  /**
   * Builds the raw cache path for a given set of dimension variables.
   * Path format: {rawCachePath}/{partitionKey}/response.json
   * Example: s3://bucket/.raw/type=regional_income/year=2020/tablename=CAGDP2/response.json
   */
  private String buildRawCachePath(Map<String, String> variables) {
    // Use local filesystem cache if available (avoids S3 PUT per API response)
    String basePath = localRawCachePath != null ? localRawCachePath : rawCachePath;
    StringBuilder path = new StringBuilder(basePath);
    if (!basePath.endsWith("/")) {
      path.append("/");
    }

    // Build partition key from sorted variables
    List<String> sortedKeys = new ArrayList<String>(variables.keySet());
    Collections.sort(sortedKeys);
    for (String key : sortedKeys) {
      String value = variables.get(key);
      if (value != null && !value.isEmpty()) {
        path.append(key).append("=").append(sanitizePathComponent(value)).append("/");
      }
    }

    path.append("response.json");
    return path.toString();
  }

  /**
   * Sanitizes a path component by removing or replacing invalid characters.
   */
  private String sanitizePathComponent(String value) {
    // Replace invalid path characters with underscores
    return value.replaceAll("[/\\\\:*?\"<>|]", "_");
  }

  /**
   * Checks if a raw cached response exists and is not expired.
   *
   * @param cachePath Path to the cached response
   * @return true if cache hit, false otherwise
   */
  private boolean hasValidRawCache(String cachePath) {
    try {
      // Immutable data - if cache exists, it's valid
      // Staleness is determined by IncrementalTracker, not by TTL
      boolean exists;
      if (isLocalPath(cachePath)) {
        exists = new File(cachePath).exists();
      } else {
        exists = storageProvider != null && storageProvider.exists(cachePath);
      }
      if (!exists) {
        LOGGER.debug("Raw cache miss: {}", cachePath);
        return false;
      }
      LOGGER.debug("Raw cache hit: {}", cachePath);
      return true;
    } catch (IOException e) {
      LOGGER.debug("Error checking raw cache: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Reads raw cached response from storage provider.
   *
   * @param cachePath Path to the cached response
   * @return Cached response content
   * @throws IOException if read fails
   */
  private String readRawCache(String cachePath) throws IOException {
    if (isLocalPath(cachePath)) {
      byte[] bytes = Files.readAllBytes(Paths.get(cachePath));
      String content = new String(bytes, StandardCharsets.UTF_8);
      LOGGER.info("Raw cache hit (local): {} ({} bytes)", cachePath, content.length());
      return content;
    }
    try (InputStream is = storageProvider.openInputStream(cachePath)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int len;
      while ((len = is.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }
      String content = baos.toString(StandardCharsets.UTF_8.name());
      LOGGER.info("Raw cache hit: {} ({} bytes)", cachePath, content.length());
      return content;
    }
  }

  /**
   * Streams a delimited (CSV/TSV) cache file, applying a per-record transformer to each row.
   *
   * <p>Combines {@link #parseDelimitedResponseStreaming} (lazy, unbuffered) with a wrapping
   * iterator that calls {@link PerRecordResponseTransformer#transformRecord} per row. This avoids
   * loading the entire file into memory before transformation.
   */
  private Iterator<Map<String, Object>> streamDelimitedFromRawCache(
      final String cachePath,
      final char delimiter,
      final String url,
      final Map<String, String> params,
      final Map<String, String> variables,
      final PerRecordResponseTransformer transformer) throws IOException {

    final RequestContext context = RequestContext.builder()
        .url(url)
        .parameters(params)
        .headers(config.getHeaders())
        .dimensionValues(variables)
        .build();

    final Iterator<Map<String, Object>> base =
        parseDelimitedResponseStreaming(cachePath, delimiter);

    return new Iterator<Map<String, Object>>() {
      @Override public boolean hasNext() {
        return base.hasNext();
      }
      @Override public Map<String, Object> next() {
        Map<String, Object> row = base.next();
        transformer.transformRecord(row, context);
        return row;
      }
    };
  }

  /**
   * Streams a JSON array from the raw cache file without loading it into a String.
   *
   * <p>Used when the transformer implements {@link PerRecordResponseTransformer}: instead of
   * reading the entire file into memory, this method opens an InputStream, navigates to the
   * JSON array (either a bare array or a {@code results} field inside an object), and returns
   * a lazy Iterator that decodes one row at a time.
   */
  private Iterator<Map<String, Object>> streamFromRawCache(
      final String cachePath,
      final String url,
      final Map<String, String> params,
      final Map<String, String> variables,
      final PerRecordResponseTransformer transformer) throws IOException {

    final RequestContext context = RequestContext.builder()
        .url(url)
        .parameters(params)
        .headers(config.getHeaders())
        .dimensionValues(variables)
        .build();

    final InputStream is = isLocalPath(cachePath)
        ? new FileInputStream(cachePath)
        : storageProvider.openInputStream(cachePath);

    final JsonParser parser;
    try {
      parser = OBJECT_MAPPER.getFactory().createParser(is);
      JsonToken token = parser.nextToken();
      if (token == JsonToken.START_OBJECT) {
        boolean found = false;
        while (parser.nextToken() != null) {
          if ("results".equals(parser.currentName())
              && parser.nextToken() == JsonToken.START_ARRAY) {
            found = true;
            break;
          }
          parser.skipChildren();
        }
        if (!found) {
          parser.close();
          LOGGER.warn("streamFromRawCache: no results array in {}", cachePath);
          return Collections.emptyIterator();
        }
      } else if (token != JsonToken.START_ARRAY) {
        parser.close();
        LOGGER.warn("streamFromRawCache: unexpected token {} in {}", token, cachePath);
        return Collections.emptyIterator();
      }
    } catch (IOException e) {
      is.close();
      throw e;
    }

    LOGGER.info("Streaming from raw cache: {}", cachePath);

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending = null;
      private boolean exhausted = false;

      @Override public boolean hasNext() {
        if (exhausted) {
          return false;
        }
        if (pending != null) {
          return true;
        }
        try {
          if (parser.nextToken() == JsonToken.START_OBJECT) {
            Map<String, Object> row = OBJECT_MAPPER.readValue(parser, Map.class);
            transformer.transformRecord(row, context);
            pending = normalizeRow(row, variables);
            return true;
          }
        } catch (IOException e) {
          LOGGER.error("streamFromRawCache: error reading {}: {}", cachePath, e.getMessage());
        }
        exhausted = true;
        try {
          parser.close();
        } catch (IOException e) {
          LOGGER.debug("streamFromRawCache: error closing parser for {}: {}", cachePath,
              e.getMessage());
        }
        return false;
      }

      @Override public Map<String, Object> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = pending;
        pending = null;
        return row;
      }
    };
  }

  private Map<String, Object> normalizeRow(Map<String, Object> row,
      Map<String, String> context) {
    if (variableNormalizer == null) {
      return row;
    }
    Map<String, Object> normalized = new LinkedHashMap<String, Object>(row.size());
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String fieldName = entry.getKey();
      String normalizedName;
      if (variableNormalizer.shouldPreserve(fieldName)) {
        normalizedName = fieldName;
      } else {
        normalizedName = variableNormalizer.normalize(fieldName, context);
        if (normalizedName == null) {
          normalizedName = fieldName;
        }
      }
      normalized.put(normalizedName, entry.getValue());
    }
    return normalized;
  }

  /**
   * Writes response to raw cache in storage provider.
   *
   * @param cachePath Path to write the cached response
   * @param content Response content to cache
   */
  private void writeRawCache(String cachePath, String content) {
    try {
      if (isLocalPath(cachePath)) {
        File file = new File(cachePath);
        file.getParentFile().mkdirs();
        Files.write(Paths.get(cachePath), content.getBytes(StandardCharsets.UTF_8));
        LOGGER.debug("Raw cache written (local): {} ({} bytes)", cachePath, content.length());
        evictLocalCacheIfNeeded();
        return;
      }
      // Ensure parent directory exists
      String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
      storageProvider.createDirectories(parentPath);

      // Write content
      storageProvider.writeFile(cachePath, content.getBytes(StandardCharsets.UTF_8));
      LOGGER.info("Raw cache written: {} ({} bytes)", cachePath, content.length());
    } catch (IOException e) {
      LOGGER.warn("Failed to write raw cache: {} - {}", cachePath, e.getMessage());
    }
  }

  /** Tracks bytes written since last eviction check to avoid scanning on every write. */
  private static final java.util.concurrent.atomic.AtomicLong bytesSinceEvictionCheck =
      new java.util.concurrent.atomic.AtomicLong(0);

  /** Check eviction every 50MB of writes. */
  private static final long EVICTION_CHECK_INTERVAL_BYTES = 50L * 1024 * 1024;

  /**
   * Evicts oldest cache files when local raw cache exceeds the size limit.
   * The limit is configurable via ETL_RAW_CACHE_MAX_MB (default: 2048MB).
   * Eviction removes the oldest files (by last-modified time) until the cache
   * is at 80% of the limit.
   */
  private void evictLocalCacheIfNeeded() {
    if (localRawCachePath == null) {
      return;
    }

    // Only check periodically to avoid scanning on every write
    if (bytesSinceEvictionCheck.addAndGet(1024) < EVICTION_CHECK_INTERVAL_BYTES) {
      return;
    }
    bytesSinceEvictionCheck.set(0);

    // Find the cache root - same priority as computeLocalRawCachePath:
    // 1. ETL_LOCAL_RAW_CACHE env var, 2. operatingDirectory, 3. fallback
    String cacheRoot = System.getenv("ETL_LOCAL_RAW_CACHE");
    if (cacheRoot == null && operatingDirectory != null) {
      cacheRoot = operatingDirectory + "/cache/raw";
    }
    if (cacheRoot == null) {
      String workDir = System.getProperty("user.dir", System.getProperty("user.home", "/tmp"));
      cacheRoot = workDir + "/.aperio/cache/raw";
    }
    File rootDir = new File(cacheRoot);
    if (!rootDir.exists()) {
      return;
    }

    long maxBytes =
        Long.parseLong(
            System.getProperty("calcite.etl.rawCacheMaxMb", System.getenv("ETL_RAW_CACHE_MAX_MB") != null
            ? System.getenv("ETL_RAW_CACHE_MAX_MB") : "2048")) * 1024L * 1024L;

    // Collect all files with their sizes and timestamps
    List<File> allFiles = new ArrayList<File>();
    collectFiles(rootDir, allFiles);

    long totalSize = 0;
    for (File f : allFiles) {
      totalSize += f.length();
    }

    if (totalSize <= maxBytes) {
      return;
    }

    // Sort by last-modified ascending (oldest first)
    Collections.sort(allFiles, new Comparator<File>() {
      @Override public int compare(File a, File b) {
        return Long.compare(a.lastModified(), b.lastModified());
      }
    });

    long targetSize = (long) (maxBytes * 0.8); // Evict down to 80%
    long evictedBytes = 0;
    int evictedCount = 0;
    for (File f : allFiles) {
      if (totalSize <= targetSize) {
        break;
      }
      long fileSize = f.length();
      if (f.delete()) {
        totalSize -= fileSize;
        evictedBytes += fileSize;
        evictedCount++;
        // Clean up empty parent directories
        File parent = f.getParentFile();
        while (parent != null && !parent.equals(rootDir)) {
          String[] children = parent.list();
          if (children != null && children.length == 0) {
            parent.delete();
            parent = parent.getParentFile();
          } else {
            break;
          }
        }
      }
    }

    if (evictedCount > 0) {
      LOGGER.info("Raw cache eviction: removed {} files ({} MB) to stay within {} MB limit",
          evictedCount, evictedBytes / (1024 * 1024), maxBytes / (1024 * 1024));
    }
  }

  /** Recursively collects all files under a directory. */
  private static void collectFiles(File dir, List<File> result) {
    File[] children = dir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        collectFiles(child, result);
      } else {
        result.add(child);
      }
    }
  }

  /**
   * Cache entry with expiration.
   */
  private static class CacheEntry {
    private final List<Map<String, Object>> data;
    private final long expiresAt;

    CacheEntry(List<Map<String, Object>> data, long expiresAt) {
      this.data = data;
      this.expiresAt = expiresAt;
    }

    List<Map<String, Object>> getData() {
      return data;
    }

    boolean isExpired() {
      return System.currentTimeMillis() > expiresAt;
    }
  }
}
