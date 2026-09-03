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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
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
  private static final String DEFAULT_USER_AGENT =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      + "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
  @SuppressWarnings("UnusedVariable")
  private static final Pattern VAR_PATTERN = Pattern.compile("\\{([^}]+)\\}");
  @SuppressWarnings("UnusedVariable")
  private static final Pattern ENV_PATTERN = Pattern.compile("env:(.+)");

  private final HttpSourceConfig config;
  private final Map<String, CacheEntry> cache;
  private final ResponseTransformer responseTransformer;
  private final VariableNormalizer variableNormalizer;
  private final StorageProvider storageProvider;
  private final String rawCachePath;
  /** Operating directory from model operands (e.g., .aperio/<schema>), used for local cache base. */
  private final String operatingDirectory;
  /**
   * When true, {@link #hasValidRawCache} always reports a miss regardless of what's on disk —
   * forces a live fetch instead of reading a possibly-corrupted cached response (e.g. one
   * written by a since-fixed response-parsing bug), and the live fetch's normal cache-write
   * path then overwrites the stale entry with the corrected one. See {@code EtlPipeline}'s
   * {@code GOVDATA_FORCE_DOWNLOAD_TABLES} handling for how this gets set.
   */
  private final boolean bypassRawCache;
  /**
   * Fetch-time keys of the columns the table declares as textual. Delimited formats carry no
   * types, so {@link #parseValue} otherwise infers one per value — and an all-digit identifier
   * (FIPS code, ZIP, CIK) parses as a number, dropping its leading zeros. The declared type is
   * authoritative over that inference: a key in this set is never coerced.
   */
  private final Set<String> textualSourceKeys;
  /** CAS-based slot reservation for lock-free rate limiting across parallel threads. */
  private final AtomicLong nextAllowedNanos = new AtomicLong();

  /**
   * DQ sample row cap (0 = uncapped). When &gt; 0 the source runs in capped mode: the raw-cache
   * key gets a {@code cap=<N>} segment (isolating the DQ sample from the full prod cache), and a
   * paginated iterator commits its accumulated cache on {@code close()}. A row cap stops a cursor
   * before its natural EOF, so without this the cache is never committed and the source
   * re-downloads on every DQ run. Set by EtlPipeline in DQ sample mode; left 0 in prod so the
   * full cache (committed only on EOF) is unaffected. Volatile: set once before the parallel
   * fetch loop, read by fetch threads.
   */
  private volatile int rawCacheRowCap = 0;

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
    this(config, hooksConfig, storageProvider, rawCachePath, operatingDirectory, null, false);
  }

  /**
   * Creates a new HttpSource that resolves delimited-format value types against the columns
   * the table declares.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   * @param storageProvider Storage provider for raw response caching (S3, local, etc.)
   * @param rawCachePath Base path for raw response cache (e.g., s3://bucket/.raw)
   * @param operatingDirectory Operating directory for local cache (e.g., .aperio/schema); may be null
   * @param columns Declared columns of the table being fetched; may be null when the table
   *                declares none, in which case delimited values fall back to inference
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig,
      StorageProvider storageProvider, String rawCachePath, String operatingDirectory,
      List<ColumnConfig> columns) {
    this(config, hooksConfig, storageProvider, rawCachePath, operatingDirectory, columns, false);
  }

  /**
   * Creates a new HttpSource with control over whether an existing raw cache entry may be read.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   * @param storageProvider Storage provider for raw response caching (S3, local, etc.)
   * @param rawCachePath Base path for raw response cache (e.g., s3://bucket/.raw)
   * @param operatingDirectory Operating directory for local cache (e.g., .aperio/schema); may be null
   * @param columns Declared columns of the table being fetched; may be null when the table
   *                declares none, in which case delimited values fall back to inference
   * @param bypassRawCache when true, treat every raw cache lookup as a miss and always fetch
   *                        live, overwriting whatever was cached
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig,
      StorageProvider storageProvider, String rawCachePath, String operatingDirectory,
      List<ColumnConfig> columns, boolean bypassRawCache) {
    this.config = config;
    this.cache = config.getCache().isEnabled()
        ? new ConcurrentHashMap<String, CacheEntry>()
        : null;
    // Rate limiting uses AtomicLong nextAllowedNanos (CAS-based, no init needed)
    this.responseTransformer = loadResponseTransformer(hooksConfig);
    this.variableNormalizer = loadVariableNormalizer(hooksConfig);
    // A raw cache path names a location only storageProvider knows how to write. EtlPipeline
    // sets the two together -- it derives rawCachePath inside the branch that already resolved a
    // provider -- but nothing here enforced it, so a caller could pair a path with no provider
    // and get a NullPointerException several layers down in a write instead of at the mistake.
    if (rawCachePath != null && storageProvider == null) {
      throw new IllegalArgumentException(
          "rawCachePath '" + rawCachePath + "' requires a storageProvider to write it");
    }
    this.storageProvider = storageProvider;
    this.rawCachePath = rawCachePath;
    this.operatingDirectory = operatingDirectory;
    this.textualSourceKeys = textualSourceKeys(columns);
    this.bypassRawCache = bypassRawCache;
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
    this.textualSourceKeys = Collections.emptySet();
    this.bypassRawCache = false;
  }

  /**
   * Sets the DQ sample row cap. See {@link #rawCacheRowCap}. A non-positive value leaves the
   * source uncapped (the default).
   */
  public void setRawCacheRowCap(int cap) {
    this.rawCacheRowCap = Math.max(0, cap);
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
          .rateLimit(config.getRateLimit())
          .build();
      return ((StreamingResponseTransformer) responseTransformer).fetchAndTransform(ctx);
    }

    // Check raw cache first (persistent storage-based)
    String rawCacheFilePath = null;
    if (isRawCacheEnabled()) {
      rawCacheFilePath = buildRawCachePath(variables);
      // CSV_STREAM: stage the FULL response into the raw cache first (uncapped) so the cache-read
      // below — and every later run — is a hit; dqRowLimit then caps only the raw→parquet read, not
      // the download. A skipped partition (4xx skipOn / no matching entry) caches nothing and
      // yields no rows.
      if (!hasValidRawCache(rawCacheFilePath)
          && config.getResponse().getPagination().getType()
              == HttpSourceConfig.PaginationType.CSV_STREAM) {
        if (!fetchCsvStreamToRawCache(url, params, variables, rawCacheFilePath)) {
          return java.util.Collections.<Map<String, Object>>emptyIterator();
        }
      }
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
        if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.FIXED_WIDTH
            && responseTransformer == null) {
          LOGGER.info("Streaming fixed-width from raw cache: {}", rawCacheFilePath);
          return parseFixedWidthResponseStreaming(rawCacheFilePath);
        }
        // For JSON with a per-record transformer, stream directly from cache file
        if (responseTransformer instanceof PerRecordResponseTransformer
            && respConfig.getFormat() == HttpSourceConfig.ResponseFormat.JSON) {
          return streamFromRawCache(rawCacheFilePath, url, params, variables,
              (PerRecordResponseTransformer) responseTransformer);
        }
        // A paginated JSON source's cache holds the merged {"results":[...]} envelope, not any
        // upstream body. With no transformer to extract the records itself, parseResponse would
        // see that envelope as one object and emit a single row whose every source column is
        // null — so stream the envelope's array instead.
        if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.JSON
            && responseTransformer == null
            && respConfig.getPagination().getType() != HttpSourceConfig.PaginationType.NONE) {
          return streamJsonFromRawCache(rawCacheFilePath, variables);
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
      if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.FIXED_WIDTH
          && responseTransformer == null) {
        return parseFixedWidthResponseStreaming(cachePath);
      }

      // For JSON, read from cache, transform, and parse.
      // When rawCacheFilePath is null (rawCache disabled), executeRequest returns the raw
      // response body directly (not a path) — use it as-is.
      // When skipResponseBody=true, cachePath is "" — no content.
      String content = rawCacheFilePath == null ? cachePath
          : (cachePath.isEmpty() ? "" : readFromCache(cachePath));
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

  private class PaginatedIterator implements Iterator<Map<String, Object>>, java.io.Closeable {
    private final String url;
    private final Map<String, String> baseParams;
    private final Map<String, String> variables;
    private final HttpSourceConfig.PaginationConfig pagination;
    @SuppressWarnings("UnusedVariable")
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
    // Live connection for a CSV_STREAM pull; disconnected on close() so an early stop
    // (e.g. dqRowLimit) aborts the download instead of transferring the whole (often huge)
    // body.
    private HttpURLConnection csvConn = null;

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

    @Override public void close() {
      // Capped (DQ) mode: a row cap stops the cursor before its natural EOF, so none of the EOF
      // commit points were reached and the accumulated pages would be discarded — the reason a
      // capped cursor table re-downloads every DQ run. Commit now under the cap-scoped key. This is
      // idempotent and narrowly scoped: writeMergedCache guards on mergedCacheWritten (EOF/error
      // paths already committed → no-op) and on a null generator (CSV_STREAM, which caches via a
      // different path, and uncapped reads → no-op). Capped mode only, so an uncapped prod run that
      // closes early never persists a partial full-cache. The accumulated cache holds whole fetched
      // pages, so a later DQ run re-reads, re-transforms (incl. fan-out) and re-caps to the same
      // deterministic sample.
      if (rawCacheRowCap > 0) {
        writeMergedCache();
      }
      // Disconnect FIRST: killing the socket means a subsequent reader close can't drain or
      // re-read the rest of the entry. This is what makes an early stop (dqRowLimit) actually
      // abort the download rather than transfer the whole body.
      if (csvConn != null) {
        csvConn.disconnect();
        csvConn = null;
      }
      if (csvReader != null) {
        try {
          csvReader.close();
        } catch (IOException e) {
          LOGGER.debug("Failed to close CSV_STREAM reader: {}", e.getMessage());
        }
        csvReader = null;
      }
    }

    private boolean fetchNextPage() {
      if (!hasMore) {
        return false;
      }

      // Body-cursor (POST/GraphQL) pagination is driven separately: the cursor is templated
      // into the request body, not the query string, and termination follows hasNextPath.
      if (pagination.getType() == HttpSourceConfig.PaginationType.CURSOR
          && pagination.isCursorInBody()) {
        return fetchNextCursorBodyPage();
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

      // fallback-guard: allow logs the fetch failure at ERROR and halts pagination (hasMore=false, return false), a legitimate failure signal, not a fabricated page
      } catch (IOException e) {
        LOGGER.error("Error fetching paginated data: {}", e.getMessage());
        hasMore = false;
        return false;
      }
    }

    /**
     * Fetches the next page of a body-cursor (POST/GraphQL) crawl. The cursor, page size, and
     * incremental bound are injected as native typed values into the request body's GraphQL
     * {@code variables} object (so the cursor is JSON null on the first page, a JSON string
     * thereafter); the query string uses {@code $}-style GraphQL variables and is sent verbatim.
     * Termination follows the configured {@code hasNextPath} boolean read from the raw envelope.
     * A transformed page that is empty (the transform may legitimately drop all rows on a page)
     * is skipped so the outer iterator never observes an empty page while more pages remain.
     * Fetch failures propagate — a failed page fails the crawl rather than silently truncating it.
     */
    @SuppressWarnings("unchecked")
    private boolean fetchNextCursorBodyPage() {
      while (true) {
        // Build the per-page GraphQL variables with native types (a null cursor → JSON null).
        Map<String, Object> body = config.getBody() != null
            ? new LinkedHashMap<String, Object>(config.getBody())
            : new LinkedHashMap<String, Object>();
        Map<String, Object> gqlVars = new LinkedHashMap<String, Object>();
        Object existing = body.get("variables");
        if (existing instanceof Map) {
          gqlVars.putAll((Map<String, Object>) existing);
        }
        gqlVars.put(pagination.getCursorParam(), cursor); // null on the first page
        if (pagination.getLimitParam() != null) {
          gqlVars.put(pagination.getLimitParam(), pageSize);
        }
        if (pagination.getBoundParam() != null) {
          // Incremental lower bound (e.g. updatedSince) injected into the fetch variables
          // upstream; null (→ full crawl) when no bound is set yet.
          String bound = variables.get(pagination.getBoundParam());
          gqlVars.put(pagination.getBoundParam(),
              bound == null || bound.isEmpty() ? null : bound);
        }
        body.put("variables", gqlVars);

        String rawResponse;
        try {
          rawResponse = executeRequest(url, baseParams, variables, null, body);
        } catch (IOException e) {
          throw new RuntimeException("Body-cursor pagination failed at cursor="
              + cursor + ": " + e.getMessage(), e);
        }

        accumulateRawPage(rawResponse);
        List<Map<String, Object>> pageData;
        try {
          String transformed = transformResponse(rawResponse, url, baseParams, variables);
          pageData = parseResponse(transformed);
        } catch (IOException e) {
          throw new RuntimeException("Failed to transform/parse page: " + e.getMessage(), e);
        }
        pageData = normalizeRecords(pageData, variables);

        // Termination and the next cursor come from the raw envelope, not the transformed rows.
        boolean morePages;
        String nextCursor = null;
        try {
          JsonNode root = OBJECT_MAPPER.readTree(rawResponse);
          String hnp = pagination.getHasNextPath();
          JsonNode hn = (hnp != null && !hnp.isEmpty()) ? navigateToPath(root, hnp) : null;
          morePages = hn != null && hn.isBoolean() && hn.asBoolean();
          String cp = pagination.getCursorPath();
          if (cp != null && !cp.isEmpty()) {
            JsonNode cn = navigateToPath(root, cp);
            nextCursor = (cn != null && cn.isTextual()) ? cn.asText() : null;
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to read pagination state: " + e.getMessage(), e);
        }
        boolean canAdvance = morePages && nextCursor != null && !nextCursor.isEmpty();

        if (!pageData.isEmpty()) {
          currentPageIterator = pageData.iterator();
          if (canAdvance) {
            cursor = nextCursor;
          } else {
            hasMore = false;
            writeMergedCache();
          }
          LOGGER.debug("Body-cursor page: {} records (total yielded: {})",
              pageData.size(), totalYielded);
          return true;
        }

        // Transformed page is empty — keep paging if the source says there is more.
        if (canAdvance) {
          cursor = nextCursor;
          continue;
        }
        hasMore = false;
        writeMergedCache();
        return false;
      }
    }

    private boolean fetchNextCsvBatch() {
      try {
        if (csvReader == null) {
          // Open the streaming connection on first call
          enforceRateLimit();
          String fullUrl = buildUrlWithParams(url, baseParams);
          CsvStream cs = openCsvStream(fullUrl, variables);
          if (cs == null) {
            // Skipped (a 4xx matching skipOn) or no matching zip entry — an expected partition
            // gap (weekends/holidays/future dates on date-partitioned feeds), not a failure.
            hasMore = false;
            return false;
          }
          csvConn = cs.conn;
          csvReader = new BufferedReader(new InputStreamReader(cs.stream, StandardCharsets.UTF_8));
          // Read the header as a complete record (in case header itself has quoted multi-line cell)
          csvHeaderLine = CsvRecordReader.readRecord(csvReader);
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
        // Use CsvRecordReader so quoted multi-line fields (e.g. patent summaries, FDA
        // adverse event narratives, clinical trial descriptions) are not truncated.
        while (linesRead < batchSize && (line = CsvRecordReader.readRecord(csvReader)) != null) {
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

      // fallback-guard: allow logs the batch failure at ERROR and halts the stream (hasMore=false, return false), a legitimate failure signal, not a fabricated batch
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
          tempCacheFile = File.createTempFile("http-raw-cache-", ".json");
          tempCacheFile.deleteOnExit();
          tempCacheStream = new FileOutputStream(tempCacheFile);
          cacheGenerator =
              OBJECT_MAPPER.getFactory().createGenerator(tempCacheStream, com.fasterxml.jackson.core.JsonEncoding.UTF8);
          cacheGenerator.writeStartObject();
          cacheGenerator.writeArrayFieldStart("results");
        }
        com.fasterxml.jackson.databind.JsonNode root = OBJECT_MAPPER.readTree(rawResponse);
        com.fasterxml.jackson.databind.JsonNode resultsNode;
        String configuredDataPath = config.getResponse() == null
            ? null : config.getResponse().getDataPath();
        if (configuredDataPath != null && !configuredDataPath.isEmpty()) {
          resultsNode = navigateToPath(root, configuredDataPath);
        } else if (root.has("results")) {
          resultsNode = root.get("results");
        } else {
          resultsNode = root;
        }
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
        storageProvider.createDirectories(parentPath);
        try (java.io.InputStream in = new FileInputStream(tempCacheFile)) {
          storageProvider.writeFile(rawCacheFilePath, in);
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

  /** A live CSV stream opened from a CSV_STREAM source, with its owning connection. */
  private static final class CsvStream {
    final HttpURLConnection conn;
    final InputStream stream;
    CsvStream(HttpURLConnection conn, InputStream stream) {
      this.conn = conn;
      this.stream = stream;
    }
  }

  /**
   * Connects to a CSV_STREAM source and returns its raw CSV stream — the first zip entry matching
   * {@code extractPattern}, or the (optionally gunzipped) body — with the connection so the caller
   * can disconnect it. Returns {@code null} when the request should be skipped: a 4xx matching
   * skipOn (an expected gap on date-partitioned feeds), or no zip entry matched the pattern.
   */
  private CsvStream openCsvStream(String fullUrl, Map<String, String> variables) throws IOException {
    failFastIfKnown404(fullUrl);
    HttpURLConnection conn =
        (HttpURLConnection) java.net.URI.create(fullUrl).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000); // 5 min for large CSV files
    conn.setRequestProperty("User-Agent", DEFAULT_USER_AGENT);
    for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
      conn.setRequestProperty(e.getKey(), e.getValue());
    }
    applyAuth(conn, variables);
    int status = conn.getResponseCode();
    if (status >= 400) {
      if (shouldSkip(status, config.getRateLimit())) {
        LOGGER.debug("CSV_STREAM skip: HTTP {} for {} (skipOn match)", status, fullUrl);
        conn.disconnect();
        return null;
      }
      rememberIf404(fullUrl, status);
      throw new IOException("HTTP " + status + " for CSV_STREAM: " + fullUrl);
    }
    InputStream is = conn.getInputStream();
    String extractPattern = config.getExtractPattern();
    if (extractPattern != null && !extractPattern.isEmpty()) {
      String resolvedPattern = substituteVariables(extractPattern, variables);
      ZipInputStream zis = new ZipInputStream(is);
      ZipEntry zipEntry;
      boolean matched = false;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (!zipEntry.isDirectory() && zipEntryMatches(zipEntry.getName(), resolvedPattern)) {
          LOGGER.info("CSV_STREAM zip entry: {}", zipEntry.getName());
          matched = true;
          break;
        }
        zis.closeEntry();
      }
      if (!matched) {
        zis.close();
        conn.disconnect();
        return null;
      }
      is = zis;
    } else if ("gzip".equalsIgnoreCase(config.getResponse().getCompressed())) {
      is = new GZIPInputStream(is);
    }
    return new CsvStream(conn, is);
  }

  /**
   * Stage 1 of the CSV_STREAM two-step (full-to-raw, then capped raw-to-parquet): stream the full
   * matched entry from the remote into the raw cache, uncapped, so the cache-read that follows — and
   * every later run — is a hit and {@code dqRowLimit} caps only the raw→parquet read, never the
   * download. The temp is committed to {@code rawCacheFilePath} only after a complete copy; a
   * dropped/aborted stream leaves it uncommitted, so a partial download never poisons the cache.
   * Returns {@code false} when the request was skipped (4xx skipOn / no matching zip entry).
   */
  private boolean fetchCsvStreamToRawCache(String url, Map<String, String> params,
      Map<String, String> variables, String rawCacheFilePath) throws IOException {
    enforceRateLimit();
    String fullUrl = buildUrlWithParams(url, params);
    CsvStream cs = openCsvStream(fullUrl, variables);
    if (cs == null) {
      return false;
    }
    File tempFile = File.createTempFile("http-csv-cache-", ".csv");
    tempFile.deleteOnExit();
    try {
      try (InputStream in = cs.stream; FileOutputStream out = new FileOutputStream(tempFile)) {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
      } finally {
        cs.conn.disconnect();
      }
      // Copy completed (no exception) → commit the temp to the raw cache.
      String parentPath = rawCacheFilePath.substring(0, rawCacheFilePath.lastIndexOf('/'));
      storageProvider.createDirectories(parentPath);
      try (InputStream in = new FileInputStream(tempFile)) {
        storageProvider.writeFile(rawCacheFilePath, in);
      }
      LOGGER.info("CSV_STREAM cached full response to raw: {} ({} bytes)",
          rawCacheFilePath, tempFile.length());
      return true;
    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
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
   * URLs that returned HTTP 404 during this JVM (worker) run. A 404 means the resource is not
   * published yet; re-requesting the same URL within the run is wasted work — and a 404 URL shared
   * across several tables would otherwise re-fire once per table (observed on edu IPEDS / census
   * ACS). Process-wide + in-memory; dedupes within the run, complementing the cross-run tracker
   * "unavailable" backoff. Only genuine throw-404s are recorded (not declared skipOn gaps or
   * skipResponseBody triggers), so the short-circuit reproduces the same throw without the network.
   */
  private static final java.util.Set<String> KNOWN_404_URLS =
      java.util.concurrent.ConcurrentHashMap.newKeySet();

  private static void failFastIfKnown404(String urlString) throws IOException {
    if (KNOWN_404_URLS.contains(urlString)) {
      throw new IOException("HTTP 404 (known unavailable this run, request skipped): " + urlString);
    }
  }

  private static void rememberIf404(String urlString, int status) {
    if (status == 404) {
      KNOWN_404_URLS.add(urlString);
    }
  }

  /**
   * Performs the actual HTTP request with a specific body.
   */
  private String doRequestWithBody(String urlString, Map<String, String> variables,
      Map<String, Object> body) throws IOException {
    failFastIfKnown404(urlString);
    java.net.URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod(config.getMethod().name());
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(120000);

      applyHeadersAndAuth(conn, variables);

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
        rememberIf404(urlString, responseCode);
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
    return executeRequest(baseUrl, params, variables, rawCachePath, null);
  }

  /**
   * @param bodyOverride per-request POST/PUT body to serialize instead of {@code config.getBody()}
   *                     (used by body-cursor pagination to inject typed GraphQL variables); null
   *                     to use the configured body.
   */
  private String executeRequest(String baseUrl, Map<String, String> params,
      Map<String, String> variables, String rawCachePath,
      Map<String, Object> bodyOverride) throws IOException {

    // Rate limiting
    enforceRateLimit();

    // Build full URL with query parameters. When a urlResolver is configured, the configured
    // url + params address a JSON resolver endpoint; the resolved (self-contained) download
    // URL is fetched instead, so query params are not re-applied to it.
    HttpSourceConfig.UrlResolverConfig resolver = config.getUrlResolver();
    String fullUrl;
    if (resolver != null) {
      fullUrl = resolveDownloadUrl(buildUrlWithParams(baseUrl, params), resolver, variables);
    } else {
      fullUrl = buildUrlWithParams(baseUrl, params);
    }

    // Circuit breaker: if this origin has returned >= threshold consecutive 503s, treat it as down
    // and fast-skip (no request, no retries) until a success resets it. EtlPipeline catches
    // SkippedBatchException and skips the batch gracefully.
    java.util.concurrent.atomic.AtomicInteger open = CONSECUTIVE_503.get(baseUri(fullUrl));
    if (open != null && open.get() >= CIRCUIT_503_THRESHOLD) {
      throw new SkippedBatchException("Circuit open — origin returned >= " + CIRCUIT_503_THRESHOLD
          + " consecutive 503s, fast-skipping: " + baseUri(fullUrl));
    }

    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    int retries = 0;
    IOException lastException = null;

    while (retries <= rateLimit.getMaxRetries()) {
      try {
        String response = doRequest(fullUrl, variables, rawCachePath, bodyOverride);
        return response;
      } catch (IOException e) {
        lastException = e;

        // Check if we should retry
        if (shouldRetry(e, rateLimit)) {
          retries++;
          if (retries <= rateLimit.getMaxRetries()) {
            long backoff;
            if (e instanceof RetryableHttpException
                && ((RetryableHttpException) e).retryAfterMs >= 0L) {
              // Server told us exactly how long to wait (503/429 Retry-After) — honor it (capped).
              backoff = ((RetryableHttpException) e).retryAfterMs;
            } else {
              backoff = rateLimit.getRetryBackoffMs() * (1L << (retries - 1));
            }
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

  // ── 503 circuit breaker + Retry-After ──────────────────────────────────────────────────────
  /** Consecutive HTTP-503 responses per base URI (scheme+host+path, no query). When a base URI
   *  reaches {@link #CIRCUIT_503_THRESHOLD} its origin is treated as down: further requests to it
   *  fast-skip (the batch is skipped) instead of retrying, until any 2xx resets the count. A 503
   *  means the request never reached the origin's app layer, so retrying it endlessly only wastes
   *  the worker — once it's clearly persistent we stop calling that URI. */
  private static final java.util.concurrent.ConcurrentMap<String, java.util.concurrent.atomic.AtomicInteger>
      CONSECUTIVE_503 =
          new java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger>();
  private static final int CIRCUIT_503_THRESHOLD = 5;
  /** Cap on how long we honor a server-provided Retry-After — a maintenance window can be hours,
   *  and we must not block a worker indefinitely. */
  private static final long RETRY_AFTER_CAP_MS = 300_000L;

  /** A retryable HTTP error carrying the server's Retry-After delay in ms (-1 if not provided). */
  private static final class RetryableHttpException extends IOException {
    private final long retryAfterMs;
    RetryableHttpException(String message, long retryAfterMs) {
      super(message);
      this.retryAfterMs = retryAfterMs;
    }
  }

  /** The circuit-breaker key for a request: the URL with the query string stripped. */
  private static String baseUri(String fullUrl) {
    int q = fullUrl.indexOf('?');
    return q >= 0 ? fullUrl.substring(0, q) : fullUrl;
  }

  /** Parses a Retry-After header (delta-seconds or HTTP-date) to ms, capped; -1 if absent. */
  private static long parseRetryAfter(HttpURLConnection conn) {
    String ra = conn.getHeaderField("Retry-After");
    if (ra == null || ra.trim().isEmpty()) {
      return -1L;
    }
    try {
      return Math.min(RETRY_AFTER_CAP_MS, Long.parseLong(ra.trim()) * 1000L);
    // fallback-guard: allow Retry-After parsing tries delta-seconds first and, on NumberFormatException, retries with HTTP-date parsing via conn.getHeaderFieldDate — a genuine alternate-format parse strategy, not a masked failure.
    } catch (NumberFormatException notSeconds) {
      long when = conn.getHeaderFieldDate("Retry-After", -1L);
      if (when > 0L) {
        long delta = when - System.currentTimeMillis();
        return delta <= 0L ? 0L : Math.min(RETRY_AFTER_CAP_MS, delta);
      }
      return -1L;
    }
  }

  /**
   * Sets the default User-Agent (when not overridden), the configured headers (skipping
   * values that are empty after substitution), and authentication on the connection.
   */
  private void applyHeadersAndAuth(HttpURLConnection conn, Map<String, String> variables) {
    if (config.getHeaders().get("User-Agent") == null) {
      conn.setRequestProperty("User-Agent", DEFAULT_USER_AGENT);
    }
    for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
      String value = substituteVariables(e.getValue(), variables);
      if (value != null && !value.isEmpty()) {
        conn.setRequestProperty(e.getKey(), value);
      }
    }
    applyAuth(conn, variables);
  }

  /**
   * Resolves the real download URL by fetching a JSON endpoint and extracting a URL from it.
   *
   * <p>Used for signed-URL services: {@code resolverUrl} returns JSON pointing at the actual
   * download location (e.g. a presigned S3 URL). The field is selected by
   * {@link HttpSourceConfig.UrlResolverConfig#getUrlField()} (literal top-level key, with
   * {@code {var}} substitution); when absent the response must be a single-field object and
   * its only value is used.
   *
   * @param resolverUrl Fully built resolver endpoint URL (variables and params already applied)
   * @param resolver Resolver config controlling field selection
   * @param variables Variable substitution map (for headers/auth and urlField)
   * @return The resolved download URL
   */
  private String resolveDownloadUrl(String resolverUrl,
      HttpSourceConfig.UrlResolverConfig resolver, Map<String, String> variables)
      throws IOException {
    URL url = java.net.URI.create(resolverUrl).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(120000);
      applyHeadersAndAuth(conn, variables);

      int responseCode = conn.getResponseCode();
      if (responseCode < 200 || responseCode >= 300) {
        String errorBody = readResponse(conn.getErrorStream());
        throw new IOException("URL resolver HTTP " + responseCode + " for " + resolverUrl
            + ": " + errorBody);
      }
      JsonNode root = OBJECT_MAPPER.readTree(readResponse(conn.getInputStream()));
      String field = resolver.getUrlField();
      JsonNode urlNode;
      if (field != null && !field.isEmpty()) {
        urlNode = root.get(substituteVariables(field, variables));
      } else {
        if (!root.isObject() || root.size() != 1) {
          throw new IOException("URL resolver expected a single-field JSON object from "
              + resolverUrl + ", got: " + root);
        }
        urlNode = root.elements().next();
      }
      if (urlNode == null || !urlNode.isTextual() || urlNode.asText().isEmpty()) {
        throw new IOException("URL resolver could not extract a download URL from "
            + resolverUrl + ": " + root);
      }
      String resolved = urlNode.asText();
      LOGGER.debug("Resolved download URL from {} -> {}", resolverUrl, resolved);
      return resolved;
    } finally {
      conn.disconnect();
    }
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
    return doRequest(urlString, variables, rawCachePath, null);
  }

  private String doRequest(String urlString, Map<String, String> variables,
      String rawCachePath, Map<String, Object> bodyOverride) throws IOException {
    failFastIfKnown404(urlString);
    URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod(config.getMethod().name());
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(120000);

      // Log headers being used for debugging
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("HTTP {} {} with {} custom headers",
            config.getMethod(), urlString, config.getHeaders().size());
        for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
          LOGGER.debug("  Header: {}={}", e.getKey(),
              e.getKey().toLowerCase().contains("key") ? "[REDACTED]" : e.getValue());
        }
      }

      applyHeadersAndAuth(conn, variables);

      // Handle POST/PUT body if needed
      if (config.getMethod() == HttpSourceConfig.HttpMethod.POST
          || config.getMethod() == HttpSourceConfig.HttpMethod.PUT) {
        conn.setDoOutput(true);
        Map<String, Object> requestBody = bodyOverride != null ? bodyOverride : config.getBody();
        if (requestBody != null && !requestBody.isEmpty()) {
          String bodyContent = serializeBody(requestBody, config.getBodyFormat(), variables);
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
        // Success — clear the circuit-breaker 503 count for this origin.
        java.util.concurrent.atomic.AtomicInteger ok = CONSECUTIVE_503.get(baseUri(urlString));
        if (ok != null) {
          ok.set(0);
        }
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
        HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
        InputStream responseStream = conn.getInputStream();
        if ("gzip".equalsIgnoreCase(respConfig.getCompressed())) {
          responseStream = new GZIPInputStream(responseStream);
        }

        // For non-JSON formats, cache raw bytes directly — no API error check needed
        if (respConfig.getFormat() != HttpSourceConfig.ResponseFormat.JSON) {
          return cacheResponse(responseStream, rawCachePath);
        }

        // Read response into memory first to check for API-level errors before caching
        String responseBody = readResponse(responseStream);
        String apiError = checkForApiError(responseBody, respConfig);
        if (apiError != null) {
          throw new IOException("API error (not cached): " + apiError);
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
        // 503/429: capture any Retry-After so the retry honors it. Count consecutive 503s per base
        // URI so a persistently-down origin trips the circuit breaker (subsequent calls fast-skip).
        if (responseCode == 503 || responseCode == 429) {
          long retryAfterMs = parseRetryAfter(conn);
          if (responseCode == 503) {
            int n = CONSECUTIVE_503
                .computeIfAbsent(baseUri(urlString),
                    k -> new java.util.concurrent.atomic.AtomicInteger())
                .incrementAndGet();
            if (n == CIRCUIT_503_THRESHOLD) {
              LOGGER.warn("Circuit OPEN: {} consecutive 503s for {} — origin down, "
                  + "fast-skipping further calls until a success", n, baseUri(urlString));
            }
          }
          throw new RetryableHttpException("HTTP " + responseCode + ": " + errorBody, retryAfterMs);
        }
        rememberIf404(urlString, responseCode);
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
  @SuppressWarnings("UnusedMethod")
  private String cacheResponse(InputStream input, String cachePath) throws IOException {
    if (cachePath == null) {
      return readResponse(input);
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
    String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
    storageProvider.createDirectories(parentPath);
    File tmpCache = File.createTempFile("cache-str-", ".tmp");
    try {
      try (java.io.Writer w = new java.io.BufferedWriter(
          new java.io.OutputStreamWriter(new FileOutputStream(tmpCache), StandardCharsets.UTF_8))) {
        w.write(response);
      }
      try (InputStream is = new java.io.FileInputStream(tmpCache)) {
        storageProvider.writeFile(cachePath, is);
      }
    } finally {
      tmpCache.delete();
    }
    LOGGER.info("Cached response: {}", cachePath);
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
      HttpSourceConfig.ResponseConfig respConfig) throws IOException {
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
      // This response was declared JSON format and already passed HTTP-success checks, so a
      // parse/navigation failure here means the body is not the well-formed JSON we expected
      // (truncated, HTML error page, etc). Silently treating that as "no error" would let a
      // broken response get cached as valid — and the raw cache is immutable, so a bad entry
      // would cause repeated downstream failures with no signal of the real cause.
      throw new IOException("Failed to check for API error in response (format=JSON, "
          + "errorPath=" + respConfig.getErrorPath() + "): " + e.getMessage(), e);
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
   * Adds derived variables computed from the given dimension variables.
   *
   * @param variables dimension variables for this batch
   * @return variables map extended with derived entries
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
    try (java.io.InputStream fis = new FileInputStream(tempFile)) {
      String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
      storageProvider.createDirectories(parentPath);
      storageProvider.writeFile(cachePath, fis);
    }
    tempFile.delete();
    LOGGER.info("Cached {} MB: {}", totalBytes / (1024 * 1024), cachePath);
    return cachePath;
  }

  /** Glob-matches a zip entry name against a {@code *.csv}-style extract pattern. */
  private static boolean zipEntryMatches(String name, String pattern) {
    String regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");
    return name.matches(regex) || name.endsWith(pattern.replace("*", ""));
  }

  /**
   * Extracts every zip entry matching {@code pattern} into one concatenated cache file — a
   * wildcard pattern (e.g. {@code *__ALL_AREAS_*.csv}) can legitimately match many entries (BEA's
   * SAINC.zip carries one CSV per SAINC table), and dropping every match after the first silently
   * discards the rest. For a CSV response, the header row of every match after the first is
   * skipped so the concatenated file stays one valid CSV rather than a header repeated mid-file.
   */
  private String extractFromZip(InputStream input, String pattern, String cachePath)
      throws IOException {
    boolean csvFormat = config.getResponse().getFormat() == HttpSourceConfig.ResponseFormat.CSV;
    // Each matched entry lands in its own temp file first. Merging needs every entry's header
    // before it can write the first row, and the zip arrives as a one-shot stream that cannot be
    // rewound; staging on disk keeps the merge streaming row-by-row rather than in memory.
    List<File> parts = new ArrayList<File>();
    List<String> partNames = new ArrayList<String>();
    try (ZipInputStream zis = new ZipInputStream(input)) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (zipEntryMatches(name, pattern)) {
          LOGGER.info("Extracting from ZIP: {}", name);
          File part = File.createTempFile("http-source-part-", ".tmp");
          part.deleteOnExit();
          try (FileOutputStream pos = new FileOutputStream(part)) {
            copyZipEntry(zis, pos, false);
          }
          parts.add(part);
          partNames.add(name);
        }
        zis.closeEntry();
      }
    }
    if (parts.isEmpty()) {
      throw new IOException("No file matching pattern '" + pattern + "' found in ZIP");
    }

    File tempFile = File.createTempFile("http-source-", ".tmp");
    tempFile.deleteOnExit();
    try {
      mergeZipParts(parts, partNames, tempFile, csvFormat);
      long totalBytes = tempFile.length();
      try (InputStream fis = new FileInputStream(tempFile)) {
        String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
        storageProvider.createDirectories(parentPath);
        storageProvider.writeFile(cachePath, fis);
      }
      LOGGER.info("Cached {} MB: {}", totalBytes / (1024 * 1024), cachePath);
    } finally {
      tempFile.delete();
      for (File part : parts) {
        part.delete();
      }
    }
    return cachePath;
  }

  /**
   * Merges the entries matched inside one archive into the single file the raw cache holds.
   *
   * <p>Non-CSV entries are concatenated byte-for-byte, and so is a lone CSV entry. Several CSV
   * entries are concatenated under one header: when they all declare the same columns that is a
   * plain header-strip, and when they do not the rows are rewritten under the union of every
   * entry's columns.
   *
   * <p>Aligning those rows by column NAME rather than by position is the point. A bulk archive
   * whose members cover different ranges — BEA's {@code SAINC.zip} pairs {@code *_1929_1957}
   * files with {@code *_1958_2001} and {@code *_1998_2025} ones, each a block of year-named
   * columns — otherwise has every later member's values read against the first member's column
   * names, which silently reattributes them to the wrong year rather than failing.
   */
  private static void mergeZipParts(List<File> parts, List<String> names, File out,
      boolean csvFormat) throws IOException {
    if (!csvFormat || parts.size() == 1) {
      try (FileOutputStream fos = new FileOutputStream(out)) {
        for (File part : parts) {
          try (InputStream in = new FileInputStream(part)) {
            copyZipEntry(in, fos, false);
          }
        }
      }
      return;
    }

    List<List<String>> headers = new ArrayList<List<String>>();
    for (File part : parts) {
      headers.add(readCsvHeader(part));
    }
    boolean uniform = true;
    for (int i = 1; i < headers.size(); i++) {
      if (!headers.get(0).equals(headers.get(i))) {
        uniform = false;
        break;
      }
    }
    if (uniform) {
      try (FileOutputStream fos = new FileOutputStream(out)) {
        for (int i = 0; i < parts.size(); i++) {
          try (InputStream in = new FileInputStream(parts.get(i))) {
            copyZipEntry(in, fos, i > 0);
          }
        }
      }
      return;
    }

    Map<String, Integer> unionIndex = new LinkedHashMap<String, Integer>();
    for (List<String> header : headers) {
      for (String col : header) {
        if (!unionIndex.containsKey(col)) {
          unionIndex.put(col, unionIndex.size());
        }
      }
    }
    List<String> union = new ArrayList<String>(unionIndex.keySet());
    LOGGER.info("ZIP entries {} declare different CSV columns; merging under the union of {} "
        + "columns, matched by name", names, union.size());

    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8))) {
      writeCsvRow(writer, union.toArray(new String[0]));
      for (int i = 0; i < parts.size(); i++) {
        List<String> header = headers.get(i);
        int[] target = new int[header.size()];
        for (int c = 0; c < header.size(); c++) {
          target[c] = unionIndex.get(header.get(c)).intValue();
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(parts.get(i)), StandardCharsets.UTF_8))) {
          CsvRecordReader.readRecord(reader);  // header, already parsed
          String record;
          while ((record = CsvRecordReader.readRecord(reader)) != null) {
            if (record.isEmpty()) {
              continue;
            }
            List<String> fields = CsvRecordReader.splitFields(record, ',');
            String[] row = new String[union.size()];
            int n = Math.min(fields.size(), target.length);
            for (int c = 0; c < n; c++) {
              row[target[c]] = fields.get(c);
            }
            writeCsvRow(writer, row);
          }
        }
      }
    }
  }

  /** Reads one CSV file's header as a list of column names. */
  private static List<String> readCsvHeader(File file) throws IOException {
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
      String header = CsvRecordReader.readRecord(reader);
      List<String> cols = new ArrayList<String>();
      if (header != null) {
        for (String col : CsvRecordReader.splitFields(header, ',')) {
          cols.add(col.trim());
        }
      }
      return cols;
    }
  }

  /** Writes one RFC4180 CSV row; null cells become empty, and only cells that need it are quoted. */
  private static void writeCsvRow(BufferedWriter writer, String[] row) throws IOException {
    for (int i = 0; i < row.length; i++) {
      if (i > 0) {
        writer.write(',');
      }
      String value = row[i];
      if (value == null || value.isEmpty()) {
        continue;
      }
      if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0
          || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
        writer.write('"');
        writer.write(value.replace("\"", "\"\""));
        writer.write('"');
      } else {
        writer.write(value);
      }
    }
    writer.write('\n');
  }

  /**
   * Copies one zip entry's bytes to {@code out}, optionally dropping its first line (used to
   * strip a repeated CSV header when concatenating multiple matched entries into one file).
   */
  private static long copyZipEntry(InputStream in, OutputStream out, boolean skipFirstLine)
      throws IOException {
    if (skipFirstLine) {
      int b;
      while ((b = in.read()) != -1 && b != '\n') {
        // discard header bytes up to and including the newline
      }
    }
    byte[] buffer = new byte[65536];
    long total = 0;
    long lastLogTime = System.currentTimeMillis();
    int len;
    while ((len = in.read(buffer)) > 0) {
      out.write(buffer, 0, len);
      total += len;
      long now = System.currentTimeMillis();
      if (now - lastLogTime > 5000) {
        LOGGER.info("Extracting... {} MB", total / (1024 * 1024));
        lastLogTime = now;
      }
    }
    return total;
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

    if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.FIXED_WIDTH
        && responseTransformer == null) {
      throw new IOException(
          "FIXED_WIDTH format must be served from raw cache; raw cache must be enabled");
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
          return new ArrayList<Map<String, Object>>();
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
   * Returns the delimiter character for the given response config.
   *
   * @param respConfig response configuration
   * @return delimiter character (comma for CSV, tab for TSV, or custom)
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
    InputStream inputStream = storageProvider.openInputStream(cachePath);
    return new LazyCSVIterator(inputStream, cachePath, delimiter,
        config.getRowFilter(), config.getWideToNarrow(),
        respConfig.isHasHeader(), respConfig.getColumnNames(), respConfig.isQuoted());
  }

  private Iterator<Map<String, Object>> parseFixedWidthResponseStreaming(String cachePath)
      throws IOException {
    LOGGER.info("Streaming fixed-width from cache: {}", cachePath);
    HttpSourceConfig.FixedWidthConfig fwConfig = config.getFixedWidth();
    if (fwConfig == null || fwConfig.getColumns().isEmpty()) {
      throw new IOException(
          "FIXED_WIDTH format requires fixedWidth.columns configuration");
    }
    InputStream inputStream = storageProvider.openInputStream(cachePath);
    return new LazyFixedWidthIterator(inputStream, cachePath, fwConfig);
  }

  /**
   * Lazy iterator that reads fixed-width (positional) records one line at a time.
   * Each line is sliced into fields using the start+length column definitions.
   * Leading/trailing whitespace is trimmed from each field value.
   */
  private static class LazyFixedWidthIterator implements Iterator<Map<String, Object>>, java.io.Closeable {
    private final BufferedReader reader;
    private final List<HttpSourceConfig.FixedWidthConfig.Column> columns;
    private Map<String, Object> nextRow;
    private boolean exhausted;
    @SuppressWarnings("UnusedVariable")
    private int lineNumber;

    LazyFixedWidthIterator(InputStream inputStream, String cachePath,
        HttpSourceConfig.FixedWidthConfig fwConfig) throws IOException {
      this.columns = fwConfig.getColumns();
      this.exhausted = false;
      this.lineNumber = 0;
      java.nio.charset.Charset charset;
      try {
        charset = java.nio.charset.Charset.forName(fwConfig.getEncoding());
      } catch (Exception e) {
        throw new IOException("Unknown encoding: " + fwConfig.getEncoding(), e);
      }
      this.reader = new BufferedReader(new InputStreamReader(inputStream, charset));
      // Skip header/trailer lines
      for (int i = 0; i < fwConfig.getSkipLines(); i++) {
        if (reader.readLine() == null) {
          exhausted = true;
          return;
        }
        lineNumber++;
      }
      advance();
    }

    private void advance() {
      nextRow = null;
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          lineNumber++;
          if (line.isEmpty()) {
            continue;
          }
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          for (HttpSourceConfig.FixedWidthConfig.Column col : columns) {
            int end = col.getStart() + col.getLength();
            String value;
            if (col.getStart() >= line.length()) {
              value = "";
            } else {
              value = line.substring(col.getStart(), Math.min(end, line.length())).trim();
            }
            row.put(col.getName(), value);
          }
          nextRow = row;
          return;
        }
        exhausted = true;
      } catch (IOException e) {
        exhausted = true;
      }
    }

    @Override
    public boolean hasNext() {
      return nextRow != null && !exhausted;
    }

    @Override
    public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      advance();
      return row;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  /**
   * Lazy iterator that reads CSV rows one at a time from storage provider.
   * Parses rows on-demand to avoid loading entire file into memory.
   * Supports wide-to-narrow transformation (unpivot) for bulk CSV files.
   */
  private class LazyCSVIterator implements Iterator<Map<String, Object>>, java.io.Closeable {
    private final BufferedReader reader;
    private final char delimiter;
    private final boolean quoted;
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
        boolean hasHeader, String columnNames, boolean quoted) throws IOException {
      this.delimiter = delimiter;
      this.quoted = quoted;
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
        this.headers = parseDelimitedLine(columnNames, delimiter, quoted);
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
        // A UTF-8 BOM (e.g. FAA's ReleasableAircraft.zip MASTER.txt) lands on the first header
        // token since InputStreamReader with an explicit UTF-8 charset does not strip it, and
        // String.trim() only removes chars <= U+0020 so it survives the per-row trim below too.
        // Left in place it makes the first column's header text never match the schema's quoted
        // src."<name>" reference, silently nulling that column for every row.
        if (!headerLine.isEmpty() && headerLine.charAt(0) == (char) 0xFEFF) {
          headerLine = headerLine.substring(1);
        }
        this.headers = parseDelimitedLine(headerLine, delimiter, quoted);
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
        String[] firstFields = parseDelimitedLine(firstLine, delimiter, quoted);
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
          // Dropping the filter here would admit every row — for a table that exists to hold a
          // filtered subset (e.g. only SEC-registered LEIs) that silently ingests the entire
          // source and looks like a successful load. The real headers are in hand, so this is a
          // definite configuration error, not a guess.
          throw new IllegalStateException(
              "rowFilter column '" + filterColumn + "' is not among the CSV headers. It is"
                  + " matched against the raw source header, not a renamed column. Headers: "
                  + java.util.Arrays.toString(headers));
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

          String[] values = parseDelimitedLine(line, delimiter, quoted);

          // Apply filter if configured
          if (filterColumnIndex >= 0 && filterRegex != null) {
            if (filterColumnIndex >= values.length) {
              skippedRows++;
              continue;
            }
            String filterValue = stripQuotesIfPresent(values[filterColumnIndex].trim(), quoted);
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
                String value = stripQuotesIfPresent(values[idx].trim(), quoted);
                // Apply column name mapping: source name -> output name
                String outputName = wideToNarrow.getOutputColumnName(header);
                baseRow.put(outputName, parseValue(outputName, value));
              }
            }

            // Create one output row per value column
            for (int i = 0; i < valueColumnIndices.size(); i++) {
              int idx = valueColumnIndices.get(i);
              if (idx < values.length) {
                String valueStr = stripQuotesIfPresent(values[idx].trim(), quoted);

                // Skip null/empty values based on config
                if (wideToNarrow.shouldSkipValue(valueStr)) {
                  continue;
                }

                Map<String, Object> row = new LinkedHashMap<String, Object>(baseRow);
                row.put(wideToNarrow.getKeyColumnName(), valueColumnNames.get(i));  // e.g., "2020"
                String valueColumn = wideToNarrow.getValueColumnName();
                row.put(valueColumn, parseValue(valueColumn, valueStr));  // e.g., 12345.0
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
              String value = stripQuotesIfPresent(values[j].trim(), quoted);
              Object parsed = parseValue(header, value);
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
      boolean quoted = respConfig.isQuoted();
      if (!respConfig.isHasHeader() && respConfig.getColumnNames() != null) {
        // Headerless file with explicit column names
        headers = parseDelimitedLine(respConfig.getColumnNames(), delimiter, quoted);
        LOGGER.debug("Using {} explicit column names for headerless CSV", headers.length);
      } else {
        // Parse header row from file
        String headerLine = reader.readLine();
        if (headerLine == null) {
          return result;
        }
        headers = parseDelimitedLine(headerLine, delimiter, quoted);
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
          // See the streaming path above: an unmatched filter column admits every row.
          throw new IllegalStateException(
              "rowFilter column '" + filterColumn + "' is not among the CSV headers. It is"
                  + " matched against the raw source header, not a renamed column. Headers: "
                  + java.util.Arrays.toString(headers));
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

        String[] values = parseDelimitedLine(line, delimiter, quoted);

        // Apply filter if configured
        if (filterColumnIndex >= 0 && filterRegex != null) {
          if (filterColumnIndex >= values.length) {
            skippedRows++;
            continue;
          }
          String filterValue = stripQuotesIfPresent(values[filterColumnIndex].trim(), quoted);
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
              String value = stripQuotesIfPresent(values[idx].trim(), quoted);
              baseRow.put(header, parseValue(header, value));
            }
          }

          // Create one output row per value column
          for (int i = 0; i < valueColumnIndices.size(); i++) {
            int idx = valueColumnIndices.get(i);
            if (idx < values.length) {
              String valueStr = stripQuotesIfPresent(values[idx].trim(), quoted);

              // Skip null/empty values based on config
              if (wideToNarrow.shouldSkipValue(valueStr)) {
                continue;
              }

              Map<String, Object> row = new LinkedHashMap<String, Object>(baseRow);
              row.put(wideToNarrow.getKeyColumnName(), valueColumnNames.get(i));  // e.g., "2020"
              String valueColumn = wideToNarrow.getValueColumnName();
              row.put(valueColumn, parseValue(valueColumn, valueStr));  // e.g., 12345.0
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
            String value = stripQuotesIfPresent(values[j].trim(), quoted);

            // Try to parse as number
            Object parsed = parseValue(header, value);
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
   * Parses a single delimited line, treating {@code "} as an RFC4180 quote character.
   *
   * @param line The line to parse
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return Array of field values
   */
  private String[] parseDelimitedLine(String line, char delimiter) {
    return parseDelimitedLine(line, delimiter, true);
  }

  /**
   * Parses a single delimited line.
   *
   * @param line The line to parse
   * @param delimiter The delimiter character (comma for CSV, tab for TSV, "|" for FEC bulk files)
   * @param quoted whether {@code "} is an RFC4180 quote character wrapping a field (doubled to
   *     escape a literal quote) rather than ordinary literal data. When false, {@code "} is
   *     treated like any other character and every occurrence of {@code delimiter} splits a new
   *     field — this is what sources with no real quoting convention (e.g. FEC's pipe-delimited
   *     bulk files, which use {@code "} literally in names like {@code SMITH, JOHN "JACK"})
   *     actually need: applying quote semantics there desyncs field boundaries for the rest of
   *     the line whenever a field has an odd count of literal quotes.
   * @return Array of field values
   */
  private String[] parseDelimitedLine(String line, char delimiter, boolean quoted) {
    if (!quoted) {
      List<String> fields = new ArrayList<String>();
      int start = 0;
      for (int i = 0; i < line.length(); i++) {
        if (line.charAt(i) == delimiter) {
          fields.add(line.substring(start, i));
          start = i + 1;
        }
      }
      fields.add(line.substring(start));
      return fields.toArray(new String[0]);
    }

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
   * Strips a leading+trailing RFC4180 quote pair from a field value, or returns it unchanged
   * when {@code quoted} is false — a source with no real quoting convention may legitimately
   * have literal leading/trailing quote characters (e.g. a nickname like {@code "JACK"}) that
   * must not be stripped.
   */
  private static String stripQuotesIfPresent(String value, boolean quoted) {
    if (quoted && value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  /**
   * Collects the fetch-time keys of every column the table declares with a textual type.
   *
   * <p>Fetched rows are keyed by the raw source field name, so a column is indexed under its
   * effective source; its logical name is added too, so a {@code source:} rename still matches
   * whichever spelling the parser puts in the row.
   *
   * @param columns Declared columns; may be null
   * @return the textual keys, empty when the table declares no columns
   */
  private static Set<String> textualSourceKeys(List<ColumnConfig> columns) {
    if (columns == null || columns.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> keys = new HashSet<String>();
    for (ColumnConfig column : columns) {
      String type = column.getType();
      if (type == null) {
        continue;
      }
      String normalized = type.trim().toLowerCase(java.util.Locale.ROOT);
      if (normalized.equals("string") || normalized.equals("varchar")
          || normalized.equals("char") || normalized.startsWith("varchar(")
          || normalized.startsWith("char(")) {
        if (column.getEffectiveSource() != null) {
          keys.add(column.getEffectiveSource());
        }
        if (column.getName() != null) {
          keys.add(column.getName());
        }
      }
    }
    return keys;
  }

  /**
   * Converts a delimited-format field to its row value.
   *
   * <p>When the table declares {@code key} as a textual column that declaration is
   * authoritative and the text is kept verbatim — inferring a type here would turn an
   * all-digit identifier into a number and drop its leading zeros. Otherwise the value's
   * type is inferred, which is all a delimited format on its own supports.
   *
   * @param key Row key the value will be stored under
   * @param value Raw field text
   * @return the row value, or null for an empty field or a CSV null marker
   */
  private Object parseValue(String key, String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }

    // Treat common CSV null markers as SQL NULL
    if ("NULL".equalsIgnoreCase(value) || "NA".equals(value) || "N/A".equals(value)) {
      return null;
    }

    if (textualSourceKeys.contains(key)) {
      return value;
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

    // Build request context for the transformer. The 4th arg is the full fetch-variables map
    // (dimension values plus any injected delta bound / watermark), surfaced via both
    // getDimensionValues() (legacy) and getVariables() so a transformer can read the watermark.
    RequestContext context = RequestContext.builder()
        .url(url)
        .parameters(params)
        .headers(config.getHeaders())
        .dimensionValues(dimensionValues)
        .variables(dimensionValues)
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

  // --- Freshness Probe ---

  /**
   * Result of a freshness probe: HTTP response headers plus an optional small body.
   *
   * <p>Headers are captured for {@code ETag}, {@code Last-Modified}, {@code Content-Length},
   * {@code Content-MD5}, {@code x-goog-hash}, and similar pre-download signals. The body
   * is populated only for {@code VERSION}, {@code COUNT}, and sidecar-{@code CHECKSUM}
   * probes that issue a small GET.
   */
  public static final class ProbeResult {
    private final Map<String, String> headers;
    private final String body;

    public ProbeResult(Map<String, String> headers, String body) {
      this.headers = headers != null ? headers : Collections.<String, String>emptyMap();
      this.body = body;
    }

    /** Response headers (case-preserving). */
    public Map<String, String> getHeaders() {
      return headers;
    }

    /** Optional probe body (non-null only for VERSION / COUNT / sidecar-CHECKSUM types). */
    public String getBody() {
      return body;
    }
  }

  /**
   * Performs the cheap pre-download freshness probe described by {@code freshnessConfig}.
   *
   * <p>For {@code etag / last_modified / size / checksum(object_metadata)}: issues a
   * {@code HEAD} against the probe URL (defaulting to the source URL), captures the
   * response headers, and returns them with a null body.
   *
   * <p>For {@code version / count}: issues a {@code GET} against the probe URL and
   * returns the response body (typically a tiny JSON payload) together with response
   * headers.
   *
   * <p>For sidecar-{@code checksum}: issues a {@code GET} against {@code checksum_url}.
   *
   * <p>For {@code graphql}: issues a {@code POST} of {@code {"query": <query>}} against the
   * probe URL (defaulting to the source URL) and returns the JSON response body; the
   * caller extracts the comparable value via the configured {@code path}.
   *
   * <p>For {@code hash}: no network request is needed here (the caller will hash the
   * fully downloaded body); returns an empty {@link ProbeResult}.
   *
   * @param freshnessConfig the freshness configuration (type + probe URL options)
   * @param variables       dimension variables for URL substitution
   * @return a {@link ProbeResult}; never null
   * @throws IOException if the probe request fails
   */
  /**
   * Drops this batch's raw cache entry so the next fetch goes to the source.
   *
   * <p>Called when the freshness gate has determined the upstream content changed. An entry here
   * is validated by existence alone — staleness is decided by the tracker, not a TTL — so without
   * this, a lookback that reopens a revised period does the probe, sees the change, declines to
   * skip, and is then handed the very bytes the probe just said were out of date. The revision is
   * written off as already seen, and the new token is recorded, so no later run looks again.
   *
   * <p>Best-effort: a delete that fails leaves the entry in place, which is the pre-existing
   * behaviour rather than a new failure mode, and is logged rather than thrown.
   *
   * @param variables the batch's dimension values
   * @return whether an entry was removed
   */
  public boolean invalidateRawCache(Map<String, String> variables) {
    if (rawCachePath == null || storageProvider == null) {
      return false;
    }
    String path = buildRawCachePath(variables);
    try {
      if (!storageProvider.exists(path)) {
        return false;
      }
      boolean removed = storageProvider.delete(path);
      if (removed) {
        LOGGER.info("Freshness changed — dropped raw cache entry {}", path);
      }
      return removed;
    // fallback-guard: a cache entry that cannot be removed is left alone; the fetch then reads it,
    // which is the behaviour before this method existed, so this degrades rather than fails.
    } catch (IOException e) {
      LOGGER.warn("Could not drop raw cache entry {}: {}", path, e.getMessage());
      return false;
    }
  }

  public ProbeResult probe(FreshnessConfig freshnessConfig,
      Map<String, String> variables) throws IOException {
    if (freshnessConfig == null) {
      return new ProbeResult(null, null);
    }

    Map<String, String> vars =
        variables != null ? variables : Collections.<String, String>emptyMap();

    switch (freshnessConfig.getType()) {
    case ETAG:
    case LAST_MODIFIED:
    case SIZE:
      return probeHead(effectiveProbeUrl(freshnessConfig, vars), vars);

    case CHECKSUM:
      if (freshnessConfig.isObjectMetadata()) {
        // Object metadata is in the HEAD response (ETag / Content-MD5 / x-goog-hash)
        return probeHead(effectiveProbeUrl(freshnessConfig, vars), vars);
      }
      // Sidecar checksum file: GET the sidecar URL
      if (freshnessConfig.getChecksumUrl() != null) {
        return probeGet(substituteVariables(freshnessConfig.getChecksumUrl(), vars), vars);
      }
      return probeHead(effectiveProbeUrl(freshnessConfig, vars), vars);

    case VERSION:
      return probeGet(effectiveProbeUrl(freshnessConfig, vars), vars);

    case COUNT:
      if (freshnessConfig.getCountUrl() != null) {
        return probeGet(substituteVariables(freshnessConfig.getCountUrl(), vars), vars);
      }
      return probeGet(effectiveProbeUrl(freshnessConfig, vars), vars);

    case GRAPHQL:
      return probePost(effectiveProbeUrl(freshnessConfig, vars),
          substituteVariables(freshnessConfig.getQuery(), vars), vars);

    case HASH:
      // Hash is computed over the downloaded body — no separate probe needed.
      return new ProbeResult(null, null);

    default:
      return new ProbeResult(null, null);
    }
  }

  /**
   * Resolves the effective probe URL: the configured {@code probe_url} if present,
   * otherwise the source URL with variables substituted.
   */
  private String effectiveProbeUrl(FreshnessConfig freshnessConfig,
      Map<String, String> vars) {
    if (freshnessConfig.getProbeUrl() != null && !freshnessConfig.getProbeUrl().isEmpty()) {
      return substituteVariables(freshnessConfig.getProbeUrl(), vars);
    }
    return substituteVariables(config.getEffectiveUrl(vars), vars);
  }

  /**
   * Issues a {@code HEAD} request to the given URL, returning headers.
   * Reuses the source's User-Agent and auth configuration.
   */
  private ProbeResult probeHead(String urlString, Map<String, String> vars) throws IOException {
    URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("HEAD");
      conn.setConnectTimeout(30000);
      // Throttled upstreams (e.g. the USDA FIA datamart, ~800 KB/s) can take well over 15s just
      // to answer a HEAD. A short read timeout here makes the per-unit freshness probe fail and
      // fall back to a full fetch + re-materialise on every run — the exact re-write this gate is
      // meant to prevent. Match FiaStateArchive's 60s HEAD; a HEAD body is tiny, so this only ever
      // waits longer for slow servers and never delays fast ones.
      conn.setReadTimeout(60000);
      conn.setInstanceFollowRedirects(true);
      applyProbeHeaders(conn, vars);
      conn.connect();
      int code = conn.getResponseCode();
      LOGGER.debug("Freshness HEAD {} -> {}", urlString, code);
      Map<String, String> headers = captureHeaders(conn);
      return new ProbeResult(headers, null);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Issues a {@code GET} request to the given URL, returning headers + body.
   * Used for VERSION, COUNT, and sidecar-CHECKSUM probes.
   */
  private ProbeResult probeGet(String urlString, Map<String, String> vars) throws IOException {
    URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(15000);
      conn.setReadTimeout(30000);
      conn.setInstanceFollowRedirects(true);
      applyProbeHeaders(conn, vars);
      int code = conn.getResponseCode();
      LOGGER.debug("Freshness GET {} -> {}", urlString, code);
      Map<String, String> headers = captureHeaders(conn);
      String body = null;
      if (code >= 200 && code < 300) {
        body = readResponse(conn.getInputStream());
      }
      return new ProbeResult(headers, body);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Issues a GraphQL {@code POST} to the given endpoint with {@code {"query": <query>}}
   * as the JSON body, returning headers + response body. Used for the GRAPHQL freshness
   * probe (e.g. read the global max {@code updatedAt} with one cheap query).
   */
  private ProbeResult probePost(String urlString, String query, Map<String, String> vars)
      throws IOException {
    if (query == null || query.isEmpty()) {
      LOGGER.debug("Freshness GraphQL probe skipped: no query configured for {}", urlString);
      return new ProbeResult(null, null);
    }
    String requestBody =
        OBJECT_MAPPER.writeValueAsString(Collections.singletonMap("query", query));
    URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setConnectTimeout(15000);
      conn.setReadTimeout(30000);
      conn.setInstanceFollowRedirects(true);
      conn.setDoOutput(true);
      applyProbeHeaders(conn, vars);
      if (conn.getRequestProperty("Content-Type") == null) {
        conn.setRequestProperty("Content-Type", "application/json");
      }
      try (OutputStream os = conn.getOutputStream()) {
        os.write(requestBody.getBytes(StandardCharsets.UTF_8));
        os.flush();
      }
      int code = conn.getResponseCode();
      LOGGER.debug("Freshness GraphQL POST {} -> {}", urlString, code);
      Map<String, String> headers = captureHeaders(conn);
      String body = null;
      if (code >= 200 && code < 300) {
        body = readResponse(conn.getInputStream());
      }
      return new ProbeResult(headers, body);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Applies the configured User-Agent, custom headers, and auth to a probe connection.
   * Mirrors the header/auth logic in {@link #doRequest} without the body handling.
   */
  private void applyProbeHeaders(HttpURLConnection conn, Map<String, String> vars) {
    if (config.getHeaders().get("User-Agent") == null) {
      conn.setRequestProperty("User-Agent", DEFAULT_USER_AGENT);
    }
    for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
      String value = substituteVariables(e.getValue(), vars);
      if (value != null && !value.isEmpty()) {
        conn.setRequestProperty(e.getKey(), value);
      }
    }
    applyAuth(conn, vars);
  }

  /**
   * Collects all response headers from a connection into a plain {@code Map<String,String>}.
   * The first entry in each header field list is used (standard HTTP behaviour).
   */
  private Map<String, String> captureHeaders(HttpURLConnection conn) {
    Map<String, String> result = new LinkedHashMap<String, String>();
    Map<String, List<String>> fields = conn.getHeaderFields();
    if (fields != null) {
      for (Map.Entry<String, List<String>> e : fields.entrySet()) {
        String name = e.getKey();
        List<String> values = e.getValue();
        if (name != null && values != null && !values.isEmpty()) {
          result.put(name, values.get(0));
        }
      }
    }
    return result;
  }

  // --- Raw Response Caching (StorageProvider-based, with local filesystem optimization) ---

  /**
   * Checks if raw cache is enabled and available.
   */
  private boolean isRawCacheEnabled() {
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
    return buildRawCachePath(rawCachePath, variables, config.getRawCache().getKeyVars(),
        rawCacheRowCap, "gzip".equalsIgnoreCase(config.getResponse().getCompressed()));
  }

  /**
   * Builds the raw cache path for a given set of dimension variables. Static/parameterized form
   * of {@link #buildRawCachePath(Map)} so other components (e.g. a {@code TableLifecycleListener}
   * that pre-populates the cache from a differently-shaped bulk fetch) can compute the exact same
   * path {@link HttpSource} will look for, without duplicating this key-building logic.
   *
   * @param rawCachePath base raw-cache directory
   * @param variables dimension variables for this request
   * @param keyVars when non-null, restricts the key to only these variable names (see
   *                {@link HttpSourceConfig.RawCacheConfig#getKeyVars()}); when null the full
   *                variable set is used
   * @param rowCap DQ sample row cap (0 = uncapped); &gt; 0 isolates the cache under {@code cap=<N>}
   * @param gzip whether the source response is gzip-compressed (distinct cache filename)
   */
  public static String buildRawCachePath(String rawCachePath, Map<String, String> variables,
      List<String> keyVars, int rowCap, boolean gzip) {
    String basePath = rawCachePath;
    StringBuilder path = new StringBuilder(basePath);
    if (!basePath.endsWith("/")) {
      path.append("/");
    }

    // Build partition key from sorted variables. When rawCache.keyVars is configured, restrict
    // the key to ONLY those variables (the ones that actually determine the downloaded bytes) so
    // tables issuing the identical request but differing in output-only dimensions (e.g. a
    // partition `type` discriminator) resolve to the SAME cached file — enabling cross-table
    // sharing with sharedKey. When keyVars is null the full variable set is used (original
    // behavior), leaving non-configured tables unaffected.
    List<String> sortedKeys = new ArrayList<String>(
        keyVars != null ? keyVars : variables.keySet());
    Collections.sort(sortedKeys);
    for (String key : sortedKeys) {
      String value = variables.get(key);
      if (value != null && !value.isEmpty()) {
        path.append(key).append("=").append(sanitizePathComponent(value)).append("/");
      }
    }

    // Capped (DQ sample) caches are keyed separately under cap=<N> so an uncapped prod run never
    // reads a partial sample, and repeat DQ runs reuse the capped sample instead of re-downloading.
    if (rowCap > 0) {
      path.append("cap=").append(rowCap).append("/");
    }

    // Use a distinct filename for gzip sources so old undecompressed caches are never reused
    if (gzip) {
      path.append("response_gzip.json");
    } else {
      path.append("response.json");
    }
    return path.toString();
  }

  /**
   * Sanitizes a path component by removing or replacing invalid characters.
   * Components exceeding 200 chars are replaced with an MD5 hash to stay
   * under the OS 255-char filename limit (e.g. NWS pagination cursors).
   */
  static String sanitizePathComponent(String value) {
    String sanitized = value.replaceAll("[/\\\\:*?\"<>|]", "_");
    if (sanitized.length() <= 200) {
      return sanitized;
    }
    try {
      java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return "h_" + hex.toString();
    // fallback-guard: allow MD5 is a JVM-mandated algorithm so NoSuchAlgorithmException is effectively unreachable; the fallback (truncate to 200 chars) is a reasonable degrade for an unreachable path.
    } catch (java.security.NoSuchAlgorithmException e) {
      return sanitized.substring(0, 200);
    }
  }

  /**
   * Checks if a raw cached response exists and is not expired.
   *
   * @param cachePath Path to the cached response
   * @return true if cache hit, false otherwise
   */
  private boolean hasValidRawCache(String cachePath) {
    if (bypassRawCache) {
      LOGGER.debug("Raw cache bypassed (force-download): {}", cachePath);
      return false;
    }
    try {
      // Immutable data - if cache exists, it's valid
      // Staleness is determined by IncrementalTracker, not by TTL
      boolean exists = storageProvider != null && storageProvider.exists(cachePath);
      if (exists) {
        LOGGER.debug("Raw cache hit: {}", cachePath);
        return true;
      }
      LOGGER.debug("Raw cache miss: {}", cachePath);
      return false;
    // fallback-guard: allow hasValidRawCache treats an IOException while checking cache existence as a cache miss, which forces a re-fetch rather than trusting an unverifiable cache — the safe direction, logged at debug.
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
   * Opens the raw cache file and positions a parser on the first element of its record array.
   *
   * <p>The file is either a bare JSON array (a single unpaginated response cached verbatim) or
   * the {@code {"results":[...]}} envelope {@link PaginatedIterator#writeMergedCache} writes to
   * merge a paginated source's pages. Anything else is a corrupt cache and is reported as one
   * rather than parsed into whatever it happens to resemble.
   *
   * @param cachePath raw cache file to read
   * @return a parser positioned so that the next {@code START_OBJECT} is the first record
   */
  private JsonParser openRawCacheArray(String cachePath) throws IOException {
    InputStream is = storageProvider.openInputStream(cachePath);
    JsonParser parser = null;
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
          throw new IOException("Malformed raw cache for " + cachePath
              + ": no top-level 'results' array found");
        }
      } else if (token != JsonToken.START_ARRAY) {
        throw new IOException("Malformed raw cache for " + cachePath
            + ": unexpected first token " + token);
      }
      return parser;
    } catch (IOException e) {
      if (parser != null) {
        parser.close();
      }
      is.close();
      throw e;
    }
  }

  /**
   * Streams records straight out of the raw cache, with no transformer in the path.
   *
   * <p>Used for a paginated JSON source, whose cache is always the merged
   * {@code {"results":[...]}} envelope rather than any upstream body — so {@code dataPath} has
   * already been applied per page by {@link PaginatedIterator#accumulateRawPage} and must not be
   * applied again here.
   */
  private Iterator<Map<String, Object>> streamJsonFromRawCache(
      final String cachePath, final Map<String, String> variables) throws IOException {

    final JsonParser parser = openRawCacheArray(cachePath);

    LOGGER.info("Streaming JSON records from raw cache: {}", cachePath);

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> nextRow;
      private boolean exhausted;

      @Override public boolean hasNext() {
        if (nextRow != null) {
          return true;
        }
        if (exhausted) {
          return false;
        }
        try {
          if (parser.nextToken() == JsonToken.START_OBJECT) {
            @SuppressWarnings("unchecked") Map<String, Object> row =
                OBJECT_MAPPER.readValue(parser, Map.class);
            nextRow = normalizeRow(row, variables);
            return true;
          }
        } catch (IOException e) {
          throw new RuntimeException("Error reading raw cache " + cachePath, e);
        }
        exhausted = true;
        try {
          parser.close();
        } catch (IOException e) {
          LOGGER.debug("streamJsonFromRawCache: error closing parser for {}: {}", cachePath,
              e.getMessage());
        }
        return false;
      }

      @Override public Map<String, Object> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = nextRow;
        nextRow = null;
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

    final JsonParser parser = openRawCacheArray(cachePath);

    LOGGER.info("Streaming from raw cache: {}", cachePath);

    return new Iterator<Map<String, Object>>() {
      private final java.util.Deque<Map<String, Object>> pending =
          new java.util.ArrayDeque<Map<String, Object>>();
      private boolean exhausted = false;

      @Override public boolean hasNext() {
        if (!pending.isEmpty()) {
          return true;
        }
        if (exhausted) {
          return false;
        }
        try {
          while (pending.isEmpty() && parser.nextToken() == JsonToken.START_OBJECT) {
            Map<String, Object> source = OBJECT_MAPPER.readValue(parser, Map.class);
            List<Map<String, Object>> rows = transformer.transformRecordToMany(source, context);
            if (rows != null) {
              for (Map<String, Object> row : rows) {
                pending.add(normalizeRow(row, variables));
              }
            }
          }
        } catch (IOException e) {
          LOGGER.error("streamFromRawCache: error reading {}: {}", cachePath, e.getMessage());
        }
        if (!pending.isEmpty()) {
          return true;
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
        return pending.removeFirst();
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
  @SuppressWarnings("UnusedMethod")
  private void writeRawCache(String cachePath, String content) {
    try {
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
