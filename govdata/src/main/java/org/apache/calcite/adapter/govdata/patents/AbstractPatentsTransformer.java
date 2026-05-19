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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Base class for all Patents schema transformers.
 *
 * <p>Provides shared utilities for:
 * <ul>
 *   <li>Downloading large ZIP files via HTTP</li>
 *   <li>Extracting TSV files from ZIP archives</li>
 *   <li>Storage-provider-backed caching with quarterly TTL (90 days)</li>
 *   <li>TSV parsing with header detection</li>
 *   <li>JSON field helpers (null-safe)</li>
 * </ul>
 *
 * <p>All full-dump files (g_patent.tsv, g_assignee_disambiguated.tsv, etc.) are
 * cached at GOVDATA_CACHE_DIR/patents/. Per-year files (g_claims_{year}.tsv) are
 * also cached to avoid re-downloading on each invocation.
 */
public abstract class AbstractPatentsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPatentsTransformer.class);

  /** Quarterly cache TTL: 90 days in milliseconds. */
  protected static final long CACHE_TTL_MS = 90L * 24 * 60 * 60 * 1000;

  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 600_000; // 10 min for large files

  private volatile StorageProvider storageProvider;

  /** Returns the storage provider for the govdata cache, initializing lazily on first call. */
  protected StorageProvider storageProvider() {
    if (storageProvider == null) {
      synchronized (this) {
        if (storageProvider == null) {
          String cacheDir = StorageProviderFactory.getGovDataCacheDir();
          if (cacheDir != null && cacheDir.startsWith("s3://")) {
            // S3 cache — build provider from process environment credentials.
            Map<String, Object> s3Config = new HashMap<String, Object>();
            String keyId = System.getenv("AWS_ACCESS_KEY_ID");
            String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
            String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
            String region = System.getenv("AWS_REGION");
            if (keyId != null && !keyId.isEmpty()) {
              s3Config.put("accessKeyId", keyId);
            }
            if (secret != null && !secret.isEmpty()) {
              s3Config.put("secretAccessKey", secret);
            }
            if (endpoint != null && !endpoint.isEmpty()) {
              s3Config.put("endpoint", endpoint);
            }
            s3Config.put("region", (region != null && !region.isEmpty()) ? region : "auto");
            s3Config.put("directory", cacheDir);
            storageProvider = StorageProviderFactory.createFromType("s3", s3Config);
            LOGGER.info("Patents transformer: using S3 cache storage at {}", cacheDir);
          } else {
            storageProvider = StorageProviderFactory.createForGovDataCache();
          }
        }
      }
    }
    return storageProvider;
  }

  // ── Cache path resolution ─────────────────────────────────────────────────

  /** Returns the patents cache directory path (GOVDATA_CACHE_DIR/patents/). */
  protected String cacheDir() {
    return storageProvider().resolvePath(StorageProviderFactory.getGovDataCacheDir(), "patents");
  }

  /** Returns the cache path for a given filename. */
  protected String cacheFile(String filename) {
    return storageProvider().resolvePath(cacheDir(), filename);
  }

  /** Returns true if the cache path exists and is within TTL. */
  protected boolean isCacheValid(String path) {
    try {
      if (!storageProvider().exists(path)) {
        return false;
      }
      StorageProvider.FileMetadata meta = storageProvider().getMetadata(path);
      return meta != null
          && (System.currentTimeMillis() - meta.getLastModified()) < CACHE_TTL_MS;
    } catch (IOException e) {
      return false;
    }
  }

  // ── Download and cache ─────────────────────────────────────────────────────

  /**
   * Downloads a ZIP from {@code url}, streams the first TSV/TXT entry directly to
   * {@code destPath} via the storage provider, and returns the path.
   * Re-uses cached file if valid.
   *
   * <p>Streams HTTP → ZipInputStream → StorageProvider with no intermediate buffering,
   * allowing arbitrarily large files (the full-dump files exceed 2–8 GB uncompressed).
   */
  protected String downloadAndCacheTsv(String url, String destPath) throws IOException {
    if (isCacheValid(destPath)) {
      LOGGER.debug("Patents cache hit: {}", destPath);
      return destPath;
    }
    LOGGER.info("Patents downloading: {}", url);
    extractZipEntryToFile(url, destPath, ".tsv", ".txt");
    LOGGER.info("Patents cached to {}", destPath);
    return destPath;
  }

  /**
   * Streams an HTTP ZIP response, locates the first entry whose name ends with one of
   * {@code extensions}, and writes it to {@code destPath} via the storage provider.
   *
   * <p>No intermediate byte[] or String buffer is used; memory footprint is O(buffer_size).
   */
  protected void extractZipEntryToFile(String url, String destPath, String... extensions)
      throws IOException {
    extractZipEntryToFile(url, null, destPath, extensions);
  }

  /**
   * Streams an HTTP ZIP response with optional request headers (e.g., API key), locates the
   * first entry whose name ends with one of {@code extensions}, and writes it to {@code destPath}
   * via the storage provider. Detects WAF/HTML responses and throws a descriptive IOException.
   */
  protected void extractZipEntryToFile(String url, Map<String, String> requestHeaders,
      String destPath, String... extensions) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    if (requestHeaders != null) {
      for (Map.Entry<String, String> h : requestHeaders.entrySet()) {
        conn.setRequestProperty(h.getKey(), h.getValue());
      }
    }
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    String contentType = conn.getContentType();
    if (contentType != null && contentType.contains("text/html")) {
      throw new IOException(
          "Expected ZIP but got HTML response from: " + url
          + " — source may be WAF-blocked or unavailable");
    }
    ZipInputStream zis = new ZipInputStream(conn.getInputStream());
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName().toLowerCase();
        boolean matches = false;
        for (String ext : extensions) {
          if (name.endsWith(ext)) {
            matches = true;
            break;
          }
        }
        if (matches) {
          // Wrap in non-closing stream so writeFile doesn't close the ZipInputStream
          final ZipInputStream finalZis = zis;
          java.io.InputStream nonClosingEntry = new java.io.FilterInputStream(finalZis) {
            @Override public void close() {
              // intentionally empty — caller closes the ZipInputStream
            }
          };
          storageProvider().writeFile(destPath, nonClosingEntry);
          return;
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }
    throw new IOException("No matching entry found in ZIP from: " + url);
  }

  // ── TSV parsing ────────────────────────────────────────────────────────────

  /**
   * Reads a TSV file, filters rows where {@code yearColumn} matches {@code yearValue},
   * and returns the filtered rows as a list of header→value maps.
   *
   * <p>Uses streaming to avoid loading the entire file into memory.
   */
  protected List<Map<String, String>> readTsvFilteredByYear(
      String path, String yearColumn, String yearValue) throws IOException {
    List<Map<String, String>> result = new ArrayList<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }
      String[] headers = splitTsv(headerLine);
      int yearIdx = -1;
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase(yearColumn)) {
          yearIdx = i;
          break;
        }
      }

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = splitTsv(line);
        // Filter by year if we found the year column
        if (yearIdx >= 0 && yearIdx < parts.length) {
          if (!yearValue.equals(parts[yearIdx].trim())) {
            continue;
          }
        }
        Map<String, String> row = new HashMap<>();
        for (int c = 0; c < headers.length; c++) {
          row.put(headers[c], c < parts.length ? parts[c].trim() : "");
        }
        result.add(row);
      }
    } finally {
      reader.close();
    }
    return result;
  }

  /**
   * Reads a TSV file into a lookup map keyed by keyColumn, retaining ONLY rows whose key
   * is present in {@code keysToRetain}. Streams the file — memory is O(keysToRetain.size()).
   */
  protected Map<String, Map<String, String>> readTsvAsLookupForKeys(
      String path, String keyColumn, Set<String> keysToRetain,
      String... retainColumns) throws IOException {
    Map<String, Map<String, String>> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }
      String[] headers = splitTsv(headerLine);
      int keyIdx = -1;
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
          break;
        }
      }
      if (keyIdx < 0) {
        LOGGER.warn("Patents lookup: key column '{}' not found in {}", keyColumn, path);
        return result;
      }

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = splitTsv(line);
        if (keyIdx >= parts.length) {
          continue;
        }
        String key = parts[keyIdx].trim();
        if (key.isEmpty() || !keysToRetain.contains(key)) {
          continue;
        }
        Map<String, String> row = new HashMap<>();
        for (String col : retainColumns) {
          for (int i = 0; i < headers.length; i++) {
            if (headers[i].equalsIgnoreCase(col)) {
              row.put(col, i < parts.length ? parts[i].trim() : "");
              break;
            }
          }
        }
        result.put(key, row);
      }
    } finally {
      reader.close();
    }
    return result;
  }

  /**
   * Reads a TSV file into a lookup map keyed by the specified key column.
   * Only loads the specified columns to limit memory usage.
   */
  protected Map<String, Map<String, String>> readTsvAsLookup(
      String path, String keyColumn, String... retainColumns) throws IOException {
    Map<String, Map<String, String>> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }
      String[] headers = splitTsv(headerLine);
      int keyIdx = -1;
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
          break;
        }
      }
      if (keyIdx < 0) {
        LOGGER.warn("Patents lookup: key column '{}' not found in {}", keyColumn, path);
        return result;
      }

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = splitTsv(line);
        if (keyIdx >= parts.length) {
          continue;
        }
        String key = parts[keyIdx].trim();
        if (key.isEmpty()) {
          continue;
        }
        Map<String, String> row = new HashMap<>();
        for (String col : retainColumns) {
          for (int i = 0; i < headers.length; i++) {
            if (headers[i].equalsIgnoreCase(col)) {
              row.put(col, i < parts.length ? parts[i].trim() : "");
              break;
            }
          }
        }
        result.put(key, row);
      }
    } finally {
      reader.close();
    }
    return result;
  }

  /** Splits a TSV line on tabs, preserving empty fields. Strips surrounding double-quotes. */
  protected String[] splitTsv(String line) {
    String[] parts = line.split("\t", -1);
    for (int i = 0; i < parts.length; i++) {
      String p = parts[i];
      if (p.length() >= 2 && p.charAt(0) == '"' && p.charAt(p.length() - 1) == '"') {
        parts[i] = p.substring(1, p.length() - 1);
      }
    }
    return parts;
  }

  /**
   * Reads g_patent.tsv, filters rows where {@code patent_date} starts with {@code yearStr},
   * and returns the set of patent_id values for that year. Memory is O(patent count per year).
   */
  protected Set<String> readPatentIdsForYear(String path, String yearStr) throws IOException {
    Set<String> result = new HashSet<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }
      String[] headers = splitTsv(headerLine);
      int patentIdIdx = -1;
      int patentDateIdx = -1;
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase("patent_id")) {
          patentIdIdx = i;
        }
        if (headers[i].equalsIgnoreCase("patent_date")) {
          patentDateIdx = i;
        }
      }
      if (patentIdIdx < 0 || patentDateIdx < 0) {
        LOGGER.warn("readPatentIdsForYear: required columns not found in {}", path);
        return result;
      }
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = splitTsv(line);
        if (patentDateIdx < parts.length && parts[patentDateIdx].startsWith(yearStr)) {
          if (patentIdIdx < parts.length) {
            String id = parts[patentIdIdx].trim();
            if (!id.isEmpty()) {
              result.add(id);
            }
          }
        }
      }
    } finally {
      reader.close();
    }
    return result;
  }

  /**
   * Reads a TSV file, collecting values of {@code keyColumn} for rows whose patent_id
   * is present in {@code patentIds}. Used by tables with no year column.
   */
  protected Set<String> readTsvKeysByPatentIds(
      String path, Set<String> patentIds, String keyColumn) throws IOException {
    Set<String> result = new HashSet<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }
      String[] headers = splitTsv(headerLine);
      int patentIdIdx = -1;
      int keyIdx = -1;
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase("patent_id")) {
          patentIdIdx = i;
        }
        if (headers[i].equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
        }
      }
      if (patentIdIdx < 0 || keyIdx < 0) {
        LOGGER.warn("readTsvKeysByPatentIds: required columns not found in {}", path);
        return result;
      }
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = splitTsv(line);
        if (patentIdIdx < parts.length && patentIds.contains(parts[patentIdIdx].trim())) {
          if (keyIdx < parts.length) {
            String key = parts[keyIdx].trim();
            if (!key.isEmpty()) {
              result.add(key);
            }
          }
        }
      }
    } finally {
      reader.close();
    }
    return result;
  }

  // ── Dimension helpers ─────────────────────────────────────────────────────

  /** Extracts the year dimension value from the request context. */
  protected String getYear(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    return dims != null ? dims.get("year") : null;
  }

  // ── Header / field helpers ────────────────────────────────────────────────

  /** Builds a lowercase column-name → column-index map from a header array. */
  protected Map<String, Integer> buildHeaderMap(String[] headers) {
    Map<String, Integer> map = new HashMap<>();
    for (int i = 0; i < headers.length; i++) {
      map.put(headers[i].toLowerCase(), i);
    }
    return map;
  }

  /** Returns the trimmed field value for the named column, or null if absent or blank. */
  protected String getField(String[] parts, Map<String, Integer> headerMap, String column) {
    Integer idx = headerMap.get(column.toLowerCase());
    if (idx == null || idx >= parts.length) {
      return null;
    }
    String v = parts[idx].trim();
    return v.isEmpty() ? null : v;
  }

  /**
   * Streams a TSV file collecting the {@code keyColumn} value for rows where
   * {@code yearColumn} equals {@code yearValue}. Returns the set of non-blank keys.
   */
  protected Set<String> readTsvYearColumnKeys(
      String path, String yearColumn, String yearValue, String keyColumn) throws IOException {
    Set<String> result = new HashSet<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }
      String[] headers = splitTsv(headerLine);
      int yearIdx = -1;
      int keyIdx = -1;
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase(yearColumn)) {
          yearIdx = i;
        }
        if (headers[i].equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
        }
      }
      if (keyIdx < 0) {
        LOGGER.warn("readTsvYearColumnKeys: key column '{}' not found in {}",
            keyColumn, path);
        return result;
      }
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = splitTsv(line);
        if (yearIdx >= 0 && yearIdx < parts.length
            && !yearValue.equals(parts[yearIdx].trim())) {
          continue;
        }
        if (keyIdx < parts.length) {
          String key = parts[keyIdx].trim();
          if (!key.isEmpty()) {
            result.add(key);
          }
        }
      }
    } finally {
      reader.close();
    }
    return result;
  }

  // ── Typed row-map value helpers ───────────────────────────────────────────

  protected Object strVal(String v) {
    return (v == null || v.isEmpty()) ? null : v;
  }

  protected Object intVal(String v) {
    if (v == null || v.isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  protected Object doubleVal(String v) {
    if (v == null || v.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(v.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  protected Object boolVal(String v) {
    if (v == null || v.isEmpty()) {
      return null;
    }
    String lv = v.trim().toLowerCase();
    return "1".equals(lv) || "true".equals(lv) || "t".equals(lv) || "yes".equals(lv);
  }

  /** Returns the value, or null if it is blank. */
  protected String blankToNull(String val) {
    if (val == null || val.trim().isEmpty()) {
      return null;
    }
    return val.trim();
  }
}
