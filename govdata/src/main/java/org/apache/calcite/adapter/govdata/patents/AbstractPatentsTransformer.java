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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
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

  /**
   * ODP bulk-download base for the disambiguated granted-patent tables (product PVGPATDIS):
   * g_patent, g_patent_abstract, g_application, g_figures, g_inventor_disambiguated,
   * g_assignee_disambiguated, g_location_disambiguated, g_cpc_current.
   */
  protected static final String ODP_PVGPATDIS_BASE =
      "https://api.uspto.gov/api/v1/datasets/products/files/PVGPATDIS/";

  /**
   * ODP bulk-download base for the per-year long-text tables (product PVGPATTXT):
   * g_claims_{year}, g_brf_sum_text_{year}.
   */
  protected static final String ODP_PVGPATTXT_BASE =
      "https://api.uspto.gov/api/v1/datasets/products/files/PVGPATTXT/";

  /**
   * Env var / system property holding the USPTO Open Data Portal API key. One account-level
   * key serves every ODP product (PVGPATDIS, PVGPATTXT, the trademark TRCFECO2 snapshot).
   * GovDataSchemaFactory injects it as a system property from the model operand; the worker
   * may also export it as an environment variable.
   */
  protected static final String USPTO_API_KEY_PROP = "USPTO_API_KEY";

  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 600_000; // 10 min for large files

  /**
   * Resolves the USPTO ODP API key: environment variable first, then system property
   * (factory-injected). Throws if neither is set — per CLAUDE.md there is no silent
   * fallback; the design guarantee is that the worker env or GovDataSchemaFactory provides it.
   */
  protected String usptoApiKey() {
    String key = System.getenv(USPTO_API_KEY_PROP);
    if (key == null || key.isEmpty()) {
      key = System.getProperty(USPTO_API_KEY_PROP);
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalStateException(
          "USPTO_API_KEY not set — required for USPTO Open Data Portal bulk downloads. "
          + "Export it as an environment variable or inject it via the GovDataSchemaFactory "
          + "model operand. Register at https://data.uspto.gov/apis/getting-started");
    }
    return key;
  }

  /** Request headers required for every ODP bulk download: the API key. */
  protected Map<String, String> odpRequestHeaders() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Api-Key", usptoApiKey());
    return headers;
  }

  /**
   * Returns the current-quarter token (e.g. {@code 2026Q2}). The quarter number comes from
   * the {@code quarter} dimension (fed by ${GOVDATA_CURRENT_QUARTER}); the calendar year is
   * read from the factory-injected GOVDATA_CURRENT_YEAR so the token is unique across years.
   * Stamped into full-dump cache filenames so a new quarter lands a new path and forces a
   * fresh ODP download — the token is never sent to the API. Throws if either input is
   * absent (no silent fallback).
   */
  protected String quarterToken(RequestContext context) {
    String quarter = context.getDimensionValues().get("quarter");
    if (quarter == null || quarter.isEmpty()) {
      throw new IllegalStateException(
          "quarter dimension not set — required for quarterly cache busting of patents "
          + "full-dump files and for versioning the snapshot tables. The schema YAML must "
          + "declare a quarter dimension fed by ${GOVDATA_CURRENT_QUARTER_TOKEN}.");
    }
    // The quarter dimension already carries the year-unique release token (e.g. 2026Q2),
    // injected by GovDataSchemaFactory. Used as the full-dump cache-bust stamp and as the
    // partition value for the append-only snapshot tables.
    return quarter;
  }

  /** Returns the storage provider for the govdata cache. */
  protected StorageProvider storageProvider() {
    return StorageProviderFactory.createForGovDataCache();
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

  /** Returns true if the cache path exists and was modified within {@code ttlMs}. */
  protected boolean isFresh(String path, long ttlMs) {
    try {
      if (!storageProvider().exists(path)) {
        return false;
      }
      StorageProvider.FileMetadata meta = storageProvider().getMetadata(path);
      return meta != null
          && (System.currentTimeMillis() - meta.getLastModified()) < ttlMs;
    } catch (IOException e) {
      return false;
    }
  }

  /** Returns true if the cache path exists and is within the quarterly TTL. */
  protected boolean isCacheValid(String path) {
    return isFresh(path, CACHE_TTL_MS);
  }

  // ── Release-freshness gate ─────────────────────────────────────────────────
  // ODP bulk files are IMMUTABLE per release: g_patent.tsv covers 1976→latest and is re-cut only
  // when USPTO publishes a new release (≈annually); each release URI is unchanging once posted.
  // Re-downloading an unchanged release is pure waste — and worse, the file endpoint enforces an
  // ANNUAL per-(key, URI) request cap (HTTP 429: "you have submitted N requests … in 31536000
  // sec"), so a quarter-stamped cache path or an expired TTL that forces a re-pull silently spends
  // that yearly budget. The gate below keys reuse on the upstream RELEASE DATE (from the product
  // METADATA endpoint, a different URI than the capped file) so we fetch each release exactly once.

  /** ODP product-metadata base — distinct from the per-file download URI, so probing it does not
   * consume the file's annual per-URI quota. */
  private static final String ODP_PRODUCTS_BASE =
      "https://api.uspto.gov/api/v1/datasets/products/";

  /** Re-probe a product's metadata at most this often, to bound metadata-endpoint requests well
   * under any per-URI annual cap (≈365/yr/product at this TTL) while still catching a new release
   * within a day — ample for ~annual bulk releases. */
  private static final long METADATA_TTL_MS = 24L * 60 * 60 * 1000;

  private static final com.fasterxml.jackson.databind.ObjectMapper METADATA_MAPPER =
      new com.fasterxml.jackson.databind.ObjectMapper();

  /** Per-JVM memo so a single worker run probes each product's metadata at most once. */
  private static final Map<String, com.fasterxml.jackson.databind.JsonNode> METADATA_MEMO =
      new java.util.concurrent.ConcurrentHashMap<String, com.fasterxml.jackson.databind.JsonNode>();

  /** Splits a {@code .../products/files/<PRODUCT>/<…>/<fileName>} URL into [product, fileName],
   * or null if it is not an ODP bulk-file URL. */
  private static String[] productAndFile(String url) {
    int i = url.indexOf("/products/files/");
    if (i < 0) {
      return null;
    }
    String rest = url.substring(i + "/products/files/".length());
    int slash = rest.indexOf('/');
    if (slash <= 0) {
      return null;
    }
    return new String[] {rest.substring(0, slash), rest.substring(rest.lastIndexOf('/') + 1)};
  }

  /** Fetches (and JVM-memoizes + disk-caches with a short TTL) the ODP metadata for a product.
   * Returns null on any failure — the caller then falls back to time-based caching, never blocking
   * a download. */
  private com.fasterxml.jackson.databind.JsonNode productMetadata(String productId) {
    com.fasterxml.jackson.databind.JsonNode memo = METADATA_MEMO.get(productId);
    if (memo != null) {
      return memo;
    }
    String diskPath = cacheFile(".freshness/metadata_" + productId + ".json");
    try {
      if (isFresh(diskPath, METADATA_TTL_MS)) {
        java.io.InputStream in = storageProvider().openInputStream(diskPath);
        try {
          com.fasterxml.jackson.databind.JsonNode disk = METADATA_MAPPER.readTree(in);
          METADATA_MEMO.put(productId, disk);
          return disk;
        } finally {
          in.close();
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Patents freshness: disk metadata read failed for {}: {}", productId, e.getMessage());
    }
    try {
      HttpURLConnection conn = (HttpURLConnection)
          URI.create(ODP_PRODUCTS_BASE + productId).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(30_000);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("X-Api-Key", usptoApiKey());
      if (conn.getResponseCode() != 200) {
        LOGGER.debug("Patents freshness: metadata HTTP {} for product {}",
            conn.getResponseCode(), productId);
        return null;
      }
      byte[] body = readAllBytes(conn.getInputStream());
      try {
        storageProvider().writeFile(diskPath, body);
      } catch (IOException ignore) {
        LOGGER.debug("Patents freshness: could not cache metadata for {}", productId);
      }
      com.fasterxml.jackson.databind.JsonNode root = METADATA_MAPPER.readTree(body);
      METADATA_MEMO.put(productId, root);
      return root;
    } catch (Exception e) {
      LOGGER.debug("Patents freshness: metadata probe failed for {}: {}", productId, e.getMessage());
      return null;
    }
  }

  /** The upstream release token for the file behind {@code url} — its {@code fileReleaseDate}
   * (falling back to {@code fileDataToDate}, then the product-level last-modified) — or null when
   * it cannot be determined. A null token disables freshness reuse for this call (the caller falls
   * back to TTL-based caching). */
  protected String currentReleaseToken(String url) {
    String[] pf = productAndFile(url);
    if (pf == null) {
      return null;
    }
    com.fasterxml.jackson.databind.JsonNode root = productMetadata(pf[0]);
    if (root == null) {
      return null;
    }
    String perFile = findFileReleaseDate(root, pf[1]);
    if (perFile != null) {
      return perFile;
    }
    com.fasterxml.jackson.databind.JsonNode lm = root.findValue("lastModifiedDateTime");
    if (lm == null) {
      lm = root.findValue("productToDate");
    }
    return lm != null ? lm.asText() : null;
  }

  /** Depth-first search for the file entry whose {@code fileName} matches, returning its release
   * date ({@code fileReleaseDate}, else {@code fileDataToDate}). */
  private static String findFileReleaseDate(
      com.fasterxml.jackson.databind.JsonNode node, String fileName) {
    if (node == null) {
      return null;
    }
    if (node.isObject()) {
      com.fasterxml.jackson.databind.JsonNode fn = node.get("fileName");
      if (fn != null && fileName.equals(fn.asText())) {
        com.fasterxml.jackson.databind.JsonNode rel = node.get("fileReleaseDate");
        if (rel == null) {
          rel = node.get("fileDataToDate");
        }
        return rel != null ? rel.asText() : null;
      }
      for (com.fasterxml.jackson.databind.JsonNode child : node) {
        String r = findFileReleaseDate(child, fileName);
        if (r != null) {
          return r;
        }
      }
    } else if (node.isArray()) {
      for (com.fasterxml.jackson.databind.JsonNode child : node) {
        String r = findFileReleaseDate(child, fileName);
        if (r != null) {
          return r;
        }
      }
    }
    return null;
  }

  /** True if {@code url} is listed as a downloadable file in the product CATALOG (the ODP metadata
   * {@code fileDownloadURI} set), i.e. that exact snapshot actually exists. Lets a caller skip a
   * not-yet-published snapshot without attempting a download (which would 404/429 and spend one of
   * the file's scarce ANNUAL per-(key, URI) requests — see the cap note above).
   *
   * <p>Fails CLOSED — returns false when the catalog cannot be read — because attempting a download
   * we cannot confirm is publishable is exactly what burns the annual per-URI budget on years USPTO
   * has not posted (e.g. a future snapshot). A metadata outage is transient (24h TTL + the generous
   * weekly metadata quota) and the fetch is retried next cycle, so skipping a run costs nothing;
   * wrongly spending the annual file budget cannot be undone until the yearly window rolls over. */
  protected boolean isPublished(String url) {
    String[] pf = productAndFile(url);
    if (pf == null) {
      // Not an ODP bulk-file URL — this publication gate does not apply; let the caller proceed.
      return true;
    }
    com.fasterxml.jackson.databind.JsonNode root = productMetadata(pf[0]);
    if (root == null) {
      LOGGER.info("Patents: cannot confirm publication of {} (product metadata unavailable) — "
          + "skipping this run to preserve the annual per-URI download budget; retries next cycle",
          url);
      return false;
    }
    return catalogHasDownloadUri(root, url);
  }

  private static boolean catalogHasDownloadUri(
      com.fasterxml.jackson.databind.JsonNode node, String url) {
    if (node == null) {
      return false;
    }
    if (node.isObject()) {
      com.fasterxml.jackson.databind.JsonNode uri = node.get("fileDownloadURI");
      if (uri != null && url.equals(uri.asText())) {
        return true;
      }
      for (com.fasterxml.jackson.databind.JsonNode child : node) {
        if (catalogHasDownloadUri(child, url)) {
          return true;
        }
      }
    } else if (node.isArray()) {
      for (com.fasterxml.jackson.databind.JsonNode child : node) {
        if (catalogHasDownloadUri(child, url)) {
          return true;
        }
      }
    }
    return false;
  }

  private String freshnessMarkerPath(String url) {
    return cacheFile(".freshness/" + url.replaceAll("[^A-Za-z0-9._-]", "_") + ".marker");
  }

  /** Returns the previously-cached file path to reuse when the upstream release behind {@code url}
   * has not advanced past what we last fetched, else null (a fresh download is required). */
  protected String cachedCopyForToken(String url, String token) {
    if (token == null) {
      return null;
    }
    try {
      String path = freshnessMarkerPath(url);
      if (!storageProvider().exists(path)) {
        return null;
      }
      java.io.InputStream in = storageProvider().openInputStream(path);
      String s;
      try {
        s = new String(readAllBytes(in), StandardCharsets.UTF_8);
      } finally {
        in.close();
      }
      int tab = s.indexOf('\t');
      if (tab < 0) {
        return null;
      }
      String lastToken = s.substring(0, tab);
      String cachedPath = s.substring(tab + 1).trim();
      if (token.equals(lastToken) && storageProvider().exists(cachedPath)) {
        return cachedPath;
      }
    } catch (IOException e) {
      LOGGER.debug("Patents freshness: marker check failed for {}: {}", url, e.getMessage());
    }
    return null;
  }

  /** Records that {@code cachedPath} holds the data for the current release of {@code url}. */
  protected void recordRelease(String url, String token, String cachedPath) {
    if (token == null) {
      return;
    }
    try {
      storageProvider().writeFile(freshnessMarkerPath(url),
          (token + "\t" + cachedPath).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOGGER.debug("Patents freshness: could not write marker for {}: {}", url, e.getMessage());
    }
  }

  private static byte[] readAllBytes(java.io.InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) >= 0) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  // ── Download and cache ─────────────────────────────────────────────────────

  /**
   * Downloads a ZIP from {@code url}, extracts the first TSV/TXT entry to
   * {@code destPath} via the storage provider, and returns the path.
   * Re-uses cached file if valid.
   *
   * <p>Downloads the ZIP to a local temp file first, then streams the extracted entry
   * to the storage provider. This two-phase approach avoids HTTP idle-timeout failures
   * that occur when interleaved S3 multipart uploads stall the HTTP read loop.
   */
  protected String downloadAndCacheTsv(String url, String destPath) throws IOException {
    if (isCacheValid(destPath)) {
      LOGGER.debug("Patents cache hit: {}", destPath);
      return destPath;
    }
    // Freshness gate: reuse a prior cached copy when the upstream release has not advanced, so an
    // unchanged immutable snapshot is never re-downloaded (and the annual per-URI quota untouched).
    String token = currentReleaseToken(url);
    String reuse = cachedCopyForToken(url, token);
    if (reuse != null) {
      LOGGER.info("Patents freshness: {} unchanged (release {}) — reusing cache, no download",
          url, token);
      return reuse;
    }
    LOGGER.info("Patents downloading: {} (release {})", url, token == null ? "unknown" : token);
    extractZipEntryToFile(url, odpRequestHeaders(), destPath, ".tsv", ".txt");
    recordRelease(url, token, destPath);
    LOGGER.info("Patents cached to {}", destPath);
    return destPath;
  }

  /**
   * Downloads a ZIP from {@code url} to a local temp file, then writes the first matching
   * entry to {@code destPath} via the storage provider.
   */
  protected void extractZipEntryToFile(String url, String destPath, String... extensions)
      throws IOException {
    extractZipEntryToFile(url, null, destPath, extensions);
  }

  /**
   * Downloads a ZIP from {@code url} (with optional request headers) to a local temp file,
   * then writes the first entry whose name ends with one of {@code extensions} to
   * {@code destPath} via the storage provider.
   *
   * <p>Downloads to disk first so that the HTTP response is fully consumed before the
   * S3 multipart upload begins — avoids ZLIB EOF failures caused by the HTTP server
   * closing idle connections while we wait for S3 part uploads to complete.
   */
  protected void extractZipEntryToFile(String url, Map<String, String> requestHeaders,
      String destPath, String... extensions) throws IOException {
    // Retry only genuinely-transient network faults (truncated reads, resets, 5xx) with a 1-min
    // backoff cap. HTTP 429 is NOT retried here: on this API it is an annual per-(key, URI) cap, so
    // retrying only spends more of the yearly budget (see the 429 short-circuit in the catch). On
    // exhaustion this still hard-fails (no silent skip).
    final int maxAttempts = 10;
    IOException lastError = null;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        extractZipEntryToFileOnce(url, requestHeaders, destPath, extensions);
        return;
      } catch (IOException e) {
        lastError = e;
        if (isRateLimited(e)) {
          // HTTP 429 here is the ANNUAL per-(key, URI) download cap. Retrying cannot clear it within
          // a run, and every attempt spends another count against the yearly budget — so fail fast.
          LOGGER.warn("Patents: HTTP 429 (annual per-URI quota) for {} — not retrying; each attempt "
              + "would burn more of the yearly budget. Resolve with a new API key or wait for the "
              + "365-day window to age off.", url);
          throw e;
        }
        if (attempt < maxAttempts && isTransientDownloadError(e)) {
          long backoffMs = Math.min(60_000L, 1000L * (1L << (attempt - 1)));
          LOGGER.warn("Patents: transient download failure (attempt {}/{}) for {}: {} — retrying in {}ms",
              attempt, maxAttempts, url, e.getMessage(), backoffMs);
          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying download", ie);
          }
          continue;
        }
        throw e;
      }
    }
    throw lastError;
  }

  /**
   * True when {@code e} (or any cause in its chain) looks like a recoverable network
   * condition: truncated read, connection reset, or HTTP 5xx/429. These cases warrant
   * a retry of the entire download.
   */
  /** True when {@code e} (or any cause) is specifically an HTTP 429 (rate limit). */
  private static boolean isRateLimited(Throwable e) {
    for (Throwable cur = e; cur != null; cur = cur.getCause()) {
      String msg = cur.getMessage();
      if (msg != null && msg.contains("HTTP 429")) {
        return true;
      }
    }
    return false;
  }

  private static boolean isTransientDownloadError(Throwable e) {
    Throwable cur = e;
    while (cur != null) {
      String msg = cur.getMessage();
      if (msg != null) {
        if (msg.contains("Premature end") || msg.contains("Premature EOF")
            || msg.contains("Connection reset") || msg.contains("Connection closed")
            || msg.contains("HTTP 500") || msg.contains("HTTP 502")
            || msg.contains("HTTP 503") || msg.contains("HTTP 504")
            || msg.contains("Truncated")) {
          return true;
        }
      }
      if (cur instanceof java.net.SocketException
          || cur instanceof java.io.EOFException
          || cur instanceof java.net.SocketTimeoutException) {
        return true;
      }
      cur = cur.getCause();
    }
    return false;
  }

  private void extractZipEntryToFileOnce(String url, Map<String, String> requestHeaders,
      String destPath, String... extensions) throws IOException {
    File tempZip = File.createTempFile("patents-zip-", ".zip");
    try {
      // Phase 1: download the ZIP completely to a local temp file
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
      long contentLength = conn.getContentLengthLong();
      LOGGER.info("Patents: downloading {} ({} bytes) to temp file",
          url.substring(url.lastIndexOf('/') + 1), contentLength);
      long bytesRead = 0;
      byte[] buf = new byte[64 * 1024];
      try (java.io.InputStream httpIn = conn.getInputStream();
           OutputStream tmpOut = new FileOutputStream(tempZip)) {
        int n;
        while ((n = httpIn.read(buf)) >= 0) {
          tmpOut.write(buf, 0, n);
          bytesRead += n;
        }
      }
      // Verify the download is complete. Without this check, a server-side
      // connection close mid-stream produces a truncated cache file that
      // subsequent runs blindly reuse, marking the pipeline complete on
      // partial data.
      if (contentLength >= 0 && bytesRead != contentLength) {
        throw new IOException(
            "Truncated download from " + url + ": expected " + contentLength
            + " bytes, received " + bytesRead);
      }
      LOGGER.info("Patents: download complete, temp file size={}", tempZip.length());

      // Phase 2: stream from temp file to storage provider
      ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZip));
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
            final ZipInputStream finalZis = zis;
            java.io.InputStream nonClosingEntry = new java.io.FilterInputStream(finalZis) {
              @Override public void close() { }
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
    } finally {
      if (tempZip.exists() && !tempZip.delete()) {
        LOGGER.warn("Patents: failed to delete temp file: {}", tempZip.getAbsolutePath());
      }
    }
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
      String headerLine = CsvRecordReader.readRecord(reader);
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
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
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
      String headerLine = CsvRecordReader.readRecord(reader);
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
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
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
      String headerLine = CsvRecordReader.readRecord(reader);
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
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
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
      String headerLine = CsvRecordReader.readRecord(reader);
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
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
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
      String headerLine = CsvRecordReader.readRecord(reader);
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
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
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

  /**
   * Extracts the effective_year dimension (publish year minus dataLag) — the actual
   * data year for lagged sources. Falls back to {@code year} when no lag is configured.
   */
  protected String getEffectiveYear(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    if (dims == null) {
      return null;
    }
    String effective = dims.get("effective_year");
    return effective != null ? effective : dims.get("year");
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
      String headerLine = CsvRecordReader.readRecord(reader);
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
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
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
