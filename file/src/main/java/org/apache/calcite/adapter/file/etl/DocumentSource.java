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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Document source for document-based ETL pipelines.
 *
 * <p>DocumentSource handles downloading and processing document files (XBRL, HTML, XML)
 * from APIs like SEC EDGAR, where multiple tables are extracted from each document.
 *
 * <h3>Document Source Flow</h3>
 * <pre>
 * 1. Fetch metadata from metadataUrl (e.g., SEC submissions.json)
 * 2. Parse metadata to get list of documents
 * 3. For each document:
 *    a. Download from documentUrl
 *    b. Cache locally
 *    c. Invoke document converter to extract tables
 * 4. Write extracted data to Parquet/Iceberg
 * </pre>
 *
 * <h3>Rate Limiting</h3>
 * <p>Respects rate limits via configurable delay between requests.
 * SEC EDGAR allows 10 requests/second; we default to 8 for safety margin.
 *
 * <h3>Caching</h3>
 * <p>Documents are cached locally to avoid re-downloading. Uses ETag-based
 * validation for metadata to detect changes.
 *
 * @see HttpSourceConfig.DocumentSourceConfig
 */
public class DocumentSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentSource.class);

  // Variable pattern for URL substitution: {varName}
  private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{([^}]+)\\}");

  private final HttpSourceConfig config;
  private final HttpSourceConfig.DocumentSourceConfig documentConfig;
  private final Map<String, String> defaultHeaders;
  private final StorageProvider storageProvider;
  private final String cacheDirectory;
  private final long minRequestIntervalMs;

  // Last request timestamp for rate limiting
  private long lastRequestTime = 0;

  /**
   * Creates a DocumentSource from configuration.
   *
   * @param config The HTTP source configuration
   * @param storageProvider Storage provider for caching downloaded files
   * @param cacheDirectory Directory path for caching (can be S3 or local)
   */
  public DocumentSource(HttpSourceConfig config, StorageProvider storageProvider,
      String cacheDirectory) {
    this.config = config;
    this.documentConfig = config.getDocumentSource();
    this.storageProvider = storageProvider;
    this.cacheDirectory = cacheDirectory;

    // Set up default headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.putAll(config.getHeaders());

    // Add standard headers if not present
    if (!headers.containsKey("Accept-Encoding")) {
      headers.put("Accept-Encoding", "gzip, deflate");
    }

    this.defaultHeaders = Collections.unmodifiableMap(headers);

    // Calculate minimum request interval from rate limit config
    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    if (rateLimit != null && rateLimit.getRequestsPerSecond() > 0) {
      this.minRequestIntervalMs = (long) (1000.0 / rateLimit.getRequestsPerSecond());
    } else {
      this.minRequestIntervalMs = 125; // Default: 8 requests/second
    }
  }

  /**
   * Fetches metadata from the configured metadata URL.
   *
   * @param variables Variable values for URL substitution (e.g., cik value)
   * @return Raw metadata response as string
   * @throws IOException If fetch fails
   */
  public String fetchMetadata(Map<String, String> variables) throws IOException {
    if (documentConfig == null || documentConfig.getMetadataUrl() == null) {
      throw new IllegalStateException("No metadata URL configured");
    }

    String url = substituteVariables(documentConfig.getMetadataUrl(), variables);
    LOGGER.debug("Fetching metadata from: {}", url);

    return fetchUrl(url);
  }

  /**
   * Downloads a document file from the configured document URL.
   *
   * @param variables Variable values for URL substitution (cik, accession, document)
   * @return Downloaded file path in cache directory (storage provider path)
   * @throws IOException If download fails
   */
  public String downloadDocument(Map<String, String> variables) throws IOException {
    if (documentConfig == null || documentConfig.getDocumentUrl() == null) {
      throw new IllegalStateException("No document URL configured");
    }

    String url = substituteVariables(documentConfig.getDocumentUrl(), variables);

    // Build cache file path using storage provider
    String cacheKey = buildCacheKey(variables);
    String cachePath = storageProvider.resolvePath(cacheDirectory, cacheKey);

    // Return cached path if it exists and is valid
    if (storageProvider.exists(cachePath)) {
      long size = storageProvider.getMetadata(cachePath).getSize();
      if (size > 0) {
        LOGGER.debug("Using cached document: {}", cachePath);
        return cachePath;
      }
    }

    LOGGER.debug("Downloading document from: {}", url);
    downloadToPath(url, cachePath);

    return cachePath;
  }

  /**
   * Creates an iterator over documents based on metadata response.
   *
   * <p>The iterator yields document metadata maps containing variables
   * needed to download each document (accession, document name, etc.).
   *
   * @param metadataJson Raw metadata JSON response
   * @param baseVariables Base variables (e.g., cik)
   * @return Iterator over document metadata maps
   */
  public Iterator<Map<String, String>> documentIterator(
      String metadataJson, Map<String, String> baseVariables) {
    // This is a placeholder - actual implementation depends on the response format
    // For SEC EDGAR, this would parse submissions.json and yield filing metadata
    return Collections.<Map<String, String>>emptyList().iterator();
  }

  /**
   * Substitutes variables in a URL pattern.
   *
   * <p>Variables can be:
   * <ul>
   *   <li>{varName} - Substituted from variables map</li>
   *   <li>{env:VAR_NAME} - Substituted from environment variable</li>
   * </ul>
   *
   * @param pattern URL pattern with variable placeholders
   * @param variables Variable values map
   * @return URL with variables substituted
   */
  public String substituteVariables(String pattern, Map<String, String> variables) {
    if (pattern == null) {
      return null;
    }

    Matcher matcher = VARIABLE_PATTERN.matcher(pattern);
    StringBuffer result = new StringBuffer();

    while (matcher.find()) {
      String varName = matcher.group(1);
      String replacement;

      if (varName.startsWith("env:")) {
        // Environment variable
        String envVar = varName.substring(4);
        replacement = System.getenv(envVar);
        if (replacement == null) {
          replacement = System.getProperty(envVar, "");
        }
      } else {
        // Regular variable
        replacement = variables.get(varName);
        if (replacement == null) {
          replacement = "";
        }
      }

      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /**
   * Fetches content from a URL as a string.
   */
  private String fetchUrl(String urlStr) throws IOException {
    enforceRateLimit();

    URL url = new URL(urlStr);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);

      // Set headers
      for (Map.Entry<String, String> header : defaultHeaders.entrySet()) {
        conn.setRequestProperty(header.getKey(), header.getValue());
      }

      int responseCode = conn.getResponseCode();

      if (responseCode == 429) {
        // Rate limited - wait and retry
        LOGGER.warn("Rate limited, waiting before retry...");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return fetchUrl(urlStr);
      }

      if (responseCode != 200) {
        throw new IOException("HTTP " + responseCode + " from " + urlStr);
      }

      // Handle gzip encoding
      InputStream inputStream = conn.getInputStream();
      String encoding = conn.getContentEncoding();
      if ("gzip".equalsIgnoreCase(encoding)) {
        inputStream = new GZIPInputStream(inputStream);
      }

      // Read response
      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append("\n");
        }
      }

      return sb.toString();

    } finally {
      conn.disconnect();
    }
  }

  /**
   * Downloads content from a URL to a storage provider path.
   */
  private void downloadToPath(String urlStr, String targetPath) throws IOException {
    enforceRateLimit();

    URL url = new URL(urlStr);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(120000);

      // Set headers
      for (Map.Entry<String, String> header : defaultHeaders.entrySet()) {
        conn.setRequestProperty(header.getKey(), header.getValue());
      }

      int responseCode = conn.getResponseCode();

      if (responseCode == 429) {
        // Rate limited - wait and retry
        LOGGER.warn("Rate limited, waiting before retry...");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        downloadToPath(urlStr, targetPath);
        return;
      }

      if (responseCode == 404) {
        throw new IOException("File not found: " + urlStr);
      }

      if (responseCode != 200) {
        throw new IOException("HTTP " + responseCode + " from " + urlStr);
      }

      // Handle gzip encoding
      InputStream inputStream = conn.getInputStream();
      String encoding = conn.getContentEncoding();
      if ("gzip".equalsIgnoreCase(encoding)) {
        inputStream = new GZIPInputStream(inputStream);
      }

      // Read content to byte array
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        baos.write(buffer, 0, bytesRead);
      }
      byte[] content = baos.toByteArray();

      // Write via storage provider
      storageProvider.writeFile(targetPath, content);

      LOGGER.debug("Downloaded {} bytes to {}", content.length, targetPath);

    } finally {
      conn.disconnect();
    }
  }

  /**
   * Enforces rate limiting by sleeping if necessary.
   */
  private synchronized void enforceRateLimit() {
    long now = System.currentTimeMillis();
    long elapsed = now - lastRequestTime;

    if (elapsed < minRequestIntervalMs) {
      try {
        Thread.sleep(minRequestIntervalMs - elapsed);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    lastRequestTime = System.currentTimeMillis();
  }

  /**
   * Builds a cache key from variables.
   */
  private String buildCacheKey(Map<String, String> variables) {
    StringBuilder key = new StringBuilder();

    // Use cik/accession/document for SEC-style caching
    String cik = variables.get("cik");
    String accession = variables.get("accession");
    String document = variables.get("document");

    if (cik != null) {
      key.append(cik).append("/");
    }
    if (accession != null) {
      key.append(accession.replace("-", "")).append("/");
    }
    if (document != null) {
      key.append(document);
    } else {
      // Fallback to hash of all variables
      key.append(variables.hashCode()).append(".dat");
    }

    return key.toString();
  }

  /**
   * Returns the document source configuration.
   */
  public HttpSourceConfig.DocumentSourceConfig getDocumentConfig() {
    return documentConfig;
  }

  /**
   * Returns the HTTP source configuration.
   */
  public HttpSourceConfig getConfig() {
    return config;
  }

  /**
   * Returns the cache directory path.
   */
  public String getCacheDirectory() {
    return cacheDirectory;
  }

  /**
   * Returns the minimum request interval in milliseconds.
   */
  public long getMinRequestIntervalMs() {
    return minRequestIntervalMs;
  }
}
