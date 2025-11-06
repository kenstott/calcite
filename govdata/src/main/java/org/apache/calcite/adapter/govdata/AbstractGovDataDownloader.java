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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Cross-schema base class for government data downloaders (ECON, GEO, SEC).
 *
 * <p>Provides shared infrastructure only: HTTP client with retry/backoff and
 * rate limiting, plus small diagnostics helpers for file reads. Schema/domain
 * specifics remain in their respective abstract classes.</p>
 */
public abstract class AbstractGovDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGovDataDownloader.class);

  /** Cache directory for storing downloaded raw data (e.g., $GOVDATA_CACHE_DIR/...) */
  protected final String cacheDirectory;
  /** Operating directory for storing operational metadata (e.g., .aperio/<schema>/) */
  protected final String operatingDirectory;
  /** Parquet directory for storing converted parquet files */
  protected final String parquetDirectory;
  /** Storage provider for reading/writing raw cache files (JSON, CSV, XML) */
  protected final StorageProvider cacheStorageProvider;
  /** Storage provider for reading/writing parquet files (supports local and S3) */
  protected final StorageProvider storageProvider;

  /** Shared JSON mapper for convenience */
  protected final ObjectMapper MAPPER = new ObjectMapper();

  /** HTTP client for API/HTTP requests */
  protected final HttpClient httpClient;

  /** Timestamp of last request for rate limiting */
  protected long lastRequestTime = 0L;

  protected AbstractGovDataDownloader(
      String cacheDirectory,
      String operatingDirectory,
      String parquetDirectory,
      StorageProvider cacheStorageProvider,
      StorageProvider storageProvider) {
    this.cacheDirectory = cacheDirectory;
    this.operatingDirectory = operatingDirectory;
    this.parquetDirectory = parquetDirectory;
    this.cacheStorageProvider = cacheStorageProvider;
    this.storageProvider = storageProvider;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
  }

  /** Minimum interval between HTTP requests in milliseconds (subclass-defined). */
  protected abstract long getMinRequestIntervalMs();
  /** Max retry attempts for transient failures (subclass-defined). */
  protected abstract int getMaxRetries();
  /** Initial backoff delay in milliseconds (subclass-defined). */
  protected abstract long getRetryDelayMs();

  /** Optional: override to customize default User-Agent. */
  protected String getDefaultUserAgent() { return "Calcite-GovData/1.0"; }

  /** Enforce a simple per-instance rate limit. */
  protected final void enforceRateLimit() throws InterruptedException {
    long minInterval = getMinRequestIntervalMs();
    if (minInterval <= 0) {
      return; // No rate limit
    }
    synchronized (this) {
      long now = System.currentTimeMillis();
      long elapsed = now - lastRequestTime;
      if (elapsed < minInterval) {
        long waitTime = minInterval - elapsed;
        LOGGER.trace("Rate limiting: waiting {} ms", waitTime);
        Thread.sleep(waitTime);
      }
      lastRequestTime = System.currentTimeMillis();
    }
  }

  /** Execute an HTTP request with retry/backoff and rate limiting. */
  protected final HttpResponse<String> executeWithRetry(HttpRequest request)
      throws IOException, InterruptedException {
    int maxRetries = Math.max(1, getMaxRetries());
    long retryDelay = Math.max(0, getRetryDelayMs());

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        enforceRateLimit();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 429 || response.statusCode() >= 500) {
          if (attempt < maxRetries - 1) {
            long delay = retryDelay * (long) Math.pow(2, attempt);
            LOGGER.warn("Request failed with status {} - retrying in {} ms (attempt {}/{})",
                response.statusCode(), delay, attempt + 1, maxRetries);
            Thread.sleep(delay);
            continue;
          }
        }
        return response; // success or non-retryable
      } catch (IOException e) {
        if (attempt < maxRetries - 1) {
          long delay = retryDelay * (long) Math.pow(2, attempt);
          LOGGER.warn("Request failed: {} - retrying in {} ms (attempt {}/{})",
              e.getMessage(), delay, attempt + 1, maxRetries);
          Thread.sleep(delay);
        } else {
          throw e;
        }
      }
    }
    throw new IOException("Failed after " + maxRetries + " attempts");
  }

  /** Convenience: download a URL to bytes with default headers and retry. */
  protected byte[] downloadFile(String url) throws IOException {
    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(60))
          .header("User-Agent", getDefaultUserAgent())
          .header("Accept", "*/*")
          .GET()
          .build();
      HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      if (code >= 200 && code < 300) {
        return response.body();
      }
      throw new IOException("HTTP " + code + " for " + url);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while downloading: " + url, e);
    }
  }

  // ===== Diagnostics helpers (optional use) =====

  /** Log file size if available using the given provider. */
  protected void logFileSize(StorageProvider provider, String fullPath, String label) {
    try {
      long size = provider.getMetadata(fullPath).getSize();
      LOGGER.debug("[DEBUG] {} exists, size={} bytes: {}", label, size, fullPath);
    } catch (Exception e) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", fullPath, e.getMessage());
    }
  }

  /** Preview the head of a file for debugging (non-destructive). */
  protected void previewHead(StorageProvider provider, String fullPath, int bytes, String label) {
    try (InputStream in = provider.openInputStream(fullPath);
         BufferedInputStream bin = new BufferedInputStream(in)) {
      bin.mark(bytes + 1);
      byte[] head = bin.readNBytes(bytes);
      LOGGER.debug("[DEBUG] Head of {} ({} bytes):\n{}", label, head.length, new String(head, StandardCharsets.UTF_8));
      bin.reset();
    } catch (Exception e) {
      LOGGER.debug("[DEBUG] Unable to preview head for {}: {}", fullPath, e.getMessage());
    }
  }

  /** Log basic JSON shape information for quick diagnostics. */
  protected void logJsonShape(JsonNode root, String label) {
    try {
      LOGGER.debug("[DEBUG] {} JSON root type: {}", label, root.getNodeType());
      if (root.isObject()) {
        List<String> keys = new ArrayList<>();
        root.fieldNames().forEachRemaining(keys::add);
        LOGGER.debug("[DEBUG] {} JSON object keys: {}", label, keys);
      } else if (root.isArray()) {
        LOGGER.debug("[DEBUG] {} JSON array size: {}", label, root.size());
        if (root.size() > 0) {
          List<String> keys = new ArrayList<>();
          root.get(0).fieldNames().forEachRemaining(keys::add);
          LOGGER.debug("[DEBUG] {} first element keys: {}", label, keys);
        }
      }
    } catch (Exception ignore) {
      // best-effort logging only
    }
  }
}
