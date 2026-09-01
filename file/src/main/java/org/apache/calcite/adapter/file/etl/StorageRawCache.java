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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLConnection;
import java.util.Map;

/**
 * The {@link RawCache} a pipeline hands to a {@link DataProvider}, backed by the same storage and
 * the same key layout {@link HttpSource} uses.
 *
 * <p>Downloads land in a temp file first and are only committed to storage once the copy has
 * finished without throwing. A half-written entry would otherwise be indistinguishable from a
 * complete one on the next run — {@code exists()} is the whole validity check, since these entries
 * are immutable and staleness is decided by the tracker rather than by a TTL — and would be served
 * as though it were the full file.
 */
final class StorageRawCache implements RawCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageRawCache.class);

  private final StorageProvider storageProvider;
  private final String cacheFilePath;
  private final boolean enabled;

  /**
   * @param storageProvider storage holding the raw cache, or null when unavailable
   * @param cacheFilePath fully-resolved entry path for this batch, or null when not caching
   * @param bypass whether this run must ignore existing entries (force-download / full reprocess)
   */
  StorageRawCache(StorageProvider storageProvider, String cacheFilePath, boolean bypass) {
    this.storageProvider = storageProvider;
    this.cacheFilePath = cacheFilePath;
    this.enabled = storageProvider != null && cacheFilePath != null && !bypass;
  }

  /**
   * Builds the handle for one batch, or a non-caching one when the table has no raw cache.
   *
   * @param config the pipeline config
   * @param variables the batch's dimension values
   * @param storageProvider storage holding the raw cache
   * @param rawCachePath base path (table name, or the table's rawCache.sharedKey)
   * @param bypass whether this run must ignore existing entries
   * @return a handle; caching when the table and run allow it, downloading every time otherwise
   */
  static RawCache forBatch(EtlPipelineConfig config, Map<String, String> variables,
      StorageProvider storageProvider, String rawCachePath, boolean bypass) {
    HttpSourceConfig source = config != null ? config.getSource() : null;
    HttpSourceConfig.RawCacheConfig rawCache = source != null ? source.getRawCache() : null;
    if (rawCache == null || !rawCache.isEnabled() || storageProvider == null
        || rawCachePath == null) {
      return new StorageRawCache(null, null, true);
    }
    boolean gzip = source.getResponse() != null
        && "gzip".equalsIgnoreCase(source.getResponse().getCompressed());
    String path = HttpSource.buildRawCachePath(rawCachePath, variables, rawCache.getKeyVars(),
        0, gzip);
    return new StorageRawCache(storageProvider, path, bypass);
  }

  @Override public boolean isEnabled() {
    return enabled;
  }

  @Override public InputStream openStream(String url) throws IOException {
    if (enabled && exists()) {
      LOGGER.debug("Provider raw cache hit: {}", cacheFilePath);
      return storageProvider.openInputStream(cacheFilePath);
    }
    if (storageProvider == null || cacheFilePath == null) {
      return download(url);
    }
    LOGGER.debug("Provider raw cache miss, downloading: {}", url);
    File temp = File.createTempFile("provider-raw-cache-", ".bin");
    temp.deleteOnExit();
    try {
      try (InputStream in = download(url); FileOutputStream out = new FileOutputStream(temp)) {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
      }
      // Committed only after the copy completed, so a failed download leaves no entry behind for
      // the next run to mistake for a whole file.
      int slash = cacheFilePath.lastIndexOf('/');
      if (slash > 0) {
        storageProvider.createDirectories(cacheFilePath.substring(0, slash));
      }
      try (InputStream in = new FileInputStream(temp)) {
        storageProvider.writeFile(cacheFilePath, in);
      }
      LOGGER.info("Provider cached response to raw: {} ({} bytes)", cacheFilePath, temp.length());
      return storageProvider.openInputStream(cacheFilePath);
    } finally {
      if (temp.exists() && !temp.delete()) {
        LOGGER.debug("Could not delete temp file {}", temp);
      }
    }
  }

  private boolean exists() {
    try {
      return storageProvider.exists(cacheFilePath);
    // fallback-guard: an unreadable cache must re-fetch rather than be trusted; logged at debug.
    } catch (IOException e) {
      LOGGER.debug("Error checking provider raw cache: {}", e.getMessage());
      return false;
    }
  }

  private InputStream download(String url) throws IOException {
    URLConnection conn = URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(60000);
    conn.setReadTimeout(600000);
    if (conn instanceof HttpURLConnection) {
      ((HttpURLConnection) conn).setInstanceFollowRedirects(true);
    }
    return conn.getInputStream();
  }
}
