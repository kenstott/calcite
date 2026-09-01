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
  private final String cacheDir;
  private final boolean enabled;

  /**
   * @param storageProvider storage holding the raw cache, or null when unavailable
   * @param cacheDir resolved cache directory for this batch, or null when not caching
   * @param bypass whether this run must ignore existing entries (force-download / full reprocess)
   */
  StorageRawCache(StorageProvider storageProvider, String cacheDir, boolean bypass) {
    this.storageProvider = storageProvider;
    this.cacheDir = cacheDir;
    this.enabled = storageProvider != null && cacheDir != null && !bypass;
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
    // Keep the directory the built-in path resolves to, but drop its fixed response.json leaf:
    // a provider may fetch several URLs within one batch (the IRS business master file is four
    // regional shards), and a single fixed filename would make them overwrite one another and
    // serve whichever landed last for all four.
    int slash = path.lastIndexOf('/');
    String dir = slash > 0 ? path.substring(0, slash) : path;
    return new StorageRawCache(storageProvider, dir, bypass);
  }

  /**
   * The entry for one URL within this batch's directory.
   *
   * <p>Named from the URL's last segment for legibility, with a digest of the whole URL appended
   * so two different URLs cannot collide on a shared basename — {@code .../2024/data.csv} and
   * {@code .../2025/data.csv} both end in {@code data.csv}.
   *
   * <p>Including the URL means a source that moves its file re-fetches rather than replaying the
   * old entry, which is the safe direction: a moved file is new content, and an entry here is
   * validated by existence alone.
   */
  private String entryFor(String url) {
    String tail = url;
    int q = tail.indexOf('?');
    if (q >= 0) {
      tail = tail.substring(0, q);
    }
    int slash = tail.lastIndexOf('/');
    if (slash >= 0 && slash < tail.length() - 1) {
      tail = tail.substring(slash + 1);
    }
    if (tail.isEmpty()) {
      tail = "response";
    }
    return cacheDir + "/" + HttpSource.sanitizePathComponent(tail) + "_" + digest(url);
  }

  /** Short, stable digest of the full URL. */
  private static String digest(String url) {
    try {
      java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
      byte[] d = md.digest(url.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (int i = 0; i < 6; i++) {
        hex.append(String.format("%02x", d[i]));
      }
      return hex.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 unavailable", e);
    }
  }

  @Override public boolean isEnabled() {
    return enabled;
  }

  @Override public InputStream openStream(String url) throws IOException {
    return openStream(url, () -> download(url));
  }

  @Override public InputStream openStream(String key, ContentSupplier onMiss) throws IOException {
    if (storageProvider == null || cacheDir == null) {
      return onMiss.open();
    }
    String entry = entryFor(key);
    if (enabled && exists(entry)) {
      LOGGER.debug("Provider raw cache hit: {}", entry);
      return storageProvider.openInputStream(entry);
    }
    LOGGER.debug("Provider raw cache miss, fetching: {}", key);
    File temp = File.createTempFile("provider-raw-cache-", ".bin");
    temp.deleteOnExit();
    try {
      try (InputStream in = onMiss.open(); FileOutputStream out = new FileOutputStream(temp)) {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
      }
      // Committed only after the copy completed, so a failed download leaves no entry behind for
      // the next run to mistake for a whole file.
      storageProvider.createDirectories(cacheDir);
      try (InputStream in = new FileInputStream(temp)) {
        storageProvider.writeFile(entry, in);
      }
      LOGGER.info("Provider cached response to raw: {} ({} bytes)", entry, temp.length());
      return storageProvider.openInputStream(entry);
    } finally {
      if (temp.exists() && !temp.delete()) {
        LOGGER.debug("Could not delete temp file {}", temp);
      }
    }
  }

  private boolean exists(String entry) {
    try {
      return storageProvider.exists(entry);
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
