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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streaming, conditionally-refreshed per-state cache of the FIA datamart CSV archives,
 * stored via the govdata cache {@link StorageProvider} at
 * {@code $GOVDATA_CACHE_DIR/lands/<ST>_CSV.zip}.
 *
 * <p>Each USDA per-state archive is 80-450 MB and bundles ~70 CSV tables for one state
 * (e.g. {@code AK_COND.csv}, {@code AK_TREE.csv}, {@code AK_PLOT.csv}, …). Cached objects
 * persist in MinIO/R2 so every DQ worker in the pool reads them at LAN speed instead of
 * re-downloading from upstream (which throttles individual connections to ~800 KB/s).
 *
 * <p>Refresh per state is gated by HTTP {@code If-Modified-Since} against the cached
 * object's mtime — USDA regenerates the archives near-daily but rarely changes content.
 *
 * <p>Download synchronization is per-state ({@link #STATE_LOCKS}) so concurrent
 * transformers iterating different states share none of their critical sections —
 * 4-way concurrent downloads across distinct states are supported and recommended.
 *
 * <p>{@link #openEntry} returns a {@link ZipInputStream} layered over the storage
 * provider's input stream — decompression and remote reads happen on-demand, never
 * materializing the {@code <ST>_*.csv} content in memory or on local disk.
 */
final class FiaStateArchive {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaStateArchive.class);

  private static final String ARCHIVE_URL_PATTERN =
      "https://apps.fs.usda.gov/fia/datamart/CSV/%s_CSV.zip";
  private static final String CACHE_PATH_PATTERN = "lands/%s_CSV.zip";

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 1_800_000; // 30 min ceiling per state archive

  /**
   * Reuse a cached archive without any network round-trip if it is younger than this. The FIA
   * datamart server ignores {@code If-Modified-Since} (it always answers 200), so the conditional
   * GET below would otherwise re-download every state on every run (~100 MB each). FIA datamarts
   * refresh only ~annually, so a short client-side TTL skips the redundant pulls while still
   * re-checking periodically.
   */
  private static final long CACHE_TTL_MS = 7L * 24 * 60 * 60 * 1000;

  /**
   * Per-state download locks. Distinct states download concurrently; concurrent
   * transformers requesting the same state share a single download.
   */
  private static final ConcurrentMap<String, Object> STATE_LOCKS = new ConcurrentHashMap<>();

  private FiaStateArchive() {
  }

  /** Returns the storage-provider path for the cached per-state archive. */
  static String cachedPath(String state) {
    String base = StorageProviderFactory.getGovDataCacheDir();
    if (base == null || base.isEmpty()) {
      throw new IllegalStateException("GOVDATA_CACHE_DIR is not set");
    }
    String suffix = String.format(Locale.ROOT, CACHE_PATH_PATTERN, normalize(state));
    return base.endsWith("/") ? base + suffix : base + "/" + suffix;
  }

  /**
   * Ensures the per-state archive is present in the govdata cache. Uses HTTP
   * {@code If-Modified-Since} against the cached object's mtime so we only re-download
   * when the upstream artifact has actually changed.
   *
   * <p>Synchronized per-state — concurrent transformers requesting the same state share
   * the single download; requests for different states proceed in parallel.
   */
  static String ensureCached(String state) throws IOException {
    String st = normalize(state);
    String url = String.format(Locale.ROOT, ARCHIVE_URL_PATTERN, st);
    String target = cachedPath(st);
    Object lock = STATE_LOCKS.computeIfAbsent(st, k -> new Object());
    synchronized (lock) {
      StorageProvider sp = StorageProviderFactory.createForGovDataCache();
      long cachedMtime = -1L;
      if (sp.exists(target)) {
        cachedMtime = sp.getMetadata(target).getLastModified();
        if (System.currentTimeMillis() - cachedMtime < CACHE_TTL_MS) {
          LOGGER.info("FIA {} archive cache fresh (within {}-day TTL), reusing without refetch: {}",
              st, CACHE_TTL_MS / 86_400_000L, target);
          return target;
        }
      }
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      if (cachedMtime > 0) {
        conn.setIfModifiedSince(cachedMtime);
      }
      int status;
      try {
        status = conn.getResponseCode();
      } catch (IOException e) {
        conn.disconnect();
        throw e;
      }
      if (status == HttpURLConnection.HTTP_NOT_MODIFIED) {
        conn.disconnect();
        LOGGER.info("FIA {} archive cache fresh: {}", st, target);
        return target;
      }
      if (status != HttpURLConnection.HTTP_OK) {
        conn.disconnect();
        throw new IOException("HTTP " + status + " from " + url);
      }
      long expectedLength = conn.getContentLengthLong();
      LOGGER.info("FIA {} archive downloading: {} bytes -> {}", st, expectedLength, target);
      try (InputStream in = conn.getInputStream()) {
        sp.writeFile(target, in);
      } finally {
        conn.disconnect();
      }
      long writtenLength = sp.exists(target) ? sp.getMetadata(target).getSize() : -1L;
      if (expectedLength > 0 && writtenLength != expectedLength) {
        throw new IOException("FIA " + st + " archive incomplete: expected "
            + expectedLength + " bytes, got " + writtenLength + " in " + target);
      }
      LOGGER.info("FIA {} archive downloaded: {} ({} bytes)", st, target, writtenLength);
      return target;
    }
  }

  /**
   * Opens a streaming {@link InputStream} for the named entry inside the cached
   * per-state archive. The returned stream decompresses lazily as bytes are read —
   * never buffers the entire entry into memory and never downloads more of the
   * archive than is required to reach and read the entry.
   *
   * <p>Caller must close the returned handle (use try-with-resources).
   *
   * @param state USPS state code (e.g. {@code "CA"})
   * @param entrySuffix entry name after the {@code <ST>_} prefix (e.g. {@code "COND.csv"})
   */
  static EntryHandle openEntry(String state, String entrySuffix) throws IOException {
    String st = normalize(state);
    String archive = ensureCached(st);
    String entryName = st + "_" + entrySuffix;
    StorageProvider sp = StorageProviderFactory.createForGovDataCache();
    InputStream remote = sp.openInputStream(archive);
    ZipInputStream zis;
    try {
      zis = new ZipInputStream(remote);
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().equals(entryName)) {
          return new EntryHandle(zis);
        }
        zis.closeEntry();
      }
    } catch (IOException e) {
      try {
        remote.close();
      } catch (IOException ignored) {
        // ignore
      }
      throw e;
    }
    try {
      zis.close();
    } catch (IOException ignored) {
      // ignore
    }
    throw new IOException("Entry " + entryName + " not found in FIA archive for " + st);
  }

  private static String normalize(String state) {
    if (state == null || state.isEmpty()) {
      throw new IllegalArgumentException("FiaStateArchive: state code is required");
    }
    return state.trim().toUpperCase(Locale.ROOT);
  }

  /** Holds the {@link ZipInputStream} positioned at a specific entry. Closing the
   *  stream also closes the underlying remote storage stream. */
  static final class EntryHandle implements AutoCloseable {
    final InputStream stream;
    private final ZipInputStream zis;

    EntryHandle(ZipInputStream zis) {
      this.zis = zis;
      this.stream = zis;
    }

    @Override public void close() throws IOException {
      zis.close();
    }
  }
}
