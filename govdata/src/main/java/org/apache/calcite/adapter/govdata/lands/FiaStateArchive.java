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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
 * <p>Refresh per state is gated by the upstream {@code Last-Modified} header (the FIA datamart
 * ignores {@code If-Modified-Since} but does send {@code Last-Modified}): a HEAD compares it to the
 * value recorded for the cached bytes and re-downloads only when it changes — never on a timer.
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

  /** Sidecar that records the upstream {@code Last-Modified} we cached the archive at. */
  private static final String LASTMOD_SIDECAR_SUFFIX = ".lastmodified";

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
      String sidecar = target + LASTMOD_SIDECAR_SUFFIX;
      // Freshness is determined by an upstream validator, never a timer. We prefer the content-based
      // ETag (robust to a server re-stamping Last-Modified without a content change) and fall back to
      // Last-Modified when no ETag is sent. A HEAD reads the current validator; we reuse the cached
      // copy only when it matches what we recorded for the cached bytes. A changed (or first-seen)
      // validator triggers a full re-download.
      String upstreamValidator = headValidator(url);
      if (sp.exists(target) && upstreamValidator != null
          && upstreamValidator.equals(readSidecar(sp, sidecar))) {
        LOGGER.info("FIA {} archive unchanged (validator {}), reusing: {}",
            st, upstreamValidator, target);
        return target;
      }
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      int status;
      try {
        status = conn.getResponseCode();
      } catch (IOException e) {
        conn.disconnect();
        throw e;
      }
      if (status != HttpURLConnection.HTTP_OK) {
        conn.disconnect();
        throw new IOException("HTTP " + status + " from " + url);
      }
      // Prefer the GET response's validator (authoritative for the bytes we are about to write)
      // and fall back to the HEAD value.
      String downloadedValidator = validatorOf(conn);
      if (downloadedValidator == null) {
        downloadedValidator = upstreamValidator;
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
      // Record the validator so the next run can reuse these bytes when unchanged.
      if (downloadedValidator != null) {
        writeSidecar(sp, sidecar, downloadedValidator);
      }
      LOGGER.info("FIA {} archive downloaded: {} ({} bytes, validator {})",
          st, target, writtenLength, downloadedValidator);
      return target;
    }
  }

  /**
   * Extracts a cache validator from a response: prefers the content-based {@code ETag}, falls back
   * to {@code Last-Modified}. The two are prefixed ({@code etag:}/{@code lm:}) so they can never be
   * confused, and so an older Last-Modified-only sidecar safely fails to match (one re-download on
   * upgrade, then stable). Returns null if the server sends neither.
   */
  private static String validatorOf(HttpURLConnection conn) {
    String etag = conn.getHeaderField("ETag");
    if (etag != null && !etag.trim().isEmpty()) {
      return "etag:" + etag.trim();
    }
    String lm = conn.getHeaderField("Last-Modified");
    if (lm != null && !lm.trim().isEmpty()) {
      return "lm:" + lm.trim();
    }
    return null;
  }

  /** Issues a HEAD and returns the upstream cache validator (ETag preferred), or null if unavailable. */
  private static String headValidator(String url) {
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setRequestMethod("HEAD");
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(CONNECT_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return validatorOf(conn);
      }
      return null;
    } catch (IOException e) {
      // No freshness signal available — caller re-downloads (cannot prove the cache is current).
      LOGGER.warn("FIA HEAD probe failed for {}: {}", url, e.getMessage());
      return null;
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /** Reads the cached upstream Last-Modified sidecar, or null if absent. */
  private static String readSidecar(StorageProvider sp, String sidecar) {
    try {
      if (!sp.exists(sidecar)) {
        return null;
      }
      try (InputStream in = sp.openInputStream(sidecar)) {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[256];
        int n;
        while ((n = in.read(buf)) != -1) {
          bos.write(buf, 0, n);
        }
        String value = new String(bos.toByteArray(), StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? null : value;
      }
    } catch (IOException e) {
      LOGGER.warn("FIA Last-Modified sidecar read failed for {}: {}", sidecar, e.getMessage());
      return null;
    }
  }

  /** Writes the upstream Last-Modified sidecar next to the cached archive. */
  private static void writeSidecar(StorageProvider sp, String sidecar, String value) {
    try {
      sp.writeFile(sidecar,
          new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      LOGGER.warn("FIA Last-Modified sidecar write failed for {}: {}", sidecar, e.getMessage());
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
