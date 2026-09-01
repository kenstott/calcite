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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the raw cache handed to a {@link DataProvider}.
 *
 * <p>A provider is used to reach what the built-in path cannot — a binary the response
 * transformer's {@code String} signature cannot carry, or a URL that has to be discovered by
 * crawling. That has nothing to do with whether the bytes are worth keeping, but because a
 * provider replaces {@link HttpSource}, and the cache lives inside it, providers that wanted
 * caching each grew their own with their own key layout and their own invalidation. These tests
 * pin the shared handle they can use instead.
 *
 * <p>Content is exercised as bytes, not text, because caching a shapefile zip is the main reason
 * this exists.
 */
@Tag("unit")
public class StorageRawCacheTest {

  private static byte[] drain(InputStream in) throws java.io.IOException {
    try (InputStream s = in) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int n;
      while ((n = s.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    }
  }

  /** Bytes that are not valid UTF-8, standing in for a zip. */
  private static byte[] binaryPayload() {
    byte[] b = new byte[512];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i % 256);
    }
    b[0] = 'P';
    b[1] = 'K';
    return b;
  }

  private static EtlPipelineConfig config(boolean cacheEnabled) {
    Map<String, Object> rawCache = new LinkedHashMap<String, Object>();
    rawCache.put("enabled", cacheEnabled);
    Map<String, Object> source = new LinkedHashMap<String, Object>();
    source.put("url", "https://example.gov/{year}.zip");
    source.put("rawCache", rawCache);

    Map<String, Object> table = new LinkedHashMap<String, Object>();
    table.put("name", "shapes");
    table.put("source", source);
    table.put("materialize", java.util.Collections.singletonMap("enabled", false));
    return EtlPipelineConfig.fromMap(table);
  }

  private static Map<String, String> batch(String year) {
    Map<String, String> v = new LinkedHashMap<String, String>();
    v.put("year", year);
    return v;
  }

  /** A miss downloads, stores the bytes, and serves them; the entry then exists on disk. */
  @Test void aMissDownloadsAndStoresTheBytes(@TempDir Path tmp) throws Exception {
    byte[] payload = binaryPayload();
    File origin = tmp.resolve("origin.zip").toFile();
    Files.write(origin.toPath(), payload);
    StorageProvider sp = new LocalFileStorageProvider();
    Path cacheRoot = tmp.resolve("raw");

    RawCache cache = StorageRawCache.forBatch(config(true), batch("2024"), sp,
        cacheRoot.toString() + "/shapes", false);
    assertTrue(cache.isEnabled());

    byte[] got = drain(cache.openStream(origin.toURI().toString()));
    assertArrayEquals(payload, got, "binary content must survive the round trip byte for byte");
    assertTrue(Files.walk(cacheRoot).anyMatch(p -> p.toFile().isFile()),
        "the download must have been committed to the cache");
  }

  /** A hit is served from storage — proven by deleting the origin before the second read. */
  @Test void aHitIsServedFromStorageNotTheNetwork(@TempDir Path tmp) throws Exception {
    byte[] payload = binaryPayload();
    File origin = tmp.resolve("origin.zip").toFile();
    Files.write(origin.toPath(), payload);
    StorageProvider sp = new LocalFileStorageProvider();
    String base = tmp.resolve("raw").toString() + "/shapes";
    String url = origin.toURI().toString();

    drain(StorageRawCache.forBatch(config(true), batch("2024"), sp, base, false).openStream(url));
    assertTrue(origin.delete(), "origin removed so a second fetch cannot succeed over the wire");

    byte[] second = drain(
        StorageRawCache.forBatch(config(true), batch("2024"), sp, base, false).openStream(url));
    assertArrayEquals(payload, second, "second read must come from the cache");
  }

  /** Different batches are different entries, so one batch cannot serve another's bytes. */
  @Test void eachBatchGetsItsOwnEntry(@TempDir Path tmp) throws Exception {
    File a = tmp.resolve("a.zip").toFile();
    File b = tmp.resolve("b.zip").toFile();
    Files.write(a.toPath(), "AAAA".getBytes(StandardCharsets.UTF_8));
    Files.write(b.toPath(), "BBBB".getBytes(StandardCharsets.UTF_8));
    StorageProvider sp = new LocalFileStorageProvider();
    String base = tmp.resolve("raw").toString() + "/shapes";

    drain(StorageRawCache.forBatch(config(true), batch("2024"), sp, base, false)
        .openStream(a.toURI().toString()));
    byte[] got = drain(StorageRawCache.forBatch(config(true), batch("2025"), sp, base, false)
        .openStream(b.toURI().toString()));

    assertEquals("BBBB", new String(got, StandardCharsets.UTF_8),
        "2025 must not be served 2024's entry");
  }

  /**
   * A run that is bypassing the cache re-downloads. Existence is the whole validity check for
   * these entries, so a force-reprocess that trusted one would replay exactly the stale bytes it
   * was run to replace.
   */
  @Test void bypassRefetchesRatherThanTrustingAnEntry(@TempDir Path tmp) throws Exception {
    File origin = tmp.resolve("origin.zip").toFile();
    Files.write(origin.toPath(), "first".getBytes(StandardCharsets.UTF_8));
    StorageProvider sp = new LocalFileStorageProvider();
    String base = tmp.resolve("raw").toString() + "/shapes";
    String url = origin.toURI().toString();

    drain(StorageRawCache.forBatch(config(true), batch("2024"), sp, base, false).openStream(url));
    Files.write(origin.toPath(), "second".getBytes(StandardCharsets.UTF_8));

    RawCache bypassing = StorageRawCache.forBatch(config(true), batch("2024"), sp, base, true);
    assertFalse(bypassing.isEnabled());
    assertEquals("second", new String(drain(bypassing.openStream(url)), StandardCharsets.UTF_8),
        "a bypassing run must see the current bytes, not the cached ones");
  }

  /** rawCache.enabled: false must not start caching just because a provider asked for a handle. */
  @Test void aTableWithCachingOffDoesNotSuddenlyCache(@TempDir Path tmp) throws Exception {
    File origin = tmp.resolve("origin.zip").toFile();
    Files.write(origin.toPath(), "payload".getBytes(StandardCharsets.UTF_8));
    StorageProvider sp = new LocalFileStorageProvider();
    Path cacheRoot = tmp.resolve("raw");

    RawCache cache = StorageRawCache.forBatch(config(false), batch("2024"), sp,
        cacheRoot.toString() + "/shapes", false);
    assertFalse(cache.isEnabled());

    assertEquals("payload",
        new String(drain(cache.openStream(origin.toURI().toString())), StandardCharsets.UTF_8),
        "content is still delivered");
    assertFalse(cacheRoot.toFile().exists(), "but nothing was written to the cache");
  }

  /**
   * A plain provider is not cache-aware, so the pipeline must keep calling its two-argument form.
   * The distinction has to be answerable by type: a proxy-based provider does not run interface
   * defaults, so a default method would hand back null and the pipeline would read that as the
   * provider declining the batch and quietly fall through to HttpSource.
   */
  @Test void aPlainProviderIsNotTreatedAsCacheAware() throws Exception {
    DataProvider plain = (cfg, vars) ->
        Arrays.<Map<String, Object>>asList(
            new LinkedHashMap<String, Object>(java.util.Collections.singletonMap("n", 1)))
            .iterator();

    assertFalse(plain instanceof CachingDataProvider);
    assertTrue(plain.fetch(config(true), batch("2024")).hasNext());
  }

  /** A cache-aware provider is recognised by type and reads its bytes through the handle. */
  @Test void aCacheAwareProviderReadsThroughTheHandle(@TempDir Path tmp) throws Exception {
    File origin = tmp.resolve("origin.zip").toFile();
    Files.write(origin.toPath(), "rows".getBytes(StandardCharsets.UTF_8));
    final String url = origin.toURI().toString();

    CachingDataProvider caching = (cfg, vars, cache) -> {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("body", new String(drain(cache.openStream(url)), StandardCharsets.UTF_8));
      return Arrays.<Map<String, Object>>asList(row).iterator();
    };

    assertTrue(caching instanceof DataProvider, "must still satisfy the plain contract");
    StorageProvider sp = new LocalFileStorageProvider();
    RawCache cache = StorageRawCache.forBatch(config(true), batch("2024"), sp,
        tmp.resolve("raw").toString() + "/shapes", false);

    assertEquals("rows",
        caching.fetch(config(true), batch("2024"), cache).next().get("body"));
  }
}
