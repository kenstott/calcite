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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HttpSource shared-cache mirroring between the local mirror
 * and the cloud-backed {@link StorageProvider}.
 *
 * <p>Covers: write-through mirror to s3 on local cache write, read-through
 * hydration of the local mirror when only the s3 copy exists, and no-op
 * behavior when rawCachePath is local-only.
 */
@Tag("unit")
class HttpSourceRawCacheTest {

  @TempDir
  Path tempDir;

  /**
   * In-memory StorageProvider that records every writeFile call into a map
   * keyed by virtual path. Only the methods exercised by the cache code are
   * implemented; the rest throw to surface unintended usage.
   */
  private static StorageProvider inMemoryProvider(Map<String, byte[]> store) {
    StorageProvider sp = mock(StorageProvider.class);
    try {
      // exists()
      when(sp.exists(anyString())).thenAnswer(inv -> store.containsKey(inv.<String>getArgument(0)));
      // openInputStream()
      when(sp.openInputStream(anyString())).thenAnswer(inv -> {
        byte[] bytes = store.get(inv.<String>getArgument(0));
        if (bytes == null) {
          throw new IOException("not found: " + inv.getArgument(0));
        }
        return new ByteArrayInputStream(bytes);
      });
      // writeFile(byte[])
      doAnswer(inv -> {
        store.put(inv.getArgument(0), inv.getArgument(1));
        return null;
      }).when(sp).writeFile(anyString(), any(byte[].class));
      // writeFile(InputStream)
      doAnswer(inv -> {
        InputStream in = inv.getArgument(1);
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) {
          baos.write(buf, 0, n);
        }
        store.put(inv.getArgument(0), baos.toByteArray());
        return null;
      }).when(sp).writeFile(anyString(), any(InputStream.class));
      // createDirectories(): no-op for in-memory store
      doAnswer(inv -> null).when(sp).createDirectories(anyString());
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    return sp;
  }

  private HttpSource newSource(StorageProvider sp, String rawCachePath, Path operatingDir) {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    return new HttpSource(config, (HooksConfig) null, sp, rawCachePath,
        operatingDir == null ? null : operatingDir.toString());
  }

  private static Object invoke(Object target, String name, Class<?>[] argTypes, Object... args)
      throws Exception {
    Method m = HttpSource.class.getDeclaredMethod(name, argTypes);
    m.setAccessible(true);
    return m.invoke(target, args);
  }

  // ------------------------------------------------------------------------
  // mirrorLocalCacheToS3
  // ------------------------------------------------------------------------

  @Test void mirrorWritesLocalContentToS3() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    HttpSource source = newSource(sp, "s3://bucket/raw", tempDir);
    try {
      Path local = tempDir.resolve("cache/response.json");
      Files.createDirectories(local.getParent());
      byte[] payload = "{\"hello\":\"world\"}".getBytes(StandardCharsets.UTF_8);
      Files.write(local, payload);

      String s3Path = "s3://bucket/raw/year=2024/response.json";
      invoke(source, "mirrorLocalCacheToS3",
          new Class[] {String.class, String.class}, local.toString(), s3Path);

      assertTrue(store.containsKey(s3Path), "s3 object should have been written");
      assertArrayEquals(payload, store.get(s3Path));
    } finally {
      source.close();
    }
  }

  @Test void mirrorNoopWhenS3PathNull() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    HttpSource source = newSource(sp, "s3://bucket/raw", tempDir);
    try {
      Path local = tempDir.resolve("cache/response.json");
      Files.createDirectories(local.getParent());
      Files.write(local, "x".getBytes(StandardCharsets.UTF_8));

      invoke(source, "mirrorLocalCacheToS3",
          new Class[] {String.class, String.class}, local.toString(), (String) null);

      assertTrue(store.isEmpty(), "no writes expected when s3 path is null");
      verify(sp, never()).writeFile(anyString(), any(byte[].class));
      verify(sp, never()).writeFile(anyString(), any(InputStream.class));
    } finally {
      source.close();
    }
  }

  @Test void mirrorNoopWhenLocalMissing() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    HttpSource source = newSource(sp, "s3://bucket/raw", tempDir);
    try {
      Path local = tempDir.resolve("cache/never-existed.json");
      String s3Path = "s3://bucket/raw/never-existed.json";

      invoke(source, "mirrorLocalCacheToS3",
          new Class[] {String.class, String.class}, local.toString(), s3Path);

      assertFalse(store.containsKey(s3Path),
          "no write expected when local file does not exist");
    } finally {
      source.close();
    }
  }

  // ------------------------------------------------------------------------
  // hasValidRawCache(local, s3): hydration from s3 on local miss
  // ------------------------------------------------------------------------

  @Test void cacheHitFromS3HydratesLocal() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    HttpSource source = newSource(sp, "s3://bucket/raw", tempDir);
    try {
      String s3Path = "s3://bucket/raw/year=2024/response.json";
      byte[] payload = "{\"from\":\"s3\"}".getBytes(StandardCharsets.UTF_8);
      store.put(s3Path, payload);

      Path local = tempDir.resolve("cache/year=2024/response.json");
      assertFalse(Files.exists(local), "local mirror should not exist yet");

      Boolean hit = (Boolean) invoke(source, "hasValidRawCache",
          new Class[] {String.class, String.class}, local.toString(), s3Path);

      assertTrue(hit, "should report cache hit via s3 fallback");
      assertTrue(Files.exists(local), "local mirror should have been hydrated");
      assertArrayEquals(payload, Files.readAllBytes(local));
    } finally {
      source.close();
    }
  }

  @Test void cacheMissWhenNeitherLocalNorS3() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    HttpSource source = newSource(sp, "s3://bucket/raw", tempDir);
    try {
      Path local = tempDir.resolve("cache/missing.json");
      String s3Path = "s3://bucket/raw/missing.json";

      Boolean hit = (Boolean) invoke(source, "hasValidRawCache",
          new Class[] {String.class, String.class}, local.toString(), s3Path);

      assertFalse(hit, "miss expected when neither tier holds the object");
      assertFalse(Files.exists(local));
    } finally {
      source.close();
    }
  }

  @Test void cacheHitLocalDoesNotConsultS3() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    HttpSource source = newSource(sp, "s3://bucket/raw", tempDir);
    try {
      Path local = tempDir.resolve("cache/hit.json");
      Files.createDirectories(local.getParent());
      Files.write(local, "local".getBytes(StandardCharsets.UTF_8));

      Boolean hit = (Boolean) invoke(source, "hasValidRawCache",
          new Class[] {String.class, String.class}, local.toString(),
          "s3://bucket/raw/hit.json");

      assertTrue(hit);
      // s3 exists() may be called once for the local path (it's local so it skips),
      // but we must not have touched openInputStream or writeFile to mirror.
      verify(sp, never()).openInputStream(anyString());
      verify(sp, never()).writeFile(anyString(), any(byte[].class));
      verify(sp, never()).writeFile(anyString(), any(InputStream.class));
    } finally {
      source.close();
    }
  }

  // ------------------------------------------------------------------------
  // Local-only mode (rawCachePath is a local path): no storage-provider calls
  // ------------------------------------------------------------------------

  @Test void localOnlyRawCachePathSkipsS3Mirror() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    // rawCachePath itself is a local path -> buildS3RawCachePath should return null.
    HttpSource source =
        newSource(sp, tempDir.resolve("rawroot").toString(), tempDir);
    try {
      Map<String, String> vars = new LinkedHashMap<>();
      vars.put("year", "2024");

      String s3Path = (String) invoke(source, "buildS3RawCachePath",
          new Class[] {Map.class}, vars);
      assertNull(s3Path, "buildS3RawCachePath must be null for local-only rawCachePath");

      // mirror invoked with null s3Path is a no-op
      Path local = tempDir.resolve("cache/local-only.json");
      Files.createDirectories(local.getParent());
      Files.write(local, "x".getBytes(StandardCharsets.UTF_8));
      invoke(source, "mirrorLocalCacheToS3",
          new Class[] {String.class, String.class}, local.toString(), (String) null);
      assertTrue(store.isEmpty());
    } finally {
      source.close();
    }
  }

  @Test void buildS3RawCachePathReflectsDimensionVariables() throws Exception {
    Map<String, byte[]> store = new ConcurrentHashMap<>();
    StorageProvider sp = inMemoryProvider(store);
    HttpSource source = newSource(sp, "s3://bucket/raw", tempDir);
    try {
      Map<String, String> vars = new LinkedHashMap<>();
      vars.put("year", "2024");
      vars.put("type", "gdp");
      String s3Path = (String) invoke(source, "buildS3RawCachePath",
          new Class[] {Map.class}, vars);
      assertNotNull(s3Path);
      assertTrue(s3Path.startsWith("s3://bucket/raw/"),
          "should be rooted at the configured s3 prefix: " + s3Path);
      assertTrue(s3Path.contains("year=2024"));
      assertTrue(s3Path.contains("type=gdp"));
      assertTrue(s3Path.endsWith("response.json"));
    } finally {
      source.close();
    }
  }

  @Test void buildS3RawCachePathNullWhenStorageProviderMissing() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    HttpSource source = new HttpSource(config, (HooksConfig) null, null,
        "s3://bucket/raw", tempDir.toString());
    try {
      String s3Path = (String) invoke(source, "buildS3RawCachePath",
          new Class[] {Map.class}, new LinkedHashMap<String, String>());
      assertNull(s3Path);
    } finally {
      source.close();
    }
  }
}
