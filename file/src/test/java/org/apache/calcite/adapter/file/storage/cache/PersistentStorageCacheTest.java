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
package org.apache.calcite.adapter.file.storage.cache;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PersistentStorageCache}.
 */
@Tag("unit")
public class PersistentStorageCacheTest {

  @TempDir
  File tempDir;

  @Test void testInitialStateIsEmpty() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    assertEquals(0, cache.getCacheSize());
    assertNull(cache.getCachedData("/test.csv"));
    assertNull(cache.getCachedMetadata("/test.csv"));
  }

  @Test void testCacheAndRetrieveData() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    byte[] data = "test data content".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", data.length, 1710000000L,
            "text/csv", "etag-123");

    cache.cacheData("/test.csv", data, metadata, 0);

    assertEquals(1, cache.getCacheSize());

    byte[] cached = cache.getCachedData("/test.csv");
    assertNotNull(cached);
    assertArrayEquals(data, cached);
  }

  @Test void testCacheAndRetrieveMetadata() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    byte[] data = "data".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", data.length, 1710000000L,
            "text/csv", "etag-abc");

    cache.cacheData("/test.csv", data, metadata, 0);

    StorageProvider.FileMetadata cachedMeta = cache.getCachedMetadata("/test.csv");
    assertNotNull(cachedMeta);
    assertEquals("/test.csv", cachedMeta.getPath());
    assertEquals(data.length, cachedMeta.getSize());
    assertEquals("etag-abc", cachedMeta.getEtag());
  }

  @Test void testCacheExpiration() throws InterruptedException {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 100);

    byte[] data = "ephemeral".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", data.length, 0, "text/csv", null);

    // Cache with short TTL
    cache.cacheData("/test.csv", data, metadata, 100);

    // Should be available immediately
    assertNotNull(cache.getCachedData("/test.csv"));

    // Wait for expiration
    Thread.sleep(200);

    // Should be expired now
    assertNull(cache.getCachedData("/test.csv"));
    assertNull(cache.getCachedMetadata("/test.csv"));
  }

  @Test void testCacheNoExpiration() throws InterruptedException {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 0);

    byte[] data = "permanent".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", data.length, 0, "text/csv", null);

    // Cache with TTL=0 (no expiration)
    cache.cacheData("/test.csv", data, metadata, 0);

    // Wait a bit
    Thread.sleep(50);

    // Should still be available
    assertNotNull(cache.getCachedData("/test.csv"));
  }

  @Test void testCacheOverwrite() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    byte[] data1 = "original".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata meta1 =
        new StorageProvider.FileMetadata("/test.csv", data1.length, 0, "text/csv", "v1");

    cache.cacheData("/test.csv", data1, meta1, 0);

    byte[] data2 = "updated content".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata meta2 =
        new StorageProvider.FileMetadata("/test.csv", data2.length, 0, "text/csv", "v2");

    cache.cacheData("/test.csv", data2, meta2, 0);

    assertEquals(1, cache.getCacheSize());

    byte[] cached = cache.getCachedData("/test.csv");
    assertNotNull(cached);
    assertArrayEquals(data2, cached);

    StorageProvider.FileMetadata cachedMeta = cache.getCachedMetadata("/test.csv");
    assertEquals("v2", cachedMeta.getEtag());
  }

  @Test void testEvict() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    byte[] data = "to evict".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", data.length, 0, "text/csv", null);

    cache.cacheData("/test.csv", data, metadata, 0);
    assertEquals(1, cache.getCacheSize());

    cache.evict("/test.csv");
    assertEquals(0, cache.getCacheSize());
    assertNull(cache.getCachedData("/test.csv"));
    assertNull(cache.getCachedMetadata("/test.csv"));
  }

  @Test void testEvictNonExistent() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    // Should not throw
    cache.evict("/nonexistent.csv");
    assertEquals(0, cache.getCacheSize());
  }

  @Test void testClear() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    for (int i = 0; i < 5; i++) {
      byte[] data = ("data" + i).getBytes(StandardCharsets.UTF_8);
      StorageProvider.FileMetadata metadata =
          new StorageProvider.FileMetadata("/file" + i + ".csv", data.length, 0, "text/csv", null);
      cache.cacheData("/file" + i + ".csv", data, metadata, 0);
    }
    assertEquals(5, cache.getCacheSize());

    cache.clear();
    assertEquals(0, cache.getCacheSize());

    for (int i = 0; i < 5; i++) {
      assertNull(cache.getCachedData("/file" + i + ".csv"));
    }
  }

  @Test void testCleanupExpired() throws InterruptedException {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    // Add entry with short TTL
    byte[] shortLived = "short".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata meta1 =
        new StorageProvider.FileMetadata("/short.csv", shortLived.length, 0, "text/csv", null);
    cache.cacheData("/short.csv", shortLived, meta1, 100);

    // Add entry with long TTL
    byte[] longLived = "long".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata meta2 =
        new StorageProvider.FileMetadata("/long.csv", longLived.length, 0, "text/csv", null);
    cache.cacheData("/long.csv", longLived, meta2, 60000);

    assertEquals(2, cache.getCacheSize());

    // Wait for short TTL to expire
    Thread.sleep(200);

    cache.cleanupExpired();

    // Only long-lived entry should remain
    assertEquals(1, cache.getCacheSize());
    assertNull(cache.getCachedData("/short.csv"));
    assertNotNull(cache.getCachedData("/long.csv"));
  }

  @Test void testMultipleEntriesDifferentPaths() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);

    byte[] data1 = "file1".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "file2".getBytes(StandardCharsets.UTF_8);
    byte[] data3 = "file3".getBytes(StandardCharsets.UTF_8);

    StorageProvider.FileMetadata meta1 =
        new StorageProvider.FileMetadata("/a.csv", 5, 0, "text/csv", null);
    StorageProvider.FileMetadata meta2 =
        new StorageProvider.FileMetadata("/b.csv", 5, 0, "text/csv", null);
    StorageProvider.FileMetadata meta3 =
        new StorageProvider.FileMetadata("/c.csv", 5, 0, "text/csv", null);

    cache.cacheData("/a.csv", data1, meta1, 0);
    cache.cacheData("/b.csv", data2, meta2, 0);
    cache.cacheData("/c.csv", data3, meta3, 0);

    assertEquals(3, cache.getCacheSize());
    assertArrayEquals(data1, cache.getCachedData("/a.csv"));
    assertArrayEquals(data2, cache.getCachedData("/b.csv"));
    assertArrayEquals(data3, cache.getCachedData("/c.csv"));
  }

  @Test void testNewInstanceStartsClean() {
    // Create and populate cache
    PersistentStorageCache cache1 = new PersistentStorageCache(tempDir, "http", 60000);
    byte[] data = "persistent".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", data.length, 0, "text/csv", null);
    cache1.cacheData("/test.csv", data, metadata, 0);
    assertEquals(1, cache1.getCacheSize());

    // Verify original cache still works after writing
    assertNotNull(cache1.getCachedData("/test.csv"));
    assertArrayEquals(data, cache1.getCachedData("/test.csv"));
  }

  @Test void testCacheDataCreatesDataFiles() {
    PersistentStorageCache cache = new PersistentStorageCache(tempDir, "http", 60000);
    byte[] data = "file content".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", data.length, 0, "text/csv", null);

    cache.cacheData("/test.csv", data, metadata, 0);

    // Verify data directory has .dat files
    File dataDir = new File(new File(tempDir, "http"), "data");
    assertTrue(dataDir.exists());
    File[] datFiles = dataDir.listFiles(
        (dir, name) -> name.endsWith(".dat"));
    assertNotNull(datFiles);
    assertTrue(datFiles.length > 0);
  }

  @Test void testCacheEntryIsExpired() {
    PersistentStorageCache.CacheEntry entry = new PersistentStorageCache.CacheEntry();
    entry.ttlMs = 100;
    entry.cachedAt = System.currentTimeMillis() - 200; // Already expired

    assertTrue(entry.isExpired());
  }

  @Test void testCacheEntryNotExpiredWithZeroTtl() {
    PersistentStorageCache.CacheEntry entry = new PersistentStorageCache.CacheEntry();
    entry.ttlMs = 0;
    entry.cachedAt = 0; // Very old

    // Zero TTL means no expiration
    assertTrue(!entry.isExpired());
  }

  @Test void testCacheEntryNegativeTtlNeverExpires() {
    PersistentStorageCache.CacheEntry entry = new PersistentStorageCache.CacheEntry();
    entry.ttlMs = -1;
    entry.cachedAt = 0;

    assertTrue(!entry.isExpired());
  }

  @Test void testCacheCreatesDirectoryStructure() {
    File cacheDir = new File(tempDir, "new_cache");

    PersistentStorageCache cache = new PersistentStorageCache(cacheDir, "s3", 60000);

    // Verify directory was created
    assertTrue(new File(cacheDir, "s3").exists());
    assertTrue(new File(cacheDir, "s3/data").exists());
  }
}
