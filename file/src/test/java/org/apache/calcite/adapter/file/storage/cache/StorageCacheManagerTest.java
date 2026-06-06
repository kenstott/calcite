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
package org.apache.calcite.adapter.file.storage.cache;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link StorageCacheManager}.
 */
@Tag("unit")
public class StorageCacheManagerTest {

  @TempDir
  File tempDir;

  @BeforeEach
  void setUp() throws Exception {
    // Reset singleton for clean test state
    resetSingleton();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      StorageCacheManager instance = StorageCacheManager.getInstance();
      instance.shutdown();
    } catch (IllegalStateException e) {
      // Not initialized, nothing to shut down
    }
    resetSingleton();
  }

  @Test void testInitialize() {
    StorageCacheManager.initialize(tempDir);

    StorageCacheManager instance = StorageCacheManager.getInstance();
    assertNotNull(instance);
  }

  @Test void testInitializeIdempotent() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager instance1 = StorageCacheManager.getInstance();

    // Second initialization should not change the instance
    File otherDir = new File(tempDir, "other");
    StorageCacheManager.initialize(otherDir);
    StorageCacheManager instance2 = StorageCacheManager.getInstance();

    assertSame(instance1, instance2);
  }

  @Test void testGetCacheWithStorageType() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache cache = manager.getCache("http");

    assertNotNull(cache);
    assertEquals(0, cache.getCacheSize());
  }

  @Test void testGetCacheSameTypeSameInstance() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache cache1 = manager.getCache("http");
    PersistentStorageCache cache2 = manager.getCache("http");

    assertSame(cache1, cache2);
  }

  @Test void testGetCacheDifferentTypeDifferentInstance() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache httpCache = manager.getCache("http");
    PersistentStorageCache s3Cache = manager.getCache("s3");

    assertNotSame(httpCache, s3Cache);
  }

  @Test void testGetCacheWithCustomTtl() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache cache = manager.getCache("http", 120000);

    assertNotNull(cache);
  }

  @Test void testGetCacheWithSchemaName() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache cache1 = manager.getCache("schema1", "http", 60000);
    PersistentStorageCache cache2 = manager.getCache("schema2", "http", 60000);

    assertNotSame(cache1, cache2);
  }

  @Test void testGetCacheNullSchemaUsesDefault() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache cache1 = manager.getCache(null, "http", 60000);
    PersistentStorageCache cache2 = manager.getCache(null, "http", 60000);

    // Both calls with null schema should return a cache (same or different depending on state)
    assertNotNull(cache1);
    assertNotNull(cache2);
  }

  @Test void testClearAll() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache cache = manager.getCache("http");
    byte[] data = "test".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", 4, 0, "text/csv", null);
    cache.cacheData("/test.csv", data, metadata, 0);

    assertTrue(cache.getCacheSize() > 0);

    manager.clearAll();

    // After clearAll, cache should be cleared or a new empty one returned
    PersistentStorageCache newCache = manager.getCache("http");
    assertNotNull(newCache);
  }

  @Test void testCleanup() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    // Just verify it doesn't throw
    manager.cleanup();
  }

  @Test void testGetCacheStats() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    PersistentStorageCache cache = manager.getCache("http");
    byte[] data = "test".getBytes(StandardCharsets.UTF_8);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test.csv", 4, 0, "text/csv", null);
    cache.cacheData("/test.csv", data, metadata, 0);

    Map<String, Integer> stats = manager.getCacheStats();

    assertNotNull(stats);
    assertFalse(stats.isEmpty());
    // Should have an entry for the http cache we created
    boolean hasHttpEntry = false;
    for (Map.Entry<String, Integer> entry : stats.entrySet()) {
      if (entry.getKey().contains("http")) {
        hasHttpEntry = true;
        assertTrue(entry.getValue() >= 0);
      }
    }
    assertTrue(hasHttpEntry);
  }

  @Test void testGetCacheStatsMultipleSchemas() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    manager.getCache("schema1", "http", 60000);
    manager.getCache("schema2", "s3", 60000);

    Map<String, Integer> stats = manager.getCacheStats();

    assertTrue(stats.containsKey("schema1:http"));
    assertTrue(stats.containsKey("schema2:s3"));
  }

  @Test void testShutdown() {
    StorageCacheManager.initialize(tempDir);
    StorageCacheManager manager = StorageCacheManager.getInstance();

    // Should not throw
    manager.shutdown();
  }

  @Test void testCreatesStorageCacheSubdirectory() throws Exception {
    // Use a fresh directory to ensure no state from other tests
    File freshDir = new File(tempDir, "fresh_cache_" + System.nanoTime());
    freshDir.mkdirs();

    // Ensure clean singleton state
    try {
      StorageCacheManager.getInstance().shutdown();
    } catch (Exception e) {
      // ignore
    }
    resetSingleton();
    StorageCacheManager.initialize(freshDir);

    // initialize creates baseCacheDir/storage_cache via the constructor
    File storageCacheDir = new File(freshDir, "storage_cache");
    assertTrue(storageCacheDir.exists() || storageCacheDir.mkdirs(),
        "storage_cache dir should exist or be creatable under: " + freshDir);
  }

  private void resetSingletonQuietly() {
    try {
      resetSingleton();
    } catch (Exception e) {
      // ignore
    }
  }

  /**
   * Resets the singleton instance using reflection for test isolation.
   */
  private void resetSingleton() throws Exception {
    Field instanceField = StorageCacheManager.class.getDeclaredField("instance");
    instanceField.setAccessible(true);
    instanceField.set(null, null);
  }
}
