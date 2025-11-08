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
package org.apache.calcite.adapter.file.similarity;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Persistent embedding cache backed by parquet files.
 *
 * <p>Stores embeddings in parquet format for persistence across restarts.
 * Maintains an in-memory cache for fast lookups during execution.
 * Ensures each unique text string is embedded only once.
 *
 * <p>Thread-safe for concurrent access.
 */
public class PersistentEmbeddingCache {
  private static final Logger logger = LoggerFactory.getLogger(PersistentEmbeddingCache.class);

  private final StorageProvider storageProvider;
  private final String cachePath;
  private final String providerModel;
  private final int maxMemoryCacheSize;

  // In-memory cache: text_hash -> embedding
  private final Map<String, double[]> memoryCache;

  // Track new entries that need to be flushed
  private final Map<String, CacheEntry> pendingFlush;

  // Read-write lock for thread safety
  private final ReadWriteLock lock;

  // Cache statistics
  private long cacheHits;
  private long cacheMisses;

  /**
   * Create a persistent embedding cache.
   *
   * @param storageProvider Storage provider for parquet I/O
   * @param cachePath Path to cache parquet file
   * @param providerModel Model identifier (e.g., "onnx-minilm-v6")
   */
  public PersistentEmbeddingCache(StorageProvider storageProvider,
                                  String cachePath,
                                  String providerModel) {
    this(storageProvider, cachePath, providerModel, 100_000);
  }

  /**
   * Create a persistent embedding cache with custom max size.
   *
   * @param storageProvider Storage provider for parquet I/O
   * @param cachePath Path to cache parquet file
   * @param providerModel Model identifier
   * @param maxMemoryCacheSize Maximum entries in memory cache (LRU eviction)
   */
  public PersistentEmbeddingCache(StorageProvider storageProvider,
                                  String cachePath,
                                  String providerModel,
                                  int maxMemoryCacheSize) {
    this.storageProvider = storageProvider;
    this.cachePath = cachePath;
    this.providerModel = providerModel != null ? providerModel : "unknown";
    this.maxMemoryCacheSize = maxMemoryCacheSize;
    this.memoryCache = new ConcurrentHashMap<>();
    this.pendingFlush = new ConcurrentHashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.cacheHits = 0;
    this.cacheMisses = 0;

    // Load existing cache from storage
    loadFromStorage();
  }

  /**
   * Get cached embedding for text.
   *
   * @param text Input text
   * @return Embedding array, or null if not cached
   */
  public double[] get(String text) {
    if (text == null || text.trim().isEmpty()) {
      return null;
    }

    String textHash = computeHash(text);

    lock.readLock().lock();
    try {
      double[] embedding = memoryCache.get(textHash);
      if (embedding != null) {
        cacheHits++;
        return embedding;
      } else {
        cacheMisses++;
        return null;
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Put embedding into cache.
   *
   * @param text Input text
   * @param embedding Embedding vector
   */
  public void put(String text, double[] embedding) {
    if (text == null || text.trim().isEmpty() || embedding == null) {
      return;
    }

    String textHash = computeHash(text);
    long timestamp = System.currentTimeMillis();

    lock.writeLock().lock();
    try {
      // Add to memory cache
      memoryCache.put(textHash, embedding);

      // Track for flushing
      String truncatedText = text.length() > 1000 ? text.substring(0, 1000) : text;
      pendingFlush.put(
          textHash, new CacheEntry(textHash, truncatedText, embedding,
                                                 providerModel, timestamp));

      // Check if we need to evict from memory cache (simple size-based eviction)
      if (memoryCache.size() > maxMemoryCacheSize) {
        evictOldestEntries();
      }

      // Auto-flush if we have many pending entries
      if (pendingFlush.size() >= 1000) {
        if (logger.isDebugEnabled()) {
          logger.debug("Auto-flushing cache with {} pending entries", pendingFlush.size());
        }
        flushToStorage();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Check if text has cached embedding.
   *
   * @param text Input text
   * @return true if cached
   */
  public boolean contains(String text) {
    if (text == null || text.trim().isEmpty()) {
      return false;
    }

    String textHash = computeHash(text);
    lock.readLock().lock();
    try {
      return memoryCache.containsKey(textHash);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Flush pending entries to persistent storage.
   */
  public void flushToStorage() {
    lock.writeLock().lock();
    try {
      if (pendingFlush.isEmpty()) {
        return;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Flushing {} embeddings to storage: {}", pendingFlush.size(), cachePath);
      }

      // Convert pending entries to list of maps for storage
      List<Map<String, Object>> records = new ArrayList<>();
      for (CacheEntry entry : pendingFlush.values()) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("text_hash", entry.textHash);
        record.put("text", entry.text);
        record.put("embedding", entry.embedding);
        record.put("provider_model", entry.providerModel);
        record.put("created_timestamp", entry.timestamp);
        records.add(record);
      }

      // Append to parquet file (or create if doesn't exist)
      try {
        storageProvider.appendParquet(cachePath, records, getCacheSchema());
        logger.info("Flushed {} embeddings to cache: {}", records.size(), cachePath);
        pendingFlush.clear();
      } catch (Exception e) {
        logger.warn("Failed to flush embeddings to cache: {}", e.getMessage());
      }

    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get cache statistics.
   *
   * @return Statistics object
   */
  public CacheStats getStats() {
    lock.readLock().lock();
    try {
      return new CacheStats(
          memoryCache.size(),
          cacheHits,
          cacheMisses,
          pendingFlush.size());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Close cache and flush pending entries.
   */
  public void close() {
    flushToStorage();
  }

  /**
   * Load cache from persistent storage into memory.
   */
  private void loadFromStorage() {
    try {
      if (!storageProvider.exists(cachePath)) {
        logger.debug("Cache file does not exist, starting with empty cache: {}", cachePath);
        return;
      }

      logger.debug("Loading embeddings cache from: {}", cachePath);

      // Read all records from parquet
      List<Map<String, Object>> records = storageProvider.readParquet(cachePath);

      int loaded = 0;
      for (Map<String, Object> record : records) {
        String textHash = (String) record.get("text_hash");
        Object embeddingObj = record.get("embedding");

        if (textHash != null && embeddingObj != null) {
          double[] embedding = convertToDoubleArray(embeddingObj);
          if (embedding != null) {
            memoryCache.put(textHash, embedding);
            loaded++;
          }
        }
      }

      logger.info("Loaded {} embeddings from cache: {}", loaded, cachePath);

    } catch (Exception e) {
      logger.warn("Failed to load cache from storage, starting with empty cache: {}",
                  e.getMessage());
    }
  }

  /**
   * Convert object to double array (handles List or array).
   */
  private double[] convertToDoubleArray(Object obj) {
    if (obj instanceof double[]) {
      return (double[]) obj;
    } else if (obj instanceof List) {
      List<?> list = (List<?>) obj;
      double[] array = new double[list.size()];
      for (int i = 0; i < list.size(); i++) {
        Object elem = list.get(i);
        if (elem instanceof Number) {
          array[i] = ((Number) elem).doubleValue();
        }
      }
      return array;
    }
    return null;
  }

  /**
   * Evict oldest entries from memory cache (simple FIFO eviction).
   */
  private void evictOldestEntries() {
    int toRemove = memoryCache.size() - (maxMemoryCacheSize * 3 / 4); // Evict 25%
    if (toRemove <= 0) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Evicting {} entries from memory cache", toRemove);
    }

    int removed = 0;
    for (String key : memoryCache.keySet()) {
      if (removed >= toRemove) {
        break;
      }
      // Don't evict entries pending flush
      if (!pendingFlush.containsKey(key)) {
        memoryCache.remove(key);
        removed++;
      }
    }
  }

  /**
   * Compute SHA-256 hash of text for cache key.
   */
  private String computeHash(String text) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(text.getBytes());

      // Convert to hex string
      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();

    } catch (NoSuchAlgorithmException e) {
      // Fallback to hashCode if SHA-256 unavailable
      return String.valueOf(text.hashCode());
    }
  }

  /**
   * Get schema for cache parquet file.
   */
  private List<Map<String, Object>> getCacheSchema() {
    List<Map<String, Object>> schema = new ArrayList<>();

    Map<String, Object> textHashCol = new HashMap<>();
    textHashCol.put("name", "text_hash");
    textHashCol.put("type", "string");
    schema.add(textHashCol);

    Map<String, Object> textCol = new HashMap<>();
    textCol.put("name", "text");
    textCol.put("type", "string");
    schema.add(textCol);

    Map<String, Object> embeddingCol = new HashMap<>();
    embeddingCol.put("name", "embedding");
    embeddingCol.put("type", "array<double>");
    schema.add(embeddingCol);

    Map<String, Object> providerCol = new HashMap<>();
    providerCol.put("name", "provider_model");
    providerCol.put("type", "string");
    schema.add(providerCol);

    Map<String, Object> timestampCol = new HashMap<>();
    timestampCol.put("name", "created_timestamp");
    timestampCol.put("type", "long");
    schema.add(timestampCol);

    return schema;
  }

  /**
   * Internal cache entry representation.
   */
  private static class CacheEntry {
    final String textHash;
    final String text;
    final double[] embedding;
    final String providerModel;
    final long timestamp;

    CacheEntry(String textHash, String text, double[] embedding,
               String providerModel, long timestamp) {
      this.textHash = textHash;
      this.text = text;
      this.embedding = embedding;
      this.providerModel = providerModel;
      this.timestamp = timestamp;
    }
  }

  /**
   * Cache statistics.
   */
  public static class CacheStats {
    private final int size;
    private final long hits;
    private final long misses;
    private final int pendingFlush;

    public CacheStats(int size, long hits, long misses, int pendingFlush) {
      this.size = size;
      this.hits = hits;
      this.misses = misses;
      this.pendingFlush = pendingFlush;
    }

    public int getSize() {
      return size;
    }

    public long getHits() {
      return hits;
    }

    public long getMisses() {
      return misses;
    }

    public int getPendingFlush() {
      return pendingFlush;
    }

    public double getHitRate() {
      long total = hits + misses;
      return total == 0 ? 0.0 : (double) hits / total;
    }

    @Override public String toString() {
      return String.format("CacheStats{size=%d, hits=%d, misses=%d, hitRate=%.2f%%, pending=%d}",
          size, hits, misses, getHitRate() * 100, pendingFlush);
    }
  }
}
