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
package org.apache.calcite.mcp.cache;

import com.google.gson.JsonElement;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple LRU cache for query results.
 *
 * <p>Caches query results to improve performance for repeated queries.
 * Uses a size-limited LRU eviction policy.
 */
public class QueryCache {
  private final int maxSize;
  private final long ttlMillis;
  private final Map<String, CacheEntry> cache;

  public QueryCache(int maxSize, long ttlMillis) {
    this.maxSize = maxSize;
    this.ttlMillis = ttlMillis;
    this.cache = new LinkedHashMap<String, CacheEntry>(maxSize + 1, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
        return size() > maxSize;
      }
    };
  }

  /**
   * Get cached result for a query.
   *
   * @param key Cache key (usually SQL query + parameters)
   * @return Cached result or null if not found or expired
   */
  public synchronized JsonElement get(String key) {
    CacheEntry entry = cache.get(key);
    if (entry == null) {
      return null;
    }

    // Check if expired
    if (System.currentTimeMillis() - entry.timestamp > ttlMillis) {
      cache.remove(key);
      return null;
    }

    return entry.result;
  }

  /**
   * Put result in cache.
   *
   * @param key Cache key
   * @param result Query result to cache
   */
  public synchronized void put(String key, JsonElement result) {
    cache.put(key, new CacheEntry(result, System.currentTimeMillis()));
  }

  /**
   * Clear all cached entries.
   */
  public synchronized void clear() {
    cache.clear();
  }

  /**
   * Get cache statistics.
   *
   * @return Map with cache stats (size, maxSize, etc.)
   */
  public synchronized Map<String, Object> getStats() {
    Map<String, Object> stats = new java.util.HashMap<>();
    stats.put("size", cache.size());
    stats.put("maxSize", maxSize);
    stats.put("ttlMillis", ttlMillis);
    return stats;
  }

  /**
   * Cache entry with timestamp.
   */
  private static class CacheEntry {
    final JsonElement result;
    final long timestamp;

    CacheEntry(JsonElement result, long timestamp) {
      this.result = result;
      this.timestamp = timestamp;
    }
  }
}
