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
package org.apache.calcite.adapter.govdata.econ;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cache manifest for tracking downloaded economic data to improve startup performance.
 * Maintains metadata about cached files to avoid redundant downloads.
 */
public class CacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheManifest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MANIFEST_FILENAME = "cache_manifest.json";
  
  // Default cache TTL - 24 hours for most economic data
  private static final long DEFAULT_TTL_HOURS = 24;
  private static final long CURRENT_YEAR_TTL_HOURS = 24;  // Current year data expires daily
  private static final long HISTORICAL_TTL_HOURS = Long.MAX_VALUE / (1000 * 60 * 60);  // Prior year data never expires (immutable)
  
  @JsonProperty("entries")
  private Map<String, CacheEntry> entries = new HashMap<>();
  
  @JsonProperty("version")
  private String version = "1.0";
  
  @JsonProperty("lastUpdated")
  private long lastUpdated = System.currentTimeMillis();
  
  /**
   * Check if data is cached and fresh for the given parameters.
   */
  public boolean isCached(String dataType, int year, Map<String, String> parameters) {
    String key = buildKey(dataType, year, parameters);
    CacheEntry entry = entries.get(key);
    
    if (entry == null) {
      return false;
    }
    
    // Check if file still exists
    if (!new File(entry.filePath).exists()) {
      entries.remove(key);
      return false;
    }
    
    // Check if entry is stale - use different TTL based on whether it's current year
    int currentYear = java.time.LocalDate.now().getYear();
    long ttlHours = (year == currentYear) ? CURRENT_YEAR_TTL_HOURS : HISTORICAL_TTL_HOURS;

    if (isStale(entry, ttlHours)) {
      LOGGER.debug("Cache entry is stale for {} year={} (age exceeds {} hours)", dataType, year, ttlHours);
      entries.remove(key);
      return false;
    }

    // Log when using cached data, especially for current year
    long ageHours = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis() - entry.cachedAt);
    if (year == currentYear) {
      LOGGER.info("Using cached {} data for current year {} (age: {} hours, will refresh after {} hours)",
          dataType, year, ageHours, ttlHours);
    } else {
      LOGGER.debug("Using cached {} data for year {} (age: {} hours)", dataType, year, ageHours);
    }

    return true;
  }
  
  /**
   * Mark data as cached with metadata.
   */
  public void markCached(String dataType, int year, Map<String, String> parameters, 
                        String filePath, long fileSize) {
    String key = buildKey(dataType, year, parameters);
    CacheEntry entry = new CacheEntry();
    entry.dataType = dataType;
    entry.year = year;
    entry.parameters = new HashMap<>(parameters != null ? parameters : new HashMap<>());
    entry.filePath = filePath;
    entry.fileSize = fileSize;
    entry.cachedAt = System.currentTimeMillis();
    
    entries.put(key, entry);
    lastUpdated = System.currentTimeMillis();
    
    LOGGER.debug("Marked as cached: {} (year={}, size={})", dataType, year, fileSize);
  }
  
  /**
   * Check if a cache entry is stale based on TTL.
   */
  public boolean isStale(CacheEntry entry, long maxAgeHours) {
    long ageMs = System.currentTimeMillis() - entry.cachedAt;
    long maxAgeMs = TimeUnit.HOURS.toMillis(maxAgeHours);
    return ageMs > maxAgeMs;
  }
  
  /**
   * Remove stale entries from the manifest.
   */
  public int cleanupStaleEntries() {
    int removed = 0;
    entries.entrySet().removeIf(entry -> {
      CacheEntry cacheEntry = entry.getValue();
      
      // Remove if file doesn't exist
      if (!new File(cacheEntry.filePath).exists()) {
        LOGGER.debug("Removing cache entry for missing file: {}", cacheEntry.filePath);
        return true;
      }
      
      // Remove if stale - use different TTL for current year vs historical data
      int currentYear = java.time.LocalDate.now().getYear();
      long ttlHours = (cacheEntry.year == currentYear) ? CURRENT_YEAR_TTL_HOURS : HISTORICAL_TTL_HOURS;
      if (isStale(cacheEntry, ttlHours)) {
        LOGGER.debug("Removing stale cache entry: {} year={} (age: {} hours, TTL: {} hours)",
                    cacheEntry.dataType, cacheEntry.year,
                    TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis() - cacheEntry.cachedAt),
                    ttlHours);
        return true;
      }
      
      return false;
    });
    
    if (removed > 0) {
      lastUpdated = System.currentTimeMillis();
      LOGGER.info("Cleaned up {} stale cache entries", removed);
    }
    
    return removed;
  }
  
  /**
   * Load manifest from file.
   */
  public static CacheManifest load(String cacheDir) {
    File manifestFile = new File(cacheDir, MANIFEST_FILENAME);
    
    if (!manifestFile.exists()) {
      LOGGER.debug("No cache manifest found, creating new one");
      return new CacheManifest();
    }
    
    try {
      CacheManifest manifest = MAPPER.readValue(manifestFile, CacheManifest.class);
      LOGGER.debug("Loaded cache manifest with {} entries", manifest.entries.size());
      return manifest;
    } catch (IOException e) {
      LOGGER.warn("Failed to load cache manifest, creating new one: {}", e.getMessage());
      return new CacheManifest();
    }
  }
  
  /**
   * Save manifest to file.
   */
  public void save(String cacheDir) {
    File manifestFile = new File(cacheDir, MANIFEST_FILENAME);
    
    try {
      // Ensure directory exists
      manifestFile.getParentFile().mkdirs();
      
      // Clean up before saving
      cleanupStaleEntries();
      
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(manifestFile, this);
      LOGGER.debug("Saved cache manifest with {} entries", entries.size());
    } catch (IOException e) {
      LOGGER.warn("Failed to save cache manifest: {}", e.getMessage());
    }
  }
  
  /**
   * Build cache key from parameters.
   */
  private String buildKey(String dataType, int year, Map<String, String> parameters) {
    StringBuilder key = new StringBuilder();
    key.append(dataType).append(":").append(year);
    
    if (parameters != null && !parameters.isEmpty()) {
      parameters.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(entry -> key.append(":").append(entry.getKey()).append("=").append(entry.getValue()));
    }
    
    return key.toString();
  }
  
  /**
   * Get cache statistics.
   */
  @JsonIgnore
  public CacheStats getStats() {
    CacheStats stats = new CacheStats();
    stats.totalEntries = entries.size();
    stats.freshEntries = (int) entries.values().stream()
        .filter(entry -> !isStale(entry, DEFAULT_TTL_HOURS))
        .count();
    stats.staleEntries = stats.totalEntries - stats.freshEntries;
    
    return stats;
  }
  
  /**
   * Cache entry metadata.
   */
  public static class CacheEntry {
    @JsonProperty("dataType")
    public String dataType;
    
    @JsonProperty("year")
    public int year;
    
    @JsonProperty("parameters")
    public Map<String, String> parameters = new HashMap<>();
    
    @JsonProperty("filePath")
    public String filePath;
    
    @JsonProperty("fileSize")
    public long fileSize;
    
    @JsonProperty("cachedAt")
    public long cachedAt;
  }
  
  /**
   * Cache statistics.
   */
  public static class CacheStats {
    public int totalEntries;
    public int freshEntries;
    public int staleEntries;
    
    @Override
    public String toString() {
      return String.format("Cache stats: %d total, %d fresh, %d stale", 
                          totalEntries, freshEntries, staleEntries);
    }
  }
}