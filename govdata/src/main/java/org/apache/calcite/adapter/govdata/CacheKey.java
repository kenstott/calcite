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
package org.apache.calcite.adapter.govdata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable value object representing a cache key for govdata operations.
 * Eliminates year as a special concept - year is just another partition parameter.
 *
 * <p>Cache keys consist of a table name and a map of partition parameters
 * (which may include year, frequency, series_id, etc. depending on the table).
 *
 * <p>Instances are created by the iteration framework and passed to operation lambdas
 * to ensure consistent cache key construction between download, conversion, and manifest updates.
 *
 * <p>This class is immutable and thread-safe.
 */
public final class CacheKey {
  private final String tableName;
  private final Map<String, String> parameters;
  private final String keyString;

  /**
   * Public constructor for creating cache keys.
   *
   * @param tableName The table name (e.g., "national_accounts", "series_observations")
   * @param parameters The partition parameters (may be empty, never null)
   */
  public CacheKey(String tableName, Map<String, String> parameters) {
    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException("tableName cannot be null or empty");
    }
    if (parameters == null) {
      throw new IllegalArgumentException("parameters cannot be null (use empty map instead)");
    }

    this.tableName = tableName;
    this.parameters = Collections.unmodifiableMap(new HashMap<>(parameters));
    this.keyString = buildKeyString(tableName, parameters);
  }

  /**
   * Get the cache key as a string suitable for manifest storage.
   * Format: "tableName:param1=value1:param2=value2" (parameters sorted alphabetically)
   *
   * @return The cache key string
   */
  public String asString() {
    return keyString;
  }

  /**
   * Get the table name.
   *
   * @return The table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get an immutable view of the partition parameters.
   *
   * @return The parameters map (immutable)
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * Get a specific parameter value.
   *
   * @param key The parameter key (e.g., "year", "frequency")
   * @return The parameter value, or null if not present
   */
  public String getParameter(String key) {
    return parameters.get(key);
  }

  /**
   * Build cache key string from table name and parameters.
   * Uses same logic as CacheManifest.buildKey() for consistency.
   *
   * @param tableName The table name
   * @param parameters The partition parameters
   * @return The cache key string
   */
  private static String buildKeyString(String tableName, Map<String, String> parameters) {
    StringBuilder key = new StringBuilder();
    key.append(tableName);

    if (!parameters.isEmpty()) {
      parameters.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(entry -> key.append(":").append(entry.getKey()).append("=").append(entry.getValue()));
    }

    return key.toString();
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CacheKey cacheKey = (CacheKey) o;
    return keyString.equals(cacheKey.keyString);
  }

  @Override public int hashCode() {
    return keyString.hashCode();
  }

  @Override public String toString() {
    return "CacheKey{" + keyString + "}";
  }
}
