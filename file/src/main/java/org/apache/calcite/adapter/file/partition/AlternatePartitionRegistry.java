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
package org.apache.calcite.adapter.file.partition;

import org.apache.calcite.adapter.file.partition.PartitionedTableConfig.AlternatePartitionConfig;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig.ColumnDefinition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for tracking alternate partition tables and their relationships to source tables.
 *
 * <p>This registry maintains the mapping between:
 * <ul>
 *   <li>Source tables and their alternate partition layouts</li>
 *   <li>Alternate table names (e.g., _mv_xxx) and their metadata</li>
 *   <li>Partition keys for each alternate layout</li>
 * </ul>
 *
 * <p>The registry is populated during schema initialization when processing
 * partition_alternates configuration and is used by AlternatePartitionSelectionRule
 * to find the optimal partition layout for queries.
 */
public class AlternatePartitionRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlternatePartitionRegistry.class);

  /** Map from source table name to list of alternate partition info */
  private final Map<String, List<AlternateInfo>> sourceToAlternates = new ConcurrentHashMap<String, List<AlternateInfo>>();

  /** Map from alternate table name to its info */
  private final Map<String, AlternateInfo> alternateByName = new ConcurrentHashMap<String, AlternateInfo>();

  /**
   * Holds information about an alternate partition layout.
   */
  public static class AlternateInfo {
    private final String alternateName;
    private final String sourceTableName;
    private final String pattern;
    private final List<String> partitionKeys;
    private final String icebergTableId;
    private final AlternatePartitionConfig config;
    private volatile boolean materialized;

    public AlternateInfo(String alternateName, String sourceTableName, String pattern,
        List<String> partitionKeys, String icebergTableId, AlternatePartitionConfig config) {
      this.alternateName = alternateName;
      this.sourceTableName = sourceTableName;
      this.pattern = pattern;
      this.partitionKeys = partitionKeys != null
          ? Collections.unmodifiableList(new ArrayList<String>(partitionKeys))
          : Collections.<String>emptyList();
      this.icebergTableId = icebergTableId;
      this.config = config;
      this.materialized = false;
    }

    public String getAlternateName() {
      return alternateName;
    }

    public String getSourceTableName() {
      return sourceTableName;
    }

    public String getPattern() {
      return pattern;
    }

    public List<String> getPartitionKeys() {
      return partitionKeys;
    }

    public String getIcebergTableId() {
      return icebergTableId;
    }

    public AlternatePartitionConfig getConfig() {
      return config;
    }

    public boolean isMaterialized() {
      return materialized;
    }

    public void setMaterialized(boolean materialized) {
      this.materialized = materialized;
    }

    /**
     * Checks if this alternate's partition keys cover all the given filter columns.
     *
     * @param filterColumns Set of column names being filtered on
     * @return true if all filter columns are partition keys in this alternate
     */
    public boolean coversFilters(Set<String> filterColumns) {
      if (filterColumns == null || filterColumns.isEmpty()) {
        return false;
      }
      Set<String> partKeySet = new LinkedHashSet<String>();
      for (String key : partitionKeys) {
        partKeySet.add(key.toLowerCase());
      }
      for (String filterCol : filterColumns) {
        if (!partKeySet.contains(filterCol.toLowerCase())) {
          return false;
        }
      }
      return true;
    }

    @Override public String toString() {
      return String.format("AlternateInfo{name=%s, source=%s, partitionKeys=%s, materialized=%s}",
          alternateName, sourceTableName, partitionKeys, materialized);
    }
  }

  /**
   * Registers an alternate partition for a source table.
   *
   * @param sourceTableName The source table name
   * @param alternateName The alternate table name (typically _mv_xxx)
   * @param pattern The file pattern for the alternate
   * @param partitionKeys List of partition key column names
   * @param icebergTableId Optional Iceberg table identifier
   * @param config The original configuration
   */
  public void register(String sourceTableName, String alternateName, String pattern,
      List<String> partitionKeys, String icebergTableId, AlternatePartitionConfig config) {
    AlternateInfo info =
        new AlternateInfo(alternateName, sourceTableName, pattern, partitionKeys, icebergTableId, config);

    alternateByName.put(alternateName, info);

    synchronized (sourceToAlternates) {
      List<AlternateInfo> list = sourceToAlternates.get(sourceTableName);
      if (list == null) {
        list = new ArrayList<AlternateInfo>();
        sourceToAlternates.put(sourceTableName, list);
      }
      // Remove any existing registration with the same name
      list.removeIf(existing -> existing.getAlternateName().equals(alternateName));
      list.add(info);
    }

    LOGGER.debug("Registered alternate partition: {} -> {} (partitionKeys={})",
        sourceTableName, alternateName, partitionKeys);
  }

  /**
   * Registers an alternate from a configuration object.
   *
   * @param sourceTableName The source table name
   * @param config The alternate partition configuration
   * @param icebergTableId Optional Iceberg table identifier
   */
  public void registerFromConfig(String sourceTableName, AlternatePartitionConfig config,
      String icebergTableId) {
    List<String> partitionKeys = new ArrayList<String>();
    if (config.getPartition() != null && config.getPartition().getColumnDefinitions() != null) {
      for (ColumnDefinition colDef : config.getPartition().getColumnDefinitions()) {
        partitionKeys.add(colDef.getName());
      }
    }

    register(sourceTableName, config.getName(), config.getPattern(),
        partitionKeys, icebergTableId, config);
  }

  /**
   * Gets all alternate partitions for a source table.
   *
   * @param sourceTableName The source table name
   * @return List of alternate info objects, or empty list if none
   */
  public List<AlternateInfo> getAlternates(String sourceTableName) {
    List<AlternateInfo> list = sourceToAlternates.get(sourceTableName);
    return list != null ? Collections.unmodifiableList(list) : Collections.<AlternateInfo>emptyList();
  }

  /**
   * Gets alternate info by name.
   *
   * @param alternateName The alternate table name
   * @return AlternateInfo or null if not found
   */
  public AlternateInfo getByName(String alternateName) {
    return alternateByName.get(alternateName);
  }

  /**
   * Finds the best alternate partition for a set of filter columns.
   * Uses the "fewest keys that cover all filters" heuristic.
   *
   * @param sourceTableName The source table name
   * @param filterColumns Set of column names being filtered on
   * @return The best alternate info, or null if none qualifies
   */
  public AlternateInfo findBestAlternate(String sourceTableName, Set<String> filterColumns) {
    if (filterColumns == null || filterColumns.isEmpty()) {
      return null;
    }

    List<AlternateInfo> alternates = getAlternates(sourceTableName);
    if (alternates.isEmpty()) {
      return null;
    }

    AlternateInfo best = null;
    int bestKeyCount = Integer.MAX_VALUE;

    for (AlternateInfo info : alternates) {
      // Only consider materialized alternates
      if (!info.isMaterialized()) {
        LOGGER.debug("Skipping non-materialized alternate: {}", info.getAlternateName());
        continue;
      }

      // Check if all filter columns are covered by partition keys
      if (info.coversFilters(filterColumns)) {
        int keyCount = info.getPartitionKeys().size();
        // Prefer fewer partition keys (more consolidated data)
        if (keyCount < bestKeyCount) {
          bestKeyCount = keyCount;
          best = info;
        }
      }
    }

    if (best != null) {
      LOGGER.debug("Found best alternate for {} with filters {}: {} (keyCount={})",
          sourceTableName, filterColumns, best.getAlternateName(), bestKeyCount);
    }

    return best;
  }

  /**
   * Marks an alternate as materialized (data has been written).
   *
   * @param alternateName The alternate table name
   */
  public void markMaterialized(String alternateName) {
    AlternateInfo info = alternateByName.get(alternateName);
    if (info != null) {
      info.setMaterialized(true);
      LOGGER.debug("Marked alternate as materialized: {}", alternateName);
    }
  }

  /**
   * Checks if an alternate is materialized.
   *
   * @param alternateName The alternate table name
   * @return true if materialized
   */
  public boolean isMaterialized(String alternateName) {
    AlternateInfo info = alternateByName.get(alternateName);
    return info != null && info.isMaterialized();
  }

  /**
   * Gets all registered source table names.
   *
   * @return Set of source table names
   */
  public Set<String> getSourceTables() {
    return Collections.unmodifiableSet(sourceToAlternates.keySet());
  }

  /**
   * Gets all registered alternate names.
   *
   * @return Set of alternate table names
   */
  public Set<String> getAlternateNames() {
    return Collections.unmodifiableSet(alternateByName.keySet());
  }

  /**
   * Removes an alternate registration.
   *
   * @param alternateName The alternate table name to remove
   */
  public void unregister(String alternateName) {
    AlternateInfo removed = alternateByName.remove(alternateName);
    if (removed != null) {
      synchronized (sourceToAlternates) {
        List<AlternateInfo> list = sourceToAlternates.get(removed.getSourceTableName());
        if (list != null) {
          list.removeIf(info -> info.getAlternateName().equals(alternateName));
          if (list.isEmpty()) {
            sourceToAlternates.remove(removed.getSourceTableName());
          }
        }
      }
      LOGGER.debug("Unregistered alternate partition: {}", alternateName);
    }
  }

  /**
   * Clears all registrations.
   */
  public void clear() {
    alternateByName.clear();
    sourceToAlternates.clear();
    LOGGER.debug("Cleared alternate partition registry");
  }

  /**
   * Returns the number of registered alternates.
   *
   * @return Count of registered alternates
   */
  public int size() {
    return alternateByName.size();
  }

  @Override public String toString() {
    return String.format("AlternatePartitionRegistry{sources=%d, alternates=%d}",
        sourceToAlternates.size(), alternateByName.size());
  }
}
