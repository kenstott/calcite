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

import org.apache.calcite.adapter.file.table.PartitionedParquetTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for partition information that can be looked up by schema/table name.
 * This enables partition-aware optimizations (like PartitionDistinctRule) to work
 * with JDBC/DuckDB tables by looking up the underlying partition info.
 *
 * <p>Similar to HLLSketchCache, this is a singleton that stores partition metadata
 * registered when PartitionedParquetTable instances are created.
 */
public class PartitionInfoRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionInfoRegistry.class);

  private static volatile PartitionInfoRegistry instance;
  private static final Object INSTANCE_LOCK = new Object();

  // Map: schemaName -> (tableName -> PartitionedParquetTable)
  private final Map<String, Map<String, PartitionedParquetTable>> registry =
      new ConcurrentHashMap<>();

  // Case-insensitive index: schemaName -> (lowerTableName -> actualTableName)
  private final Map<String, Map<String, String>> caseIndex = new ConcurrentHashMap<>();

  private PartitionInfoRegistry() {
    LOGGER.debug("PartitionInfoRegistry initialized");
  }

  /**
   * Get the singleton instance.
   */
  public static PartitionInfoRegistry getInstance() {
    if (instance == null) {
      synchronized (INSTANCE_LOCK) {
        if (instance == null) {
          instance = new PartitionInfoRegistry();
        }
      }
    }
    return instance;
  }

  /**
   * Register a partitioned table for later lookup.
   */
  public void register(String schemaName, String tableName, PartitionedParquetTable table) {
    if (schemaName == null || tableName == null || table == null) {
      return;
    }

    Map<String, PartitionedParquetTable> schemaRegistry =
        registry.computeIfAbsent(schemaName, k -> new ConcurrentHashMap<>());
    schemaRegistry.put(tableName, table);

    // Also register in case-insensitive index
    Map<String, String> schemaCaseIndex =
        caseIndex.computeIfAbsent(schemaName, k -> new ConcurrentHashMap<>());
    schemaCaseIndex.put(tableName.toLowerCase(), tableName);

    LOGGER.debug("Registered partition info for {}.{} with {} partition columns",
        schemaName, tableName, table.getPartitionColumns().size());
  }

  /**
   * Look up a partitioned table by schema and table name (case-insensitive).
   */
  public PartitionedParquetTable lookup(String schemaName, String tableName) {
    if (schemaName == null || tableName == null) {
      return null;
    }

    Map<String, PartitionedParquetTable> schemaRegistry = registry.get(schemaName);
    if (schemaRegistry == null) {
      // Try case-insensitive schema lookup
      for (String key : registry.keySet()) {
        if (key.equalsIgnoreCase(schemaName)) {
          schemaRegistry = registry.get(key);
          break;
        }
      }
    }

    if (schemaRegistry == null) {
      return null;
    }

    // Try exact match first
    PartitionedParquetTable table = schemaRegistry.get(tableName);
    if (table != null) {
      return table;
    }

    // Try case-insensitive match
    Map<String, String> schemaCaseIndex = caseIndex.get(schemaName);
    if (schemaCaseIndex != null) {
      String actualName = schemaCaseIndex.get(tableName.toLowerCase());
      if (actualName != null) {
        return schemaRegistry.get(actualName);
      }
    }

    // Last resort: iterate and compare case-insensitively
    for (Map.Entry<String, PartitionedParquetTable> entry : schemaRegistry.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(tableName)) {
        return entry.getValue();
      }
    }

    return null;
  }

  /**
   * Get partition columns for a table.
   */
  public List<String> getPartitionColumns(String schemaName, String tableName) {
    PartitionedParquetTable table = lookup(schemaName, tableName);
    if (table != null) {
      return table.getPartitionColumns();
    }
    return null;
  }

  /**
   * Check if a column is a partition column.
   */
  public boolean isPartitionColumn(String schemaName, String tableName, String columnName) {
    PartitionedParquetTable table = lookup(schemaName, tableName);
    if (table != null) {
      return table.isPartitionColumn(columnName);
    }
    return false;
  }

  /**
   * Get distinct values for a partition column.
   */
  public List<String> getDistinctPartitionValues(String schemaName, String tableName,
      String columnName) {
    PartitionedParquetTable table = lookup(schemaName, tableName);
    if (table != null) {
      return table.getDistinctPartitionValues(columnName);
    }
    return null;
  }

  /**
   * Clear the registry (useful for testing).
   */
  public void clear() {
    registry.clear();
    caseIndex.clear();
    LOGGER.debug("PartitionInfoRegistry cleared");
  }

  /**
   * Clear entries for a specific schema.
   */
  public void clearSchema(String schemaName) {
    registry.remove(schemaName);
    caseIndex.remove(schemaName);
    LOGGER.debug("PartitionInfoRegistry cleared for schema: {}", schemaName);
  }
}
