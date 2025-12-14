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

import java.util.Map;
import java.util.Set;

/**
 * Interface for tracking incremental processing of alternate partitions.
 *
 * <p>This allows alternate partitions to be built incrementally - only processing
 * new values of incremental keys (e.g., new years) rather than rebuilding everything.
 *
 * <p>Implementations should persist tracking state across restarts.
 */
public interface IncrementalTracker {

  /**
   * Checks if a specific combination of incremental key values has been processed.
   *
   * @param alternateName The alternate partition name (e.g., "regional_income_by_fips")
   * @param sourceTable The source table name (e.g., "regional_income")
   * @param keyValues Map of incremental key names to values (e.g., {"year": "2020"})
   * @return true if this combination has been successfully processed
   */
  boolean isProcessed(String alternateName, String sourceTable, Map<String, String> keyValues);

  /**
   * Marks a combination of incremental key values as successfully processed.
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param keyValues Map of incremental key names to values
   * @param targetPattern The target pattern used for this combination
   */
  void markProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern);

  /**
   * Gets all processed key value combinations for an alternate partition.
   *
   * @param alternateName The alternate partition name
   * @return Set of processed key value maps, or empty set if none
   */
  Set<Map<String, String>> getProcessedKeyValues(String alternateName);

  /**
   * Invalidates (removes) a processed combination, forcing reprocessing.
   *
   * @param alternateName The alternate partition name
   * @param keyValues Map of incremental key names to values to invalidate
   */
  void invalidate(String alternateName, Map<String, String> keyValues);

  /**
   * Invalidates all processed combinations for an alternate partition.
   *
   * @param alternateName The alternate partition name
   */
  void invalidateAll(String alternateName);

  /**
   * A no-op tracker that always returns false (forces full rebuild).
   * Use when incremental tracking is not available.
   */
  IncrementalTracker NOOP = new IncrementalTracker() {
    @Override
    public boolean isProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues) {
      return false;
    }

    @Override
    public void markProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern) {
      // No-op
    }

    @Override
    public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
      return java.util.Collections.emptySet();
    }

    @Override
    public void invalidate(String alternateName, Map<String, String> keyValues) {
      // No-op
    }

    @Override
    public void invalidateAll(String alternateName) {
      // No-op
    }
  };
}
