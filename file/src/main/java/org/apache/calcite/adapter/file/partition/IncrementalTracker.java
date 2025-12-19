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

import java.util.HashSet;
import java.util.List;
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

  // ===== Bulk Filtering Methods =====

  /**
   * Filters a list of dimension combinations to return only unprocessed ones.
   * This is more efficient than calling isProcessed() per combination.
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param allCombinations All dimension combinations to check
   * @return Set of combination indices that have NOT been processed
   */
  Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations);

  // ===== Table Completion Tracking =====

  /**
   * Checks if the entire pipeline was completed with the same dimension signature.
   * Used for fast-path skipping when dimensions haven't changed.
   *
   * @param pipelineName The pipeline name
   * @param dimensionSignature Hash of all dimension values
   * @return true if table was fully processed with same signature
   */
  boolean isTableComplete(String pipelineName, String dimensionSignature);

  /**
   * Marks a pipeline as complete after all combinations were processed.
   *
   * @param pipelineName The pipeline name
   * @param dimensionSignature Hash of all dimension values
   */
  void markTableComplete(String pipelineName, String dimensionSignature);

  /**
   * Invalidates table completion status, forcing reprocessing.
   *
   * @param pipelineName The pipeline name
   */
  void invalidateTableCompletion(String pipelineName);

  // ===== Dimension Signature Computation =====

  /**
   * Computes a signature for a list of dimension combinations.
   * The signature changes when dimension values change.
   *
   * @param dimensions List of dimension configurations
   * @param combinations All expanded combinations
   * @return Signature string for comparison
   */
  static String computeDimensionSignature(List<Map<String, String>> combinations) {
    if (combinations == null || combinations.isEmpty()) {
      return "empty";
    }
    // Build signature from count and sorted dimension keys
    StringBuilder sb = new StringBuilder();
    sb.append("count:").append(combinations.size());
    if (!combinations.isEmpty()) {
      Map<String, String> first = combinations.get(0);
      java.util.List<String> keys = new java.util.ArrayList<>(first.keySet());
      java.util.Collections.sort(keys);
      for (String key : keys) {
        sb.append("|").append(key);
      }
    }
    // Add hash of all values for change detection
    int hash = 0;
    for (Map<String, String> combo : combinations) {
      for (Map.Entry<String, String> entry : combo.entrySet()) {
        hash = 31 * hash + entry.getKey().hashCode();
        hash = 31 * hash + (entry.getValue() != null ? entry.getValue().hashCode() : 0);
      }
    }
    sb.append("|hash:").append(Integer.toHexString(hash));
    return sb.toString();
  }

  /**
   * A no-op tracker that always returns false (forces full rebuild).
   * Use when incremental tracking is not available.
   */
  IncrementalTracker NOOP = new IncrementalTracker() {
    @Override public boolean isProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues) {
      return false;
    }

    @Override public void markProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern) {
      // No-op
    }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
      return java.util.Collections.emptySet();
    }

    @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
      // No-op
    }

    @Override public void invalidateAll(String alternateName) {
      // No-op
    }

    @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
        List<Map<String, String>> allCombinations) {
      // NOOP returns all indices as unprocessed
      Set<Integer> all = new HashSet<>();
      for (int i = 0; i < allCombinations.size(); i++) {
        all.add(i);
      }
      return all;
    }

    @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
      return false;
    }

    @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
      // No-op
    }

    @Override public void invalidateTableCompletion(String pipelineName) {
      // No-op
    }
  };
}
