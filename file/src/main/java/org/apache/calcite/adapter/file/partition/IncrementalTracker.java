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
   * Checks if a specific combination has been processed within the TTL window.
   *
   * <p>This is useful for incremental keys like "current year" that should be
   * reprocessed periodically to pick up new data, rather than being permanently
   * marked as processed.
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param keyValues Map of incremental key names to values
   * @param ttlMillis Time-to-live in milliseconds. If the entry was processed more than
   *                  ttlMillis ago, it's considered expired and returns false.
   * @return true if this combination was processed within the TTL window
   */
  boolean isProcessedWithTtl(String alternateName, String sourceTable,
      Map<String, String> keyValues, long ttlMillis);

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
   * Marks a combination as processed with a specific row count.
   *
   * <p>When rowCount is 0 (empty result), the entry will be subject to TTL-based
   * requerying. When rowCount > 0, the entry is permanently marked as processed.
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param keyValues Map of incremental key names to values
   * @param targetPattern The target pattern used for this combination
   * @param rowCount Number of rows written (0 = empty, will be requeried after TTL)
   */
  default void markProcessedWithRowCount(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, long rowCount) {
    // Default implementation ignores row count for backward compatibility
    markProcessed(alternateName, sourceTable, keyValues, targetPattern);
  }

  /**
   * Checks if a combination needs reprocessing due to empty result TTL expiry.
   *
   * <p>This is specifically for empty results (row_count=0) that should be
   * requeried after a configured interval to check if data became available.
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param keyValues Map of incremental key names to values
   * @param emptyResultTtlMillis TTL for empty results in milliseconds
   * @return true if the combination was processed with data OR empty result is within TTL
   */
  default boolean isProcessedWithEmptyTtl(String alternateName, String sourceTable,
      Map<String, String> keyValues, long emptyResultTtlMillis) {
    // Default: fall back to simple isProcessed check
    return isProcessed(alternateName, sourceTable, keyValues);
  }

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

  /**
   * Filters combinations considering empty result TTL.
   *
   * <p>A combination is considered "needing processing" if:
   * <ul>
   *   <li>It was never processed, OR</li>
   *   <li>It was processed but returned 0 rows AND the TTL has expired</li>
   * </ul>
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param allCombinations All dimension combinations to check
   * @param emptyResultTtlMillis TTL for empty results - requery after this interval
   * @return Set of combination indices that need processing
   */
  default Set<Integer> filterUnprocessedWithEmptyTtl(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations, long emptyResultTtlMillis) {
    // Default: fall back to regular filterUnprocessed (ignores TTL)
    return filterUnprocessed(alternateName, sourceTable, allCombinations);
  }

  // ===== Error Tracking Methods =====

  /**
   * Marks a combination as processed with an error.
   *
   * <p>Error entries are subject to a separate (typically shorter) TTL than empty results,
   * allowing the system to retry failed requests periodically.
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param keyValues Map of incremental key names to values
   * @param targetPattern The target pattern used for this combination
   * @param errorMessage Optional error message for debugging
   */
  default void markProcessedWithError(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, String errorMessage) {
    // Default implementation treats errors same as empty results
    markProcessedWithRowCount(alternateName, sourceTable, keyValues, targetPattern, 0);
  }

  /**
   * Filters combinations considering both empty result TTL and error TTL.
   *
   * <p>A combination is considered "needing processing" if:
   * <ul>
   *   <li>It was never processed, OR</li>
   *   <li>It was processed with an error AND the error TTL has expired, OR</li>
   *   <li>It was processed with 0 rows AND the empty result TTL has expired</li>
   * </ul>
   *
   * @param alternateName The alternate partition name
   * @param sourceTable The source table name
   * @param allCombinations All dimension combinations to check
   * @param emptyResultTtlMillis TTL for empty results - requery after this interval
   * @param errorTtlMillis TTL for errors - retry after this interval (typically shorter)
   * @return Set of combination indices that need processing
   */
  default Set<Integer> filterUnprocessedWithTtl(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations, long emptyResultTtlMillis, long errorTtlMillis) {
    // Default: fall back to empty TTL filter (ignores error TTL)
    return filterUnprocessedWithEmptyTtl(alternateName, sourceTable, allCombinations, emptyResultTtlMillis);
  }

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

  /**
   * Clears ALL completion tracking state, forcing a complete fresh start.
   *
   * <p>This method removes all entries from both:
   * <ul>
   *   <li>partition_status - individual batch/combination tracking</li>
   *   <li>table_completion - table-level completion signatures</li>
   * </ul>
   *
   * <p>Use this when you want to force re-download of all data,
   * ignoring any previous completion state.
   */
  void clearAllCompletions();

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

    @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
        Map<String, String> keyValues, long ttlMillis) {
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

    @Override public void clearAllCompletions() {
      // No-op
    }
  };
}
