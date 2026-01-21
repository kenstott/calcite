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
   * Marks a pipeline as complete with config hash and row count for fast-path skip.
   *
   * @param pipelineName The pipeline name
   * @param configHash Hash of dimension configuration (for fast comparison)
   * @param dimensionSignature Hash of all dimension values
   * @param rowCount Total row count in the table
   */
  default void markTableCompleteWithConfig(String pipelineName, String configHash,
      String dimensionSignature, long rowCount) {
    // Default: fall back to simple markTableComplete
    markTableComplete(pipelineName, dimensionSignature);
  }

  /**
   * Gets cached completion info for a pipeline including config hash.
   * Used for fast-path skip to avoid dimension expansion.
   *
   * @param pipelineName The pipeline name
   * @return CachedCompletion with configHash, signature, rowCount, or null if not found
   */
  default CachedCompletion getCachedCompletion(String pipelineName) {
    return null; // Default: no caching support
  }

  /**
   * Marks a pipeline as complete with source file watermark for incremental detection.
   *
   * <p>The source file watermark is the max lastModified timestamp of all source files
   * at the time of processing. On subsequent runs, if any source file has a newer
   * lastModified, the table will be reprocessed.
   *
   * @param pipelineName The pipeline name
   * @param configHash Hash of dimension configuration
   * @param dimensionSignature Hash of all dimension values
   * @param rowCount Total row count in the table
   * @param sourceFileWatermark Max lastModified timestamp of source files (0 to disable)
   */
  default void markTableCompleteWithSourceWatermark(String pipelineName, String configHash,
      String dimensionSignature, long rowCount, long sourceFileWatermark) {
    // Default: fall back to config-only tracking (ignores watermark)
    markTableCompleteWithConfig(pipelineName, configHash, dimensionSignature, rowCount);
  }

  /**
   * Checks if source files have been modified since last processing.
   *
   * <p>This enables true incremental processing where only new/changed source files
   * trigger re-processing, not just changes to config or dimension values.
   *
   * @param pipelineName The pipeline name
   * @param currentSourceWatermark Current max lastModified of source files
   * @return true if source files have been modified since last completion
   */
  default boolean isSourceFilesModified(String pipelineName, long currentSourceWatermark) {
    CachedCompletion completion = getCachedCompletion(pipelineName);
    if (completion == null) {
      return true; // Never processed, treat as "modified"
    }
    return completion.isSourceFilesModified(currentSourceWatermark);
  }

  /**
   * Cached completion info for fast-path skip.
   *
   * <p>Includes source file watermark tracking to detect when source files have been
   * modified since the last successful processing. This enables true incremental
   * processing where only new/changed source files trigger re-processing.
   */
  class CachedCompletion {
    public final String configHash;
    public final String signature;
    public final long rowCount;
    public final long completedAt;
    /** Max lastModified timestamp of source files when processing completed (0 if not tracked). */
    public final long sourceFileWatermark;

    public CachedCompletion(String configHash, String signature, long rowCount) {
      this(configHash, signature, rowCount, System.currentTimeMillis(), 0L);
    }

    public CachedCompletion(String configHash, String signature, long rowCount, long completedAt) {
      this(configHash, signature, rowCount, completedAt, 0L);
    }

    public CachedCompletion(String configHash, String signature, long rowCount,
        long completedAt, long sourceFileWatermark) {
      this.configHash = configHash;
      this.signature = signature;
      this.rowCount = rowCount;
      this.completedAt = completedAt;
      this.sourceFileWatermark = sourceFileWatermark;
    }

    /**
     * Check if the empty result TTL has expired.
     * @param emptyResultTtlMillis The TTL in milliseconds
     * @return true if TTL has expired and retry is needed
     */
    public boolean isEmptyResultTtlExpired(long emptyResultTtlMillis) {
      if (rowCount > 0 || emptyResultTtlMillis <= 0) {
        return false; // Not empty or no TTL configured
      }
      return System.currentTimeMillis() > completedAt + emptyResultTtlMillis;
    }

    /**
     * Check if source files have been modified since this completion was recorded.
     *
     * @param currentSourceWatermark The current max lastModified of source files
     * @return true if source files have been modified and reprocessing is needed
     */
    public boolean isSourceFilesModified(long currentSourceWatermark) {
      if (sourceFileWatermark <= 0 || currentSourceWatermark <= 0) {
        return false; // Watermarking not enabled
      }
      return currentSourceWatermark > sourceFileWatermark;
    }
  }

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
   * Computes a hash of the dimension configuration (not expanded values).
   * This is fast to compute and captures changes to dimension config.
   * Used to determine if cached dimension signatures are still valid.
   *
   * @param dimensions Map of dimension name to configuration
   * @return Config hash string for comparison
   */
  static String computeConfigHash(
      Map<String, org.apache.calcite.adapter.file.etl.DimensionConfig> dimensions) {
    if (dimensions == null || dimensions.isEmpty()) {
      return "empty";
    }
    // Build hash from sorted dimension names and their key properties
    int hash = 0;
    java.util.List<String> sortedKeys = new java.util.ArrayList<>(dimensions.keySet());
    java.util.Collections.sort(sortedKeys);
    for (String key : sortedKeys) {
      org.apache.calcite.adapter.file.etl.DimensionConfig dim = dimensions.get(key);
      hash = 31 * hash + key.hashCode();
      hash = 31 * hash + (dim.getType() != null ? dim.getType().hashCode() : 0);
      hash = 31 * hash + (dim.getStart() != null ? dim.getStart().hashCode() : 0);
      hash = 31 * hash + (dim.getEnd() != null ? dim.getEnd().hashCode() : 0);
      hash = 31 * hash + (dim.getStep() != null ? dim.getStep().hashCode() : 0);
      hash = 31 * hash + (dim.getDataLag() != null ? dim.getDataLag().hashCode() : 0);
      hash = 31 * hash + (dim.getValues() != null ? dim.getValues().hashCode() : 0);
      hash = 31 * hash + (dim.getSql() != null ? dim.getSql().hashCode() : 0);
    }
    return "cfg:" + Integer.toHexString(hash);
  }

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
