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

import org.apache.calcite.adapter.file.etl.DimensionConfig;

import java.util.ArrayList;
import java.util.Collections;
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
   * Gets processed key value combinations for an alternate partition scoped to a specific year.
   *
   * <p>Implementations should override this to use year-partitioned storage for efficiency.
   * The default falls back to {@link #getProcessedKeyValues(String)} and filters by year.
   *
   * @param alternateName The alternate partition name
   * @param year The year to scope results to (e.g. "2025"), or null for all years
   * @return Set of processed key value maps for the given year, or empty set if none
   */
  default Set<Map<String, String>> getProcessedKeyValues(String alternateName, String year) {
    Set<Map<String, String>> all = getProcessedKeyValues(alternateName);
    if (year == null) {
      return all;
    }
    Set<Map<String, String>> filtered = new HashSet<>();
    for (Map<String, String> kv : all) {
      if (year.equals(kv.get("year"))) {
        filtered.add(kv);
      }
    }
    return filtered;
  }

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

  /**
   * Filters combinations treating ALL entries (including those with rows) as expired after the TTL.
   *
   * <p>Unlike {@link #filterUnprocessedWithEmptyTtl}, which only re-queues zero-row entries,
   * this method re-queues any entry whose tracker timestamp is older than {@code successTtlMillis}.
   * Use this for annual-cadence tables that need periodic full refresh within a release window.
   *
   * @param alternateName    The alternate partition name
   * @param sourceTable      The source table name
   * @param allCombinations  All dimension combinations to check
   * @param successTtlMillis TTL in ms; entries older than this are treated as unprocessed
   * @return Set of combination indices that need processing
   */
  default Set<Integer> filterUnprocessedWithSuccessTtl(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations, long successTtlMillis) {
    Set<Integer> result = new HashSet<>();
    for (int i = 0; i < allCombinations.size(); i++) {
      if (!isProcessedWithTtl(alternateName, sourceTable, allCombinations.get(i), successTtlMillis)) {
        result.add(i);
      }
    }
    return result;
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
   * Preloads all table completion markers into the in-memory cache.
   *
   * <p>S3-backed implementations override this to issue a single query that loads
   * all {@code _table_complete} markers, avoiding per-table S3 round-trips during
   * the table processing loop.
   */
  default void preloadAllCompletions() {
    // No-op by default
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
   * Returns true if this table was explicitly cleared (not just absent).
   * A cleared table should skip Phase 1.5 self-healing and force-reprocess all combinations.
   */
  default boolean wasTableCleared(String pipelineName) {
    return false;
  }

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

  // ===== Per-Period Completion Tracking =====

  /**
   * The literal value used in a period-completion key for any of the four period
   * slots (year, quarter, month, day) that a table does not partition on.
   */
  String PERIOD_NA = "NA";

  /**
   * Ordered period slots that make up a period-completion key. These are the only
   * dimension names the marker recognizes — the period dimensions ARE the tracking
   * mechanism. A schema with a non-canonical period name (e.g. census {@code vintage})
   * opts into per-period tracking by declaring a canonical {@code year}/{@code quarter}/
   * {@code month}/{@code day} dimension; the framework intentionally does no aliasing.
   * Non-period labels such as {@code frequency} are correctly ignored.
   */
  String[] PERIOD_SLOTS = {"year", "quarter", "month", "day"};

  /**
   * Builds the uniform per-period completion key for a pipeline.
   *
   * <p>The key is {@code year_quarter_month_day_pipelineName}, with the literal
   * {@link #PERIOD_NA} substituted for any of the four period slots the pipeline
   * does not declare. This is the single owner of the key format; both the marker
   * write and the completion check route through here so the format never drifts.
   *
   * <p>Examples: {@code 2025_NA_NA_NA_patents_patent_grants},
   * {@code 2022_NA_NA_NA_patents_patent_grants}.
   *
   * <p>Extension seam (not implemented): the key is always the canonical periods; a future
   * optional per-table {@code additionalCompletionKeys: [<dim>...]} operand could ADDITIVELY
   * append extra dimension values (e.g. {@code [state]}/{@code [cik]}) so each
   * (period + extra-keys) tuple becomes its own completion unit. Default stays periods-only.
   *
   * @param pipelineName The pipeline name (already schema-qualified)
   * @param periodValues The dimension combination; only the year/quarter/month/day
   *                     entries are read, all others are ignored
   * @return The period-completion key
   */
  static String periodCompletionKey(String pipelineName, Map<String, String> periodValues) {
    StringBuilder sb = new StringBuilder();
    for (String slot : PERIOD_SLOTS) {
      String val = periodValues != null ? periodValues.get(slot) : null;
      if (val == null || val.isEmpty()) {
        val = PERIOD_NA;
      }
      sb.append(val).append('_');
    }
    sb.append(pipelineName);
    return sb.toString();
  }

  /**
   * Returns true if the combination carries at least one canonical period slot
   * ({@code year}/{@code quarter}/{@code month}/{@code day}).
   *
   * <p>When false the combo is NOT period-tracked: its key would be all-{@code NA},
   * so every such combo for a pipeline would collide. Callers must NOT apply the
   * per-period marker/skip in that case — they fall back to the existing per-combo
   * {@code incremental} tracking, so non-canonical schemas are never made worse.
   *
   * @param periodValues The dimension combination
   * @return true if any canonical period slot is present and non-empty
   */
  static boolean hasCanonicalPeriod(Map<String, String> periodValues) {
    if (periodValues == null) {
      return false;
    }
    for (String slot : PERIOD_SLOTS) {
      String val = periodValues.get(slot);
      if (val != null && !val.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Flushes any buffered/pending tracker writes so subsequent reads observe them.
   *
   * <p>Write-behind implementations (e.g. the S3 tracker) buffer state rows in memory
   * and flush them lazily. The per-period completion sweep reads back per-combo state to
   * decide whether a period's full combo set is done, so it must flush first or it would
   * not see this run's just-written marks. Default is a no-op for non-buffered trackers.
   */
  default void flushPending() {
    // No-op for trackers that write through synchronously.
  }

  /**
   * Checks whether the given period has been completed for a pipeline.
   *
   * <p>Completion is authoritative and period-keyed: the latest append-only marker
   * for the 5-tuple {@code (year, quarter, month, day, pipelineName)} must be
   * {@code complete}. A missing or {@code invalidate} latest marker means not done.
   *
   * @param pipelineName The pipeline name
   * @param periodValues The dimension combination carrying the period slots
   * @return true if the latest marker for this period is {@code complete}
   */
  default boolean isPeriodComplete(String pipelineName, Map<String, String> periodValues) {
    // In-memory / non-persistent trackers cannot prove a period complete.
    return false;
  }

  /**
   * Appends a {@code complete} marker for the given period.
   *
   * @param pipelineName The pipeline name
   * @param periodValues The dimension combination carrying the period slots
   */
  default void markPeriodComplete(String pipelineName, Map<String, String> periodValues) {
    // No-op for non-persistent trackers.
  }

  /**
   * Appends an {@code invalidate} marker for the given period (latest-wins).
   *
   * @param pipelineName The pipeline name
   * @param periodValues The dimension combination carrying the period slots
   */
  default void invalidatePeriod(String pipelineName, Map<String, String> periodValues) {
    // No-op for non-persistent trackers.
  }

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
      Map<String, DimensionConfig> dimensions) {
    if (dimensions == null || dimensions.isEmpty()) {
      return "empty";
    }
    // Build hash from sorted dimension names and their key properties
    int hash = 0;
    List<String> sortedKeys = new ArrayList<>(dimensions.keySet());
    Collections.sort(sortedKeys);
    for (String key : sortedKeys) {
      DimensionConfig dim = dimensions.get(key);
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
   * @param combinations All expanded dimension combinations
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
      List<String> keys = new ArrayList<>(first.keySet());
      Collections.sort(keys);
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
