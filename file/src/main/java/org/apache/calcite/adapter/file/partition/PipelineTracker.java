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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Unified pipeline state tracker extending {@link IncrementalTracker}.
 *
 * <p>Models pipeline state as {@code (sourceKey, tableName, phase, state)} rows.
 * The {@code phase} is a free-form string label for a step in a linear pipeline;
 * the tracker stores it opaquely and callers define their own vocabulary:
 * <ul>
 *   <li>SEC: {@code download} -> {@code staging} -> {@code materialized}</li>
 *   <li>Weather/Crime: {@code fetched} -> {@code materialized}</li>
 * </ul>
 *
 * <p>Since this extends {@link IncrementalTracker}, any {@code PipelineTracker}
 * is-a {@code IncrementalTracker}. Existing callers keep working unchanged.
 *
 * <p>Default implementations bridge to {@link IncrementalTracker} methods by
 * encoding {@code tableName:phase} as the alternate name, so any existing
 * {@code IncrementalTracker} impl works as a {@code PipelineTracker} with
 * zero code changes.
 */
public interface PipelineTracker extends IncrementalTracker {

  /**
   * Check if a specific (sourceKey, tableName, phase) combination is complete.
   *
   * @param sourceKey  Accession number (SEC) or dimension value (weather/crime)
   * @param tableName  Output table name (e.g. "metadata", "facts", "mda")
   * @param phase      Caller-defined step label (e.g. "staging", "materialized")
   * @return true if this combination has state "complete"
   */
  default boolean isComplete(String sourceKey, String tableName, String phase) {
    // Bridge: encode as alternateName = tableName:phase, sourceKey as key value
    return isProcessed(tableName + ":" + phase, tableName,
        Collections.singletonMap("source_key", sourceKey));
  }

  /**
   * Mark a (sourceKey, tableName, phase) combination as complete.
   *
   * @param sourceKey  Accession number or dimension value
   * @param tableName  Output table name
   * @param phase      Caller-defined step label
   * @param rowCount   Number of rows written (0 = empty, -1 = unknown)
   */
  default void markComplete(String sourceKey, String tableName, String phase,
      long rowCount) {
    markProcessedWithRowCount(tableName + ":" + phase, tableName,
        Collections.singletonMap("source_key", sourceKey),
        tableName + ":" + phase, rowCount);
  }

  /**
   * Mark a (sourceKey, tableName, phase) combination as errored.
   *
   * @param sourceKey  Accession number or dimension value
   * @param tableName  Output table name
   * @param phase      Caller-defined step label
   * @param error      Error details
   */
  default void markError(String sourceKey, String tableName, String phase,
      String error) {
    markProcessedWithError(tableName + ":" + phase, tableName,
        Collections.singletonMap("source_key", sourceKey),
        tableName + ":" + phase, error);
  }

  /**
   * Mark a (sourceKey, tableName, phase) combination as cleared,
   * forcing reprocessing.
   *
   * @param sourceKey  Accession number or dimension value
   * @param tableName  Output table name
   * @param phase      Caller-defined step label
   */
  default void markCleared(String sourceKey, String tableName, String phase) {
    invalidate(tableName + ":" + phase,
        Collections.singletonMap("source_key", sourceKey));
  }

  /**
   * Get all table names that are complete for a given sourceKey and phase.
   *
   * @param sourceKey  Accession number or dimension value
   * @param phase      Caller-defined step label
   * @return Set of table names with state "complete"
   */
  default Set<String> getCompletedTables(String sourceKey, String phase) {
    // Default: no efficient way to enumerate without knowing table names.
    // Native implementations should override for efficiency.
    return Collections.emptySet();
  }

  /**
   * Check if all required tables are complete for a sourceKey and phase.
   *
   * @param sourceKey       Accession number or dimension value
   * @param phase           Caller-defined step label
   * @param requiredTables  Set of table names that must all be complete
   * @return true if every required table has state "complete"
   */
  default boolean isFullyComplete(String sourceKey, String phase,
      Set<String> requiredTables) {
    for (String table : requiredTables) {
      if (!isComplete(sourceKey, table, phase)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get all source keys that have at least one complete entry for a phase.
   *
   * @param phase  Caller-defined step label
   * @return Set of source keys
   */
  default Set<String> getSourceKeysForPhase(String phase) {
    return Collections.emptySet();
  }

  /**
   * Bulk-preload all tracker state for a phase into an in-memory cache.
   *
   * <p>After this call, subsequent {@code isComplete} and {@code getCompletedTables}
   * calls for the given phase will be served from memory instead of making
   * individual S3 round-trips.
   *
   * @param phase  Caller-defined step label (e.g. "staging")
   */
  default void preloadAll(String phase) {
    // No-op by default; S3-backed implementations override for performance.
  }

  /**
   * Bulk-retrieve completed tables for multiple source keys in a single query.
   *
   * <p>More efficient than calling {@link #getCompletedTables} in a loop when
   * checking many source keys at once (e.g., all accessions for a CIK).
   *
   * @param sourceKeys Collection of source keys to check
   * @param phase      Caller-defined step label (e.g. "staging")
   * @return Map from sourceKey to its set of completed table names.
   *         Source keys with no tracker data are absent from the map.
   */
  default Map<String, Set<String>> bulkGetCompletedTables(
      Collection<String> sourceKeys, String phase) {
    Map<String, Set<String>> result = new HashMap<String, Set<String>>();
    for (String sourceKey : sourceKeys) {
      Set<String> tables = getCompletedTables(sourceKey, phase);
      if (!tables.isEmpty()) {
        result.put(sourceKey, tables);
      }
    }
    return result;
  }

  /**
   * A no-op PipelineTracker that always returns false (forces full rebuild).
   */
  PipelineTracker NOOP_PIPELINE = new NoopPipelineTracker();

  /**
   * No-op implementation that delegates IncrementalTracker methods to
   * {@link IncrementalTracker#NOOP} and returns empty/false for pipeline methods.
   */
  class NoopPipelineTracker implements PipelineTracker {
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
      return Collections.emptySet();
    }

    @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
      // No-op
    }

    @Override public void invalidateAll(String alternateName) {
      // No-op
    }

    @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
        java.util.List<Map<String, String>> allCombinations) {
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

    @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
      return false;
    }

    @Override public void markComplete(String sourceKey, String tableName, String phase,
        long rowCount) {
      // No-op
    }

    @Override public void markError(String sourceKey, String tableName, String phase,
        String error) {
      // No-op
    }

    @Override public Set<String> getCompletedTables(String sourceKey, String phase) {
      return Collections.emptySet();
    }
  }
}
