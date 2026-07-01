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

import java.util.Map;

/**
 * Read-only {@link PipelineTracker}: every read reports "untracked" (so callers
 * fall back to processing nothing / treating all combinations as unprocessed,
 * exactly like {@link PipelineTracker.NoopPipelineTracker}), but every state
 * mutation throws {@link IllegalStateException}.
 *
 * <p>This is the fail-closed default when no writable tracker backend is
 * configured. It is deliberately distinct from {@link PipelineTracker#NOOP_PIPELINE}:
 * NOOP <em>silently swallows</em> writes (its purpose is "force a full rebuild,
 * record nothing"), which for an ETL run would mean the data is written but no
 * completion state is recorded — silent loss of idempotence. This tracker instead
 * makes any write attempt loud, so an ETL run launched without a configured
 * multi-user store fails fast rather than corrupting resume state.
 *
 * <p>Reads are inherited from {@link PipelineTracker.NoopPipelineTracker} so that
 * pure-query paths (which only ever read) keep working with zero configuration.
 */
public final class ReadOnlyPipelineTracker extends PipelineTracker.NoopPipelineTracker {

  /**
   * Raise a uniform, actionable error for any attempted write.
   *
   * @param op the mutating operation that was attempted (for the message)
   */
  private static RuntimeException readOnly(String op) {
    return new IllegalStateException(
        "Tracker is read-only: no writable tracker backend is configured, so '" + op
        + "' is refused to prevent accidental state changes. Configure a multi-user tracker "
        + "(e.g. trackerBackend=pg with trackerConfig.jdbcUrl) to enable ETL writes.");
  }

  // ===== IncrementalTracker mutators =====

  @Override public void markProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern) {
    throw readOnly("markProcessed");
  }

  @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, long rowCount) {
    throw readOnly("markProcessedWithRowCount");
  }

  @Override public void markProcessedEmpty(String alternateName, String sourceTable,
      Map<String, String> keyValues) {
    throw readOnly("markProcessedEmpty");
  }

  @Override public void markProcessedWithError(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, String errorMessage) {
    throw readOnly("markProcessedWithError");
  }

  @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
    throw readOnly("invalidate");
  }

  @Override public void invalidateAll(String alternateName) {
    throw readOnly("invalidateAll");
  }

  @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
    throw readOnly("markTableComplete");
  }

  @Override public void markTableCompleteWithConfig(String pipelineName, String configHash,
      String dimensionSignature, long rowCount) {
    throw readOnly("markTableCompleteWithConfig");
  }

  @Override public void markTableCompleteWithSourceWatermark(String pipelineName, String configHash,
      String dimensionSignature, long rowCount, long sourceFileWatermark) {
    throw readOnly("markTableCompleteWithSourceWatermark");
  }

  @Override public void invalidateTableCompletion(String pipelineName) {
    throw readOnly("invalidateTableCompletion");
  }

  @Override public void clearAllCompletions() {
    throw readOnly("clearAllCompletions");
  }

  @Override public void putFreshnessToken(String pipelineName, String token) {
    throw readOnly("putFreshnessToken");
  }

  @Override public void markPeriodComplete(String pipelineName, Map<String, String> periodValues) {
    throw readOnly("markPeriodComplete");
  }

  @Override public void invalidatePeriod(String pipelineName, Map<String, String> periodValues) {
    throw readOnly("invalidatePeriod");
  }

  @Override public void clearPeriodCompletions(String schemaName) {
    throw readOnly("clearPeriodCompletions");
  }

  @Override public void clearProcessedKeys(String schemaName) {
    throw readOnly("clearProcessedKeys");
  }

  // ===== PipelineTracker mutators =====

  @Override public void markComplete(String sourceKey, String tableName, String phase,
      long rowCount) {
    throw readOnly("markComplete");
  }

  @Override public void markError(String sourceKey, String tableName, String phase,
      String error) {
    throw readOnly("markError");
  }

  @Override public void markCleared(String sourceKey, String tableName, String phase) {
    throw readOnly("markCleared");
  }

  @Override public void markUnavailable(String alternateName, String sourceTable,
      Map<String, String> keyValues, String reason) {
    throw readOnly("markUnavailable");
  }
}
