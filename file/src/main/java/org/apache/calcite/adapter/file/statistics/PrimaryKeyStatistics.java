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
package org.apache.calcite.adapter.file.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Uniqueness of a table's declared primary key, measured once against a specific snapshot.
 *
 * <p>Answers "how many rows share a primary key" without a scan. The property is fixed at
 * write time — a committed snapshot's key distribution cannot change — so it is measured
 * during statistics collection and read back from the snapshot it was measured against.
 *
 * <p>{@link #getKeyedRowCount()} counts only rows whose key columns are all non-null, and
 * {@link #getDistinctKeyEstimate()} counts distinct key tuples among exactly those rows, so
 * {@link #duplicationFactor()} is comparable with the exact ratio a full scan would produce.
 */
public final class PrimaryKeyStatistics {
  private final List<String> keyColumns;
  private final long keyedRowCount;
  private final long distinctKeyEstimate;
  private final boolean exact;
  private final String snapshotId;
  private final String serializedSketch;

  /**
   * @param keyColumns declared primary key columns, in declaration order
   * @param keyedRowCount rows whose key columns are all non-null
   * @param distinctKeyEstimate distinct key tuples among the keyed rows
   * @param exact true when distinctKeyEstimate came from an exact DISTINCT rather than a sketch
   * @param snapshotId identity of the snapshot measured; the statistic is only valid for it
   * @param serializedSketch base64 Theta sketch over the key tuples, or null when not retained
   */
  public PrimaryKeyStatistics(List<String> keyColumns, long keyedRowCount,
                              long distinctKeyEstimate, boolean exact, String snapshotId,
                              String serializedSketch) {
    this.keyColumns = Collections.unmodifiableList(new ArrayList<>(keyColumns));
    this.keyedRowCount = keyedRowCount;
    this.distinctKeyEstimate = distinctKeyEstimate;
    this.exact = exact;
    this.snapshotId = snapshotId;
    this.serializedSketch = serializedSketch;
  }

  public List<String> getKeyColumns() {
    return keyColumns;
  }

  /**
   * Rows whose primary key columns are all non-null. Rows with a null key component are
   * excluded here and from {@link #getDistinctKeyEstimate()}, matching the verify check.
   */
  public long getKeyedRowCount() {
    return keyedRowCount;
  }

  /**
   * Distinct key tuples among the keyed rows. Exact or estimated per {@link #isExact()}.
   */
  public long getDistinctKeyEstimate() {
    return distinctKeyEstimate;
  }

  /**
   * Whether {@link #getDistinctKeyEstimate()} is an exact count. A sketch-derived estimate
   * carries relative error and must not be compared against a duplication threshold without
   * allowing for it.
   */
  public boolean isExact() {
    return exact;
  }

  /**
   * Snapshot this statistic was measured against. A new snapshot — re-ingest, compaction,
   * append — produces a different id and invalidates the statistic.
   */
  public String getSnapshotId() {
    return snapshotId;
  }

  /**
   * Base64 Theta sketch over the key tuples, or null when the sketch was not retained.
   * Retained sketches can be merged across files, which is what makes an incremental
   * recompute on append possible rather than a full re-measure.
   */
  public String getSerializedSketch() {
    return serializedSketch;
  }

  /**
   * Rows per distinct key. 1.0 means the key is unique; whole-table file duplication shows
   * up as an integral factor (2.0 for a doubled table).
   *
   * @return the ratio, or 0.0 when there are no keyed rows to describe
   */
  public double duplicationFactor() {
    if (keyedRowCount <= 0 || distinctKeyEstimate <= 0) {
      return 0.0;
    }
    return (double) keyedRowCount / (double) distinctKeyEstimate;
  }

  /**
   * Whether this statistic describes the given snapshot. A statistic measured against a
   * different snapshot describes data that is no longer there and must be recomputed.
   */
  public boolean isValidFor(String currentSnapshotId) {
    return snapshotId != null && snapshotId.equals(currentSnapshotId);
  }

  @Override public String toString() {
    return String.format("PrimaryKeyStatistics{keys=%s, keyed=%d, distinct=%d%s, snapshot=%s}",
        keyColumns, keyedRowCount, distinctKeyEstimate, exact ? "" : " (est)", snapshotId);
  }
}
