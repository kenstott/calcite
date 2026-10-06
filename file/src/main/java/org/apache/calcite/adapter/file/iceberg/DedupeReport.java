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
package org.apache.calcite.adapter.file.iceberg;

import java.util.Map;
import java.util.TreeMap;

/** What {@link IcebergTableWriter#dedupeCopies} found, and (when executed) did. */
public final class DedupeReport {
  /** Rows scanned. */
  public final long rows;
  /** Distinct rows (all columns equal). */
  public final long distinctRows;
  /** Distinct values of the key columns, or -1 when no key columns were given. */
  public final long distinctKeys;
  /** Groups scanned (e.g. accessions). */
  public final int groups;
  /** Groups whose every row appears the same number of times k &gt; 1, keyed by that k. */
  public final Map<Integer, Integer> groupsByCopies;
  /** Rows a run would remove. */
  public final long rowsToRemove;
  /** Rows actually removed (0 for a dry run). */
  public final long rowsRemoved;
  /** Snapshot to roll back to, or -1 when the table had none. */
  public final long snapshotBefore;
  /** Post-commit check result; null for a dry run. */
  public final String verification;

  DedupeReport(long rows, long distinctRows, long distinctKeys, int groups,
      Map<Integer, Integer> groupsByCopies, long rowsToRemove, long rowsRemoved,
      long snapshotBefore, String verification) {
    this.rows = rows;
    this.distinctRows = distinctRows;
    this.distinctKeys = distinctKeys;
    this.groups = groups;
    this.groupsByCopies = new TreeMap<Integer, Integer>(groupsByCopies);
    this.rowsToRemove = rowsToRemove;
    this.rowsRemoved = rowsRemoved;
    this.snapshotBefore = snapshotBefore;
    this.verification = verification;
  }

  public int groupsWithCopies() {
    int n = 0;
    for (int v : groupsByCopies.values()) {
      n += v;
    }
    return n;
  }

  @Override public String toString() {
    return "rows=" + rows + " distinctRows=" + distinctRows + " distinctKeys=" + distinctKeys
        + " groups=" + groups + " groupsWithCopies=" + groupsByCopies
        + " rowsToRemove=" + rowsToRemove + " rowsRemoved=" + rowsRemoved
        + " snapshotBefore=" + snapshotBefore
        + (verification == null ? "" : " verification=" + verification);
  }
}
