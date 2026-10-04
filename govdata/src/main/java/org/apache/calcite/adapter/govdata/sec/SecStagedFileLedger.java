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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.iceberg.IcebergMaterializer;
import org.apache.calcite.adapter.file.partition.PipelineTracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tracker-backed record of the source batch files SEC staging has uploaded and which Iceberg
 * table instance has absorbed each.
 *
 * <p>Materialization needs to know which source files to read without listing the year
 * partitions. A pass's own uploads are not enough: files an earlier pass uploaded and never
 * materialized (a killed or timed-out pass, or a table reset that left its markers behind) are
 * invisible to it, and the staging markers that make filing extraction skip those filings also
 * keep them from being offered again. This ledger keeps the file paths themselves.
 *
 * <p>Two tracker phases, both keyed by object path:
 * <ul>
 *   <li>{@value #PHASE_STAGED}: written at upload time, table name is the file's table type.
 *   <li>{@value #PHASE_ABSORBED}: written once a table's materialization has covered the file;
 *       the table name carries the Iceberg table's instance id, so a dropped-and-recreated table
 *       sees every file as unabsorbed again.
 * </ul>
 */
final class SecStagedFileLedger {
  static final String PHASE_STAGED = "staged_file";
  static final String PHASE_ABSORBED = "absorbed_file";

  private final PipelineTracker tracker;

  SecStagedFileLedger(PipelineTracker tracker) {
    this.tracker = tracker;
  }

  void recordUpload(String objectPath, String tableType) {
    tracker.markComplete(objectPath, tableType, PHASE_STAGED, 1);
  }

  /**
   * Staged files in the year range that {@code icebergTableId}'s current instance has not yet
   * absorbed, narrowed to those matching the table's source pattern.
   */
  List<String> pendingFor(String icebergTableId, String instanceId, String sourcePattern,
      int startYear, int endYear) {
    List<String> staged = new ArrayList<String>(tracker.getSourceKeysForPhase(PHASE_STAGED));
    Set<String> candidates = new TreeSet<String>();
    for (int year = startYear; year <= endYear; year++) {
      candidates.addAll(IcebergMaterializer.filterStagedFilesForBatch(
          staged, sourcePattern, String.valueOf(year)));
    }
    Map<String, Set<String>> absorbed = tracker.bulkGetCompletedTables(candidates, PHASE_ABSORBED);
    String absorbedBy = absorbedBy(icebergTableId, instanceId);
    Set<String> pending = new TreeSet<String>();
    for (String path : candidates) {
      Set<String> by = absorbed.get(path);
      if (by == null || !by.contains(absorbedBy)) {
        pending.add(path);
      }
    }
    return Collections.unmodifiableList(new ArrayList<String>(pending));
  }

  /** Records that {@code icebergTableId}'s current instance has absorbed these files. */
  void markAbsorbed(String icebergTableId, String instanceId, List<String> paths) {
    String absorbedBy = absorbedBy(icebergTableId, instanceId);
    for (String path : paths) {
      tracker.markComplete(path, absorbedBy, PHASE_ABSORBED, 1);
    }
  }

  private static String absorbedBy(String icebergTableId, String instanceId) {
    return icebergTableId + "#" + instanceId;
  }
}
