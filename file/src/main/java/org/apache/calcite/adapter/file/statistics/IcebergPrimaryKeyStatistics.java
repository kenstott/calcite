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

import org.apache.iceberg.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Reads and writes {@link PrimaryKeyStatistics} on an Iceberg table.
 *
 * <p>The statistic lives in table properties rather than a Puffin statistics file. Iceberg
 * holds one {@code StatisticsFile} per snapshot, so registering one here would displace the
 * Theta NDV blobs {@link IcebergThetaStatistics} already publishes for that snapshot;
 * properties merge instead of replacing, so the two coexist. Properties also travel with
 * table metadata, which is what lets a verify run against a remote object store read this
 * without touching a data file.
 *
 * <p>Like the NDV read path, this reads numbers only and never deserializes a sketch —
 * doing so pulls in datasketches-memory, whose static initializer rejects any JDK past 21
 * with an {@code ExceptionInInitializerError}.
 */
public final class IcebergPrimaryKeyStatistics {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergPrimaryKeyStatistics.class);

  private static final String PREFIX = "aperio.pk.";
  private static final String SNAPSHOT_PROPERTY = PREFIX + "snapshot-id";
  private static final String COLUMNS_PROPERTY = PREFIX + "columns";
  private static final String KEYED_ROWS_PROPERTY = PREFIX + "keyed-rows";
  private static final String DISTINCT_KEYS_PROPERTY = PREFIX + "distinct-keys";
  private static final String EXACT_PROPERTY = PREFIX + "exact";

  private IcebergPrimaryKeyStatistics() {
  }

  /**
   * Read the primary-key statistic recorded for the table's current snapshot.
   *
   * @return the statistic, or null when none was recorded or it describes an older snapshot —
   *     callers must then measure, never assume the key is unique
   */
  public static PrimaryKeyStatistics read(Table table) {
    if (table == null || table.currentSnapshot() == null) {
      return null;
    }
    try {
      Map<String, String> props = table.properties();
      String snapshotId = props.get(SNAPSHOT_PROPERTY);
      if (snapshotId == null) {
        return null;
      }
      // The statistic describes the snapshot it was measured from. After a re-ingest or a
      // compaction the current snapshot differs and the recorded numbers describe data that
      // is no longer there, so it must be re-measured rather than served.
      String currentSnapshotId = Long.toString(table.currentSnapshot().snapshotId());
      if (!snapshotId.equals(currentSnapshotId)) {
        return null;
      }
      String columns = props.get(COLUMNS_PROPERTY);
      String keyedRows = props.get(KEYED_ROWS_PROPERTY);
      String distinctKeys = props.get(DISTINCT_KEYS_PROPERTY);
      if (columns == null || keyedRows == null || distinctKeys == null) {
        return null;
      }
      List<String> keyColumns = new ArrayList<>(Arrays.asList(columns.split(",")));
      if (keyColumns.isEmpty()) {
        return null;
      }
      return new PrimaryKeyStatistics(keyColumns,
          Long.parseLong(keyedRows),
          Long.parseLong(distinctKeys),
          Boolean.parseBoolean(props.get(EXACT_PROPERTY)),
          snapshotId,
          null);
    // Throwable, not Exception: a missing or incompatible dependency raises an Error, and a
    // statistic that cannot be read must degrade to "not measured" — which makes the caller
    // measure it — rather than fail the run that happened to touch this table.
    } catch (Throwable t) {
      LOGGER.debug("Could not read primary-key statistics for {}: {}", table, t.toString());
      return null;
    }
  }

  /**
   * Record a primary-key statistic against the table's current snapshot.
   *
   * <p>Sets only its own properties, leaving every other property and all statistics files
   * in place. Throws on failure so a caller that promised to persist can report that it did
   * not, rather than silently serving a scan on every subsequent run.
   */
  public static void write(Table table, PrimaryKeyStatistics stats) {
    if (table == null || stats == null) {
      return;
    }
    if (table.currentSnapshot() == null) {
      throw new IllegalStateException(
          "Cannot record primary-key statistics for a table with no current snapshot");
    }
    table.updateProperties()
        .set(SNAPSHOT_PROPERTY, Long.toString(table.currentSnapshot().snapshotId()))
        .set(COLUMNS_PROPERTY, String.join(",", stats.getKeyColumns()))
        .set(KEYED_ROWS_PROPERTY, Long.toString(stats.getKeyedRowCount()))
        .set(DISTINCT_KEYS_PROPERTY, Long.toString(stats.getDistinctKeyEstimate()))
        .set(EXACT_PROPERTY, Boolean.toString(stats.isExact()))
        .commit();
    LOGGER.debug("Recorded primary-key statistics for {}: {}", table, stats);
  }
}
