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
package org.apache.calcite.adapter.govdata.ref;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A sweep of a source it has already swept reads only the rows that differ from the snapshot it
 * last swept, instead of every row. These cover the pieces that decide and build that diff: when
 * a full scan is forced, and which rows the diff returns.
 */
@Tag("unit")
class ChunkOrganizerSnapshotDiffTest {

  private static final int VERSION = 2;

  @Test void aSourceNeverSweptNeedsAFullScan() {
    String reason = ChunkOrganizer.fullScanReason(null, VERSION, false);
    assertNotNull(reason);
    assertTrue(reason.contains("no snapshot baseline"), reason);
  }

  @Test void aChangedChunkerVersionForcesAFullScan() {
    // The version bump exists to reprocess parents whose source rows did NOT change, which a
    // diff against the previous snapshot would never return.
    String reason = ChunkOrganizer.fullScanReason(
        new ChunkOrganizer.SweptBaseline(7L, VERSION - 1), VERSION, false);
    assertNotNull(reason);
    assertTrue(reason.contains("chunker version changed"), reason);
  }

  @Test void aBaselineWithNoRecordedChunkerVersionForcesAFullScan() {
    assertNotNull(ChunkOrganizer.fullScanReason(
        new ChunkOrganizer.SweptBaseline(7L, null), VERSION, false));
  }

  @Test void skippingTheHashCompareForcesAFullScan() {
    // That mode rewrites every parent, so it must read every row.
    assertNotNull(ChunkOrganizer.fullScanReason(
        new ChunkOrganizer.SweptBaseline(7L, VERSION), VERSION, true));
  }

  @Test void aCurrentBaselineAllowsTheDiff() {
    assertNull(ChunkOrganizer.fullScanReason(
        new ChunkOrganizer.SweptBaseline(7L, VERSION), VERSION, false));
  }

  @Test void snapshotScanPinsTheSnapshot() {
    assertEquals(
        "iceberg_scan('s3://b/transport/fmcsa_carriers', allow_moved_paths=true, "
            + "snapshot_from_id=42)",
        ChunkOrganizer.snapshotScan("s3://b/transport/fmcsa_carriers", 42L));
  }

  /** Runs {@link ChunkOrganizer#diffRelation} over two plain tables standing in for two scans. */
  private static List<String> diff(String[] oldRows, String[] newRows) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      st.execute("CREATE TABLE old_snap (dot VARCHAR, name VARCHAR, drivers INTEGER)");
      st.execute("CREATE TABLE new_snap (dot VARCHAR, name VARCHAR, drivers INTEGER)");
      for (String r : oldRows) {
        st.execute("INSERT INTO old_snap VALUES " + r);
      }
      for (String r : newRows) {
        st.execute("INSERT INTO new_snap VALUES " + r);
      }
      String rel = ChunkOrganizer.diffRelation("new_snap", "old_snap",
          Arrays.asList("dot", "name", "drivers"));
      List<String> out = new ArrayList<String>();
      try (ResultSet rs = st.executeQuery("SELECT dot FROM " + rel + " ORDER BY dot")) {
        while (rs.next()) {
          out.add(rs.getString(1));
        }
      }
      return out;
    }
  }

  @Test void anUnchangedTableReturnsNothing() throws Exception {
    String[] rows = {"('1','A',3)", "('2','B',NULL)"};
    assertEquals(Collections.<String>emptyList(), diff(rows, rows));
  }

  @Test void anAddedRowIsReturned() throws Exception {
    assertEquals(Arrays.asList("3"),
        diff(new String[] {"('1','A',3)"}, new String[] {"('1','A',3)", "('3','C',5)"}));
  }

  @Test void aRowChangedInAnyComparedColumnIsReturned() throws Exception {
    assertEquals(Arrays.asList("1", "2"),
        diff(new String[] {"('1','A',3)", "('2','B',4)"},
            new String[] {"('1','A',9)", "('2','B2',4)"}));
  }

  @Test void aColumnThatStaysNullIsNotADifference() throws Exception {
    assertEquals(Collections.<String>emptyList(),
        diff(new String[] {"('1','A',NULL)"}, new String[] {"('1','A',NULL)"}));
  }

  @Test void aDeletedRowIsNotReturned() throws Exception {
    // A sweep has never tombstoned parents that vanish from the source; the diff keeps that.
    assertEquals(Collections.<String>emptyList(),
        diff(new String[] {"('1','A',3)", "('2','B',4)"}, new String[] {"('1','A',3)"}));
  }
}
