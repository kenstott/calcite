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
package org.apache.calcite.adapter.file.etl;

import java.util.ArrayList;
import java.util.List;

/**
 * Process-wide record of the tables whose ETL failed, for a launcher that must turn them into a
 * non-zero exit code.
 *
 * <p>ETL runs as a side effect of opening a schema connection, so the caller never sees the
 * {@link SchemaResult}; without this ledger a launcher can only learn that the connection opened.
 * Recording is off until a launcher calls {@link #enable()}, so a long-lived server does not
 * accumulate entries.
 */
public final class EtlFailureLedger {

  private static final List<String> ENTRIES = new ArrayList<String>();
  private static boolean enabled;

  private EtlFailureLedger() {
  }

  /** Starts recording failures and clears any earlier entries. */
  public static synchronized void enable() {
    enabled = true;
    ENTRIES.clear();
  }

  static synchronized void record(String schema, String table, EtlResult result) {
    if (!enabled) {
      return;
    }
    ENTRIES.add("schema=" + schema + " table=" + table + " "
        + (result.isPartiallyFailed()
            ? result.getFailedBatches() + " of " + result.getTotalBatches() + " period(s) failed"
            : "table failed")
        + ": " + result.getFailureMessage());
  }

  /**
   * Returns and clears the recorded failures.
   *
   * @return one line per failed table, each naming the failed periods; empty when none failed
   */
  public static synchronized List<String> drain() {
    List<String> copy = new ArrayList<String>(ENTRIES);
    ENTRIES.clear();
    return copy;
  }
}
