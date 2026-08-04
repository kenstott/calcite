/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * Change Date: 2030-01-01
 * On the Change Date, the license converts to the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.file;

import java.io.IOException;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Availability of the {@code duckdb} command-line binary.
 *
 * <p>Several suites build their Parquet fixtures by shelling out to the DuckDB
 * CLI. That is a different dependency from the {@code duckdb_jdbc} driver on the
 * test classpath: the binary has to be installed on the machine, and a runner
 * without it cannot run those tests. Asking before use turns that into a skip
 * rather than an {@code IOException} in a test whose subject is not DuckDB.
 */
public final class DuckDbCli {
  /** Resolved once; the binary does not appear part-way through a run. */
  private static final boolean AVAILABLE = probe();

  private DuckDbCli() {
  }

  private static boolean probe() {
    try {
      final Process process = new ProcessBuilder("duckdb", "-version")
          .redirectErrorStream(true)
          .start();
      return process.waitFor() == 0;
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /** Skips the calling test unless the DuckDB CLI is installed. */
  public static void assumeAvailable() {
    assumeTrue(AVAILABLE, "the duckdb command-line binary is not installed");
  }
}
