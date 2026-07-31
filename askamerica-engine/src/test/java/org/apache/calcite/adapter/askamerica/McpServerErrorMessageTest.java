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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A cross-schema {@code corr()}/{@code regr_*()} call that fails to push down to DuckDB is
 * meant to fail with a clean message from {@code DuckDBStatsFunctions}'s {@code result()}
 * stub. When a second aggregate (e.g. {@code COUNT(*)}) shares the same
 * {@code EnumerableAggregate}, Calcite's generated code instead fails to compile before that
 * stub ever runs, surfacing a raw Janino "No applicable constructor/method" error naming the
 * stub class. {@link McpServer#compactErrorMessage} must recognize that shape too and return
 * the same actionable message rather than leaking the compiler error.
 */
@Tag("unit")
class McpServerErrorMessageTest {

  private static final String JANINO_MESSAGE =
      "Line 114, Column 87: No applicable constructor/method found for actual parameters "
      + "\"java.lang.Double, java.lang.Double, java.lang.Long\"; candidates are: "
      + "\"public static java.lang.Double "
      + "org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions$RegrUdaf.add"
      + "(java.lang.Double, java.lang.Double, java.lang.Double)\"";

  @Test @DisplayName("Janino compile failure on a stats UDAF is translated to the clean message")
  void translatesJaninoCompileFailure() {
    String compact = McpServer.compactErrorMessage(new RuntimeException(JANINO_MESSAGE));
    assertTrue(compact.contains("failed to push down to the DuckDB engine"),
        "expected the actionable message, got: " + compact);
    assertTrue(compact.contains("fetch_aligned_series"),
        "expected a pointer to the workaround, got: " + compact);
  }

  @Test @DisplayName("the same failure wrapped as a cause is still recognized")
  void translatesJaninoCompileFailureFromCause() {
    RuntimeException wrapped =
        new RuntimeException("Error while executing SQL", new RuntimeException(JANINO_MESSAGE));
    String compact = McpServer.compactErrorMessage(wrapped);
    assertTrue(compact.contains("failed to push down to the DuckDB engine"),
        "expected the actionable message, got: " + compact);
  }

  @Test @DisplayName("an unrelated Janino-shaped message is left alone")
  void leavesUnrelatedCompileFailureAlone() {
    String msg = "No applicable constructor/method found for actual parameters "
        + "\"java.lang.String\"; candidates are: \"some.other.Thing.add(java.lang.String)\"";
    assertEquals(msg, McpServer.compactErrorMessage(new RuntimeException(msg)));
  }
}
