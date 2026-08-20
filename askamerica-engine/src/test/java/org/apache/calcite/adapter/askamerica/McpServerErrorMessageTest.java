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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code corr()}/{@code regr_*()} and the other DuckDB statistical aggregates now have real
 * Java implementations, so failing to push down to DuckDB is no longer a failure at all — the
 * query runs locally and returns the same answer. What remains is the case where the local
 * path ITSELF breaks, which surfaces as a raw Janino "No applicable constructor/method" error
 * naming the generated stub class.
 *
 * <p>{@link McpServer#compactErrorMessage} must still recognize that shape, but must no longer
 * explain it as a cross-schema join. That explanation was wrong whenever the operand was a
 * derived relation — a {@code VALUES} literal has no join and no schema — and it cost a real
 * investigation: an agent hitting it went looking for a cross-schema problem that did not
 * exist, and worked around a defect rather than reporting it. The message now carries the
 * underlying error instead of a guess the caller cannot verify.
 */
@Tag("unit")
class McpServerErrorMessageTest {

  private static final String JANINO_MESSAGE =
      "Line 114, Column 87: No applicable constructor/method found for actual parameters "
      + "\"java.lang.Double, java.lang.Double, java.lang.Long\"; candidates are: "
      + "\"public static java.lang.Double "
      + "org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions$RegrUdaf.add"
      + "(java.lang.Double, java.lang.Double, java.lang.Double)\"";

  @Test @DisplayName("a stats-UDAF compile failure reports the real cause, not a guessed one")
  void translatesJaninoCompileFailure() {
    String compact = McpServer.compactErrorMessage(new RuntimeException(JANINO_MESSAGE));
    assertTrue(compact.contains("could not be evaluated"),
        "expected the actionable message, got: " + compact);
    // The underlying error must survive: it names the actual type that failed to bind, which
    // is the only part a reader can act on.
    assertTrue(compact.contains("No applicable constructor/method"),
        "the raw cause must be carried through, got: " + compact);
  }

  @Test @DisplayName("the message never blames a cross-schema join it cannot know about")
  void doesNotFabricateACrossSchemaJoin() {
    String compact = McpServer.compactErrorMessage(new RuntimeException(JANINO_MESSAGE));
    assertFalse(compact.contains("join across two different schemas"),
        "this explanation is false for a VALUES/derived-relation operand and misdirected a "
        + "real investigation; got: " + compact);
    assertFalse(compact.contains("fetch_aligned_series"),
        "fetch_aligned_series needs warehouse tables, so it is not a remedy for inline data "
        + "and must not be offered as one; got: " + compact);
  }

  @Test @DisplayName("the same failure wrapped as a cause is still recognized")
  void translatesJaninoCompileFailureFromCause() {
    RuntimeException wrapped =
        new RuntimeException("Error while executing SQL", new RuntimeException(JANINO_MESSAGE));
    String compact = McpServer.compactErrorMessage(wrapped);
    assertTrue(compact.contains("could not be evaluated"),
        "expected the actionable message, got: " + compact);
  }

  @Test @DisplayName("an unrelated Janino-shaped message is left alone")
  void leavesUnrelatedCompileFailureAlone() {
    String msg = "No applicable constructor/method found for actual parameters "
        + "\"java.lang.String\"; candidates are: \"some.other.Thing.add(java.lang.String)\"";
    assertEquals(msg, McpServer.compactErrorMessage(new RuntimeException(msg)));
  }
}
