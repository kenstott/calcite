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

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A call with no {@code sql}/{@code outcome} (both default to {@code ""} via Jackson's
 * {@code MissingNode.asText()}) used to reach {@code StatsEngine.extractColumns} with an empty
 * SQL string, which surfaced as a raw {@code IndexOutOfBoundsException} ("Index 0 out of bounds
 * for length 0") from the SQL parser — unlike every sibling stats tool, which validates its
 * required arguments up front and fails with an actionable message instead.
 */
@Tag("unit")
class McpServerOlsRegressionValidationTest {

  @Test @DisplayName("empty sql and outcome fail with an actionable message, not a parser crash")
  void rejectsEmptyCallBeforeTouchingSql() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> McpServer.olsRegressionTool("", "", Collections.emptyList()));
    assertTrue(e.getMessage().contains("sql and outcome are required"),
        "expected an actionable message, got: " + e.getMessage());
  }

  @Test @DisplayName("blank (whitespace-only) sql is treated the same as empty")
  void rejectsBlankSql() {
    assertThrows(IllegalArgumentException.class,
        () -> McpServer.olsRegressionTool("   ", "gdp", Collections.emptyList()));
  }

  @Test @DisplayName("missing outcome alone is rejected even with sql present")
  void rejectsMissingOutcome() {
    assertThrows(IllegalArgumentException.class,
        () -> McpServer.olsRegressionTool("SELECT 1", null, Collections.emptyList()));
  }
}
