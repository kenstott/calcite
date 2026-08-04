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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.format.csv.CsvTypeConverter;
import org.apache.calcite.adapter.file.util.NullEquivalents;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that encode the contracts decided in docs/testing/contradictions.md. Where the current code
 * already honors the contract, the test is active; where the resolution requires a not-yet-applied
 * code fix, the target test is {@code @Disabled("blocked on C-NN")} so it goes green when the fix lands.
 */
@Tag("unit")
public class ResolvedBugTargetsTest {

  private static CsvTypeConverter converter() {
    return new CsvTypeConverter(NullEquivalents.DEFAULT_NULL_EQUIVALENTS, false);
  }

  // FILE-008 (C-16, amended) — a value that doesn't fit its column's type becomes null with a WARN
  // rather than raising. See the C-16 amendment in docs/testing/contradictions.md: the mismatch is
  // expected by construction (declared header type, or a confidenceThreshold-promoted column per
  // C-08), and raising instead fails the whole CSV→Parquet conversion, dropping the table from the
  // schema. This pins the null so a future change can't silently make one bad row remove a table.
  @Test @Tag("FILE-008") void badNumericValueBecomesNull() {
    assertNull(converter().convert("not_a_number", SqlTypeName.INTEGER));
    assertNull(converter().convert("not_a_number", SqlTypeName.DOUBLE));
  }

  // C-17 — an unparseable DATE used to throw NullPointerException (a crash inside convert()); it
  // must degrade to null like every other non-conforming value, never an NPE.
  @Test @Tag("FILE-101") void badDateIsNullNotNpe() {
    assertNull(converter().convert("not-a-date", SqlTypeName.DATE));
    assertNull(converter().convert("not-a-time", SqlTypeName.TIME));
    assertNull(converter().convert("not-a-timestamp", SqlTypeName.TIMESTAMP));
  }



  // FILE-125 (C-19) — the DuckDB HLL rule must not write hard-coded /tmp debug artifacts on load.
  // (Static initializer writes the marker on first class load; the fix gates it behind a debug flag.)
  @Test @Tag("FILE-125")
  @Disabled("blocked on C-19: gate debug behind a flag — no hard-coded /tmp markers")
  void duckdbHllRuleWritesNoTmpArtifacts() throws Exception {
    File loaded = new File("/tmp/duckdb_hll_rule_loaded.txt");
    File matched = new File("/tmp/duckdb_hll_rule_matched.txt");
    loaded.delete();
    matched.delete();
    Class.forName("org.apache.calcite.adapter.file.duckdb.DuckDBHLLCountDistinctRule");
    assertFalse(loaded.exists(), "rule must not write a hard-coded /tmp marker on load");
  }
}
