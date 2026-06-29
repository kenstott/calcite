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
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that encode the contracts decided in docs/testing/contradictions.md. Where the current code
 * already honors the contract, the test is active; where the resolution requires a not-yet-applied
 * code fix, the target test is {@code @Disabled("blocked on C-NN")} so it goes green when the fix lands.
 */
@Tag("unit")
public class ResolvedBugTargetsTest {

  private static CsvTypeConverter converter() {
    Map<SqlTypeName, DateTimeFormatter> formatters = new HashMap<>();
    formatters.put(SqlTypeName.DATE, DateTimeFormatter.ISO_LOCAL_DATE);
    formatters.put(SqlTypeName.TIME, DateTimeFormatter.ISO_LOCAL_TIME);
    return new CsvTypeConverter(formatters, NullEquivalents.DEFAULT_NULL_EQUIVALENTS, false);
  }

  // FILE-008 (C-16) — a non-null value violating an inferred type must surface an error, not coerce.
  // Numerics already comply (Integer/Double.valueOf throw); this pins that good behavior.
  @Test @Tag("FILE-008") void badNumericValueRaises() {
    assertThrows(NumberFormatException.class, () -> converter().convert("not_a_number", SqlTypeName.INTEGER));
    assertThrows(NumberFormatException.class, () -> converter().convert("not_a_number", SqlTypeName.DOUBLE));
  }

  // The temporal path is where the silent fallback lives — currently returns null instead of raising.
  @Test @Tag("FILE-008") @Tag("FILE-101")
  @Disabled("blocked on C-16: an unparseable TIME/TIMESTAMP must raise, not silently return null")
  void badTemporalValueRaises_target() {
    assertThrows(RuntimeException.class, () -> converter().convert("not-a-time", SqlTypeName.TIME));
  }

  // FILE-101 (C-17) — an unparseable DATE currently throws NullPointerException (a crash); the fix
  // must surface a clean parse error instead.
  @Test @Tag("FILE-101")
  @Disabled("blocked on C-17: unparseable DATE must be a clean parse error, not a NullPointerException")
  void badDateIsCleanErrorNotNpe_target() {
    Throwable t = assertThrows(Throwable.class, () -> converter().convert("not-a-date", SqlTypeName.DATE));
    assertFalse(t instanceof NullPointerException, "should be a clean parse error, not an NPE");
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
