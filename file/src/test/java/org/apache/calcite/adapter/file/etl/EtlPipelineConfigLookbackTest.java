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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies how {@code lookbackPeriods} is declared, and that its predecessor cannot be.
 *
 * <p>The rejection matters as much as the parse. The lookback moved out of {@code freshness:}
 * because a setting there was silently inert on tables that had no period axis; accepting the old
 * key — or ignoring it — would reproduce exactly that failure, so a stale key fails the load.
 */
@Tag("unit")
public class EtlPipelineConfigLookbackTest {

  private static Map<String, Object> pipeline(Map<String, Object> extra) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("name", "econ.demo");
    // source and materialize blocks are mandatory; their contents are irrelevant here.
    m.put("source", map("type", "http", "url", "https://example.invalid/data"));
    // Materialization is switched off: this test asserts config parsing, not writing, and a
    // disabled block skips the output/format validation that is irrelevant here.
    m.put("materialize", map("enabled", false));
    m.putAll(extra);
    return m;
  }

  private static Map<String, Object> map(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(String.valueOf(kv[i]), kv[i + 1]);
    }
    return m;
  }

  @Test void parsesAnIntegerLookback() {
    EtlPipelineConfig c =
        EtlPipelineConfig.fromMap(pipeline(map("lookbackPeriods", 6)));
    assertEquals(Integer.valueOf(6), c.getLookbackPeriods());
  }

  /** YAML may hand back a numeric string; it is accepted just as readily. */
  @Test void parsesANumericString() {
    EtlPipelineConfig c =
        EtlPipelineConfig.fromMap(pipeline(map("lookbackPeriods", "72")));
    assertEquals(Integer.valueOf(72), c.getLookbackPeriods());
  }

  @Test void absentLookbackLeavesItDisabled() {
    assertNull(EtlPipelineConfig.fromMap(pipeline(map())).getLookbackPeriods());
  }

  @Test void zeroOrNegativeIsRejected() {
    for (Object bad : new Object[] {0, -1}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> EtlPipelineConfig.fromMap(pipeline(map("lookbackPeriods", bad))));
      assertTrue(e.getMessage().contains("lookbackPeriods"), e.getMessage());
    }
  }

  @Test void nonNumericIsRejected() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> EtlPipelineConfig.fromMap(pipeline(map("lookbackPeriods", "soon"))));
    assertTrue(e.getMessage().contains("positive integer"), e.getMessage());
  }

  /**
   * The superseded key must fail the load rather than be honoured or ignored — silently ignoring it
   * would leave a table looking configured while doing nothing, which is the failure the move was
   * made to eliminate.
   */
  @Test void supersededFreshnessKeyFailsTheLoad() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> EtlPipelineConfig.fromMap(
            pipeline(map("freshness", map("type", "hash", "trailing_window", 2)))));
    assertTrue(e.getMessage().contains("econ.demo"), "must name the offending table");
    assertTrue(e.getMessage().contains("lookbackPeriods"), "must name the replacement");
  }

  /** A freshness block without the superseded key still parses normally. */
  @Test void freshnessWithoutTheSupersededKeyIsUnaffected() {
    EtlPipelineConfig c = EtlPipelineConfig.fromMap(
        pipeline(map("freshness", map("type", "hash"), "lookbackPeriods", 3)));
    assertEquals(FreshnessConfig.Type.HASH, c.getFreshness().getType());
    assertEquals(Integer.valueOf(3), c.getLookbackPeriods());
  }
}
