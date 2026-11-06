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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@code excludeCombinations} drops the listed dimension-value combinations and nothing else. */
@Tag("unit")
class EtlPipelineConfigExcludeCombinationsTest {

  private static Map<String, Object> table(Object excludeCombinations) {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("type", "http");
    source.put("url", "https://example.invalid/data");
    Map<String, Object> dims = new LinkedHashMap<String, Object>();
    dims.put("frequency", Arrays.asList("A", "Q"));
    dims.put("tablename", Arrays.asList("T1", "T2", "T3"));
    Map<String, Object> table = new LinkedHashMap<String, Object>();
    table.put("name", "demo");
    table.put("source", source);
    table.put("dimensions", dims);
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("enabled", false);
    table.put("materialize", materialize);
    if (excludeCombinations != null) {
      table.put("excludeCombinations", excludeCombinations);
    }
    return table;
  }

  private static Map<String, Object> entry(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(String.valueOf(kv[i]), kv[i + 1]);
    }
    return m;
  }

  private static Map<String, String> combo(String frequency, String tablename) {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("frequency", frequency);
    m.put("tablename", tablename);
    return m;
  }

  @Test void noEntriesExcludeNothing() {
    EtlPipelineConfig config = EtlPipelineConfig.fromMap(table(null));
    assertTrue(config.getExcludeCombinations().isEmpty());
    assertFalse(config.isExcluded(combo("Q", "T1")));
  }

  @Test void entryMustMatchEveryDimensionItNames() {
    EtlPipelineConfig config = EtlPipelineConfig.fromMap(table(Collections.singletonList(
        entry("frequency", "Q", "tablename", Arrays.asList("T2", "T3")))));
    assertTrue(config.isExcluded(combo("Q", "T2")));
    assertTrue(config.isExcluded(combo("Q", "T3")));
    assertFalse(config.isExcluded(combo("A", "T2")));
    assertFalse(config.isExcluded(combo("Q", "T1")));
  }

  @Test void anyEntryExcludes() {
    EtlPipelineConfig config = EtlPipelineConfig.fromMap(table(Arrays.asList(
        entry("frequency", "Q", "tablename", "T2"), entry("tablename", "T1"))));
    assertTrue(config.isExcluded(combo("A", "T1")));
    assertTrue(config.isExcluded(combo("Q", "T2")));
    assertFalse(config.isExcluded(combo("A", "T2")));
  }

  @Test void malformedEntriesAreRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> EtlPipelineConfig.fromMap(table("frequency")));
    assertThrows(IllegalArgumentException.class,
        () -> EtlPipelineConfig.fromMap(table(Collections.singletonList("frequency"))));
    assertThrows(IllegalArgumentException.class,
        () -> EtlPipelineConfig.fromMap(table(Collections.singletonList(
            entry("frequency", Collections.emptyList())))));
  }
}
