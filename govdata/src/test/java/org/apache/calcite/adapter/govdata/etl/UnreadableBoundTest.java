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
package org.apache.calcite.adapter.govdata.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The bound on unreadable declared tables a seed may be published over. */
@Tag("unit")
class UnreadableBoundTest {
  private static Map<String, Integer> counts(Object... kv) {
    Map<String, Integer> m = new LinkedHashMap<String, Integer>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], (Integer) kv[i + 1]);
    }
    return m;
  }

  @Test void fewUnreadableTablesDoNotBlockTheSeed() {
    List<String> b = GovDataModelVerificationRunner.unreadableBreaches(685, 14,
        counts("health", 60, "patents", 20), counts("health", 3, "patents", 3));
    assertTrue(b.isEmpty(), b.toString());
  }

  @Test void noUnreadableTablesPass() {
    assertTrue(GovDataModelVerificationRunner.unreadableBreaches(685, 0,
        counts("health", 60), counts()).isEmpty());
  }

  @Test void exactlyAtTheBoundPassesAndOneOverFails() {
    // 5% of 200 is 10
    assertTrue(GovDataModelVerificationRunner.unreadableBreaches(200, 10,
        counts("a", 100, "b", 100), counts("a", 5, "b", 5)).isEmpty());
    List<String> over = GovDataModelVerificationRunner.unreadableBreaches(200, 11,
        counts("a", 100, "b", 100), counts("a", 6, "b", 5));
    assertEquals(1, over.size());
    assertTrue(over.get(0).contains("exceeds the bound 10"), over.toString());
  }

  @Test void probingAnUnreadableTableDoesNotFailTheRunASecondTime() {
    Map<String, String> errored = new LinkedHashMap<String, String>();
    errored.put("sec.New_Table", "Object 'new_table' not found");
    Set<String> unreadable =
        new LinkedHashSet<String>(Arrays.asList("sec.new_table", "sec.other_new"));
    assertTrue(GovDataModelVerificationRunner.tableFailures(errored,
        Collections.singletonList("sec.other_new"), unreadable).isEmpty());
  }

  @Test void readableTablesThatErrorOrAreMissingAreNamed() {
    Map<String, String> errored = new LinkedHashMap<String, String>();
    errored.put("sec.a_view", "bad\n  column");
    List<String> f = GovDataModelVerificationRunner.tableFailures(errored,
        Collections.singletonList("sec.gone"),
        Collections.<String>singleton("sec.new_table"));
    assertEquals(
        Arrays.asList("sec.a_view: error: bad column", "sec.gone: declared but not exposed"), f);
  }

  @Test void aWholeUnreadableSchemaFailsEvenUnderTheBound() {
    List<String> b = GovDataModelVerificationRunner.unreadableBreaches(685, 3,
        counts("fiscal", 3, "health", 60), counts("fiscal", 3));
    assertEquals(1, b.size());
    assertTrue(b.get(0).contains("schema fiscal"), b.toString());
  }
}
