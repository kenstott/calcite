/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExclusionProbeTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static String relax(String sql) throws Exception {
    return ExclusionProbe.relax(sql, Collections.<String>emptySet());
  }

  private static ArrayNode rows(String... names) {
    ArrayNode arr = MAPPER.createArrayNode();
    for (String n : names) {
      arr.add(MAPPER.createObjectNode().put("state_name", n).put("v", 1));
    }
    return arr;
  }

  @Test void dropsIsNotNullKeepsOtherConjuncts() throws Exception {
    assertEquals("SELECT state_name FROM t b WHERE b.yr = 2020",
        relax("SELECT state_name FROM t b WHERE b.yr = 2020 AND b.min14 IS NOT NULL"));
  }

  @Test void dropsWholeWhereWhenEveryTermIsAnExclusion() throws Exception {
    assertEquals("SELECT * FROM t WHERE 1 = 1",
        relax("SELECT * FROM t WHERE x IS NOT NULL AND name <> 'Ohio'"));
  }

  @Test void dropsNotInAndNotEquals() throws Exception {
    assertEquals("SELECT * FROM t WHERE yr = 2020",
        relax("SELECT * FROM t WHERE yr = 2020 AND s NOT IN ('A', 'B') AND n != 'C'"));
  }

  @Test void leavesOrAlone() {
    assertThrows(ExclusionProbe.Unavailable.class,
        () -> relax("SELECT * FROM t WHERE x IS NOT NULL OR y = 1"));
  }

  @Test void keepsAliasedColumnsAndCasingAndMultiline() throws Exception {
    assertEquals("SELECT a.State_Name FROM Sch.t a\nWHERE a.yr = 1",
        relax("SELECT a.State_Name FROM Sch.t a\nWHERE a.yr = 1\n  AND a.z IS NOT NULL;"));
  }

  @Test void relaxesSubqueryWhere() throws Exception {
    assertEquals("SELECT * FROM (SELECT * FROM t WHERE 1 = 1) b",
        relax("SELECT * FROM (SELECT * FROM t WHERE m IS NOT NULL) b"));
  }

  @Test void differencedColumnNullFilterIsNotAnExclusion() {
    assertThrows(ExclusionProbe.Unavailable.class, () -> ExclusionProbe.relax(
        "SELECT * FROM t WHERE d IS NOT NULL", Collections.singleton("d")));
  }

  @Test void unparseableSqlIsReportedNotSkipped() {
    ObjectNode w = MAPPER.createObjectNode();
    ExclusionProbe.annotate(w, "SELEC nonsense WHERE", null, Collections.<String>emptySet(),
        s -> rows("A"));
    assertTrue(w.has("excluded_units_unavailable"));
    assertFalse(w.has("excluded_units"));
  }

  @Test void namesUnitsOnlyInTheUnfilteredResult() {
    ObjectNode w = MAPPER.createObjectNode();
    ExclusionProbe.annotate(w,
        "SELECT state_name, v FROM t b WHERE b.min14 IS NOT NULL", rows("Ohio", "Iowa"),
        Collections.<String>emptySet(),
        s -> {
          assertEquals("SELECT state_name, v FROM t b WHERE 1 = 1", s);
          return rows("Ohio", "Delaware", "Iowa", "Rhode Island");
        });
    assertEquals("state_name", w.get("excluded_unit_column").asText());
    assertEquals(2, w.get("excluded_unit_count").asInt());
    assertEquals("Delaware", w.get("excluded_units").get(0).asText());
    assertEquals("Rhode Island", w.get("excluded_units").get(1).asText());
  }

  @Test void reportsWhenNoRunnerIsAvailable() {
    ObjectNode w = MAPPER.createObjectNode();
    ExclusionProbe.annotate(w, "SELECT * FROM t WHERE x IS NOT NULL", null,
        Collections.<String>emptySet(), null);
    assertTrue(w.has("excluded_units_unavailable"));
  }

  @Test void diagnosticsWarningCarriesTheUnits() {
    ArrayNode kept = rows("Ohio");
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT state_name, v FROM t b WHERE b.min14 IS NOT NULL", kept, 500,
        s -> rows("Ohio", "Delaware"));
    ObjectNode w = null;
    for (com.fasterxml.jackson.databind.JsonNode x : env.get("diagnostics").get("warnings")) {
      if ("explicit_exclusion".equals(x.get("type").asText())) {
        w = (ObjectNode) x;
      }
    }
    assertEquals("Delaware", w.get("excluded_units").get(0).asText());
  }
}
