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

  @Test void pagedResultIsComparedAgainstTheFullOriginalNotThePage() {
    final String sql = "SELECT state_name, v FROM t b WHERE b.min14 IS NOT NULL ORDER BY 1";
    ObjectNode env = QuestionDiagnostics.forQuery(null, sql, rows("Iowa"), 1, 2,
        s -> s.equals(sql)
            ? rows("Delaware", "Iowa", "Ohio") : rows("Delaware", "Iowa", "Ohio", "Utah"));
    ObjectNode w = null;
    for (com.fasterxml.jackson.databind.JsonNode x : env.get("diagnostics").get("warnings")) {
      if ("explicit_exclusion".equals(x.get("type").asText())) {
        w = (ObjectNode) x;
      }
    }
    assertEquals(1, w.get("excluded_unit_count").asInt());
    assertEquals("Utah", w.get("excluded_units").get(0).asText());
  }

  @Test void predicateThatRemovedNothingProducesNoWarning() {
    final String sql = "SELECT state_name, v FROM t b WHERE b.min14 IS NOT NULL";
    ObjectNode w = MAPPER.createObjectNode();
    assertTrue(ExclusionProbe.annotate(w, sql, rows("Ohio", "Iowa"),
        Collections.<String>emptySet(), s -> rows("Ohio", "Iowa")));
    ObjectNode env = QuestionDiagnostics.forQuery(null, sql, rows("Ohio", "Iowa"), 500,
        s -> rows("Ohio", "Iowa"));
    for (com.fasterxml.jackson.databind.JsonNode x : env.get("diagnostics").get("warnings")) {
      assertFalse("explicit_exclusion".equals(x.get("type").asText()));
    }
  }

  @Test void aggregateWithUnchangedCountsProducesNoWarning() {
    ArrayNode agg = MAPPER.createArrayNode();
    agg.add(MAPPER.createObjectNode().put("river", "Ohio").put("locks", 20));
    ObjectNode w = MAPPER.createObjectNode();
    assertTrue(ExclusionProbe.annotate(w,
        "SELECT river, COUNT(*) AS locks FROM t WHERE year_open IS NOT NULL GROUP BY river",
        agg, Collections.<String>emptySet(), s -> agg));
  }

  /** Issue #961 query shape: a single-row aggregate whose alias contains "county". */
  private static final String ISSUE_961_SQL =
      "WITH cz AS (SELECT c.county_fips, z.cbsa_code, SUM(c.res_ratio*z.res_ratio) AS share "
      + "FROM geo.county_zip_crosswalk c JOIN geo.zip_cbsa_crosswalk z ON c.zip=z.zip "
      + "AND z.\"year\"='2020' WHERE c.\"year\"='2020' AND z.cbsa_code <> '99999' "
      + "GROUP BY c.county_fips, z.cbsa_code) "
      + "SELECT COUNT(*) AS county_cbsa_rows, COUNT(DISTINCT county_fips) AS counties "
      + "FROM cz WHERE share > 0.5";

  private static ArrayNode countRow(int n) {
    ArrayNode arr = MAPPER.createArrayNode();
    arr.add(MAPPER.createObjectNode().put("county_cbsa_rows", n).put("counties", n));
    return arr;
  }

  @Test void aggregateCountIsNeverTheUnitColumn() {
    ObjectNode w = MAPPER.createObjectNode();
    ExclusionProbe.annotate(w, ISSUE_961_SQL, countRow(1914), Collections.<String>emptySet(),
        s -> countRow(3211));
    assertFalse(w.has("excluded_units"));
    assertFalse(w.has("excluded_unit_column"));
    assertTrue(w.get("excluded_units_unavailable").asText().contains("aggregate"));
  }

  @Test void diagnosticsForIssue961EmitNoInventedUnit() {
    ObjectNode env = QuestionDiagnostics.forQuery(null, ISSUE_961_SQL, countRow(1914), 500,
        s -> countRow(3211));
    boolean sawWarning = false;
    for (com.fasterxml.jackson.databind.JsonNode x : env.get("diagnostics").get("warnings")) {
      if ("explicit_exclusion".equals(x.get("type").asText())) {
        sawWarning = true;
        assertFalse(x.has("excluded_units"));
        assertTrue(x.has("excluded_units_unavailable"));
      }
    }
    assertTrue(sawWarning);
  }

  @Test void groupKeyIdentifierStillLabelsUnitsBesideAnAggregate() {
    ArrayNode kept = MAPPER.createArrayNode();
    kept.add(MAPPER.createObjectNode().put("county_fips", "01001").put("county_rows", 2));
    ArrayNode wide = kept.deepCopy();
    wide.add(MAPPER.createObjectNode().put("county_fips", "01003").put("county_rows", 1));
    ObjectNode w = MAPPER.createObjectNode();
    ExclusionProbe.annotate(w,
        "SELECT county_fips, COUNT(*) AS county_rows FROM t WHERE x IS NOT NULL "
        + "GROUP BY county_fips", kept, Collections.<String>emptySet(), s -> wide);
    assertEquals("county_fips", w.get("excluded_unit_column").asText());
    assertEquals("01003", w.get("excluded_units").get(0).asText());
  }

  @Test void computedExpressionAliasIsNotAUnitColumn() {
    ArrayNode kept = MAPPER.createArrayNode();
    kept.add(MAPPER.createObjectNode().put("state_share", 0.5));
    ArrayNode wide = MAPPER.createArrayNode();
    wide.add(MAPPER.createObjectNode().put("state_share", 0.7));
    ObjectNode w = MAPPER.createObjectNode();
    ExclusionProbe.annotate(w, "SELECT a / b AS state_share FROM t WHERE a IS NOT NULL", kept,
        Collections.<String>emptySet(), s -> wide);
    assertFalse(w.has("excluded_units"));
    assertTrue(w.has("excluded_units_unavailable"));
  }
}
