/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The diagnostics envelope is only worth shipping if it fires on the defects that actually
 * corrupt an answer and stays quiet on the queries that are fine. A warning that never fires
 * teaches nothing; one that fires on everything is ignored within two calls, which is worse
 * than silence because it also costs the host context on every result.
 *
 * <p>These exercise the detection logic directly rather than through a live catalog, so they
 * assert on behaviour rather than on whichever tables happen to be ingested.
 */
class QuestionDiagnosticsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static ArrayNode rows(String json) {
    try {
      return (ArrayNode) MAPPER.readTree(json);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static ArrayNode warningsOf(ObjectNode envelope) {
    return (ArrayNode) envelope.get("diagnostics").get("warnings");
  }

  private static JsonNode firstOfType(ObjectNode envelope, String type) {
    for (JsonNode w : warningsOf(envelope)) {
      if (type.equals(w.path("type").asText())) {
        return w;
      }
    }
    return null;
  }

  private static boolean hasType(ObjectNode envelope, String type) {
    return firstOfType(envelope, type) != null;
  }

  /** Fifty-one one-row-per-state records, the shape every state-grain analysis produces. */
  private static ArrayNode fiftyOneStates() {
    ArrayNode arr = MAPPER.createArrayNode();
    for (int i = 1; i <= 51; i++) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("state_fips", String.format("%02d", i));
      row.put("spending", 1000.0 + i);
      row.put("score", 200.0 + i);
      arr.add(row);
    }
    return arr;
  }

  // ── AC1: an uncontrolled state-level correlation says so, in one response ──

  @Test void uncontrolledStateCorrelationNamesSmallNAndTheMissingCovariate() {
    String sql = "SELECT corr(score, spending) AS r, COUNT(*) AS n "
        + "FROM edu.naep_state JOIN edu.finance_state USING (state_fips)";
    ObjectNode env = QuestionDiagnostics.forQuery(null, sql,
        rows("[{\"r\": 0.42, \"n\": 51}]"), 500);

    JsonNode smallN = firstOfType(env, "small_n");
    assertNotNull(smallN, "a 51-unit correlation must be flagged: " + env);
    assertEquals(51, smallN.get("n").asInt());

    JsonNode uncontrolled = firstOfType(env, "uncontrolled_confound");
    assertNotNull(uncontrolled, "corr() conditions on nothing and must say so: " + env);
    assertEquals("caution", uncontrolled.get("severity").asText());

    // The count came from the aggregate row, not from counting the one row returned.
    assertEquals(51, env.get("diagnostics").get("n").asInt());
  }

  @Test void aStatisticWithNoCountReportsTheCountAsUnknownRatherThanAsOne() {
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT corr(a, b) AS r FROM econ.series", rows("[{\"r\": 0.9}]"), 500);
    assertTrue(env.get("diagnostics").get("n").isNull(),
        "one returned row is not one observation: " + env);
    assertTrue(env.get("diagnostics").get("n_basis").asText().contains("COUNT(*)"),
        "the envelope should say how to supply the missing count");
    // Absent n is not evidence of a small sample, so small_n must not be invented.
    assertFalse(hasType(env, "small_n"));
  }

  @Test void aPlainGroupedResultCountsItsOwnRows() {
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT state_fips, AVG(score) AS score FROM edu.naep_state GROUP BY state_fips",
        fiftyOneStates(), 500);
    assertEquals(51, env.get("diagnostics").get("n").asInt());
    assertEquals("state", env.get("diagnostics").get("grain").asText());
    assertTrue(hasType(env, "small_n"));
  }

  @Test void aResultThatFilledItsLimitIsAFloorNotACountSoSmallNStaysQuiet() {
    ArrayNode capped = MAPPER.createArrayNode();
    for (int i = 0; i < 25; i++) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("state_fips", String.format("%02d", i));
      capped.add(row);
    }
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT state_fips FROM econ.series", capped, 25);
    assertFalse(hasType(env, "small_n"),
        "25 rows under a limit of 25 means at least 25, not exactly 25: " + env);
    assertTrue(env.get("diagnostics").get("n_basis").asText().contains("floor"));
  }

  @Test void countyGrainWithManyUnitsDrawsNoSmallNOrGrainWarning() {
    ArrayNode arr = MAPPER.createArrayNode();
    for (int i = 0; i < 3000; i++) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("county_fips", String.format("%05d", i));
      row.put("rate", 5.0);
      arr.add(row);
    }
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT county_fips, AVG(rate) AS rate FROM health.x GROUP BY county_fips", arr, 5000);
    assertEquals("county", env.get("diagnostics").get("grain").asText());
    assertFalse(hasType(env, "small_n"));
    assertFalse(hasType(env, "grain_mismatch"));
  }

  @Test void grainReadsTheFinestIdentifierPresentNotTheFirst() {
    assertEquals("county",
        QuestionDiagnostics.grainOf(Arrays.asList("state_fips", "county_fips", "value")));
    assertEquals("state", QuestionDiagnostics.grainOf(Arrays.asList("state_fips", "value")));
    assertNull(QuestionDiagnostics.grainOf(Arrays.asList("year", "value")));
  }

  // ── AC2: a fan-out join names the key it duplicated on ────────────────────

  @Test void partialKeyJoinIsReportedWithTheKeyAndTheDuplicationFactor() {
    // geo.counties keys on (county_fips, year) and holds one copy per TIGER vintage, so a
    // join on county_fips alone silently multiplies every downstream aggregate.
    ArrayNode joined = rows(
        "[{\"county_fips\":\"06037\",\"permits\":10},"
        + "{\"county_fips\":\"06037\",\"permits\":10},"
        + "{\"county_fips\":\"06037\",\"permits\":10},"
        + "{\"county_fips\":\"48201\",\"permits\":7},"
        + "{\"county_fips\":\"48201\",\"permits\":7}]");
    ObjectNode w = QuestionDiagnostics.detectFanout("geo.counties",
        Arrays.asList("county_fips", "year"), joined,
        Arrays.asList("county_fips", "permits"));

    assertNotNull(w, "three rows for one county is a fan-out");
    assertEquals("row_fanout", w.get("type").asText());
    assertEquals("high", w.get("severity").asText());
    assertEquals("county_fips", w.get("key").get(0).asText());
    assertEquals("year", w.get("missing_key_columns").get(0).asText());
    assertEquals(3, w.get("max_rows_per_key").asInt());
    assertEquals(2, w.get("distinct_keys").asInt());
  }

  @Test void aJoinCarryingTheWholeKeyIsNotFanout() {
    ArrayNode joined = rows(
        "[{\"county_fips\":\"06037\",\"year\":2022},"
        + "{\"county_fips\":\"06037\",\"year\":2023}]");
    assertNull(QuestionDiagnostics.detectFanout("geo.counties",
        Arrays.asList("county_fips", "year"), joined,
        Arrays.asList("county_fips", "year")),
        "one row per (county, year) is the table's own grain, not duplication");
  }

  @Test void aSingleColumnKeyCannotProduceThisDefect() {
    assertNull(QuestionDiagnostics.detectFanout("geo.state_ref",
        Collections.singletonList("state_fips"),
        rows("[{\"state_fips\":\"06\"},{\"state_fips\":\"06\"}]"),
        Collections.singletonList("state_fips")));
  }

  @Test void fanoutDetectionIsCaseInsensitiveAboutColumnNames() {
    ArrayNode joined = rows(
        "[{\"COUNTY_FIPS\":\"06037\"},{\"COUNTY_FIPS\":\"06037\"}]");
    ObjectNode w = QuestionDiagnostics.detectFanout("geo.counties",
        Arrays.asList("county_fips", "year"), joined,
        Collections.singletonList("COUNTY_FIPS"));
    assertNotNull(w, "a declared key is lowercase; the result label may not be");
    assertEquals("COUNTY_FIPS", w.get("key").get(0).asText());
  }

  @Test void distinctKeysAreCountedOnPartsNotOnConcatenation() {
    // "ab"+"c" and "a"+"bc" must stay two keys; a naive concatenation collapses them and
    // reports a fan-out where there is none.
    ArrayNode joined = rows(
        "[{\"a\":\"ab\",\"b\":\"c\"},{\"a\":\"a\",\"b\":\"bc\"}]");
    assertNull(QuestionDiagnostics.detectFanout("s.t",
        Arrays.asList("a", "b", "year"), joined, Arrays.asList("a", "b")));
  }

  // ── Pre-execution partial-key detection (critique_query) ──────────────────

  @Test void sqlNamingOnlyPartOfAMultiColumnKeyIsFlaggedBeforeItRuns() {
    ObjectNode w = QuestionDiagnostics.detectPartialKeyJoin("geo.counties",
        Arrays.asList("county_fips", "year"),
        "SELECT h.permits FROM housing.permits h "
        + "JOIN geo.counties c ON c.county_fips = h.county_fips");
    assertNotNull(w);
    assertEquals("row_fanout", w.get("type").asText());
    assertEquals("year", w.get("key_columns_not_named").get(0).asText());
  }

  @Test void sqlNamingTheWholeKeyIsNotFlagged() {
    assertNull(QuestionDiagnostics.detectPartialKeyJoin("geo.counties",
        Arrays.asList("county_fips", "year"),
        "SELECT h.permits FROM housing.permits h JOIN geo.counties c "
        + "ON c.county_fips = h.county_fips AND c.\"year\" = h.\"year\""));
  }

  @Test void aTableTheSqlNeverTouchesIsNotFlagged() {
    assertNull(QuestionDiagnostics.detectPartialKeyJoin("geo.counties",
        Arrays.asList("county_fips", "year"),
        "SELECT * FROM econ.employment WHERE state_fips = '06'"));
  }

  // ── Empty results and coverage ────────────────────────────────────────────

  @Test void anEmptyResultIsFlaggedAsCoverageNotAsZero() {
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT * FROM census.acs_population WHERE \"year\" = '2026'",
        MAPPER.createArrayNode(), 500);
    JsonNode w = firstOfType(env, "low_coverage");
    assertNotNull(w);
    assertEquals("high", w.get("severity").asText());
    assertTrue(w.get("note").asText().contains("not the same as a zero"));
  }

  @Test void aPartialUniverseIsReportedFromItsOwnCoverageColumn() {
    ArrayNode arr = rows(
        "[{\"state_fips\":\"06\",\"population_coverage_pct\":55.0,\"crimes\":10},"
        + "{\"state_fips\":\"48\",\"population_coverage_pct\":61.0,\"crimes\":12}]");
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT state_fips, population_coverage_pct, crimes FROM crime.state_totals", arr, 500);
    JsonNode w = firstOfType(env, "low_coverage");
    assertNotNull(w, "a 58%-covered universe cannot support a total: " + env);
    assertEquals("population_coverage_pct", w.get("column").asText());
    assertEquals(58.0, w.get("mean_coverage").asDouble(), 0.01);
  }

  @Test void fullCoverageDrawsNoWarning() {
    ArrayNode arr = rows("[{\"population_coverage_pct\":99.0},{\"population_coverage_pct\":97.0}]");
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT population_coverage_pct FROM crime.state_totals", arr, 500);
    assertFalse(hasType(env, "low_coverage"));
  }

  // ── Broken fields ─────────────────────────────────────────────────────────

  @Test void aProportionOutsideZeroToOneHundredIsFlagged() {
    ArrayNode arr = rows("[{\"poverty_pct\":14.2},{\"poverty_pct\":-3.0},{\"poverty_pct\":9.1}]");
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT poverty_pct FROM census.acs", arr, 500);
    JsonNode w = firstOfType(env, "broken_field");
    assertNotNull(w);
    assertEquals("out_of_domain", w.get("issue").asText());
    assertEquals("poverty_pct", w.get("column").asText());
    assertEquals(1, w.get("rows_affected").asInt());
  }

  @Test void aPercentChangeIsLegitimatelyNegativeAndOverOneHundred() {
    ArrayNode arr = rows("[{\"pct_change\":-12.0},{\"pct_change\":340.0}]");
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT pct_change FROM econ.series", arr, 500);
    assertFalse(hasType(env, "broken_field"),
        "flagging every percent change would make the warning noise");
  }

  @Test void anAlmostEntirelyNullColumnIsFlaggedAsUnusable() {
    ArrayNode arr = MAPPER.createArrayNode();
    for (int i = 0; i < 30; i++) {
      ObjectNode row = MAPPER.createObjectNode();
      if (i == 0) {
        row.put("income", 50000);
      } else {
        row.putNull("income");
      }
      arr.add(row);
    }
    ObjectNode env = QuestionDiagnostics.forQuery(null, "SELECT income FROM census.acs", arr, 500);
    JsonNode w = firstOfType(env, "broken_field");
    assertNotNull(w);
    assertEquals("null_dominant", w.get("issue").asText());
    assertEquals(29, w.get("null_rows").asInt());
  }

  // ── Stats-tool envelope ───────────────────────────────────────────────────

  @Test void twoNearDuplicateControlsAreReportedWithTheirCorrelation() {
    // poverty_pct and median_income as near-mirror images: each explains the other, so
    // neither gets credit and the model looks like nothing matters.
    int n = 80;
    double[][] cols = new double[n][2];
    for (int i = 0; i < n; i++) {
      cols[i][0] = i;
      cols[i][1] = -2.0 * i + 3;
    }
    ObjectNode env = QuestionDiagnostics.forExtraction("SELECT * FROM census.acs",
        Arrays.asList("poverty_pct", "median_income"), cols, n, n, 0);
    JsonNode w = firstOfType(env, "collinear_controls");
    assertNotNull(w, "r = -1.0 between two controls must be reported: " + env);
    assertEquals("poverty_pct", w.get("covariates").get(0).asText());
    assertEquals(-1.0, w.get("r").asDouble(), 1e-9);
  }

  @Test void orthogonalControlsDrawNoCollinearityWarning() {
    double[][] cols = {{1, 1}, {1, -1}, {-1, 1}, {-1, -1}};
    ObjectNode env = QuestionDiagnostics.forExtraction("SELECT * FROM census.acs",
        Arrays.asList("a", "b"), cols, 4, 4, 0);
    assertFalse(hasType(env, "collinear_controls"));
  }

  @Test void aSinglePredictorModelIsReportedAsUnconditioned() {
    ObjectNode env = QuestionDiagnostics.forExtraction("SELECT * FROM edu.naep",
        Collections.singletonList("spending"), new double[][]{{1}, {2}, {3}}, 3, 3, 0);
    assertTrue(hasType(env, "uncontrolled_confound"));
    assertTrue(hasType(env, "small_n"));
  }

  @Test void heavyNullDroppingIsReportedBecauseTheSurvivorsAreNotTheSelection() {
    ObjectNode env = QuestionDiagnostics.forExtraction("SELECT * FROM health.x",
        Arrays.asList("a", "b"), null, 400, 1000, 600);
    JsonNode w = firstOfType(env, "broken_field");
    assertNotNull(w);
    assertEquals("high", w.get("severity").asText());
    assertEquals(0.6, w.get("dropped_share").asDouble(), 1e-9);
  }

  @Test void aModestNullDropIsNotWorthAWarning() {
    ObjectNode env = QuestionDiagnostics.forExtraction("SELECT * FROM health.x",
        Arrays.asList("a", "b"), null, 990, 1000, 10);
    assertFalse(hasType(env, "broken_field"));
  }

  // ── Envelope invariants ───────────────────────────────────────────────────

  @Test void everyEnvelopeStatesThatSilenceIsNotValidity() {
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT 1 AS x FROM geo.state_ref", rows("[{\"x\":1}]"), 500);
    assertEquals(QuestionDiagnostics.BASIS_NOTE,
        env.get("diagnostics").get("basis").asText());
  }

  @Test void everyWarningCarriesATypeAndASeverityAHostCanRouteOn() {
    ArrayNode arr = rows("[{\"poverty_pct\":-3.0}]");
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT corr(a, b) AS r, poverty_pct FROM census.acs", arr, 500);
    assertTrue(warningsOf(env).size() > 0);
    for (JsonNode w : warningsOf(env)) {
      String severity = w.path("severity").asText();
      assertTrue(Arrays.asList("info", "caution", "high").contains(severity),
          "unroutable severity '" + severity + "' in " + w);
      assertFalse(w.path("type").asText().isEmpty());
      assertFalse(w.path("note").asText().isEmpty());
    }
  }

  @Test void aFailedCheckSaysSoRatherThanReturningAnEmptyWarningList() {
    ObjectNode env = QuestionDiagnostics.incomplete("connection refused");
    JsonNode w = firstOfType(env, "diagnostics_incomplete");
    assertNotNull(w);
    assertEquals("connection refused", w.get("reason").asText());
    assertTrue(w.get("note").asText().contains("not a clean result"));
  }

  @Test void aRefusalNamesTheRunnableAlternative() {
    ObjectNode env = QuestionDiagnostics.forRefusal("no_pushdown", "cannot execute",
        "use fetch_aligned_series");
    assertTrue(env.get("diagnostics").get("refused").asBoolean());
    JsonNode w = firstOfType(env, "no_pushdown");
    assertNotNull(w);
    assertEquals("use fetch_aligned_series", w.get("runnable_alternative").asText());
  }

  @Test void bothStatsFailureShapesAreRecognisedAsUnevaluable() {
    // The compile shape is the live one. The stub shape can only come from an older engine
    // jar, since these aggregates now have Java implementations; both must still be typed as
    // un-runnable so a host can route on them rather than parse prose.
    String compileShape = McpServer.compactErrorMessage(new RuntimeException(
        "No applicable constructor/method found for class "
        + "DuckDBStatsFunctions$CorrUdaf.result()"));
    String stubShape = McpServer.compactErrorMessage(new UnsupportedOperationException(
        "corr is a DuckDB-only aggregate and must be pushed down to the DuckDB engine; "
        + "it has no Calcite enumerable implementation."));
    assertTrue(QuestionDiagnostics.isPushdownFailure(compileShape));
    assertTrue(QuestionDiagnostics.isPushdownFailure(stubShape));
    // Each carries its OWN underlying cause now, rather than being flattened into one
    // generic sentence that named a cause neither of them had.
    assertTrue(compileShape.contains("No applicable constructor/method"),
        "the real cause must survive, got: " + compileShape);
    assertFalse(compileShape.contains("fetch_aligned_series"),
        "that tool needs warehouse tables and is no remedy for inline data");
    assertFalse(QuestionDiagnostics.isPushdownFailure("Table 'sec.nope' not found"));
  }

  // ── Coverage and vintage, against the real catalog ────────────────────────
  //
  // The two checks below read Catalog.coverage, so a made-up table name silently turns them
  // into no-ops — the check runs, finds no declared window, and returns without a warning,
  // which looks exactly like a passing test. These use tables the catalog actually declares.
  // Discovered rather than hardcoded, because ASKAMERICA_SCHEMAS is routinely narrowed and a
  // pinned table name would make this fail for a reason unrelated to what it tests.

  /** A real schema.table with both bounds declared, plus its window. */
  private static String[] boundedTable(int skip) {
    int seen = 0;
    for (String schema : McpServer.DEFAULT_SCHEMAS.split(",")) {
      String s = schema.trim();
      for (String table : Catalog.tableNames(s)) {
        ObjectNode cov = Catalog.coverage(s, table);
        if (cov != null && cov.has("first_year") && cov.has("last_year")) {
          if (seen++ < skip) {
            continue;
          }
          return new String[]{s + "." + table,
              cov.get("first_year").asText(), cov.get("last_year").asText()};
        }
      }
    }
    return null;
  }

  @Test void aYearPastATablesDeclaredWindowIsReportedAsUnpublished() {
    String[] t = boundedTable(0);
    assertNotNull(t, "the catalog must declare at least one bounded window");
    int beyond = Integer.parseInt(t[2]) + 5;
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT * FROM " + t[0] + " WHERE \"year\" = '" + beyond + "'",
        MAPPER.createArrayNode(), 500);

    JsonNode w = null;
    for (JsonNode candidate : warningsOf(env)) {
      if ("low_coverage".equals(candidate.path("type").asText()) && candidate.has("year")) {
        w = candidate;
      }
    }
    assertNotNull(w, "a year " + beyond + " past " + t[0] + "'s window " + t[1] + "-" + t[2]
        + " must be named, not returned as an empty result: " + env);
    assertEquals(t[0], w.get("table").asText());
    assertEquals(beyond, w.get("year").asInt());
    assertEquals(Integer.parseInt(t[2]), w.get("declared_last_year").asInt());
    assertEquals("high", w.get("severity").asText(),
        "an empty result outside coverage is the case most likely to be read as a zero");
  }

  @Test void aYearInsideTheWindowDrawsNoCoverageWarningOfItsOwn() {
    String[] t = boundedTable(0);
    assertNotNull(t);
    ArrayNode arr = rows("[{\"x\":1}]");
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT x FROM " + t[0] + " WHERE \"year\" = '" + t[2] + "'", arr, 500);
    for (JsonNode w : warningsOf(env)) {
      assertFalse("low_coverage".equals(w.path("type").asText()) && w.has("year"),
          "the last declared year is inside the window and must not be flagged: " + w);
    }
  }

  @Test void theVintageBlockCarriesTheDeclaredWindowOfEveryTableNamed() {
    String[] t = boundedTable(0);
    assertNotNull(t);
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT x FROM " + t[0], rows("[{\"x\":1}]"), 500);
    JsonNode tables = env.get("diagnostics").path("vintage").path("tables");
    assertEquals(t[1] + "-" + t[2], tables.path(t[0]).asText(),
        "a caller cannot tell an empty result from an unpublished period without this: " + env);
  }

  @Test void joiningTwoTablesWithDifferentWindowsReportsTheMisalignment() {
    String[] a = boundedTable(0);
    assertNotNull(a);
    String[] b = null;
    for (int skip = 1; skip < 400 && b == null; skip++) {
      String[] candidate = boundedTable(skip);
      if (candidate == null) {
        break;
      }
      if (!candidate[1].equals(a[1]) || !candidate[2].equals(a[2])) {
        b = candidate;
      }
    }
    assertNotNull(b, "the catalog must declare two tables with differing windows");

    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT * FROM " + a[0] + " x JOIN " + b[0] + " y ON x.state_fips = y.state_fips",
        rows("[{\"x\":1}]"), 500);
    JsonNode w = firstOfType(env, "vintage_misalignment");
    assertNotNull(w, "joining " + a[0] + " (" + a[1] + "-" + a[2] + ") to " + b[0]
        + " (" + b[1] + "-" + b[2] + ") spans different periods: " + env);
    JsonNode windows = w.get("declared_windows");
    assertEquals(a[1] + "-" + a[2], windows.path(a[0]).asText());
    assertEquals(b[1] + "-" + b[2], windows.path(b[0]).asText());
  }

  @Test void aSingleTableQueryIsNeverMisalignedWithItself() {
    String[] t = boundedTable(0);
    assertNotNull(t);
    ObjectNode env = QuestionDiagnostics.forQuery(null,
        "SELECT x FROM " + t[0], rows("[{\"x\":1}]"), 500);
    assertFalse(hasType(env, "vintage_misalignment"),
        "misalignment needs two windows to disagree");
  }

  // ── SQL surface reading ───────────────────────────────────────────────────

  @Test void referencedTablesSkipsMetaSchemas() {
    java.util.Set<String> refs = QuestionDiagnostics.referencedTables(
        "SELECT * FROM information_schema.columns c JOIN sec.filing_metadata f ON true");
    assertEquals(Collections.singleton("sec.filing_metadata"), refs);
  }

  @Test void yearLiteralsAreReadOutOfTheSql() {
    assertEquals(new java.util.LinkedHashSet<>(Arrays.asList(2019, 2023)),
        QuestionDiagnostics.yearLiterals(
            "SELECT * FROM econ.x WHERE \"year\" BETWEEN '2019' AND '2023'"));
  }

  @Test void aBareNumberThatIsNotAYearIsNotReadAsOne() {
    List<Integer> years = new java.util.ArrayList<>(
        QuestionDiagnostics.yearLiterals("SELECT * FROM econ.x WHERE amount > 150000"));
    assertTrue(years.isEmpty(), "found " + years);
  }

  @Test void bivariateAggregatesAreRecognisedRegardlessOfSpacing() {
    assertTrue(QuestionDiagnostics.bivariateAssociation("SELECT CORR (a,b) FROM t"));
    assertTrue(QuestionDiagnostics.bivariateAssociation("select regr_slope(y,x) from t"));
    assertFalse(QuestionDiagnostics.bivariateAssociation("SELECT AVG(a) FROM t"));
    // A column merely named like an aggregate is not a call to one.
    assertFalse(QuestionDiagnostics.bivariateAssociation("SELECT corr_id FROM t"));
  }
}
