/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Year;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The coverage window and external-source pointers both exist to stop an empty result
 * from reading as a zero. These assert the two halves of that: that a lagging source
 * reports a ceiling below the current year, and that every external suggestion arrives
 * with the provenance caveat attached.
 *
 * <p>Everything here answers from the bundled catalog and needs no warehouse connection.
 * The one assertion that did — describe_table's live wiring to the coverage window — lives
 * in {@link DescribeTableCoverageIntegrationTest}, because a tag applies to every test in
 * the class and tagging this one {@code integration} would have hidden all the rest from
 * the unit run.
 */
@Tag("unit")
class CoverageAndExternalSourcesTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // ── Coverage ──────────────────────────────────────────────────────────────

  /**
   * A coverage window from whichever schemas this run has enabled, matching the given
   * filter. Hardcoding one table couples these assertions to ASKAMERICA_SCHEMAS, which is
   * routinely narrowed to route around a schema whose tables are not ingested yet.
   */
  private static ObjectNode findCoverage(java.util.function.Predicate<ObjectNode> filter) {
    for (String schema : McpServer.DEFAULT_SCHEMAS.split(",")) {
      for (String table : Catalog.tableNames(schema.trim())) {
        ObjectNode cov = Catalog.coverage(schema.trim(), table);
        if (cov != null && filter.test(cov)) {
          return cov;
        }
      }
    }
    return null;
  }

  @Test void laggingSourceReportsCeilingBelowCurrentYear() {
    // A source declaring a publication lag must show a ceiling below today, or a caller
    // reads "no 2026 rows" as "the thing being measured did not happen".
    ObjectNode cov = findCoverage(c -> c.path("publication_lag_years").asInt() > 0
        && c.has("last_year"));
    assertNotNull(cov, "some table must declare a publication lag");

    int currentYear = Year.now(ZoneOffset.UTC).getValue();
    int lag = cov.path("publication_lag_years").asInt();
    assertEquals(currentYear - lag, cov.path("last_year").asInt(),
        "ceiling is current year minus the declared lag: " + cov);
    assertTrue(cov.path("last_year").asInt() < currentYear,
        "a lagging source must not claim coverage through today");
  }

  @Test void floorRespectsTheSourcesOwnEarliestYear() {
    // minYear is the floor the source itself imposes; the configured start cannot
    // reach below it.
    ObjectNode cov = findCoverage(c -> c.has("source_earliest_year") && c.has("first_year"));
    assertNotNull(cov, "some table must declare a source floor");
    assertTrue(cov.path("first_year").asInt() >= cov.path("source_earliest_year").asInt(),
        "first_year never predates source_earliest_year: " + cov);
  }

  @Test void windowIsOrderedAndLabelledDeclared() {
    ObjectNode cov = findCoverage(c -> c.has("first_year") && c.has("last_year"));
    assertNotNull(cov, "some table must resolve a full window");
    assertTrue(cov.path("first_year").asInt() <= cov.path("last_year").asInt());
    assertEquals("declared", cov.path("basis").asText(),
        "basis must stay honest — this is the schema's declared window, not a row scan");
    assertTrue(cov.path("note").asText().contains("not a row scan"),
        "the note must not let a declared window be mistaken for a measured one");
  }

  @Test void everyDeclaredWindowResolvesWithoutBlowingUp() {
    // A schema whose year range resolves to no usable bound must yield a partial
    // window, not an exception — an unresolvable 'end' once NPE'd on unboxing, and
    // one bad YAML would have taken down describe_table for the whole schema.
    int resolved = 0;
    StringBuilder incomplete = new StringBuilder();
    for (String schema : McpServer.DEFAULT_SCHEMAS.split(",")) {
      for (String table : Catalog.tableNames(schema.trim())) {
        ObjectNode cov = Catalog.coverage(schema.trim(), table);
        if (cov == null) {
          continue;
        }
        resolved++;
        if (cov.has("first_year") && cov.has("last_year")) {
          assertTrue(cov.path("first_year").asInt() <= cov.path("last_year").asInt(),
              schema + "." + table + " has an inverted window: " + cov);
        } else if (!"partitionColumn".equals(cov.path("declared_from").asText())) {
          // A partitionColumn table legitimately has no declared floor — the global start
          // governs it and inventing one would be worse than omitting it. Every other
          // form states both bounds, so a missing one there is a resolution failure.
          incomplete.append("\n  ").append(schema.trim()).append('.').append(table)
              .append(" -> ").append(cov);
        }
      }
    }
    assertTrue(resolved > 50,
        "expected many partitioned tables to declare a year range, saw " + resolved);
    // A half-resolved window is the quiet failure mode here: describe_table still answers,
    // just without the bound that would have told the caller where coverage stops. The
    // YAMLs spell bounds three ways today; a fourth spelling would land right here.
    assertEquals(0, incomplete.length(),
        "every declared window must resolve both bounds, but these did not:" + incomplete);
  }

  @Test void tableWithoutAYearRangeReportsNoCoverage() {
    // Absent metadata must yield null, never an invented window.
    assertNull(Catalog.coverage("census", "no_such_table_at_all"));
    assertNull(Catalog.coverage("no_such_schema", "whatever"));
  }

  @Test void observedWindowIsAbsentUntilMeasuredAndNeverBlocks() {
    // The first ask must return immediately with nothing observed — the whole point of
    // the lazy probe is that describe_table never waits on a scan. The second ask
    // reports a status rather than silently re-running the probe on every call.
    long started = System.currentTimeMillis();
    ObjectNode first = IngestedYears.observed("census", "acs_population", "year");
    long elapsed = System.currentTimeMillis() - started;
    assertNull(first, "the first ask must not wait for the probe");
    assertTrue(elapsed < 1000, "scheduling must be non-blocking, took " + elapsed + "ms");

    ObjectNode second = IngestedYears.observed("census", "acs_population", "year");
    assertNotNull(second, "once scheduled, the state must be reportable");
    String status = second.path("status").asText();
    assertTrue("measuring".equals(status) || "measured".equals(status)
        || "unavailable".equals(status) || "empty".equals(status),
        "unexpected probe status: " + status);
  }

  // ── External sources ──────────────────────────────────────────────────────

  @Test void everyResultCarriesTheProvenanceCaveat() throws Exception {
    JsonNode out = MAPPER.readTree(ExternalSources.suggest("weather forecast", 5));
    String caveat = out.path("caveat").asText();
    assertTrue(caveat.contains("NOT askamerica data"), "the caveat must lead with provenance");
    assertTrue(caveat.contains("report_issue"), "a persistent gap should route to report_issue");
  }

  @Test void topicRanksTheRelevantSourceFirst() throws Exception {
    JsonNode out = MAPPER.readTree(ExternalSources.suggest("weather forecast alert", 3));
    assertTrue(out.path("sources").size() > 0, "a covered topic must match something");
    assertEquals("nws", out.path("sources").get(0).path("id").asText(),
        "the NWS forecast API is the best match for a forecast question");
  }

  @Test void schemaNameMatchesTheSourceThatComplementsIt() throws Exception {
    JsonNode out = MAPPER.readTree(ExternalSources.suggest("sec", 3));
    assertEquals("edgar", out.path("sources").get(0).path("id").asText());
  }

  @Test void unmatchedTopicRefusesRatherThanImprovises() throws Exception {
    JsonNode out = MAPPER.readTree(
        ExternalSources.suggest("zzz nonexistent topic qqq", 5));
    assertEquals(0, out.path("sources").size());
    assertTrue(out.path("note").asText().contains("unavailable"),
        "an unmatched topic must steer toward saying so, not toward guessing");
  }

  @Test void everyCatalogedSourceIsKeylessAndDocumented() throws Exception {
    // The whole premise is that the caller can actually call these without credentials.
    JsonNode out = MAPPER.readTree(ExternalSources.suggest("", 50));
    assertTrue(out.path("sources").size() >= 10, "the catalog should be substantive");
    for (JsonNode s : out.path("sources")) {
      String id = s.path("id").asText();
      String auth = s.path("auth").asText();
      assertTrue("none".equals(auth) || "optional".equals(auth),
          id + " must be usable without a mandatory API key, was auth=" + auth);
      assertTrue(s.path("base_url").asText().startsWith("https://"), id + " needs an https URL");
      assertFalse(s.path("docs").asText().isEmpty(), id + " needs a docs link");
      assertFalse(s.path("gap").asText().isEmpty(), id + " must say what gap it fills");
    }
  }

  @Test void limitIsHonoured() throws Exception {
    JsonNode out = MAPPER.readTree(ExternalSources.suggest("", 3));
    assertEquals(3, out.path("sources").size());
  }

  /**
   * Reproduces the reported bug: a nonsense topic sharing no real relevance with any source
   * still matched NWS and openFDA at a full-confidence-looking score. Root cause: "for" (a
   * stopword) is a substring of the NWS topic keyword "forecast" and of "forecasts"/"forecast"
   * in its prose fields — three incidental contains() hits added up to a nonzero score with no
   * floor to filter it out.
   */
  @Test void nonsenseTopicWithAStopwordInsideARealKeywordRefuses() throws Exception {
    JsonNode out = MAPPER.readTree(
        ExternalSources.suggest("recipe for chocolate chip cookies", 5));
    assertEquals(0, out.path("sources").size(),
        "\"for\" incidentally substring-matching \"forecast\" must not count as relevance");
    assertTrue(out.path("note").asText().contains("unavailable"));
  }

  @Test void aGenuineStopwordOnlyTopicAlsoRefuses() throws Exception {
    JsonNode out = MAPPER.readTree(ExternalSources.suggest("the of and for", 5));
    assertEquals(0, out.path("sources").size());
  }
}
