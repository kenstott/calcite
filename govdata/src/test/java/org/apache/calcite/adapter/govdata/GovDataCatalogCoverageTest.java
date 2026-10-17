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
package org.apache.calcite.adapter.govdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the contract that a table's reported coverage is what it actually holds.
 *
 * <p>The declared window routinely disagrees with reality in both directions — a source that
 * has not published the newest year leaves it wide, a backfill below the configured floor
 * leaves it narrow — and reporting the declaration as the answer is what let a table
 * advertising 2023-2024 while holding 2011-2024 look correct.
 */
@Tag("unit")
public class GovDataCatalogCoverageTest {

  private static JsonNode coverageOf(ArrayNode catalog, String schema, String table) {
    for (JsonNode s : catalog) {
      if (!schema.equals(s.path("schema").asText())) {
        continue;
      }
      for (JsonNode t : s.path("tables")) {
        if (table.equals(t.path("name").asText())) {
          return t.get("coverage");
        }
      }
    }
    return null;
  }

  /**
   * f33_district_finance is the case this behavior was added for: its declared floor is the
   * yearRange start, while what is loaded reaches back further.
   */
  @Test void reportsObservedCoverageAlongsideDeclared() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("edu"));
    JsonNode cov = coverageOf(catalog, "edu", "f33_district_finance");
    assertNotNull(cov, "f33_district_finance should have a coverage node");

    assertTrue(cov.hasNonNull("observed_first_year"), "observed bounds must be emitted");
    assertTrue(cov.hasNonNull("observed_last_year"), "observed bounds must be emitted");
    assertEquals("observed", cov.path("authoritative").asText(),
        "consumers must be told which bounds to trust");

    // The declared side is retained, not replaced - it explains what the pipeline aimed at.
    assertTrue(cov.hasNonNull("start"), "declared start is still reported");
  }

  /**
   * A bulk-sourced table declares no yearRange at all and reaches the partitionColumn branch,
   * which emits no floor whatsoever. That branch is exactly where a caller was left with
   * first_year null, so it must carry observed bounds too.
   */
  @Test void partitionColumnTablesAlsoCarryObservedBounds() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("econ"));
    JsonNode cov = coverageOf(catalog, "econ", "state_gdp");
    assertNotNull(cov, "state_gdp should have a coverage node");
    assertEquals("partitionColumn", cov.path("form").asText(),
        "state_gdp is bulk-sourced and declares no year dimension");
    assertTrue(cov.hasNonNull("observed_first_year"),
        "the branch that emits no declared floor must still report what is there");
    assertEquals("observed", cov.path("authoritative").asText());
  }

  /** The measurement has a timestamp; dropping it would present stale data as current. */
  @Test void observedBoundsCarryTheirCheckedAtTimestamp() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("edu"));
    JsonNode cov = coverageOf(catalog, "edu", "f33_district_finance");
    assertNotNull(cov);
    assertTrue(cov.hasNonNull("observed_checked_at"),
        "observed coverage is a measurement, so it must say when it was taken");
  }

  /**
   * A table with no time axis must SAY so. An omitted coverage node reads equally as "no time
   * axis", "coverage unknown", and "someone forgot to declare it" — a caller deciding whether a
   * table can answer a year-over-year question cannot act on that ambiguity.
   */
  @Test void saysExplicitlyWhenATableHasNoTimeAxis() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("geo", "ref", "edu"));
    int checked = 0;
    for (JsonNode s : catalog) {
      for (JsonNode t : s.path("tables")) {
        JsonNode cov = t.get("coverage");
        assertNotNull(cov, t.path("name").asText() + " must always carry a coverage node");
        if ("none".equals(cov.path("form").asText())) {
          assertTrue(cov.path("time_varying").isBoolean(), "must state time_varying");
          assertTrue(!cov.path("time_varying").asBoolean(), "form=none means not time-varying");
          assertTrue(cov.path("note").asText().length() > 0, "must explain, not just flag");
          checked++;
        }
      }
    }
    assertTrue(checked > 0, "expected at least one table with no year axis across geo/ref/edu");
  }

  /**
   * drinking_water_violations declares a coverageWindow because 19 rows carry 1900-1930
   * placeholders; its observed range must start at the real first year and say how many rows the
   * window left out, not present the range as the whole table.
   */
  @Test void coverageWindowExclusionIsReported() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("environment"));
    JsonNode cov = coverageOf(catalog, "environment", "drinking_water_violations");
    assertNotNull(cov, "drinking_water_violations should have a coverage node");
    assertEquals(1976, cov.path("observed_first_year").asInt());
    assertTrue(cov.path("observed_excluded_rows").asInt() > 0, "excluded rows must be reported");
    assertEquals(1970, cov.path("observed_window_first_year").asInt());
    for (JsonNode s : catalog) {
      for (JsonNode t : s.path("tables")) {
        if ("drinking_water_violations".equals(t.path("name").asText())) {
          assertTrue(t.path("comment").asText().contains("not counting"),
              "the table description must carry the exclusion");
        }
      }
    }
  }

  /** A table with no observedCoverage block must not claim authority it does not have. */
  @Test void omitsAuthoritativeWhenNothingWasObserved() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("edu"));
    for (JsonNode s : catalog) {
      for (JsonNode t : s.path("tables")) {
        JsonNode cov = t.get("coverage");
        if (cov != null && !cov.hasNonNull("observed_first_year")) {
          assertTrue(cov.path("authoritative").isMissingNode(),
              t.path("name").asText() + " has no observed bounds so must not be authoritative");
        }
      }
    }
  }

  /** A view declares no column list, so a year it selects must not be reported as absent. */
  @Test void viewSelectingYearIsTimeVarying() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("census"));
    for (String view : Arrays.asList("education_attainment", "unemployment_rate")) {
      JsonNode cov = coverageOf(catalog, "census", view);
      assertNotNull(cov, view + " must carry a coverage node");
      assertEquals("columnOnly", cov.path("form").asText(), view + " selects year");
      assertTrue(cov.path("time_varying").asBoolean(), view + " is time-varying");
    }
  }

  /** A table fanned out by congress has a time axis (two-year terms) and must not claim none. */
  @Test void congressDimensionedTableIsTimeVarying() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("officials"));
    JsonNode cov = coverageOf(catalog, "officials", "members");
    assertNotNull(cov, "members must carry a coverage node");
    assertEquals("congressRange", cov.path("form").asText());
    assertEquals("congress", cov.path("column").asText());
    assertTrue(cov.path("time_varying").asBoolean(), "congress is a time dimension");
    assertTrue(!cov.path("note").asText().contains("cannot be filtered"),
        "must not tell callers the table cannot be trended over time");
  }

  /**
   * house_price_index carries data_year + period, not a column named year, and declares it via
   * timeColumn. It must not be reported as having no time axis (#958).
   */
  @Test void declaredTimeColumnTableIsTimeVarying() {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList("housing"));
    JsonNode cov = coverageOf(catalog, "housing", "house_price_index");
    assertNotNull(cov, "house_price_index must carry a coverage node");
    assertEquals("columnOnly", cov.path("form").asText());
    assertEquals("data_year", cov.path("column").asText());
    assertTrue(cov.path("time_varying").asBoolean(), "data_year is a time dimension");
    assertTrue(!cov.path("note").asText().contains("cannot be filtered"),
        "must not tell callers the table cannot be trended over time");
  }

  /**
   * The observed range of a table whose timeColumn is not an integer year must reach the catalog
   * the same way a year column's does (#963): a DATE column (enforcement_actions.effective_date),
   * a string date (kev_catalog.date_added) and a school-year label
   * (cdc_school_vaccination_coverage.school_year) each carry first/last year, and the table
   * description repeats them.
   */
  @Test void dateAndLabelTimeColumnsCarryObservedBounds() {
    assertObservedFor("banking", "enforcement_actions", "effective_date");
    assertObservedFor("cyber_vuln", "kev_catalog", "date_added");
    assertObservedFor("health", "cdc_school_vaccination_coverage", "school_year");
  }

  private static void assertObservedFor(String schema, String table, String column) {
    ArrayNode catalog = GovDataCatalog.build(Arrays.asList(schema));
    JsonNode cov = coverageOf(catalog, schema, table);
    assertNotNull(cov, table + " must carry a coverage node");
    assertEquals(column, cov.path("column").asText());
    assertTrue(cov.hasNonNull("observed_first_year") && cov.hasNonNull("observed_last_year"),
        table + " must carry the measured first/last year");
    assertTrue(cov.path("observed_first_year").asInt() <= cov.path("observed_last_year").asInt());
    assertEquals("observed", cov.path("authoritative").asText());
    for (JsonNode s : catalog) {
      for (JsonNode t : s.path("tables")) {
        if (table.equals(t.path("name").asText())) {
          assertTrue(t.path("comment").asText().contains("Observed coverage: "),
              table + " description must state the observed coverage");
        }
      }
    }
  }

  /**
   * Every table or view that declares {@code timeColumn} must name one of its own columns. The
   * catalog throws for the schema it is asked about, but only when that schema is requested; this
   * walks every bundled schema YAML so a typo in any of them fails the build rather than the
   * first caller to describe that schema.
   */
  @Test void everyDeclaredTimeColumnNamesAnExistingColumn() throws Exception {
    java.net.URL anchor = GovDataCatalog.class.getResource(GovDataCatalog.resourcePath("ref"));
    assertNotNull(anchor, "ref schema resource must be on the classpath");
    assertEquals("file", anchor.getProtocol(), "schema resources must be an exploded directory");
    Path root = Paths.get(anchor.toURI()).getParent().getParent();
    List<Path> yamls;
    try (Stream<Path> walk = Files.walk(root)) {
      yamls = walk.filter(p -> p.getFileName().toString().endsWith("-schema.yaml"))
          .collect(Collectors.toList());
    }
    assertTrue(yamls.size() >= 25, "expected the bundled schema YAMLs, found " + yamls.size());

    List<String> bad = new ArrayList<>();
    int declared = 0;
    for (Path yaml : yamls) {
      JsonNode doc;
      try (InputStream is = Files.newInputStream(yaml)) {
        doc = YamlUtils.parseYamlOrJson(is, yaml.toString());
      }
      for (String section : Arrays.asList("partitionedTables", "tables", "views")) {
        for (JsonNode t : doc.path(section)) {
          if (!t.hasNonNull("timeColumn")) {
            continue;
          }
          declared++;
          String col = t.get("timeColumn").asText();
          boolean found = false;
          for (JsonNode c : t.path("columns")) {
            if (col.equalsIgnoreCase(c.path("name").asText())) {
              found = true;
              break;
            }
          }
          if (!found) {
            bad.add(yaml.getFileName() + ": " + t.path("name").asText() + " -> " + col);
          }
        }
      }
    }
    assertTrue(declared >= 50, "expected the declared timeColumn tables, found " + declared);
    assertTrue(bad.isEmpty(), "timeColumn names a column the table does not have: " + bad);
  }
}
