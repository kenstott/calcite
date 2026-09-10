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

import java.util.Arrays;

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
}
