/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Loads the catalog the way the shipped engine jar does: with only the engine's own resources
 * on the classpath. Run by the {@code shippedClasspathTest} task, which leaves :govdata and
 * :file off the classpath; the ordinary test task has both and would hide a resource the jar
 * does not bundle.
 */
@Tag("shippedClasspath")
class CatalogShippedClasspathTest {

  @Test void seedListingComesFromTheEnginesOwnResources() {
    URL seed = Catalog.class.getResource("/duckdb/seed/govdata-seed.schema");
    assertNotNull(seed, "the seed schema listing is not bundled in the engine's resources");
    assertTrue(seed.toString().contains("askamerica-engine"),
        "the seed listing must come from the engine module, was " + seed);
  }

  @Test void searchCatalogFindsHitsFromTheBundledSeed() {
    ArrayNode hits = Catalog.search("occupational employment by industry", 10);
    assertTrue(hits.size() > 0, "search_catalog returned no hits from the bundled catalog");
  }

  @Test void catalogIsAvailable() {
    assertEquals(true, Catalog.available());
  }
}
