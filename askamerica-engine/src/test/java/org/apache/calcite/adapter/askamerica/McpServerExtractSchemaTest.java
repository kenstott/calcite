/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code extractSchema} exists only to reject SQL that names no schema at all; it must
 * not be read as the set of sources a query needs, since queries run against the
 * connection that mounts every allowed schema.
 */
class McpServerExtractSchemaTest {

  private static String extract(String sql) throws Exception {
    Method m = McpServer.class.getDeclaredMethod("extractSchema", String.class);
    m.setAccessible(true);
    return (String) m.invoke(null, sql);
  }

  @Test void qualifiedSqlPassesTheGuard() throws Exception {
    assertEquals("sec", extract("SELECT * FROM sec.filing_metadata"));
  }

  @Test void crossSchemaJoinPassesTheGuard() throws Exception {
    assertNotNull(extract("SELECT h.total_units, c.total_population "
        + "FROM housing.housing_permits_by_county h "
        + "JOIN census.acs_population c ON c.county_fips = h.county_fips"));
  }

  @Test void metaSchemasDoNotSatisfyTheGuardOnTheirOwn() throws Exception {
    assertNull(extract("SELECT * FROM information_schema.columns"));
  }

  @Test void unqualifiedSqlIsRejected() throws Exception {
    assertNull(extract("SELECT 1"));
  }

  /**
   * The source set a query connects with is the full allowed set, independent of which
   * schemas the SQL happens to name — this is what makes cross-schema joins resolvable.
   */
  @Test void allowedSchemasCoversEveryDefaultSchema() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        System.getenv("ASKAMERICA_SCHEMAS") == null,
        "ASKAMERICA_SCHEMAS overrides the default set");
    Method m = McpServer.class.getDeclaredMethod("allowedSchemas");
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Set<String> allowed = (java.util.Set<String>) m.invoke(null);
    for (String s : McpServer.DEFAULT_SCHEMAS.split(",")) {
      assertTrue(allowed.contains(s.trim()), "missing schema: " + s);
    }
    assertTrue(allowed.contains("census"));
    assertTrue(allowed.contains("housing"));
    assertTrue(allowed.contains("disasters"));
  }
}
