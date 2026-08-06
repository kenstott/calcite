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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * describe_table's wiring from a live schema through to the declared coverage window.
 *
 * <p>Split out of {@link CoverageAndExternalSourcesTest} because it is the only assertion
 * there that opens a real warehouse connection: building the census schema takes minutes
 * against S3 and needs credentials. Tagged {@code integration} so the unit run stays fast,
 * which is what previously kept the whole untagged class out of BOTH tagged runs — the
 * catalog-only assertions included.
 */
@Tag("integration")
class DescribeTableCoverageIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test void describeTableSurfacesCoverageForAPartitionedTable() throws Exception {
    // describeTable opens a live schema connection for information_schema, which needs
    // credentials this environment may not have — so a connection failure skips rather
    // than passes silently, and only a real response satisfies the assertion.
    Field logField = McpServer.class.getDeclaredField("log");
    logField.setAccessible(true);
    boolean logWasUnset = logField.get(null) == null;
    if (logWasUnset) {
      logField.set(null, new PrintStream(new ByteArrayOutputStream()));
    }
    try {
      Method m =
          McpServer.class.getDeclaredMethod("describeTable", String.class, String.class);
      m.setAccessible(true);
      String json;
      try {
        json = (String) m.invoke(null, "census", "acs_population");
      } catch (InvocationTargetException e) {
        Assumptions.abort("no live schema connection here: " + e.getCause());
        return;
      }
      JsonNode out = MAPPER.readTree(json);
      assertTrue(out.has("coverage"), "describe_table must carry the coverage window");
      assertEquals("year", out.path("coverage").path("column").asText());
      assertTrue(out.path("coverage").path("last_year").asInt() > 0,
          "the window must resolve to real years, not an empty shell");
    } finally {
      if (logWasUnset) {
        logField.set(null, null);
      }
    }
  }
}
