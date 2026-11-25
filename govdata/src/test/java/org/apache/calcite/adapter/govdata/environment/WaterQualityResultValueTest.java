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
package org.apache.calcite.adapter.govdata.environment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the {@code water_quality_samples.result_value} expression from environment-schema.yaml in
 * DuckDB: the column holds finite numbers only.
 */
@Tag("unit")
class WaterQualityResultValueTest {

  private static String expression() throws Exception {
    try (InputStream in = WaterQualityResultValueTest.class
        .getResourceAsStream("/environment/environment-schema.yaml")) {
      assertNotNull(in, "environment-schema.yaml on the classpath");
      JsonNode root = new ObjectMapper(new YAMLFactory()).readTree(in);
      for (JsonNode t : root.get("partitionedTables")) {
        if ("water_quality_samples".equals(t.get("name").asText())) {
          for (JsonNode c : t.get("columns")) {
            if ("result_value".equals(c.path("name").asText())) {
              return c.get("expression").asText();
            }
          }
        }
      }
    }
    throw new AssertionError("water_quality_samples.result_value not declared");
  }

  private static Double evaluate(String sourceText) throws Exception {
    String sql = "SELECT " + expression()
        + " FROM (SELECT CAST(? AS VARCHAR) AS \"ResultMeasureValue\") AS src";
    Class.forName("org.duckdb.DuckDBDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, sourceText);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        double value = rs.getDouble(1);
        return rs.wasNull() ? null : value;
      }
    }
  }

  @Test void finiteNumbersAreKeptAsPublished() throws Exception {
    assertEquals(Double.valueOf(12.5), evaluate("12.5"));
    assertEquals(Double.valueOf(-183000d), evaluate("-183000"));
    assertEquals(Double.valueOf(0d), evaluate("0"));
    assertEquals(Double.valueOf(6.857509e37), evaluate("6.857509e37"));
  }

  @Test void nonFiniteSourceTextIsNull() throws Exception {
    for (String text : new String[] {"Inf", "-Inf", "Infinity", "inf", "nan", "NaN", "1e999"}) {
      assertNull(evaluate(text), text);
    }
  }

  @Test void nonNumericAndMissingTextIsNull() throws Exception {
    assertNull(evaluate("ND"));
    assertNull(evaluate(""));
    assertNull(evaluate(null));
  }
}
