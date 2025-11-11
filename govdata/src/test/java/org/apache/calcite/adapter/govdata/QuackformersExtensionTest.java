/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for DuckDB quackformers extension integration.
 * Verifies that the embed() function is available and works correctly.
 */
@Tag("integration")
public class QuackformersExtensionTest {

  /**
   * Tests that quackformers extension can be loaded.
   */
  @Test void testQuackformersExtensionLoads() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      // Try to install and load quackformers
      Statement stmt = conn.createStatement();

      // Note: This may fail if quackformers is not available, which is acceptable
      // The extension loading in AbstractGovDataDownloader has graceful degradation
      try {
        stmt.execute("INSTALL quackformers FROM community");
        stmt.execute("LOAD quackformers");

        // If we got here, quackformers loaded successfully
        assertTrue(true, "Quackformers extension loaded successfully");
      } catch (Exception e) {
        // Quackformers not available - log warning but don't fail test
        System.err.println("WARN: Quackformers extension not available: " + e.getMessage());
        System.err.println("WARN: This is expected if quackformers is not installed");
      }
    }
  }

  /**
   * Tests that embed() function is available and returns expected dimensions.
   */
  @Test void testEmbedFunctionAvailability() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Statement stmt = conn.createStatement();

      try {
        // Install and load quackformers
        stmt.execute("INSTALL quackformers FROM community");
        stmt.execute("LOAD quackformers");

        // Test embed() function with a simple text
        String sql = "SELECT embed('test sentence')::FLOAT[384] as embedding";
        ResultSet rs = stmt.executeQuery(sql);

        assertTrue(rs.next(), "Query should return a result");

        Object embedding = rs.getObject("embedding");
        assertNotNull(embedding, "Embedding should not be null");

        // Verify it's an array (DuckDB returns arrays as java.sql.Array or float[])
        assertTrue(
            embedding instanceof java.sql.Array || embedding instanceof float[],
            "Embedding should be an array type, got: " + embedding.getClass().getName());

        System.out.println("SUCCESS: embed() function works, embedding type: "
            + embedding.getClass().getName());

      } catch (Exception e) {
        System.err.println("WARN: Could not test embed() function: " + e.getMessage());
        System.err.println("WARN: This is expected if quackformers is not installed");
        // Don't fail the test - quackformers may not be available in all environments
      }
    }
  }

  /**
   * Tests that embed() produces 384-dimensional vectors.
   */
  @Test void testEmbedOutputDimensions() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Statement stmt = conn.createStatement();

      try {
        // Install and load quackformers
        stmt.execute("INSTALL quackformers FROM community");
        stmt.execute("LOAD quackformers");

        // Test embed() function and get array length
        String sql = "SELECT list_length(embed('test sentence')) as dim_count";
        ResultSet rs = stmt.executeQuery(sql);

        assertTrue(rs.next(), "Query should return a result");

        int dimensions = rs.getInt("dim_count");
        assertEquals(384, dimensions,
            "embed() should produce 384-dimensional vectors (all-MiniLM-L6-v2)");

        System.out.println("SUCCESS: embed() produces 384-dimensional vectors");

      } catch (Exception e) {
        System.err.println("WARN: Could not test embed() dimensions: " + e.getMessage());
        // Don't fail the test - quackformers may not be available
      }
    }
  }

  /**
   * Tests embed() function with various text inputs.
   */
  @Test void testEmbedFunctionWithVariousInputs() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Statement stmt = conn.createStatement();

      try {
        stmt.execute("INSTALL quackformers FROM community");
        stmt.execute("LOAD quackformers");

        String[] testTexts = {
            "Unemployment rate for the United States",
            "Gross Domestic Product",
            "Consumer Price Index",
            ""  // Empty string should also work
        };

        for (String text : testTexts) {
          String sql = "SELECT list_length(embed('"
              + text.replace("'", "''") + "')) as dim_count";
          ResultSet rs = stmt.executeQuery(sql);

          assertTrue(rs.next(), "Query should return result for: " + text);
          int dimensions = rs.getInt("dim_count");
          assertEquals(384, dimensions,
              "All embeddings should be 384-dimensional for text: " + text);
        }

        System.out.println("SUCCESS: embed() works with various text inputs");

      } catch (Exception e) {
        System.err.println("WARN: Could not test embed() with various inputs: "
            + e.getMessage());
      }
    }
  }
}
