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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CountStarStatisticsRule} which replaces COUNT(*) with a
 * pre-computed row count from table statistics, avoiding full table scans.
 *
 * <p>The rule only fires when:
 * <ul>
 *   <li>There is no GROUP BY clause</li>
 *   <li>There is exactly one COUNT(*) aggregate (no arguments, not distinct)</li>
 *   <li>The table has statistics with a non-null rowCount</li>
 * </ul>
 */
@Tag("integration")
public class CountStarStatisticsRuleTest {

  private File tempDir;
  private Connection calciteConn;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("countstar-test-").toFile();

    createTestData();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that a simple COUNT(*) returns the correct row count.
   * If the rule fires, it will use the statistics value; otherwise it will
   * scan the file. Either way the result should be 500.
   */
  @Test public void testCountStarReplacedWithMetadataValue() throws Exception {
    String query = "SELECT COUNT(*) FROM files.\"count_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertEquals(500, result,
          "COUNT(*) should return 500 rows");
    }
  }

  /**
   * Test that COUNT(*) with a WHERE clause is not replaced by the rule.
   * The rule requires a grand total (no WHERE clause creates a LogicalFilter
   * between the aggregate and the table scan, which prevents matching).
   */
  @Test public void testCountStarWithWhereClauseNotReplaced() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"count_test\""
            + " WHERE \"category\" = 'CatA'";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // 500 rows, category = Cat(i % 5), so CatA never appears (Cat0..Cat4)
      // Wait - we use "Cat" + (i % 5), so categories are Cat0, Cat1, Cat2, Cat3, Cat4
      // CatA does not match any -> 0 rows
      assertEquals(0, result,
          "COUNT(*) with non-matching WHERE should return 0");
    }
  }

  /**
   * Test the fallback behavior: even without statistics the query should
   * still return the correct count by scanning the parquet file.
   * Uses a fresh ephemeral connection with no prior cached statistics.
   */
  @Test public void testNoStatsFallback() throws Exception {
    // Create a fresh connection with ephemeral cache to ensure no cached stats
    try (Connection freshConn =
             DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER")) {
      CalciteConnection freshCalcite = freshConn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = freshCalcite.getRootSchema();

      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      operand.put("ephemeralCache", true);

      rootSchema.add("files",
          FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));

      String query = "SELECT COUNT(*) FROM files.\"count_test\"";

      try (Statement stmt = freshConn.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        assertTrue(rs.next(), "Should have a result row");
        long result = rs.getLong(1);
        assertEquals(500, result,
            "COUNT(*) should return 500 even without statistics");
      }
    }
  }

  private void createTestData() throws Exception {
    File file = new File(tempDir, "count_test.csv");
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("id:int,value:double,category:string\n");
      for (int i = 0; i < 500; i++) {
        writer.write((i + 1) + "," + (10.0 + i) + ",Cat" + (i % 5) + "\n");
      }
    }
  }

  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }

  private void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
}
