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

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.adapter.file.statistics.TableStatistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for planner rules exercised through real Calcite queries.
 * Tests verify that rules are properly registered and that queries execute correctly
 * when rules are present in the planner.
 */
@Tag("integration")
public class RulesIntegrationTest extends BaseFileTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RulesIntegrationTest.class);

  @TempDir
  Path tempDir;

  // --- FileStatisticsRules ---

  @Test public void testFileStatisticsRulesConstants() {
    assertEquals("FileStatisticsRules:FilterPushdown",
        FileStatisticsRules.STATISTICS_FILTER_PUSHDOWN_NAME);
    assertEquals("FileStatisticsRules:JoinReorder",
        FileStatisticsRules.STATISTICS_JOIN_REORDER_NAME);
    assertEquals("FileStatisticsRules:ColumnPruning",
        FileStatisticsRules.STATISTICS_COLUMN_PRUNING_NAME);
  }

  @Test public void testEstimateSelectivityWithNullStats() {
    double selectivity = FileStatisticsRules.estimateSelectivity("condition", null);
    assertEquals(0.3, selectivity, 0.001);
  }

  @Test public void testEstimateSelectivityWithStats() {
    TableStatistics stats = new TableStatistics(1000, 5000,
        java.util.Collections.emptyMap(), "hash123");
    double selectivity = FileStatisticsRules.estimateSelectivity("condition", stats);
    assertEquals(0.3, selectivity, 0.001);
  }

  @Test public void testGetTableStatisticsReturnsNull() {
    assertNull(FileStatisticsRules.getTableStatistics(new Object()));
  }

  // --- Rule instance verification ---

  @Test public void testSimpleFileColumnPruningRuleInstance() {
    assertNotNull(SimpleFileColumnPruningRule.INSTANCE);
  }

  @Test public void testSimpleFileFilterPushdownRuleInstance() {
    assertNotNull(SimpleFileFilterPushdownRule.INSTANCE);
  }

  @Test public void testSimpleFileJoinReorderRuleInstance() {
    assertNotNull(SimpleFileJoinReorderRule.INSTANCE);
  }

  @Test public void testSimpleHLLCountDistinctRuleInstance() {
    assertNotNull(SimpleHLLCountDistinctRule.INSTANCE);
    assertNotNull(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE);
  }

  @Test public void testCountStarStatisticsRuleInstance() {
    assertNotNull(CountStarStatisticsRule.INSTANCE);
  }

  @Test public void testHLLCountDistinctRuleInstance() {
    assertNotNull(HLLCountDistinctRule.INSTANCE);
  }

  @Test public void testPartitionDistinctRuleInstance() {
    assertNotNull(PartitionDistinctRule.INSTANCE);
  }

  @Test public void testAlternatePartitionSelectionRuleInstance() {
    assertNotNull(AlternatePartitionSelectionRule.INSTANCE);
  }

  // --- Query execution with rules through Calcite JDBC ---

  @Test public void testQueryWithFilterOnCsvData() throws Exception {
    // Create test CSV data
    Path dataDir = tempDir.resolve("data");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("employees.csv"),
        "id,name,department,salary\n1,Alice,Engineering,100000\n2,Bob,Marketing,80000\n3,Charlie,Engineering,110000\n4,Diana,Marketing,90000\n".getBytes());

    String model = buildTestModel("TEST", dataDir.toString());

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {

      // Simple filter query - rules may or may not fire depending on table type
      ResultSet rs = stmt.executeQuery(
          "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY name");

      assertTrue(rs.next());
      assertEquals("Alice", rs.getString("name"));
      assertTrue(rs.next());
      assertEquals("Charlie", rs.getString("name"));
      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testQueryWithProjection() throws Exception {
    Path dataDir = tempDir.resolve("data2");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("products.csv"),
        "id,name,category,price,stock\n1,Widget,A,9.99,100\n2,Gadget,B,19.99,50\n3,Doohickey,A,4.99,200\n".getBytes());

    String model = buildTestModel("TEST", dataDir.toString());

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {

      // Projection query - column pruning rule may apply
      ResultSet rs = stmt.executeQuery(
          "SELECT name, price FROM products ORDER BY name");

      assertTrue(rs.next());
      assertEquals("Doohickey", rs.getString("name"));
      assertEquals("4.99", rs.getString("price"));

      assertTrue(rs.next());
      assertEquals("Gadget", rs.getString("name"));

      assertTrue(rs.next());
      assertEquals("Widget", rs.getString("name"));

      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testQueryWithAggregation() throws Exception {
    Path dataDir = tempDir.resolve("data3");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("sales.csv"),
        "region,product,amount\nNorth,A,100\nNorth,B,200\nSouth,A,150\nSouth,B,250\nNorth,A,300\n".getBytes());

    String model = buildTestModel("TEST", dataDir.toString());

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT region, SUM(amount) AS total "
          + "FROM sales GROUP BY region ORDER BY region");

      assertTrue(rs.next());
      assertEquals("North", rs.getString("region"));
      assertEquals(600, rs.getInt("total"));

      assertTrue(rs.next());
      assertEquals("South", rs.getString("region"));
      assertEquals(400, rs.getInt("total"));

      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testQueryWithCountDistinct() throws Exception {
    Path dataDir = tempDir.resolve("data4");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("events.csv"),
        "user_id,event_type,timestamp\nA,click,1\nB,click,2\nA,view,3\nC,click,4\nB,view,5\nA,click,6\n".getBytes());

    String model = buildTestModel("TEST", dataDir.toString());

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {

      // COUNT DISTINCT query - HLL rules may apply
      ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(DISTINCT user_id) AS unique_users FROM events");

      assertTrue(rs.next());
      assertEquals(3, rs.getInt("unique_users"));
      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testQueryWithJoin() throws Exception {
    Path dataDir = tempDir.resolve("data5");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("orders.csv"),
        "order_id,customer_id,amount\n1,C1,100\n2,C2,200\n3,C1,150\n".getBytes());
    Files.write(dataDir.resolve("customers.csv"),
        "customer_id,name\nC1,Alice\nC2,Bob\nC3,Charlie\n".getBytes());

    String model = buildTestModel("TEST", dataDir.toString());

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {

      // Join query - join reorder rule may apply
      ResultSet rs = stmt.executeQuery(
          "SELECT c.name, o.amount "
          + "FROM orders o "
          + "JOIN customers c ON o.customer_id = c.customer_id "
          + "ORDER BY o.order_id");

      assertTrue(rs.next());
      assertEquals("Alice", rs.getString("name"));
      assertEquals("100", rs.getString("amount"));

      assertTrue(rs.next());
      assertEquals("Bob", rs.getString("name"));

      assertTrue(rs.next());
      assertEquals("Alice", rs.getString("name"));
      assertEquals("150", rs.getString("amount"));

      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testQueryWithCountStar() throws Exception {
    Path dataDir = tempDir.resolve("data6");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("items.csv"),
        "id,name\n1,A\n2,B\n3,C\n4,D\n5,E\n".getBytes());

    String model = buildTestModel("TEST", dataDir.toString());

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {

      // COUNT(*) query - CountStarStatisticsRule may apply
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM items");

      assertTrue(rs.next());
      assertEquals(5, rs.getInt("cnt"));
      assertFalse(rs.next());
      rs.close();
    }
  }

  @Test public void testQueryWithMultipleFilters() throws Exception {
    Path dataDir = tempDir.resolve("data7");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("records.csv"),
        "id,category,status,val\n1,A,active,10\n2,B,inactive,20\n3,A,active,30\n4,B,active,40\n5,A,inactive,50\n".getBytes());

    String model = buildTestModel("TEST", dataDir.toString());

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT id, val FROM records "
          + "WHERE category = 'A' AND status = 'active' "
          + "ORDER BY id");

      assertTrue(rs.next());
      assertEquals("1", rs.getString("id"));
      assertEquals("10", rs.getString("val"));

      assertTrue(rs.next());
      assertEquals("3", rs.getString("id"));
      assertEquals("30", rs.getString("val"));

      assertFalse(rs.next());
      rs.close();
    }
  }
}
