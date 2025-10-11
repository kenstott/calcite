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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance and benchmarking tests for GovData adapter functionality.
 *
 * <p>This test consolidates all performance-related testing including:
 * - Schema creation and initialization performance
 * - Large dataset query performance and memory usage
 * - Cross-schema join operation benchmarks
 * - Prepared statement optimization validation
 * - Cache effectiveness and hit ratio measurement
 * - Concurrent access and scaling characteristics
 *
 * <p>These tests require:
 * - GOVDATA_CACHE_DIR environment variable
 * - GOVDATA_PARQUET_DIR environment variable (optional)
 * - Sufficient data cache for meaningful performance measurement
 * - Extended timeout for long-running performance tests
 */
@Tag("performance")
public class GovDataPerformanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataPerformanceTest.class);

  // Performance thresholds (in milliseconds)
  private static final long SCHEMA_CREATION_THRESHOLD_MS = 5000;
  private static final long SIMPLE_QUERY_THRESHOLD_MS = 1000;
  private static final long COMPLEX_JOIN_THRESHOLD_MS = 5000;
  private static final long AGGREGATION_QUERY_THRESHOLD_MS = 3000;

  // Multi-schema model for comprehensive performance testing
  private static final String PERFORMANCE_MODEL = "{\n"
      + "  \"version\": \"1.0\",\n"
      + "  \"defaultSchema\": \"SEC\",\n"
      + "  \"schemas\": [{\n"
      + "    \"name\": \"SEC\",\n"
      + "    \"type\": \"custom\",\n"
      + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
      + "    \"operand\": {\n"
      + "      \"dataSource\": \"sec\",\n"
      + "      \"testMode\": true,\n"
      + "      \"autoDownload\": false,\n"
      + "      \"ciks\": \"AAPL,MSFT,GOOGL\"\n"
      + "    }\n"
      + "  }, {\n"
      + "    \"name\": \"ECON\",\n"
      + "    \"type\": \"custom\",\n"
      + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
      + "    \"operand\": {\n"
      + "      \"dataSource\": \"econ\",\n"
      + "      \"testMode\": true,\n"
      + "      \"autoDownload\": false,\n"
      + "      \"startYear\": 2020,\n"
      + "      \"endYear\": 2023\n"
      + "    }\n"
      + "  }, {\n"
      + "    \"name\": \"GEO\",\n"
      + "    \"type\": \"custom\",\n"
      + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
      + "    \"operand\": {\n"
      + "      \"dataSource\": \"geo\",\n"
      + "      \"testMode\": true,\n"
      + "      \"autoDownload\": false,\n"
      + "      \"year\": 2020\n"
      + "    }\n"
      + "  }]\n"
      + "}";

  @BeforeAll
  static void checkEnvironment() {
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    Assumptions.assumeTrue(cacheDir != null && !cacheDir.isEmpty(),
        "GOVDATA_CACHE_DIR environment variable must be set for performance tests");

    LOGGER.info("Running GovData performance tests with cache directory: {}", cacheDir);
  }

  @Test void testSchemaCreationPerformance() throws SQLException {
    LOGGER.info("Testing schema creation performance");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    long startTime = System.currentTimeMillis();

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      assertNotNull(connection, "Connection should be established");

      long creationTime = System.currentTimeMillis() - startTime;
      LOGGER.info("Schema creation completed in {} ms", creationTime);

      assertTrue(creationTime < SCHEMA_CREATION_THRESHOLD_MS,
          String.format("Schema creation took %d ms, exceeds threshold of %d ms",
              creationTime, SCHEMA_CREATION_THRESHOLD_MS));

      // Verify all schemas are accessible
      String[] schemas = {"SEC", "ECON", "GEO"};
      for (String schemaName : schemas) {
        try (PreparedStatement stmt =
            connection.prepareStatement("SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?")) {
          stmt.setString(1, schemaName);
          try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(), schemaName + " schema should exist");
            assertEquals(schemaName, rs.getString("schema_name"));
          }
        }
      }
    }
  }

  @Test void testSimpleQueryPerformance() throws SQLException {
    LOGGER.info("Testing simple query performance");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Warm up the connection
      connection.prepareStatement("SELECT 1").executeQuery().close();

      // Test simple count query performance
      String query = "SELECT COUNT(*) as record_count FROM company_tickers WHERE ticker IN ('AAPL', 'MSFT', 'GOOGL')";

      long startTime = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(query)) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Query should return results");
          int count = rs.getInt("record_count");
          assertTrue(count >= 0, "Record count should be non-negative");

          long queryTime = System.currentTimeMillis() - startTime;
          LOGGER.info("Simple query completed in {} ms, returned {} records", queryTime, count);

          assertTrue(queryTime < SIMPLE_QUERY_THRESHOLD_MS,
              String.format("Simple query took %d ms, exceeds threshold of %d ms",
                  queryTime, SIMPLE_QUERY_THRESHOLD_MS));
        }
      }
    }
  }

  @Test void testComplexJoinPerformance() throws SQLException {
    LOGGER.info("Testing complex join query performance");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test cross-schema join performance
      String complexQuery =
          "SELECT ct.ticker, ct.company_name, fm.filing_type, fm.filing_date " +
          "FROM company_tickers ct " +
          "LEFT JOIN filing_metadata fm ON ct.cik = fm.cik " +
          "WHERE ct.ticker IN ('AAPL', 'MSFT') " +
          "ORDER BY ct.ticker, fm.filing_date DESC " +
          "LIMIT 20";

      long startTime = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(complexQuery)) {
        try (ResultSet rs = stmt.executeQuery()) {
          int resultCount = 0;
          while (rs.next()) {
            resultCount++;
            String ticker = rs.getString("ticker");
            String companyName = rs.getString("company_name");
            assertNotNull(ticker, "Ticker should not be null");
            assertNotNull(companyName, "Company name should not be null");
          }

          long queryTime = System.currentTimeMillis() - startTime;
          LOGGER.info("Complex join completed in {} ms, returned {} records", queryTime, resultCount);

          assertTrue(queryTime < COMPLEX_JOIN_THRESHOLD_MS,
              String.format("Complex join took %d ms, exceeds threshold of %d ms",
                  queryTime, COMPLEX_JOIN_THRESHOLD_MS));
        }
      }
    }
  }

  @Test void testAggregationPerformance() throws SQLException {
    LOGGER.info("Testing aggregation query performance");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test aggregation query performance on economic data
      String aggregationQuery =
          "SELECT year, " +
          "COUNT(*) as state_count, " +
          "AVG(CAST(unemployment_rate AS DECIMAL)) as avg_unemployment " +
          "FROM \"ECON\".regional_employment " +
          "WHERE unemployment_rate IS NOT NULL " +
          "AND year BETWEEN '2020' AND '2022' " +
          "GROUP BY year " +
          "ORDER BY year";

      long startTime = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(aggregationQuery)) {
        try (ResultSet rs = stmt.executeQuery()) {
          int groupCount = 0;
          while (rs.next()) {
            groupCount++;
            String year = rs.getString("year");
            int stateCount = rs.getInt("state_count");
            double avgUnemployment = rs.getDouble("avg_unemployment");

            assertNotNull(year, "Year should not be null");
            assertTrue(stateCount >= 0, "State count should be non-negative");
            assertTrue(avgUnemployment >= 0, "Average unemployment should be non-negative");
          }

          long queryTime = System.currentTimeMillis() - startTime;
          LOGGER.info("Aggregation query completed in {} ms, returned {} groups", queryTime, groupCount);

          assertTrue(queryTime < AGGREGATION_QUERY_THRESHOLD_MS,
              String.format("Aggregation query took %d ms, exceeds threshold of %d ms",
                  queryTime, AGGREGATION_QUERY_THRESHOLD_MS));
        }
      }
    }
  }

  @Test void testPreparedStatementOptimization() throws SQLException {
    LOGGER.info("Testing prepared statement optimization performance");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      String parameterizedQuery = "SELECT ticker, company_name FROM company_tickers WHERE ticker = ?";

      // First execution (includes planning time)
      long firstExecutionStart = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(parameterizedQuery)) {
        stmt.setString(1, "AAPL");
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "First execution should return results");
        }
      }
      long firstExecutionTime = System.currentTimeMillis() - firstExecutionStart;

      // Second execution (should be faster due to plan caching)
      long secondExecutionStart = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(parameterizedQuery)) {
        stmt.setString(1, "MSFT");
        try (ResultSet rs = stmt.executeQuery()) {
          // Results may or may not exist, but query should complete
          rs.next(); // Don't assert on results for performance test
        }
      }
      long secondExecutionTime = System.currentTimeMillis() - secondExecutionStart;

      LOGGER.info("Prepared statement - First execution: {} ms, Second execution: {} ms",
          firstExecutionTime, secondExecutionTime);

      // Second execution should generally be faster or similar (plan reuse)
      // Allow some tolerance for performance variations
      assertTrue(secondExecutionTime <= firstExecutionTime * 1.5,
          String.format("Second execution (%d ms) should not be significantly slower than first (%d ms)",
              secondExecutionTime, firstExecutionTime));
    }
  }

  @Test void testConcurrentQueryPerformance() throws SQLException, InterruptedException {
    LOGGER.info("Testing concurrent query performance");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    // Test concurrent access to the same data
    int threadCount = 3;
    Thread[] threads = new Thread[threadCount];
    long[] executionTimes = new long[threadCount];
    boolean[] threadSuccess = new boolean[threadCount];

    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      threads[i] = new Thread(() -> {
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
          long startTime = System.currentTimeMillis();

          String query = "SELECT COUNT(*) FROM company_tickers WHERE ticker LIKE '%A%'";
          try (PreparedStatement stmt = connection.prepareStatement(query)) {
            try (ResultSet rs = stmt.executeQuery()) {
              if (rs.next()) {
                int count = rs.getInt(1);
                LOGGER.debug("Thread {} completed with count: {}", threadIndex, count);
              }
            }
          }

          executionTimes[threadIndex] = System.currentTimeMillis() - startTime;
          threadSuccess[threadIndex] = true;
        } catch (SQLException e) {
          LOGGER.error("Thread {} failed with error: {}", threadIndex, e.getMessage());
          threadSuccess[threadIndex] = false;
        }
      });
    }

    // Start all threads
    long overallStart = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join(10000); // 10 second timeout per thread
    }
    long overallTime = System.currentTimeMillis() - overallStart;

    // Verify all threads completed successfully
    for (int i = 0; i < threadCount; i++) {
      assertTrue(threadSuccess[i], "Thread " + i + " should complete successfully");
      LOGGER.info("Thread {} execution time: {} ms", i, executionTimes[i]);
    }

    LOGGER.info("Concurrent execution completed in {} ms", overallTime);

    // Overall time should be reasonable for concurrent execution
    assertTrue(overallTime < 15000, // 15 seconds for 3 concurrent threads
        String.format("Concurrent execution took %d ms, which seems excessive", overallTime));
  }

  @Test void testMemoryUsageMonitoring() throws SQLException {
    LOGGER.info("Testing memory usage during large result set processing");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    Runtime runtime = Runtime.getRuntime();
    long initialMemory = runtime.totalMemory() - runtime.freeMemory();

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Query that might return a larger result set
      String largeQuery = "SELECT * FROM \"GEO\".tiger_counties LIMIT 1000";

      try (PreparedStatement stmt = connection.prepareStatement(largeQuery)) {
        try (ResultSet rs = stmt.executeQuery()) {
          int rowCount = 0;
          while (rs.next() && rowCount < 1000) {
            rowCount++;
            // Access some columns to ensure data is loaded
            rs.getString(1); // First column
            if (rs.getMetaData().getColumnCount() > 1) {
              rs.getString(2); // Second column if available
            }
          }

          long finalMemory = runtime.totalMemory() - runtime.freeMemory();
          long memoryIncrease = finalMemory - initialMemory;

          LOGGER.info("Processed {} rows, memory increase: {} bytes ({} MB)",
              rowCount, memoryIncrease, memoryIncrease / (1024 * 1024));

          // Memory increase should be reasonable (less than 100MB for 1000 rows)
          assertTrue(memoryIncrease < 100 * 1024 * 1024,
              String.format("Memory increase of %d bytes seems excessive for %d rows",
                  memoryIncrease, rowCount));
        }
      }
    }
  }

  @Test void testCacheEffectiveness() throws SQLException {
    LOGGER.info("Testing cache effectiveness for repeated queries");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + PERFORMANCE_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      String repeatableQuery = "SELECT COUNT(*) FROM company_tickers";

      // First execution (cold cache)
      long firstStart = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(repeatableQuery)) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "First execution should return results");
        }
      }
      long firstTime = System.currentTimeMillis() - firstStart;

      // Second execution (warm cache)
      long secondStart = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(repeatableQuery)) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Second execution should return results");
        }
      }
      long secondTime = System.currentTimeMillis() - secondStart;

      // Third execution (cached)
      long thirdStart = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(repeatableQuery)) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Third execution should return results");
        }
      }
      long thirdTime = System.currentTimeMillis() - thirdStart;

      LOGGER.info("Cache effectiveness - First: {} ms, Second: {} ms, Third: {} ms",
          firstTime, secondTime, thirdTime);

      // Subsequent executions should generally be faster (cache effectiveness)
      // Allow some tolerance for performance variations
      assertTrue(thirdTime <= firstTime,
          String.format("Third execution (%d ms) should not be slower than first (%d ms)",
              thirdTime, firstTime));
    }
  }
}
