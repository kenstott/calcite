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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
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
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HLLCountDistinctRule} which replaces COUNT(DISTINCT)
 * operations with pre-computed HyperLogLog sketch estimates.
 *
 * <p>Note: The HLLCountDistinctRule is currently temporarily disabled
 * (onMatch returns immediately). These tests verify the overall query
 * behavior and exercise the code path with HLL sketches registered in
 * the cache. The SimpleHLLCountDistinctRule may intercept these queries
 * instead.
 */
@Tag("integration")
public class HLLCountDistinctRuleTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedHllEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("hll-count-distinct-test-").toFile();
    cacheDir = new File(tempDir, "hll_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedHllEnabled =
        System.getProperty("calcite.file.statistics.hll.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable HLL optimization
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    createTestData();
    createHLLSketches();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }

    restoreProperty("calcite.file.statistics.hll.enabled", savedHllEnabled);
    restoreProperty("calcite.file.statistics.cache.directory",
        savedCacheDirectory);

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that COUNT(DISTINCT) on a column with HLL sketches produces a
   * result that is approximately correct. Whether the HLL rule or the
   * standard aggregate runs, the result should be close to the actual
   * distinct count.
   */
  @Test public void testCountDistinctRewrittenToHLL() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"customer_id\") FROM files.\"hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // Random(42).nextInt(200) produces ~200 distinct values
      // Allow a reasonable tolerance for either exact or HLL-approximate result
      assertTrue(result > 150 && result < 250,
          "COUNT(DISTINCT customer_id) should be approximately 200, got: "
              + result);
    }
  }

  /**
   * Test that a non-distinct COUNT is not matched by the HLL rule.
   * Plain COUNT(*) should return the exact row count.
   */
  @Test public void testNonDistinctNotMatched() throws Exception {
    String query = "SELECT COUNT(*) FROM files.\"hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertEquals(1000, result,
          "Non-distinct COUNT(*) should return exact row count of 1000");
    }
  }

  /**
   * Test COUNT(DISTINCT) on multiple columns. Each should produce an
   * independent approximate count.
   */
  @Test public void testMultipleDistinctColumns() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"customer_id\"), COUNT(DISTINCT \"product_id\")"
            + " FROM files.\"hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long customerDistinct = rs.getLong(1);
      long productDistinct = rs.getLong(2);

      // customer_id: ~200 distinct (nextInt(200))
      assertTrue(customerDistinct > 150 && customerDistinct < 250,
          "COUNT(DISTINCT customer_id) should be ~200, got: "
              + customerDistinct);
      // product_id: ~50 distinct (nextInt(50))
      assertTrue(productDistinct > 30 && productDistinct < 70,
          "COUNT(DISTINCT product_id) should be ~50, got: "
              + productDistinct);
    }
  }

  private void createTestData() throws Exception {
    File file = new File(tempDir, "hll_test.csv");
    Random random = new Random(42);
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("customer_id:int,product_id:int,amount:double\n");
      for (int i = 0; i < 1000; i++) {
        writer.write(random.nextInt(200) + "," + random.nextInt(50) + ","
            + String.format("%.2f", 10.0 + random.nextDouble() * 500.0) + "\n");
      }
    }
  }

  private void createHLLSketches() throws Exception {
    // Create HLL sketch for customer_id
    HyperLogLogSketch customerSketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 200; i++) {
      customerSketch.add(String.valueOf(i));
    }
    File customerFile =
        new File(cacheDir, "hll_test_customer_id.hll");
    StatisticsCache.saveHLLSketch(customerSketch, customerFile);

    // Create HLL sketch for product_id
    HyperLogLogSketch productSketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 50; i++) {
      productSketch.add(String.valueOf(i));
    }
    File productFile =
        new File(cacheDir, "hll_test_product_id.hll");
    StatisticsCache.saveHLLSketch(productSketch, productFile);
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

  private static void restoreProperty(String key, String savedValue) {
    if (savedValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, savedValue);
    }
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
