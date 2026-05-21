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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.statistics.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct comparison of query performance with and without optimizations.
 */
@Tag("performance")
public class BeforeAfterComparisonTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BeforeAfterComparisonTest.class);

  @TempDir
  java.nio.file.Path tempDir;

  private File testFile;

  @BeforeEach
  public void setUp() throws Exception {
    createTestFile(25000); // 25K rows for meaningful comparison
  }

  @Test public void compareOptimizationEffectiveness() throws Exception {
    LOGGER.debug("OPTIMIZATION EFFECTIVENESS COMPARISON");

    // Test queries
    String[] queries = {
        // Query 1: Filter that can be eliminated with statistics
        "SELECT COUNT(*) FROM FILES.\"comparison_test\" WHERE \"amount\" > 2000",

        // Query 2: COUNT(DISTINCT)
        "SELECT COUNT(DISTINCT \"customer_id\") FROM FILES.\"comparison_test\"",

        // Query 3: Complex aggregation with filter
        "SELECT \"region\", COUNT(*), AVG(\"amount\") " +
        "FROM FILES.\"comparison_test\" " +
        "WHERE \"amount\" > 500 " +
        "GROUP BY \"region\"",

        // Query 4: Multiple COUNT(DISTINCT)
        "SELECT COUNT(DISTINCT \"customer_id\"), COUNT(DISTINCT \"product_id\") " +
        "FROM FILES.\"comparison_test\"",

        // Query 5: Filter with partial match
        "SELECT COUNT(*) FROM FILES.\"comparison_test\" " +
        "WHERE \"amount\" BETWEEN 400 AND 600"
    };

    String[] descriptions = {
        "Filter elimination (amount > 2000)",
        "Single COUNT(DISTINCT)",
        "Complex aggregation with GROUP BY",
        "Multiple COUNT(DISTINCT)",
        "Partial filter (amount 400-600)"
    };

    // Run WITHOUT optimizations (no statistics)
    LOGGER.debug("--- WITHOUT OPTIMIZATIONS (No Statistics) ---");
    Connection withoutOpt = createConnectionWithoutOptimizations();
    long[] timesWithout = new long[queries.length];
    Object[] resultsWithout = new Object[queries.length];

    for (int i = 0; i < queries.length; i++) {
      long time = 0;
      Object result = null;

      // Warm up
      executeQuery(withoutOpt, queries[i]);

      // Measure (average of 3 runs)
      for (int run = 0; run < 3; run++) {
        long start = System.nanoTime();
        result = executeQueryWithResult(withoutOpt, queries[i]);
        time += (System.nanoTime() - start) / 1_000_000;
      }

      timesWithout[i] = time / 3;
      resultsWithout[i] = result;
      LOGGER.debug("{}: {} ms, result: {}", descriptions[i],
          timesWithout[i], result);
    }

    // Run WITH optimizations (with statistics)
    LOGGER.debug("--- WITH OPTIMIZATIONS (Statistics + Rules) ---");
    Connection withOpt = createConnectionWithOptimizations();
    long[] timesWith = new long[queries.length];
    Object[] resultsWith = new Object[queries.length];

    for (int i = 0; i < queries.length; i++) {
      long time = 0;
      Object result = null;

      // Warm up
      executeQuery(withOpt, queries[i]);

      // Measure (average of 3 runs)
      for (int run = 0; run < 3; run++) {
        long start = System.nanoTime();
        result = executeQueryWithResult(withOpt, queries[i]);
        time += (System.nanoTime() - start) / 1_000_000;
      }

      timesWith[i] = time / 3;
      resultsWith[i] = result;
      LOGGER.debug("{}: {} ms, result: {}", descriptions[i],
          timesWith[i], result);
    }

    // Summary comparison
    for (int i = 0; i < queries.length; i++) {
      double speedup = (double) timesWithout[i] / timesWith[i];
      LOGGER.debug("{}: without={} ms, with={} ms, speedup={:.2f}x",
          descriptions[i], timesWithout[i], timesWith[i], speedup);
    }

    // Verify statistics are being used
    File cacheDir = tempDir.resolve("opt_cache").toFile();
    File statsFile = new File(cacheDir, "comparison_test.aperio_stats");
    assertTrue(statsFile.exists(),
        "Statistics file should have been created at: "
            + statsFile.getAbsolutePath());

    TableStatistics stats = StatisticsCache.loadStatistics(statsFile);
    assertNotNull(stats, "Should be able to load statistics");
    assertTrue(stats.getRowCount() > 0,
        "Statistics should report positive row count");
    LOGGER.debug("Statistics: {} rows", stats.getRowCount());

    // Verify results match between optimized and non-optimized queries
    for (int i = 0; i < queries.length; i++) {
      assertEquals(String.valueOf(resultsWithout[i]),
          String.valueOf(resultsWith[i]),
          "Query '" + descriptions[i]
              + "' should produce same results with and without optimizations");
    }

    withoutOpt.close();
    withOpt.close();
  }

  private Connection createConnectionWithoutOptimizations() throws Exception {
    // Create connection with empty cache (no statistics)
    File emptyCacheDir = tempDir.resolve("empty_cache").toFile();
    emptyCacheDir.mkdirs();

    System.setProperty("calcite.file.statistics.cache.directory", emptyCacheDir.getAbsolutePath());
    System.setProperty("calcite.file.statistics.filter.enabled", "false");
    System.setProperty("calcite.file.statistics.join.reorder.enabled", "false");
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "false");

    Connection conn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);

    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));

    return conn;
  }

  private Connection createConnectionWithOptimizations() throws Exception {
    // Generate statistics first
    File cacheDir = tempDir.resolve("opt_cache").toFile();
    cacheDir.mkdirs();

    StatisticsBuilder builder = new StatisticsBuilder();
    TableStatistics stats =
        builder.buildStatistics(new org.apache.calcite.adapter.file.DirectFileSource(testFile),
        cacheDir);

    LOGGER.debug("Generated statistics: {} rows", stats.getRowCount());

    // Create connection with statistics enabled
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
    System.setProperty("calcite.file.statistics.filter.enabled", "true");
    System.setProperty("calcite.file.statistics.join.reorder.enabled", "true");
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");

    Connection conn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);

    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));

    return conn;
  }

  private void executeQuery(Connection conn, String query) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        // Consume results
      }
    }
  }

  private Object executeQueryWithResult(Connection conn, String query) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {

      // For COUNT queries, return the count
      if (query.toUpperCase().contains("COUNT(")) {
        if (rs.next()) {
          // Handle multiple columns
          int columnCount = rs.getMetaData().getColumnCount();
          if (columnCount == 1) {
            return rs.getLong(1);
          } else {
            StringBuilder result = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
              if (i > 1) result.append(", ");
              result.append(rs.getObject(i));
            }
            return result.toString();
          }
        }
      } else {
        // For GROUP BY queries, count rows
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
        }
        return rowCount + " rows";
      }

      return "No results";
    }
  }

  @SuppressWarnings("deprecation")
  private void createTestFile(int rowCount) throws Exception {
    testFile = new File(tempDir.toFile(), "comparison_test.parquet");

    String schemaString = "{\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [" +
                         "  {\"name\": \"customer_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"product_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"amount\", \"type\": \"double\"}," +
                         "  {\"name\": \"region\", \"type\": \"string\"}" +
                         "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);

    try (ParquetWriter<GenericRecord> writer =
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(testFile.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {

      for (int i = 0; i < rowCount; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", random.nextInt(5000)); // ~5000 distinct
        record.put("product_id", random.nextInt(100));
        record.put("amount", 10.0 + random.nextDouble() * 1000.0); // 10-1010
        record.put("region", "Region" + (i % 10));
        writer.write(record);
      }
    }

    LOGGER.debug("Created test file: {} rows", rowCount);
  }
}
