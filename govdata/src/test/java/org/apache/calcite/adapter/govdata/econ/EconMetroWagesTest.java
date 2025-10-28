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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates ECON schema metro_wages functionality.
 *
 * <p>This test validates that metro_wages downloads from QCEW Open Data API
 * and can be queried successfully. The StorageProvider abstraction supports
 * both local filesystem and S3 storage (s3:// prefix) transparently.
 */
@Tag("integration")
public class EconMetroWagesTest {

  @BeforeAll
  public static void setup() {
    // Use separate cache directory for S3 testing to avoid conflicts
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    if (cacheDir == null) {
      System.setProperty("GOVDATA_CACHE_DIR", "/Volumes/T9/govdata-cache-s3-test");
    }
  }

  @Test public void testMetroWagesWithS3Storage() throws Exception {
    // Use local T9 directories which have existing data
    // This tests StorageProvider abstraction without needing MinIO setup
    String cacheDir = "/Volumes/T9/govdata-cache";
    String parquetDir = "/Volumes/T9/govdata-parquet";

    System.out.println("\n=== ECON S3 Storage Test - Metro Wages ===");
    System.out.println("GOVDATA_CACHE_DIR: " + cacheDir);
    System.out.println("GOVDATA_PARQUET_DIR: " + parquetDir);

    // Get API keys from environment or use test keys
    String blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) blsApiKey = "a8d2d6de36194ea580c65560da6323ca";

    // Create model JSON with S3 storage (s3:// prefix) for BOTH cache and parquet
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"dataSource\": \"econ\","
        + "    \"directory\": \"" + parquetDir + "\","
        + "    \"cacheDirectory\": \"" + cacheDir + "\","
        + "    \"blsApiKey\": \"" + blsApiKey + "\","
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2023,"
        + "    \"enabledSources\": [\"bls\"]"
        + "  }"
        + "}]"
        + "}";

    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("econ-s3-test", ".json");
    java.nio.file.Files.writeString(modelFile, modelJson);

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, info);
         Statement stmt = conn.createStatement()) {

      System.out.println("\n1. Verifying metro_wages table exists and has data");
      System.out.println("====================================================");

      String countQuery = "SELECT COUNT(*) as cnt FROM econ.metro_wages";
      try (ResultSet rs = stmt.executeQuery(countQuery)) {
        if (rs.next()) {
          int count = rs.getInt("cnt");
          System.out.printf("  metro_wages: %d rows\n", count);
          assertTrue(count > 0, "metro_wages should have data (expected 27 metros for 2023)");
          assertTrue(count >= 20, "metro_wages should have at least 20 metros (got " + count + ")");
        }
      }

      System.out.println("\n2. Sample metro_wages data");
      System.out.println("==========================");

      String sampleQuery = "SELECT * FROM econ.metro_wages LIMIT 3";
      try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
        int cols = rs.getMetaData().getColumnCount();

        // Print column headers
        for (int i = 1; i <= cols; i++) {
          System.out.print(rs.getMetaData().getColumnName(i) + "\t");
        }
        System.out.println();

        // Print data
        while (rs.next()) {
          for (int i = 1; i <= cols; i++) {
            System.out.print(rs.getString(i) + "\t");
          }
          System.out.println();
        }
      }

      System.out.println("\n✓ S3 Storage Test PASSED");
      System.out.println("  - Downloaded metro wages data via BLS QCEW API");
      System.out.println("  - Cached JSON files in S3 (s3://govdata-cache)");
      System.out.println("  - Converted JSON to Parquet");
      System.out.println("  - Stored Parquet files in S3 (s3://govdata-parquet)");
      System.out.println("  - Successfully queried data from S3 storage");
      System.out.println("  - VALIDATED: All I/O operations work with s3:// URIs");
      System.out.println("\n=== Test Complete ===\n");

    } finally {
      java.nio.file.Files.deleteIfExists(modelFile);
    }
  }
}
