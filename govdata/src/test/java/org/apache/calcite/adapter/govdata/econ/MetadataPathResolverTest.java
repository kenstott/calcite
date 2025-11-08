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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for metadata-driven path resolution methods in AbstractGovDataDownloader.
 *
 * <p>Tests the infrastructure for deriving JSON cache and Parquet file paths
 * from schema patterns using variable substitution.
 */
@Tag("unit")
class MetadataPathResolverTest {

  private TestEconDownloader downloader;

  /**
   * Minimal test downloader that extends AbstractEconDataDownloader.
   * Used solely for testing the metadata-driven path resolution infrastructure.
   * Exposes protected methods as public for testing.
   */
  private static class TestEconDownloader extends AbstractEconDataDownloader {
    TestEconDownloader() {
      super("/tmp/test-cache",
          new LocalFileStorageProvider(),
          new LocalFileStorageProvider());
    }

    @Override protected long getMinRequestIntervalMs() {
      return 0; // No rate limit for tests
    }

    @Override protected int getMaxRetries() {
      return 1;
    }

    @Override protected long getRetryDelayMs() {
      return 0;
    }

    // Public wrappers for testing protected methods
    @Override public Map<String, Object> loadTableMetadata(String tableName) {
      return super.loadTableMetadata(tableName);
    }

    @Override public String resolveJsonPath(String pattern, Map<String, String> variables) {
      return super.resolveJsonPath(pattern, variables);
    }

    @Override public String resolveParquetPath(String pattern, Map<String, String> variables) {
      return super.resolveParquetPath(pattern, variables);
    }
  }

  @BeforeEach
  void setUp() {
    // Create a test downloader instance for testing metadata methods
    downloader = new TestEconDownloader();
  }

  @Test void testLoadTableMetadata_FredIndicators() {
    // Load metadata for fred_indicators table
    Map<String, Object> metadata = downloader.loadTableMetadata("fred_indicators");

    assertNotNull(metadata, "Metadata should not be null");
    assertEquals("fred_indicators", metadata.get("name"), "Table name should match");
    assertNotNull(metadata.get("pattern"), "Pattern should be present");
    assertTrue(metadata.get("pattern").toString().contains("fred_indicators.parquet"),
        "Pattern should reference fred_indicators.parquet");
    assertNotNull(metadata.get("columns"), "Columns should be present");
  }

  @Test void testResolveJsonPath_SingleWildcard() {
    // Test with single year wildcard (fred_indicators pattern)
    String pattern = "type=fred_indicators/year=*/fred_indicators.parquet";
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");

    String jsonPath = downloader.resolveJsonPath(pattern, variables);

    assertEquals("type=fred_indicators/year=2020/fred_indicators.json", jsonPath,
        "JSON path should have year substituted and .json extension");
  }

  @Test void testResolveParquetPath_SingleWildcard() {
    // Test with single year wildcard
    String pattern = "type=fred_indicators/year=*/fred_indicators.parquet";
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");

    String parquetPath = downloader.resolveParquetPath(pattern, variables);

    assertEquals("type=fred_indicators/year=2020/fred_indicators.parquet", parquetPath,
        "Parquet path should have year substituted with .parquet extension");
  }

  @Test void testResolveJsonPath_MultipleWildcards() {
    // Test with multiple wildcards (frequency + year)
    String pattern = "type=employment_statistics/frequency=*/year=*/employment_statistics.parquet";
    Map<String, String> variables = new HashMap<>();
    variables.put("frequency", "monthly");
    variables.put("year", "2023");

    String jsonPath = downloader.resolveJsonPath(pattern, variables);

    assertEquals("type=employment_statistics/frequency=monthly/year=2023/employment_statistics.json",
        jsonPath, "JSON path should have both wildcards substituted");
  }

  @Test void testResolveParquetPath_MultipleWildcards() {
    // Test with multiple wildcards
    String pattern = "type=employment_statistics/frequency=*/year=*/employment_statistics.parquet";
    Map<String, String> variables = new HashMap<>();
    variables.put("frequency", "monthly");
    variables.put("year", "2023");

    String parquetPath = downloader.resolveParquetPath(pattern, variables);

    assertEquals("type=employment_statistics/frequency=monthly/year=2023/employment_statistics.parquet",
        parquetPath, "Parquet path should have both wildcards substituted");
  }

  @Test void testResolveJsonPath_MissingVariable() {
    // Test error handling when required variable is missing
    String pattern = "type=fred_indicators/year=*/fred_indicators.parquet";
    Map<String, String> variables = new HashMap<>();
    // Note: year variable is missing

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      downloader.resolveJsonPath(pattern, variables);
    }, "Should throw exception when required variable is missing");

    assertTrue(exception.getMessage().contains("Missing required variables"),
        "Exception message should indicate missing variables");
    assertTrue(exception.getMessage().contains("year"),
        "Exception message should list the missing 'year' variable");
  }

  @Test void testResolveJsonPath_WithConstantPartition() {
    // Test pattern with constant partition value (no wildcard)
    String pattern = "type=treasury_yields/frequency=daily/year=*/treasury_yields.parquet";
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2022");
    // Note: frequency is constant "daily" in pattern, not a wildcard

    String jsonPath = downloader.resolveJsonPath(pattern, variables);

    assertEquals("type=treasury_yields/frequency=daily/year=2022/treasury_yields.json",
        jsonPath, "JSON path should preserve constant partition values");
  }

  @Test void testEndToEnd_FredIndicators() {
    // End-to-end test: load metadata and resolve paths
    Map<String, Object> metadata = downloader.loadTableMetadata("fred_indicators");
    String pattern = (String) metadata.get("pattern");

    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2021");

    String jsonPath = downloader.resolveJsonPath(pattern, variables);
    String parquetPath = downloader.resolveParquetPath(pattern, variables);

    // Verify paths are derived from actual schema pattern
    assertNotNull(jsonPath, "JSON path should be derived");
    assertNotNull(parquetPath, "Parquet path should be derived");
    assertTrue(jsonPath.contains("2021"), "JSON path should contain year");
    assertTrue(parquetPath.contains("2021"), "Parquet path should contain year");
    assertTrue(jsonPath.endsWith(".json"), "JSON path should end with .json");
    assertTrue(parquetPath.endsWith(".parquet"), "Parquet path should end with .parquet");
  }

  @Test void testLoadTableMetadata_NonExistentTable() {
    // Test error handling for non-existent table
    assertThrows(IllegalArgumentException.class, () -> {
      downloader.loadTableMetadata("non_existent_table");
    }, "Should throw exception for non-existent table");
  }
}
