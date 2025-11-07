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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Comprehensive test for all economic data downloaders.
 * Tests BLS, Treasury, World Bank, FRED, and BEA data downloads.
 */
@Tag("integration")
public class EconDataDownloadTest {

  @TempDir
  Path tempDir;

  private StorageProvider createStorageProvider() {
    return StorageProviderFactory.createFromUrl("file://" + tempDir.toString());
  }

  @Test public void testBlsEmploymentStatistics() throws Exception {
    String apiKey = System.getenv("BLS_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(),
        "BLS_API_KEY not set, skipping BLS test");

    StorageProvider storageProvider = createStorageProvider();
    BlsDataDownloader downloader = new BlsDataDownloader(apiKey, tempDir.toString(), storageProvider, storageProvider);

    // Download just 1 year of employment data for testing
    downloader.downloadEmploymentStatistics(2023, 2024);

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=employment/year_range=2023_2024/employment_statistics.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "employment_statistics");
  }

  @Test public void testTreasuryYields() throws Exception {
    StorageProvider storageProvider = createStorageProvider();
    TreasuryDataDownloader downloader = new TreasuryDataDownloader(tempDir.toString(), storageProvider, storageProvider);

    // Download just 1 year of data for testing
    downloader.downloadTreasuryYields(2023, 2024);

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=treasury/year_range=2023_2024/treasury_yields.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "treasury_yields");
  }

  @Test public void testFederalDebt() throws Exception {
    StorageProvider storageProvider = createStorageProvider();
    TreasuryDataDownloader downloader = new TreasuryDataDownloader(tempDir.toString(), storageProvider, storageProvider);

    downloader.downloadFederalDebt(2023, 2024);

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=treasury/year_range=2023_2024/federal_debt.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "federal_debt");
  }

  @Test public void testWorldBankIndicators() throws Exception {
    StorageProvider storageProvider = createStorageProvider();
    WorldBankDataDownloader downloader = new WorldBankDataDownloader(tempDir.toString(), storageProvider, storageProvider);

    // Download just 2 years for G7 countries
    downloader.downloadWorldIndicators(2022, 2023);

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=worldbank/year_range=2022_2023/world_indicators.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "world_indicators");
  }

  @Test public void testWorldBankGlobalGDP() throws Exception {
    StorageProvider storageProvider = createStorageProvider();
    WorldBankDataDownloader downloader = new WorldBankDataDownloader(tempDir.toString(), storageProvider, storageProvider);

    // Download just 1 year of GDP data
    downloader.downloadGlobalGDP(2023, 2023);

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=worldbank/year_range=2023_2023/global_gdp.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "global_gdp");
  }

  // FRED download tests removed - downloadSeries() method removed in favor of metadata-driven approach
  // TODO: Add tests for executeDownload() metadata-driven downloads

  @Test public void testBeaGdpComponents() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(),
        "BEA_API_KEY not set, skipping BEA test");

    StorageProvider storageProvider = createStorageProvider();
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), tempDir.toString(), apiKey, storageProvider, storageProvider);

    // Extract NIPA tables list from schema
    List<String> nipaTablesList = extractIterationList("gdp_components", "nipaTablesList");
    assumeTrue(!nipaTablesList.isEmpty(), "nipaTablesList not found in schema");

    // Download just 1 year of GDP components using metadata-driven methods
    downloader.downloadGdpComponentsMetadata(2023, 2023, nipaTablesList);
    downloader.convertGdpComponentsMetadata(2023, 2023, nipaTablesList);

    // Verify parquet file was created (new path structure: type=gdp_components/frequency=A/year=2023/)
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "type=gdp_components/frequency=A/year=2023/gdp_components.parquet");
    assertTrue(storageProvider.exists(parquetPath), "Parquet file should exist at: " + parquetPath);

    verifyParquetReadable(parquetPath, "gdp_components");
  }

  @Test public void testBeaRegionalIncome() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(),
        "BEA_API_KEY not set, skipping BEA regional test");

    StorageProvider storageProvider = createStorageProvider();
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), tempDir.toString(), apiKey, storageProvider, storageProvider);

    // Extract line codes list from schema
    List<String> lineCodesList = extractIterationList("regional_income", "lineCodesList");
    assumeTrue(!lineCodesList.isEmpty(), "lineCodesList not found in schema");

    // Download just 1 year of regional income data using metadata-driven methods
    downloader.downloadRegionalIncomeMetadata(2023, 2023, lineCodesList);
    downloader.convertRegionalIncomeMetadata(2023, 2023, lineCodesList);

    // Verify parquet file was created (new path structure: type=regional_income/frequency=A/year=2023/)
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "type=regional_income/frequency=A/year=2023/regional_income.parquet");
    assertTrue(storageProvider.exists(parquetPath), "Parquet file should exist at: " + parquetPath);

    verifyParquetReadable(parquetPath, "regional_income");
  }

  @SuppressWarnings("unchecked")
  private List<String> extractIterationList(String tableName, String listKey) {
    try {
      InputStream schemaStream = getClass().getResourceAsStream("/econ-schema.json");
      if (schemaStream == null) {
        return Collections.emptyList();
      }
      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(schemaStream);
      JsonNode tables = root.get("tables");
      if (tables != null && tables.isArray()) {
        for (JsonNode table : tables) {
          if (tableName.equals(table.get("name").asText())) {
            JsonNode download = table.get("download");
            if (download != null) {
              JsonNode listNode = download.get(listKey);
              if (listNode != null && listNode.isArray()) {
                List<String> result = new ArrayList<>();
                for (JsonNode item : listNode) {
                  result.add(item.asText());
                }
                return result;
              }
            }
          }
        }
      }
      return Collections.emptyList();
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  /**
   * Verifies that a Parquet file can be read using DuckDB.
   */
  private void verifyParquetReadable(String parquetPath, String expectedTable) throws Exception {
    // Use DuckDB to verify the Parquet file is readable
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Query the Parquet file
        String query =
            String.format("SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetPath);

        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "Parquet file should contain data");
          System.out.printf("%s: Found %d rows in Parquet file%n", expectedTable, rowCount);
        }

        // Verify schema
        query =
            String.format("DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetPath);

        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.printf("%s schema:%n", expectedTable);
          boolean foundExpectedColumns = false;
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String columnType = rs.getString("column_type");
            System.out.printf("  %s: %s%n", columnName, columnType);

            // Check for expected columns based on table type
            if ("employment_statistics".equals(expectedTable) && "unemployment_rate".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("treasury_yields".equals(expectedTable) && "yield_percent".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("federal_debt".equals(expectedTable) && "amount_billions".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("world_indicators".equals(expectedTable) && "country_code".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("fred_indicators".equals(expectedTable) && "series_id".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("gdp_components".equals(expectedTable) && "line_description".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("regional_income".equals(expectedTable) && "geo_fips".equals(columnName)) {
              foundExpectedColumns = true;
            }
          }
          assertTrue(foundExpectedColumns, "Expected columns not found in " + expectedTable);
        }
      }
    }
  }
}
