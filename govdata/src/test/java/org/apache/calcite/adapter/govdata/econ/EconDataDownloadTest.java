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
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

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

  @Test public void testFredEconomicIndicators() throws Exception {
    String apiKey = System.getenv("FRED_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(),
        "FRED_API_KEY not set, skipping FRED test");

    StorageProvider storageProvider = createStorageProvider();
    FredDataDownloader downloader = new FredDataDownloader(tempDir.toString(), apiKey, storageProvider, storageProvider);

    // Download just a few key indicators for 1 year
    downloader.downloadEconomicIndicators(
        Arrays.asList(FredDataDownloader.Series.FED_FUNDS_RATE,
                     FredDataDownloader.Series.UNEMPLOYMENT_RATE,
                     FredDataDownloader.Series.CPI_ALL_URBAN),
        "2023-01-01", "2024-01-01");

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=fred_indicators/fred_indicators.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "fred_indicators");
  }

  @Test public void testFredCustomSeriesPartitioning() throws Exception {
    String apiKey = System.getenv("FRED_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty() && !"demo".equals(apiKey),
        "FRED_API_KEY not set or is demo key, skipping FRED custom series test");

    StorageProvider storageProvider = createStorageProvider();
    FredDataDownloader downloader = new FredDataDownloader(tempDir.toString(), apiKey, storageProvider, storageProvider);

    // Test individual series download
    downloader.downloadSeries("UNRATE", 2023, 2024);

    // Test series conversion to parquet
    String targetPath = tempDir.toString() + "/test_unrate.parquet";
    downloader.convertSeriesToParquet("UNRATE", targetPath, null);

    // Verify file exists and is readable
    assertTrue(new File(targetPath).exists(), "UNRATE parquet file should exist");
    verifyParquetReadable(targetPath, "fred_indicators");

    // Test FredSeriesGroup functionality
    FredSeriesGroup treasuryGroup =
        new FredSeriesGroup("treasuries", Arrays.asList("DGS10", "DGS30"),
        FredSeriesGroup.PartitionStrategy.AUTO, null);

    assertTrue(treasuryGroup.matchesSeries("DGS10"), "Should match DGS10");
    assertTrue(treasuryGroup.matchesSeries("DGS30"), "Should match DGS30");
    assertNotNull(treasuryGroup.getTableName(), "Should generate table name");

    // Test partition analyzer
    FredSeriesPartitionAnalyzer analyzer = new FredSeriesPartitionAnalyzer();
    FredSeriesPartitionAnalyzer.PartitionAnalysis analysis =
        analyzer.analyzeGroup(treasuryGroup, Arrays.asList("UNRATE", "DGS10", "DGS30"));

    assertNotNull(analysis, "Analysis should not be null");
    System.out.println("Partition analysis result: " + analysis);

    // Should recommend year partitioning for AUTO strategy
    assertTrue(analysis.getPartitionFields().contains("year"),
        "AUTO strategy should include year partitioning");
  }

  @Test public void testBeaGdpComponents() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(),
        "BEA_API_KEY not set, skipping BEA test");

    StorageProvider storageProvider = createStorageProvider();
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), tempDir.toString(), apiKey, storageProvider, storageProvider);

    // Download just 1 year of GDP components
    downloader.downloadGdpComponents(2023, 2023);

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=gdp_components/year_range=2023_2023/gdp_components.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "gdp_components");
  }

  @Test public void testBeaRegionalIncome() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(),
        "BEA_API_KEY not set, skipping BEA regional test");

    StorageProvider storageProvider = createStorageProvider();
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), tempDir.toString(), apiKey, storageProvider, storageProvider);

    // Download just 1 year of regional income data
    downloader.downloadRegionalIncome(2023, 2023);

    // Verify parquet file was created
    String parquetPath =
        storageProvider.resolvePath(tempDir.toString(), "source=econ/type=regional_income/year_range=2023_2023/regional_income.parquet");
    assertTrue(storageProvider.exists(parquetPath));

    verifyParquetReadable(parquetPath, "regional_income");
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
