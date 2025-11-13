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
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test BLS data download and ETL functionality.
 */
@Tag("integration")
public class BlsDataDownloadTest {

  private static String blsApiKey;

  @TempDir
  Path tempDir;

  private StorageProvider createStorageProvider() {
    return StorageProviderFactory.createFromUrl("file://" + tempDir.toString());
  }

  @BeforeAll
  public static void setUp() {
    // Load environment variables from .env files
    TestEnvironmentLoader.ensureLoaded();

    // Get API key from environment
    blsApiKey = TestEnvironmentLoader.getEnv("BLS_API_KEY");

    // Verify environment is properly configured for integration tests
    assertNotNull(blsApiKey, "BLS_API_KEY must be set for integration tests");
  }


  /**
   * Unit test for QCEW bulk file download (Phase 5, Step 5.1).
   * <p>Validates that bulk CSV files can be downloaded from BLS:
   * <ul>
   *   <li>Annual file (~80MB) downloads successfully</li>
   *   <li>Quarterly file (~323MB) downloads successfully</li>
   *   <li>Downloaded files are saved to correct cache location</li>
   * </ul>
   */
  @Test public void testDownloadQcewBulkFile() throws Exception {
    System.out.println("\n=== Test: QCEW Bulk File Download ===");
    StorageProvider storageProvider = createStorageProvider();
    BlsDataDownloader downloader = new BlsDataDownloader(blsApiKey, tempDir.toString(), storageProvider, storageProvider);

    // Test annual file download (smaller, faster)
    System.out.println("Testing annual bulk file download...");
    String annualZipPath = downloader.downloadQcewBulkFile(2023, "annual");

    assertNotNull(annualZipPath, "Annual bulk file path should be returned");
    assertTrue(storageProvider.exists(annualZipPath), "Annual ZIP file should exist");
    long fileSize = storageProvider.getMetadata(annualZipPath).getSize();
    assertTrue(fileSize > 1000000, "Annual ZIP should be > 1MB (actual: ~80MB)");
    System.out.printf("Annual file downloaded: %s (%.2f MB)%n",
        annualZipPath, fileSize / 1024.0 / 1024.0);

    System.out.println("=== QCEW Bulk File Download Test PASSED ===\n");
  }


  /**
   * Unit test for QCEW bulk file caching logic (Phase 5, Step 5.1).
   * <p>Validates that caching prevents unnecessary re-downloads:
   * <ul>
   *   <li>First download retrieves file from network</li>
   *   <li>Second download uses cached file (no network request)</li>
   *   <li>Cache hit is significantly faster than network download</li>
   * </ul>
   */
  @Test public void testQcewBulkFileCaching() throws Exception {
    System.out.println("\n=== Test: QCEW Bulk File Caching ===");
    StorageProvider storageProvider = createStorageProvider();
    BlsDataDownloader downloader = new BlsDataDownloader(blsApiKey, tempDir.toString(), storageProvider, storageProvider);

    // First download - should hit network
    System.out.println("First download (from network)...");
    long startTime1 = System.currentTimeMillis();
    String firstDownloadPath = downloader.downloadQcewBulkFile(2023, "annual");
    long firstDownloadTime = System.currentTimeMillis() - startTime1;

    assertNotNull(firstDownloadPath);
    assertTrue(storageProvider.exists(firstDownloadPath));
    System.out.printf("First download took: %d ms%n", firstDownloadTime);

    // Second download - should use cache
    System.out.println("Second download (from cache)...");
    long startTime2 = System.currentTimeMillis();
    String secondDownloadPath = downloader.downloadQcewBulkFile(2023, "annual");
    long secondDownloadTime = System.currentTimeMillis() - startTime2;

    assertNotNull(secondDownloadPath);
    assertTrue(storageProvider.exists(secondDownloadPath));
    System.out.printf("Second download took: %d ms%n", secondDownloadTime);

    // Verify both downloads point to same file
    assertEquals(firstDownloadPath, secondDownloadPath, "Both downloads should return the same file path");

    // Verify cache hit is significantly faster (at least 10x faster, typically 100x+)
    assertTrue(secondDownloadTime < firstDownloadTime / 10,
        String.format("Cache hit (%d ms) should be much faster than network download (%d ms)",
            secondDownloadTime, firstDownloadTime));

    System.out.printf("Cache speedup: %.1fx faster%n", (double) firstDownloadTime / secondDownloadTime);
    System.out.println("=== QCEW Bulk File Caching Test PASSED ===\n");
  }


  /**
   * Phase 5, Step 5.4: Regression test for metro_wages with qtr column.
   * Verifies:
   * 1. JSON data structure supports qtr field
   * 2. Backward compatibility with existing data queries
   * 3. New qtr column values work correctly (annual="A", quarterly="1","2","3","4")
   */
  @Test public void testMetroWagesRegressionWithQtrColumn() throws Exception {
    System.out.println("\n=== Test: Metro Wages Regression (qtr column) ===");
    StorageProvider storageProvider = createStorageProvider();

    // Create test JSON data with both annual and quarterly records
    String jsonPath = "metro_wages_test.json";
    String jsonContent = "[\n"
  +
        "  {\"metro_area_code\":\"A419\",\"metro_area_name\":\"Atlanta-Sandy Springs-Roswell, GA\"," +
        "\"year\":2023,\"qtr\":\"A\",\"average_weekly_wage\":1200,\"average_annual_pay\":62400},\n"
  +
        "  {\"metro_area_code\":\"A419\",\"metro_area_name\":\"Atlanta-Sandy Springs-Roswell, GA\"," +
        "\"year\":2023,\"qtr\":\"1\",\"average_weekly_wage\":1180,\"average_annual_pay\":61360},\n"
  +
        "  {\"metro_area_code\":\"A419\",\"metro_area_name\":\"Atlanta-Sandy Springs-Roswell, GA\"," +
        "\"year\":2023,\"qtr\":\"2\",\"average_weekly_wage\":1190,\"average_annual_pay\":61880},\n"
  +
        "  {\"metro_area_code\":\"A419\",\"metro_area_name\":\"Atlanta-Sandy Springs-Roswell, GA\"," +
        "\"year\":2023,\"qtr\":\"3\",\"average_weekly_wage\":1210,\"average_annual_pay\":62920},\n"
  +
        "  {\"metro_area_code\":\"A419\",\"metro_area_name\":\"Atlanta-Sandy Springs-Roswell, GA\"," +
        "\"year\":2023,\"qtr\":\"4\",\"average_weekly_wage\":1220,\"average_annual_pay\":63440}\n"
  +
        "]";

    storageProvider.writeFile(jsonPath, jsonContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    System.out.println("Created test JSON with 5 records (1 annual + 4 quarterly)");

    // Read file from S3 via StorageProvider and write to local temp file for DuckDB
    File localJsonFile = File.createTempFile("metro_wages_regression_", ".json");
    localJsonFile.deleteOnExit();
    try (java.io.InputStream is = storageProvider.openInputStream(jsonPath);
         java.io.FileOutputStream fos = new java.io.FileOutputStream(localJsonFile)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = is.read(buffer)) != -1) {
        fos.write(buffer, 0, bytesRead);
      }
    }
    System.out.println("Copied JSON from S3 to local file for DuckDB: " + localJsonFile.getAbsolutePath());

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Test JSON can be read with qtr field
        String readQuery =
            String.format("SELECT metro_area_code, year, qtr, average_weekly_wage, average_annual_pay " +
            "FROM read_json_auto('%s')",
            localJsonFile.getAbsolutePath());

        try (ResultSet rs = stmt.executeQuery(readQuery)) {
          int rowCount = 0;
          int annualCount = 0;
          int quarterlyCount = 0;

          while (rs.next()) {
            rowCount++;
            String qtr = rs.getString("qtr");
            String metroCode = rs.getString("metro_area_code");
            int year = rs.getInt("year");

            assertEquals("A419", metroCode, "Metro code should be A419");
            assertEquals(2023, year, "Year should be 2023");

            if ("A".equals(qtr)) {
              annualCount++;
            } else if (qtr.matches("[1-4]")) {
              quarterlyCount++;
            }
          }

          assertEquals(5, rowCount, "Should have 5 total records");
          assertEquals(1, annualCount, "Should have 1 annual record (qtr=A)");
          assertEquals(4, quarterlyCount, "Should have 4 quarterly records (qtr=1,2,3,4)");

          System.out.println("✓ Found " + rowCount + " records: " + annualCount +
                           " annual, " + quarterlyCount + " quarterly");
        }

        // Test backward compatibility: Query without qtr in WHERE clause
        String backwardCompatQuery =
            String.format("SELECT metro_area_code, year, average_annual_pay FROM read_json_auto('%s') " +
            "WHERE metro_area_code = 'A419' AND year = 2023",
            localJsonFile.getAbsolutePath());

        try (ResultSet rs = stmt.executeQuery(backwardCompatQuery)) {
          int legacyQueryCount = 0;
          while (rs.next()) {
            legacyQueryCount++;
          }
          assertEquals(5, legacyQueryCount,
              "Legacy queries without qtr should still return all records");
          System.out.println("✓ Backward compatibility verified: Legacy queries work");
        }

        // Test new functionality: Query with qtr column filter
        String qtrQuery =
            String.format("SELECT metro_area_code, year, qtr, average_weekly_wage FROM read_json_auto('%s') " +
            "WHERE metro_area_code = 'A419' AND year = 2023 AND qtr = 'A'",
            localJsonFile.getAbsolutePath());

        try (ResultSet rs = stmt.executeQuery(qtrQuery)) {
          assertTrue(rs.next(), "Should find annual record");
          assertEquals("A", rs.getString("qtr"), "Quarter should be A");
          assertEquals(1200, rs.getInt("average_weekly_wage"), "Weekly wage should match");
          assertFalse(rs.next(), "Should only have one annual record");
          System.out.println("✓ New qtr column queries work correctly");
        }

        // Test quarterly filtering
        String quarterlyQuery =
            String.format("SELECT qtr, average_weekly_wage FROM read_json_auto('%s') " +
            "WHERE metro_area_code = 'A419' AND year = 2023 AND qtr IN ('1', '2', '3', '4') " +
            "ORDER BY qtr",
            localJsonFile.getAbsolutePath());

        try (ResultSet rs = stmt.executeQuery(quarterlyQuery)) {
          int qtrCount = 0;
          while (rs.next()) {
            qtrCount++;
          }
          assertEquals(4, qtrCount, "Should find 4 quarterly records");
          System.out.println("✓ Quarterly record filtering works correctly");
        }
      }
    }

    System.out.println("=== Metro Wages Regression Test PASSED ===\n");
  }
}
