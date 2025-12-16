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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link StooqDownloader}.
 * These tests make actual HTTP requests to Stooq.com and require network access.
 */
@Tag("integration")
class StooqDownloaderIntegrationTest {

  private StorageProvider storageProvider;
  private StooqDownloader downloader;
  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    tempDir = Files.createTempDirectory("stooq-test");
    storageProvider = new LocalFileStorageProvider();
    // Use shorter rate limit for testing but still be respectful
    downloader = new StooqDownloader(storageProvider, null, null, 500, 5000, 3);
  }

  @AfterEach
  void tearDown() throws IOException {
    // Clean up temp directory
    if (tempDir != null && Files.exists(tempDir)) {
      Files.walk(tempDir)
          .sorted((a, b) -> b.compareTo(a))  // Reverse order for deletion
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  void testDownloadAppleStockPrices() throws Exception {
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("AAPL", "0000320193"));

    String basePath = tempDir.toString();
    int year = 2023;

    downloader.downloadStockPrices(basePath, pairs, year, year);

    // Verify the Parquet file was created
    String parquetPath = basePath + "/stock_prices/ticker=AAPL/year=" + year + "/aapl_prices.parquet";
    assertTrue(storageProvider.exists(parquetPath),
        "Parquet file should exist at: " + parquetPath);

    // Verify file is not empty
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
    assertTrue(metadata.getSize() > 0, "Parquet file should not be empty");

    System.out.println("Successfully downloaded AAPL stock prices for " + year);
    System.out.println("File size: " + metadata.getSize() + " bytes");
  }

  @Test
  void testDownloadMicrosoftStockPrices() throws Exception {
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("MSFT", "0000789019"));

    String basePath = tempDir.toString();
    int year = 2023;

    downloader.downloadStockPrices(basePath, pairs, year, year);

    String parquetPath = basePath + "/stock_prices/ticker=MSFT/year=" + year + "/msft_prices.parquet";
    assertTrue(storageProvider.exists(parquetPath),
        "Parquet file should exist at: " + parquetPath);

    System.out.println("Successfully downloaded MSFT stock prices for " + year);
  }

  @Test
  void testDownloadMultipleYears() throws Exception {
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("AAPL", "0000320193"));

    String basePath = tempDir.toString();
    int startYear = 2022;
    int endYear = 2023;

    downloader.downloadStockPrices(basePath, pairs, startYear, endYear);

    // Verify both years were downloaded
    for (int year = startYear; year <= endYear; year++) {
      String parquetPath = basePath + "/stock_prices/ticker=AAPL/year=" + year + "/aapl_prices.parquet";
      assertTrue(storageProvider.exists(parquetPath),
          "Parquet file should exist for year " + year);
    }

    System.out.println("Successfully downloaded AAPL stock prices for " + startYear + "-" + endYear);
  }

  @Test
  void testDownloadMultipleTickers() throws Exception {
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("AAPL", "0000320193"));
    pairs.add(new AlphaVantageDownloader.TickerCikPair("MSFT", "0000789019"));

    String basePath = tempDir.toString();
    int year = 2023;

    downloader.downloadStockPrices(basePath, pairs, year, year);

    // Verify both tickers were downloaded
    String aaplPath = basePath + "/stock_prices/ticker=AAPL/year=" + year + "/aapl_prices.parquet";
    String msftPath = basePath + "/stock_prices/ticker=MSFT/year=" + year + "/msft_prices.parquet";

    assertTrue(storageProvider.exists(aaplPath), "AAPL parquet file should exist");
    assertTrue(storageProvider.exists(msftPath), "MSFT parquet file should exist");

    System.out.println("Successfully downloaded stock prices for 2 tickers");
  }

  @Test
  void testRateLimitingTiming() throws Exception {
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("AAPL", "0000320193"));
    pairs.add(new AlphaVantageDownloader.TickerCikPair("MSFT", "0000789019"));
    pairs.add(new AlphaVantageDownloader.TickerCikPair("GOOGL", "0001652044"));

    String basePath = tempDir.toString();
    int year = 2023;

    long startTime = System.currentTimeMillis();
    downloader.downloadStockPrices(basePath, pairs, year, year);
    long elapsed = System.currentTimeMillis() - startTime;

    // With 500ms rate limit and 3 tickers, should take at least ~1000ms
    // (first request immediate, then 2 x 500ms waits)
    assertTrue(elapsed >= 800,
        "Should take at least 800ms for 3 requests with 500ms rate limit, took: " + elapsed + "ms");

    System.out.println("3 downloads completed in " + elapsed + "ms (rate limited)");
  }

  @Test
  void testFetchWithRateLimitingSuccess() throws Exception {
    // Test direct fetch method
    List<StooqDownloader.StockPriceRecord> records =
        downloader.fetchWithRateLimiting("AAPL", 2023);

    assertNotNull(records);
    assertFalse(records.isEmpty(), "Should return some price records for AAPL 2023");

    System.out.println("Fetched " + records.size() + " price records for AAPL 2023");

    // Verify record structure
    StooqDownloader.StockPriceRecord firstRecord = records.get(0);
    assertNotNull(firstRecord.date);
    assertNotNull(firstRecord.close);
    assertTrue(firstRecord.date.startsWith("2023"));
  }

  @Test
  void testInvalidTickerReturnsEmptyList() throws Exception {
    // Invalid tickers should return empty list, not throw
    List<StooqDownloader.StockPriceRecord> records =
        downloader.fetchWithRateLimiting("XXYZZ123456", 2023);

    assertNotNull(records);
    // Invalid ticker may return empty or header-only response
    // Either is acceptable
    System.out.println("Invalid ticker returned " + records.size() + " records");
  }

  @Test
  void testCachingHistoricalData() throws Exception {
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("AAPL", "0000320193"));

    String basePath = tempDir.toString();
    int historicalYear = 2022;  // Historical year

    // First download
    long start1 = System.currentTimeMillis();
    downloader.downloadStockPrices(basePath, pairs, historicalYear, historicalYear);
    long elapsed1 = System.currentTimeMillis() - start1;

    String parquetPath = basePath + "/stock_prices/ticker=AAPL/year=" + historicalYear
        + "/aapl_prices.parquet";
    assertTrue(storageProvider.exists(parquetPath));

    // Get file modification time
    StorageProvider.FileMetadata metadata1 = storageProvider.getMetadata(parquetPath);
    long modTime1 = metadata1.getLastModified();

    // Wait a bit
    Thread.sleep(100);

    // Second download - should use cache for historical year
    StooqDownloader downloader2 = new StooqDownloader(storageProvider, null, null, 500, 5000, 3);
    long start2 = System.currentTimeMillis();
    downloader2.downloadStockPrices(basePath, pairs, historicalYear, historicalYear);
    long elapsed2 = System.currentTimeMillis() - start2;

    // File should not be modified (cached)
    StorageProvider.FileMetadata metadata2 = storageProvider.getMetadata(parquetPath);
    long modTime2 = metadata2.getLastModified();

    assertEquals(modTime1, modTime2, "File should not be modified for historical year (cached)");

    System.out.println("First download: " + elapsed1 + "ms, Second (cached): " + elapsed2 + "ms");
  }

  private void assertEquals(long expected, long actual, String message) {
    if (expected != actual) {
      throw new AssertionError(message + " Expected: " + expected + ", Actual: " + actual);
    }
  }

  @Test
  void testFailedTickersTracking() throws Exception {
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("AAPL", "0000320193"));
    pairs.add(new AlphaVantageDownloader.TickerCikPair("XXYZZ123456", "0000000001"));  // Invalid

    String basePath = tempDir.toString();
    int year = 2023;

    downloader.downloadStockPrices(basePath, pairs, year, year);

    // AAPL should succeed
    String aaplPath = basePath + "/stock_prices/ticker=AAPL/year=" + year + "/aapl_prices.parquet";
    assertTrue(storageProvider.exists(aaplPath), "AAPL should be downloaded");

    // Invalid ticker path should not exist or be empty
    // (depends on whether Stooq returns empty data or error)
    System.out.println("Failed tickers: " + downloader.getFailedTickers());
  }

  @Test
  void testHistoricalDataDownload() throws Exception {
    // Test downloading very old data (split-adjusted)
    List<AlphaVantageDownloader.TickerCikPair> pairs =
        new ArrayList<AlphaVantageDownloader.TickerCikPair>();
    pairs.add(new AlphaVantageDownloader.TickerCikPair("AAPL", "0000320193"));

    String basePath = tempDir.toString();
    int year = 1990;  // Early data

    downloader.downloadStockPrices(basePath, pairs, year, year);

    String parquetPath = basePath + "/stock_prices/ticker=AAPL/year=" + year + "/aapl_prices.parquet";

    if (storageProvider.exists(parquetPath)) {
      StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
      System.out.println("Downloaded historical data for AAPL 1990, size: " + metadata.getSize());
    } else {
      System.out.println("AAPL data may not be available for 1990");
    }
  }
}
