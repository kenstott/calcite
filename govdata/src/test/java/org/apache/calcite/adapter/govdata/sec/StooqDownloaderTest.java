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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link StooqDownloader}.
 * Tests URL construction, CSV parsing, and rate limiter behavior.
 */
@Tag("unit")
class StooqDownloaderTest {

  private StooqDownloader downloader;

  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() {
    StorageProvider storageProvider = new LocalFileStorageProvider();
    downloader = new StooqDownloader(storageProvider, null, null);
  }

  @Test
  void testUrlConstructionBasic() {
    String url = downloader.buildStooqUrl("AAPL", null, null);
    assertEquals("https://stooq.com/q/d/l/?s=aapl.us&i=d", url);
  }

  @Test
  void testUrlConstructionWithYears() {
    String url = downloader.buildStooqUrl("AAPL", 2020, 2024);
    assertEquals("https://stooq.com/q/d/l/?s=aapl.us&i=d&d1=20200101&d2=20241231", url);
  }

  @Test
  void testUrlConstructionUpperCaseTicker() {
    String url = downloader.buildStooqUrl("MSFT", 2023, 2023);
    // Ticker should be lowercased
    assertEquals("https://stooq.com/q/d/l/?s=msft.us&i=d&d1=20230101&d2=20231231", url);
  }

  @Test
  void testUrlConstructionStartYearOnly() {
    String url = downloader.buildStooqUrl("GOOGL", 2022, null);
    assertEquals("https://stooq.com/q/d/l/?s=googl.us&i=d&d1=20220101", url);
  }

  @Test
  void testUrlConstructionEndYearOnly() {
    String url = downloader.buildStooqUrl("AMZN", null, 2023);
    assertEquals("https://stooq.com/q/d/l/?s=amzn.us&i=d&d2=20231231", url);
  }

  @Test
  void testCsvParsingSingleRow() throws Exception {
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "2024-12-13,248.49,251.14,247.48,248.13,49286500\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2024);

    assertEquals(1, records.size());

    StooqDownloader.StockPriceRecord record = records.get(0);
    assertEquals("2024-12-13", record.date);
    assertEquals(248.49, record.open, 0.01);
    assertEquals(251.14, record.high, 0.01);
    assertEquals(247.48, record.low, 0.01);
    assertEquals(248.13, record.close, 0.01);
    assertEquals(248.13, record.adjClose, 0.01);  // adj_close equals close for Stooq
    assertEquals(Long.valueOf(49286500), record.volume);
  }

  @Test
  void testCsvParsingMultipleRows() throws Exception {
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "2024-12-13,248.49,251.14,247.48,248.13,49286500\n"
        + "2024-12-12,247.96,251.35,246.35,247.96,47989300\n"
        + "2024-12-11,246.67,248.59,245.84,246.49,44169700\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2024);

    assertEquals(3, records.size());

    // Records should be in order as provided
    assertEquals("2024-12-13", records.get(0).date);
    assertEquals("2024-12-12", records.get(1).date);
    assertEquals("2024-12-11", records.get(2).date);
  }

  @Test
  void testCsvParsingYearFiltering() throws Exception {
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "2024-12-13,248.49,251.14,247.48,248.13,49286500\n"
        + "2023-12-29,192.53,194.17,191.94,192.53,37122800\n"
        + "2022-12-30,129.04,129.95,127.43,129.93,77034200\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2023);

    // Only 2023 record should be included
    assertEquals(1, records.size());
    assertEquals("2023-12-29", records.get(0).date);
  }

  @Test
  void testCsvParsingEmptyVolume() throws Exception {
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "2024-12-13,248.49,251.14,247.48,248.13,\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2024);

    assertEquals(1, records.size());
    assertNull(records.get(0).volume);
  }

  @Test
  void testCsvParsingNoVolume() throws Exception {
    // Some historical data may not have volume column
    String csv = "Date,Open,High,Low,Close\n"
        + "1984-09-07,0.0996047,0.100827,0.0984022,0.0996047\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 1984);

    assertEquals(1, records.size());
    StooqDownloader.StockPriceRecord record = records.get(0);
    assertEquals("1984-09-07", record.date);
    assertEquals(0.0996047, record.open, 0.0000001);
    assertNull(record.volume);
  }

  @Test
  void testCsvParsingInvalidRow() throws Exception {
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "2024-12-13,248.49,251.14,247.48,248.13,49286500\n"
        + "invalid row\n"  // Should be skipped
        + "2024-12-11,246.67,248.59,245.84,246.49,44169700\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2024);

    // Invalid row should be skipped
    assertEquals(2, records.size());
  }

  @Test
  void testCsvParsingEmptyResponse() throws Exception {
    String csv = "Date,Open,High,Low,Close,Volume\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2024);

    assertTrue(records.isEmpty());
  }

  @Test
  void testCsvParsingNAValues() throws Exception {
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "2024-12-13,N/A,251.14,247.48,248.13,N/A\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2024);

    assertEquals(1, records.size());
    StooqDownloader.StockPriceRecord record = records.get(0);
    assertNull(record.open);
    assertNotNull(record.high);
    assertNull(record.volume);
  }

  @Test
  void testRateLimiterInitialState() {
    StooqDownloader.RateLimiter limiter = new StooqDownloader.RateLimiter();
    assertEquals(StooqDownloader.RateLimiter.DEFAULT_BASE_RATE_LIMIT_MS,
        limiter.getCurrentBackoffMs());
    assertEquals(StooqDownloader.RateLimiter.DEFAULT_MAX_RETRIES, limiter.getMaxRetries());
  }

  @Test
  void testRateLimiterCustomConfig() {
    StooqDownloader.RateLimiter limiter = new StooqDownloader.RateLimiter(500, 10000, 5);
    assertEquals(500, limiter.getCurrentBackoffMs());
    assertEquals(5, limiter.getMaxRetries());
  }

  @Test
  void testRateLimiterBackoffIncrease() {
    StooqDownloader.RateLimiter limiter = new StooqDownloader.RateLimiter(1000, 30000, 3);

    assertEquals(1000, limiter.getCurrentBackoffMs());

    limiter.onRateLimited();
    assertEquals(2000, limiter.getCurrentBackoffMs());

    limiter.onRateLimited();
    assertEquals(4000, limiter.getCurrentBackoffMs());

    limiter.onRateLimited();
    assertEquals(8000, limiter.getCurrentBackoffMs());

    limiter.onRateLimited();
    assertEquals(16000, limiter.getCurrentBackoffMs());

    limiter.onRateLimited();
    assertEquals(30000, limiter.getCurrentBackoffMs());  // Max cap

    limiter.onRateLimited();
    assertEquals(30000, limiter.getCurrentBackoffMs());  // Still at max
  }

  @Test
  void testRateLimiterSuccessResetsBackoff() {
    StooqDownloader.RateLimiter limiter = new StooqDownloader.RateLimiter(1000, 30000, 3);

    limiter.onRateLimited();
    limiter.onRateLimited();
    assertEquals(4000, limiter.getCurrentBackoffMs());

    limiter.onSuccess();
    assertEquals(1000, limiter.getCurrentBackoffMs());  // Reset to base
  }

  @Test
  void testRateLimiterWaitTiming() throws InterruptedException {
    StooqDownloader.RateLimiter limiter = new StooqDownloader.RateLimiter(100, 1000, 3);

    // First call should not wait (no previous request)
    long start = System.currentTimeMillis();
    limiter.waitForRateLimit();
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 50, "First call should not wait significantly");

    // Second call should wait approximately 100ms
    start = System.currentTimeMillis();
    limiter.waitForRateLimit();
    elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed >= 80, "Second call should wait at least ~80ms");
    assertTrue(elapsed < 200, "Second call should not wait more than ~200ms");
  }

  @Test
  void testConstructorRequiresStorageProvider() {
    assertThrows(IllegalArgumentException.class, () -> {
      new StooqDownloader(null, null, null);
    });
  }

  @Test
  void testConstructorWithCredentials() {
    StorageProvider storageProvider = new LocalFileStorageProvider();
    StooqDownloader downloaderWithAuth = new StooqDownloader(storageProvider, "user", "pass");
    assertNotNull(downloaderWithAuth);
  }

  @Test
  void testConstructorWithCustomRateLimiting() {
    StorageProvider storageProvider = new LocalFileStorageProvider();
    StooqDownloader customDownloader = new StooqDownloader(
        storageProvider, null, null, 500, 5000, 5);
    assertNotNull(customDownloader);
  }

  @Test
  void testCsvParsingDecimalPrecision() throws Exception {
    // Test that we handle small decimal values correctly (historical split-adjusted prices)
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "1984-09-07,0.0996047,0.100827,0.0984022,0.0996047,98811715\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 1984);

    assertEquals(1, records.size());
    StooqDownloader.StockPriceRecord record = records.get(0);
    assertEquals(0.0996047, record.open, 0.0000001);
    assertEquals(0.100827, record.high, 0.0000001);
  }

  @Test
  void testCsvParsingLargeVolume() throws Exception {
    // Test handling of large volume numbers
    String csv = "Date,Open,High,Low,Close,Volume\n"
        + "2024-12-13,248.49,251.14,247.48,248.13,9999999999\n";

    BufferedReader reader = new BufferedReader(new StringReader(csv));
    List<StooqDownloader.StockPriceRecord> records = downloader.parseCsvResponse(reader, 2024);

    assertEquals(1, records.size());
    assertEquals(Long.valueOf(9999999999L), records.get(0).volume);
  }

  @Test
  void testStooqBaseUrl() {
    assertEquals("https://stooq.com/q/d/l/", StooqDownloader.STOOQ_BASE_URL);
  }
}
