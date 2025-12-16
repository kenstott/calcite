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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Downloads historical stock price data from Stooq.com and stores it in Parquet format.
 *
 * <p>Stooq provides free bulk historical stock data with permissive rate limits,
 * making it ideal for downloading prices for thousands of tickers.
 *
 * <p>Features:
 * <ul>
 *   <li>Full historical data in single request (no pagination)</li>
 *   <li>Permissive rate limiting (~1 req/sec)</li>
 *   <li>Split-adjusted prices</li>
 *   <li>Optional authentication for premium accounts</li>
 *   <li>Exponential backoff on rate limiting</li>
 * </ul>
 *
 * <p>Usage example:
 * <pre>{@code
 * StorageProvider storageProvider = ...;
 * StooqDownloader downloader = new StooqDownloader(storageProvider, null, null);
 * List<TickerCikPair> pairs = Arrays.asList(
 *     new TickerCikPair("AAPL", "0000320193"),
 *     new TickerCikPair("MSFT", "0000789019")
 * );
 * downloader.downloadStockPrices("/data", pairs, 2020, 2024);
 * }</pre>
 */
public class StooqDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(StooqDownloader.class);

  /** Base URL for Stooq CSV downloads. */
  static final String STOOQ_BASE_URL = "https://stooq.com/q/d/l/";

  private static final String USER_AGENT = "Apache Calcite SEC Adapter";

  // Parallel downloads - keep low to avoid triggering rate limits
  private static final int MAX_PARALLEL_DOWNLOADS = 1;  // Sequential recommended

  // Avro schema for stock price records (same as AlphaVantageDownloader for compatibility)
  private static final String STOCK_PRICE_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"StockPrice\","
      + "\"fields\": ["
      + "{\"name\": \"cik\", \"type\": \"string\"},"
      + "{\"name\": \"date\", \"type\": \"string\"},"
      + "{\"name\": \"open\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"high\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"low\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"close\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"adj_close\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"volume\", \"type\": [\"null\", \"long\"], \"default\": null}"
      + "]"
      + "}";

  private final Schema priceSchema = new Schema.Parser().parse(STOCK_PRICE_SCHEMA);
  private final StorageProvider storageProvider;
  private final String username;  // Optional, from STOOQ_USERNAME
  private final String password;  // Optional, from STOOQ_PASSWORD
  private final ExecutorService downloadExecutor;
  private final RateLimiter rateLimiter;
  private final List<String> failedTickers = new ArrayList<String>();

  /**
   * Creates a StooqDownloader with default rate limiting configuration.
   *
   * @param storageProvider Storage provider for Parquet output
   * @param username Optional Stooq username (null for free tier)
   * @param password Optional Stooq password (null for free tier)
   */
  public StooqDownloader(StorageProvider storageProvider, String username, String password) {
    this(storageProvider, username, password,
        getEnvLong("STOCK_PRICE_BASE_RATE_LIMIT_MS", RateLimiter.DEFAULT_BASE_RATE_LIMIT_MS),
        getEnvLong("STOCK_PRICE_MAX_BACKOFF_MS", RateLimiter.DEFAULT_MAX_BACKOFF_MS),
        getEnvInt("STOCK_PRICE_MAX_RETRIES", RateLimiter.DEFAULT_MAX_RETRIES));
  }

  /**
   * Creates a StooqDownloader with custom rate limiting configuration.
   *
   * @param storageProvider Storage provider for Parquet output
   * @param username Optional Stooq username (null for free tier)
   * @param password Optional Stooq password (null for free tier)
   * @param baseRateLimitMs Base milliseconds between requests
   * @param maxBackoffMs Maximum backoff on rate limiting
   * @param maxRetries Maximum retry attempts per ticker
   */
  public StooqDownloader(StorageProvider storageProvider, String username, String password,
                         long baseRateLimitMs, long maxBackoffMs, int maxRetries) {
    if (storageProvider == null) {
      throw new IllegalArgumentException("StorageProvider is required");
    }
    this.storageProvider = storageProvider;
    this.username = username;
    this.password = password;
    this.downloadExecutor = Executors.newFixedThreadPool(MAX_PARALLEL_DOWNLOADS);
    this.rateLimiter = new RateLimiter(baseRateLimitMs, maxBackoffMs, maxRetries);
  }

  private static long getEnvLong(String name, long defaultValue) {
    String value = System.getenv(name);
    if (value != null && !value.isEmpty()) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid value for {}: {}, using default {}", name, value, defaultValue);
      }
    }
    return defaultValue;
  }

  private static int getEnvInt(String name, int defaultValue) {
    String value = System.getenv(name);
    if (value != null && !value.isEmpty()) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid value for {}: {}, using default {}", name, value, defaultValue);
      }
    }
    return defaultValue;
  }

  /**
   * Downloads stock prices for multiple tickers.
   *
   * @param basePath Base directory for stock_prices partition
   * @param tickerCikPairs List of ticker-CIK pairs to download
   * @param startYear Start year for data range
   * @param endYear End year for data range
   */
  public void downloadStockPrices(String basePath,
                                  List<AlphaVantageDownloader.TickerCikPair> tickerCikPairs,
                                  int startYear, int endYear) {
    LOGGER.info("Starting Stooq stock price downloads for {} tickers from {} to {}",
        tickerCikPairs.size(), startYear, endYear);

    failedTickers.clear();
    List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();

    for (final AlphaVantageDownloader.TickerCikPair pair : tickerCikPairs) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(new Runnable() {
        @Override public void run() {
          try {
            downloadTickerData(basePath, pair, startYear, endYear);
          } catch (Exception e) {
            LOGGER.error("Failed to download data for {}: {}", pair.ticker, e.getMessage());
            synchronized (failedTickers) {
              failedTickers.add(pair.ticker);
            }
          }
        }
      }, downloadExecutor);
      futures.add(future);
    }

    // Wait for all downloads to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    downloadExecutor.shutdown();
    try {
      downloadExecutor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.warn("Download executor interrupted: {}", e.getMessage());
      Thread.currentThread().interrupt();
    }

    if (!failedTickers.isEmpty()) {
      LOGGER.warn("Failed to download {} tickers: {}", failedTickers.size(), failedTickers);
    }

    LOGGER.info("Completed stock price downloads");
  }

  /**
   * Downloads data for a single ticker.
   */
  private void downloadTickerData(String basePath, AlphaVantageDownloader.TickerCikPair pair,
                                  int startYear, int endYear) {
    LOGGER.debug("downloadTickerData called for ticker {} with basePath: {}", pair.ticker, basePath);

    // Get current time in EST (market timezone)
    ZoneId estZone = ZoneId.of("America/New_York");
    ZonedDateTime nowEst = ZonedDateTime.now(estZone);
    int currentYear = nowEst.getYear();

    // Market close is 4:30 PM EST (16:30)
    ZonedDateTime todayMarketClose = nowEst.toLocalDate()
        .atTime(16, 30)
        .atZone(estZone);

    for (int year = startYear; year <= endYear; year++) {
      // Build the path using StorageProvider.resolvePath for S3 compatibility
      String stockPricesDir = storageProvider.resolvePath(basePath, "stock_prices");
      String tickerDir = storageProvider.resolvePath(stockPricesDir,
          String.format("ticker=%s", pair.ticker.toUpperCase()));
      String yearDir = storageProvider.resolvePath(tickerDir, String.format("year=%d", year));

      String fullPath = storageProvider.resolvePath(yearDir,
          String.format("%s_prices.parquet", pair.ticker.toLowerCase()));

      // Check if data already exists and determine if refresh is needed
      boolean needsDownload;
      try {
        needsDownload = needsDownload(fullPath, year, currentYear, nowEst, todayMarketClose);
      } catch (IOException e) {
        LOGGER.debug("Error checking if file exists: {}", e.getMessage());
        needsDownload = true;
      }

      if (!needsDownload) {
        continue;
      }

      try {
        // Fetch data from Stooq with rate limiting
        List<StockPriceRecord> prices = fetchWithRateLimiting(pair.ticker, year);

        if (prices.isEmpty()) {
          LOGGER.warn("No data returned for {} year {} - ticker may be delisted or invalid",
              pair.ticker, year);
          continue;
        }

        // Write to Parquet using StorageProvider
        writeToParquet(fullPath, prices, pair.cik);

        LOGGER.info("Downloaded {} price records for {} year {}",
            prices.size(), pair.ticker, year);

      } catch (IOException e) {
        LOGGER.warn("Failed to download {} for year {}: {}", pair.ticker, year, e.getMessage());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Download interrupted for " + pair.ticker, e);
      }
    }
  }

  /**
   * Determines if a download is needed for the given path and year.
   * Historical years are cached forever, current year is refreshed after market close.
   */
  private boolean needsDownload(String fullPath, int year, int currentYear,
                                ZonedDateTime nowEst, ZonedDateTime todayMarketClose)
      throws IOException {
    if (!storageProvider.exists(fullPath)) {
      return true;
    }

    // For historical years (not current year), use cached data
    if (year < currentYear) {
      LOGGER.info("Using cached stock prices for year {} (historical data)", year);
      return false;
    }

    // For current year, check if we need to refresh after market close
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata(fullPath);
    long fileModifiedTime = metadata.getLastModified();
    Instant fileModifiedInstant = Instant.ofEpochMilli(fileModifiedTime);
    ZonedDateTime fileModifiedEst = fileModifiedInstant.atZone(ZoneId.of("America/New_York"));

    // Refresh if: current time is after today's market close AND
    // file was modified before today's market close
    if (nowEst.isAfter(todayMarketClose) && fileModifiedEst.isBefore(todayMarketClose)) {
      LOGGER.info("Stock prices for year {} need refresh (modified {}, market closed at {})",
          year, fileModifiedEst, todayMarketClose);
      return true;
    }

    LOGGER.info("Using cached stock prices for year {} (current year, up to date)", year);
    return false;
  }

  /**
   * Fetches data with automatic rate limiting and retry on rate limit errors.
   */
  List<StockPriceRecord> fetchWithRateLimiting(String ticker, int year)
      throws IOException, InterruptedException {

    int attempts = 0;
    IOException lastException = null;

    while (attempts < rateLimiter.getMaxRetries()) {
      attempts++;

      // Proactive: wait for rate limit window
      rateLimiter.waitForRateLimit();

      HttpURLConnection conn = null;
      try {
        String url = buildStooqUrl(ticker, year, year);
        conn = createConnection(url);
        int responseCode = conn.getResponseCode();

        // Reactive: detect rate limiting
        if (isRateLimited(conn, responseCode)) {
          LOGGER.warn("Rate limited on attempt {}/{} for ticker {}",
              attempts, rateLimiter.getMaxRetries(), ticker);
          rateLimiter.onRateLimited();
          conn.disconnect();
          continue;  // Retry with increased backoff
        }

        // Server error - not recoverable
        if (responseCode >= 500) {
          LOGGER.error("Server error {} for ticker {}", responseCode, ticker);
          throw new IOException("Server error: " + responseCode);
        }

        // Client error (except 429) - not recoverable
        if (responseCode >= 400) {
          LOGGER.error("Client error {} for ticker {}", responseCode, ticker);
          throw new IOException("Client error: " + responseCode);
        }

        // Success - parse response
        List<StockPriceRecord> records;
        BufferedReader reader = null;
        try {
          reader = new BufferedReader(
              new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
          records = parseCsvResponse(reader, year);
        } finally {
          if (reader != null) {
            reader.close();
          }
        }

        rateLimiter.onSuccess();
        return records;

      } catch (SocketTimeoutException e) {
        // Timeout - treat as rate limit, recoverable
        LOGGER.warn("Timeout on attempt {}/{} for {}: {}",
            attempts, rateLimiter.getMaxRetries(), ticker, e.getMessage());
        lastException = e;
        rateLimiter.onRateLimited();
      } catch (ConnectException e) {
        // Connection failed - not recoverable
        LOGGER.error("Connection failed for {}: {}", ticker, e.getMessage());
        throw e;
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
    }

    throw new IOException("Failed after " + attempts + " attempts: "
        + (lastException != null ? lastException.getMessage() : "rate limited"));
  }

  /**
   * Builds the Stooq URL for a given ticker and date range.
   *
   * @param ticker Stock ticker symbol
   * @param startYear Start year (optional, null for full history)
   * @param endYear End year (optional, null for current)
   * @return The constructed URL
   */
  String buildStooqUrl(String ticker, Integer startYear, Integer endYear) {
    StringBuilder url = new StringBuilder(STOOQ_BASE_URL);
    url.append("?s=").append(ticker.toLowerCase()).append(".us");
    url.append("&i=d");  // Daily interval

    if (startYear != null) {
      url.append("&d1=").append(startYear).append("0101");
    }
    if (endYear != null) {
      url.append("&d2=").append(endYear).append("1231");
    }

    return url.toString();
  }

  /**
   * Parses CSV response into StockPriceRecord objects.
   *
   * @param reader The BufferedReader for the CSV response
   * @param filterYear The year to filter by (only records from this year are included)
   * @return List of parsed stock price records
   * @throws IOException If an I/O error occurs
   */
  List<StockPriceRecord> parseCsvResponse(BufferedReader reader, int filterYear)
      throws IOException {
    List<StockPriceRecord> prices = new ArrayList<StockPriceRecord>();
    String line;
    boolean headerSkipped = false;

    while ((line = reader.readLine()) != null) {
      if (!headerSkipped) {
        headerSkipped = true;
        continue;  // Skip header: "Date,Open,High,Low,Close,Volume"
      }

      String[] parts = line.split(",");
      if (parts.length < 5) {
        continue;  // Invalid row
      }

      String dateStr = parts[0];  // YYYY-MM-DD format

      // Filter by year if specified
      if (!dateStr.startsWith(String.valueOf(filterYear))) {
        continue;
      }

      StockPriceRecord record = new StockPriceRecord();
      record.date = dateStr;
      record.open = parseDouble(parts[1]);
      record.high = parseDouble(parts[2]);
      record.low = parseDouble(parts[3]);
      record.close = parseDouble(parts[4]);
      record.adjClose = record.close;  // Stooq returns adjusted prices
      record.volume = parts.length > 5 ? parseLong(parts[5]) : null;

      prices.add(record);
    }

    return prices;
  }

  /**
   * Creates an HTTP connection with optional authentication.
   */
  private HttpURLConnection createConnection(String urlString) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    // Add authentication if credentials provided
    if (username != null && password != null && !username.isEmpty()) {
      String auth = username + ":" + password;
      byte[] encodedBytes = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
      String encodedAuth = new String(encodedBytes, StandardCharsets.UTF_8);
      conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
    }

    return conn;
  }

  /**
   * Detects if response indicates rate limiting.
   * Stooq may return 429, empty response, or specific error patterns.
   */
  private boolean isRateLimited(HttpURLConnection conn, int responseCode) throws IOException {
    // Explicit 429 Too Many Requests
    if (responseCode == 429) {
      return true;
    }

    // Check for rate limit headers (if Stooq provides them)
    String retryAfter = conn.getHeaderField("Retry-After");
    if (retryAfter != null) {
      LOGGER.info("Server requested Retry-After: {}", retryAfter);
      return true;
    }

    // Check for empty or suspiciously small response (may indicate soft rate limit)
    if (responseCode == 200) {
      String contentLength = conn.getHeaderField("Content-Length");
      if (contentLength != null) {
        try {
          long length = Long.parseLong(contentLength);
          // Header-only response (~40 bytes) may indicate rate limiting
          if (length < 50) {
            LOGGER.debug("Suspiciously small response ({}B), may be rate limited", length);
            return true;
          }
        } catch (NumberFormatException e) {
          // Ignore, not rate limiting
        }
      }
    }

    return false;
  }

  private Double parseDouble(String value) {
    if (value == null || value.isEmpty() || value.equals("N/A")) {
      return null;
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private Long parseLong(String value) {
    if (value == null || value.isEmpty() || value.equals("N/A")) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Writes stock price data to a Parquet file using StorageProvider.
   */
  private void writeToParquet(String path, List<StockPriceRecord> prices, String cik)
      throws IOException {
    List<GenericRecord> records = new ArrayList<GenericRecord>();

    for (StockPriceRecord price : prices) {
      GenericRecord record = new GenericData.Record(priceSchema);
      record.put("cik", cik);
      record.put("date", price.date);
      record.put("open", price.open);
      record.put("high", price.high);
      record.put("low", price.low);
      record.put("close", price.close);
      record.put("adj_close", price.adjClose);
      record.put("volume", price.volume);
      records.add(record);
    }

    // Use StorageProvider to write Parquet file
    storageProvider.writeAvroParquet(path, priceSchema, records, "StockPrice");
  }

  /**
   * Returns the list of tickers that failed during the last download operation.
   */
  public List<String> getFailedTickers() {
    return new ArrayList<String>(failedTickers);
  }

  /**
   * Internal class for stock price records.
   */
  static class StockPriceRecord {
    String date;
    Double open;
    Double high;
    Double low;
    Double close;
    Double adjClose;
    Long volume;
  }

  /**
   * Thread-safe rate limiter that enforces minimum delay between requests
   * and handles rate limit responses with exponential backoff.
   */
  static class RateLimiter {
    static final long DEFAULT_BASE_RATE_LIMIT_MS = 1000;  // 1 second minimum
    static final long DEFAULT_MAX_BACKOFF_MS = 30000;     // 30 second max backoff
    static final int DEFAULT_MAX_RETRIES = 3;

    private final long baseRateLimitMs;
    private final long maxBackoffMs;
    private final int maxRetries;

    private long lastRequestTime = 0;
    private long currentBackoffMs;

    RateLimiter() {
      this(DEFAULT_BASE_RATE_LIMIT_MS, DEFAULT_MAX_BACKOFF_MS, DEFAULT_MAX_RETRIES);
    }

    RateLimiter(long baseRateLimitMs, long maxBackoffMs, int maxRetries) {
      this.baseRateLimitMs = baseRateLimitMs;
      this.maxBackoffMs = maxBackoffMs;
      this.maxRetries = maxRetries;
      this.currentBackoffMs = baseRateLimitMs;
    }

    /**
     * Wait until rate limit window has passed.
     * Call this BEFORE making each request.
     */
    synchronized void waitForRateLimit() throws InterruptedException {
      long now = System.currentTimeMillis();
      long timeSinceLastRequest = now - lastRequestTime;
      long waitTime = currentBackoffMs - timeSinceLastRequest;

      if (waitTime > 0) {
        LOGGER.debug("Rate limiter: waiting {}ms before next request", waitTime);
        Thread.sleep(waitTime);
      }

      lastRequestTime = System.currentTimeMillis();
    }

    /**
     * Call this after a successful request to reset backoff.
     */
    synchronized void onSuccess() {
      currentBackoffMs = baseRateLimitMs;
    }

    /**
     * Call this when rate limited. Increases backoff exponentially.
     */
    synchronized void onRateLimited() {
      currentBackoffMs = Math.min(currentBackoffMs * 2, maxBackoffMs);
      LOGGER.warn("Rate limited. Increasing backoff to {}ms", currentBackoffMs);
    }

    int getMaxRetries() {
      return maxRetries;
    }

    // For testing
    long getCurrentBackoffMs() {
      return currentBackoffMs;
    }
  }
}
