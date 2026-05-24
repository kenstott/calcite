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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 *);
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

  /** Default batch timeout: 1 hour. Configurable via STOCK_PRICE_BATCH_TIMEOUT_MINUTES env var. */
  private static final long DEFAULT_BATCH_TIMEOUT_MINUTES = 60;

  // Avro schema for stock price records - matches sec-schema.yaml stock_prices table
  private static final String STOCK_PRICE_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"StockPrice\","
      + "\"fields\": ["
      + "{\"name\": \"cik\", \"type\": \"string\"},"
      + "{\"name\": \"ticker\", \"type\": \"string\"},"
      + "{\"name\": \"date\", \"type\": \"string\"},"
      + "{\"name\": \"year\", \"type\": \"int\"},"
      + "{\"name\": \"open\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"high\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"low\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"close\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"volume\", \"type\": [\"null\", \"long\"], \"default\": null},"
      + "{\"name\": \"adjusted_close\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"split_coefficient\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"dividend\", \"type\": [\"null\", \"double\"], \"default\": null}"
      + "]"
      + "}";

  private final Schema priceSchema = new Schema.Parser().parse(STOCK_PRICE_SCHEMA);
  private final StorageProvider storageProvider;
  private final String username;  // Optional, from STOOQ_USERNAME
  private final String password;  // Optional, from STOOQ_PASSWORD
  private final ExecutorService downloadExecutor;
  private final RateLimiter rateLimiter;
  private final long batchTimeoutMs;
  private final List<String> failedTickers = new ArrayList<String>();
  private SecCacheManifest cacheManifest;  // Optional, for tracking unavailable tickers

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
        getEnvInt("STOCK_PRICE_MAX_RETRIES", RateLimiter.DEFAULT_MAX_RETRIES),
        getEnvLong("STOCK_PRICE_BATCH_TIMEOUT_MINUTES", DEFAULT_BATCH_TIMEOUT_MINUTES));
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
   * @param batchTimeoutMinutes Maximum minutes for the entire batch before aborting
   */
  public StooqDownloader(StorageProvider storageProvider, String username, String password,
                         long baseRateLimitMs, long maxBackoffMs, int maxRetries,
                         long batchTimeoutMinutes) {
    if (storageProvider == null) {
      throw new IllegalArgumentException("StorageProvider is required");
    }
    this.storageProvider = storageProvider;
    this.username = username;
    this.password = password;
    this.downloadExecutor = Executors.newFixedThreadPool(MAX_PARALLEL_DOWNLOADS);
    this.rateLimiter = new RateLimiter(baseRateLimitMs, maxBackoffMs, maxRetries);
    this.batchTimeoutMs = TimeUnit.MINUTES.toMillis(batchTimeoutMinutes);
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
                                  List<TickerCikPair> tickerCikPairs,
                                  int startYear, int endYear) {
    LOGGER.info("Starting Stooq stock price downloads for {} tickers from {} to {} "
            + "(batch timeout: {} minutes)",
        tickerCikPairs.size(), startYear, endYear,
        TimeUnit.MILLISECONDS.toMinutes(batchTimeoutMs));

    failedTickers.clear();
    final long batchDeadline = System.currentTimeMillis() + batchTimeoutMs;
    List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
    int skippedCount = 0;

    for (final TickerCikPair pair : tickerCikPairs) {
      if (cacheManifest != null && cacheManifest.isTickerProcessed(pair.ticker)) {
        LOGGER.debug("Skipping ticker {} - already processed in current round", pair.ticker);
        skippedCount++;
        continue;
      }
      CompletableFuture<Void> future = CompletableFuture.runAsync(new Runnable() {
        @Override public void run() {
          if (System.currentTimeMillis() >= batchDeadline) {
            LOGGER.warn("Batch timeout reached, skipping ticker {}", pair.ticker);
            synchronized (failedTickers) {
              failedTickers.add(pair.ticker);
            }
            return;
          }
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

    // If every ticker was already processed, the round is complete — reset for the next cycle
    if (cacheManifest != null && skippedCount == tickerCikPairs.size()) {
      LOGGER.info("All {} tickers already processed — full round complete, resetting checkpoint",
          tickerCikPairs.size());
      cacheManifest.clearTickerProcessedFlags();
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

    boolean timedOut = System.currentTimeMillis() >= batchDeadline;
    if (timedOut) {
      LOGGER.warn("Stooq batch timed out after {} minutes. "
              + "Completed tickers: {}, failed/skipped: {}. "
              + "Configure STOCK_PRICE_BATCH_TIMEOUT_MINUTES to adjust.",
          TimeUnit.MILLISECONDS.toMinutes(batchTimeoutMs),
          tickerCikPairs.size() - failedTickers.size(), failedTickers.size());
    }

    if (!failedTickers.isEmpty()) {
      LOGGER.warn("Failed to download {} tickers: {}", failedTickers.size(), failedTickers);
    }

    LOGGER.info("Completed stock price downloads");
  }

  /**
   * Downloads data for a single ticker using a single HTTP request for the full date range.
   * Records are grouped by year and only written to parquet for years that need updating.
   */
  private void downloadTickerData(String basePath, TickerCikPair pair,
                                  int startYear, int endYear) {
    LOGGER.debug("downloadTickerData called for ticker {} with basePath: {}", pair.ticker, basePath);

    // Check if this ticker is marked as unavailable (doesn't exist on Stooq)
    if (cacheManifest != null && cacheManifest.isTickerUnavailable(pair.ticker)) {
      LOGGER.debug("Skipping ticker {} - marked as unavailable in cache", pair.ticker);
      return;
    }

    // Get current time in EST (market timezone)
    ZoneId estZone = ZoneId.of("America/New_York");
    ZonedDateTime nowEst = ZonedDateTime.now(estZone);
    int currentYear = nowEst.getYear();

    // Market close is 4:30 PM EST (16:30)
    ZonedDateTime todayMarketClose = nowEst.toLocalDate()
        .atTime(16, 30)
        .atZone(estZone);

    // Determine which years need downloading and their output paths
    Map<Integer, String> yearsNeedingDownload = new HashMap<Integer, String>();
    String stockPricesDir = storageProvider.resolvePath(basePath, "stock_prices");

    for (int year = startYear; year <= endYear; year++) {
      String yearDir = storageProvider.resolvePath(stockPricesDir, String.format("year=%d", year));
      String fullPath = storageProvider.resolvePath(yearDir,
          String.format("%s_%s_prices.parquet", pair.cik, pair.ticker.toLowerCase()));

      boolean needsDownload;
      try {
        needsDownload = needsDownload(fullPath, year, currentYear, nowEst, todayMarketClose);
      } catch (IOException e) {
        LOGGER.debug("Error checking if file exists: {}", e.getMessage());
        needsDownload = true;
      }

      if (needsDownload) {
        yearsNeedingDownload.put(year, fullPath);
      }
    }

    // If all years are cached, skip the HTTP request entirely
    if (yearsNeedingDownload.isEmpty()) {
      LOGGER.debug("All years cached for ticker {}, skipping HTTP request", pair.ticker);
      return;
    }

    try {
      // Fetch ALL data from Stooq in one request for the full date range
      List<StockPriceRecord> allPrices = fetchWithRateLimiting(pair.ticker, startYear, endYear);

      if (allPrices.isEmpty()) {
        LOGGER.warn("No data returned for {} - ticker may be delisted or invalid", pair.ticker);
        return;
      }

      // Group records by year
      Map<Integer, List<StockPriceRecord>> pricesByYear =
          new HashMap<Integer, List<StockPriceRecord>>();
      for (StockPriceRecord record : allPrices) {
        int year = Integer.parseInt(record.date.substring(0, 4));
        List<StockPriceRecord> yearRecords = pricesByYear.get(year);
        if (yearRecords == null) {
          yearRecords = new ArrayList<StockPriceRecord>();
          pricesByYear.put(year, yearRecords);
        }
        yearRecords.add(record);
      }

      // Write parquet files only for years that need updating
      for (Map.Entry<Integer, String> entry : yearsNeedingDownload.entrySet()) {
        int year = entry.getKey();
        String fullPath = entry.getValue();
        List<StockPriceRecord> yearPrices = pricesByYear.get(year);

        if (yearPrices != null && !yearPrices.isEmpty()) {
          writeToParquet(fullPath, yearPrices, pair.cik, pair.ticker, year);
          LOGGER.info("Downloaded {} price records for {} year {}",
              yearPrices.size(), pair.ticker, year);
        }
      }

      if (cacheManifest != null) {
        cacheManifest.markTickerProcessed(pair.ticker);
      }

    } catch (IOException e) {
      LOGGER.warn("Failed to download {} prices: {}", pair.ticker, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Download interrupted for " + pair.ticker, e);
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
   *
   * @param ticker Stock ticker symbol
   * @param startYear Start year for date range
   * @param endYear End year for date range
   * @return List of all stock price records across the date range
   */
  List<StockPriceRecord> fetchWithRateLimiting(String ticker, int startYear, int endYear)
      throws IOException, InterruptedException {

    int attempts = 0;
    IOException lastException = null;
    int emptyResponseCount = 0;  // Track consecutive empty/no-data responses

    while (attempts < rateLimiter.getMaxRetries()) {
      attempts++;

      // Proactive: wait for rate limit window
      rateLimiter.waitForRateLimit();

      HttpURLConnection conn = null;
      try {
        String url = buildStooqUrl(ticker, startYear, endYear);
        conn = createConnection(url);
        int responseCode = conn.getResponseCode();

        // Server error - not recoverable
        if (responseCode >= 500) {
          LOGGER.error("Server error {} for ticker {}", responseCode, ticker);
          throw new IOException("Server error: " + responseCode);
        }

        // Client error (except 429) - not recoverable
        if (responseCode >= 400 && responseCode != 429) {
          LOGGER.error("Client error {} for ticker {}", responseCode, ticker);
          throw new IOException("Client error: " + responseCode);
        }

        // Explicit 429 Too Many Requests - rate limited
        if (responseCode == 429) {
          LOGGER.warn("Rate limited (429) on attempt {}/{} for ticker {}",
              attempts, rateLimiter.getMaxRetries(), ticker);
          rateLimiter.onRateLimited();
          conn.disconnect();
          continue;
        }

        // Read the response body
        List<StockPriceRecord> records;
        String responseBody = null;
        BufferedReader reader = null;
        try {
          reader =
              new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));

          // Check Content-Length for small responses that need body inspection
          String contentLength = conn.getHeaderField("Content-Length");
          boolean smallResponse = false;
          if (contentLength != null) {
            try {
              long length = Long.parseLong(contentLength);
              smallResponse = length < 50;
            } catch (NumberFormatException e) {
              // Ignore
            }
          }

          if (smallResponse) {
            // Read the full body to distinguish rate limiting from no-data
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
              sb.append(line).append('\n');
            }
            responseBody = sb.toString().trim();

            // Check if this is a daily hits limit (not per-ticker no-data)
            if (responseBody.toLowerCase().contains("limit")
                || responseBody.toLowerCase().contains("exceeded")) {
              LOGGER.warn("Stooq daily limit hit for ticker {} (response: {}), backing off",
                  ticker, responseBody);
              rateLimiter.onRateLimited();
              conn.disconnect();
              continue;
            }

            // Genuine small/empty response — could be no-data or soft rate limit
            emptyResponseCount++;
            if (emptyResponseCount >= 2) {
              LOGGER.info("Ticker {} appears to have no data on Stooq (response: {}, attempt {})",
                  ticker, responseBody, attempts);
              if (cacheManifest != null) {
                cacheManifest.markTickerUnavailable(ticker, "no_data_from_stooq");
              }
              rateLimiter.onSuccess();
              return new ArrayList<StockPriceRecord>();
            }
            // First empty — might be transient, retry
            LOGGER.debug("Small response for {}: '{}', may be rate limited, retrying",
                ticker, responseBody);
            rateLimiter.onRateLimited();
            conn.disconnect();
            continue;
          }

          // Normal-sized response — parse CSV
          records = parseCsvResponse(reader);
        } finally {
          if (reader != null) {
            reader.close();
          }
        }

        // If we got a 200 but no records, the ticker may not exist on Stooq
        if (records.isEmpty()) {
          emptyResponseCount++;
          if (emptyResponseCount >= 2) {
            LOGGER.info("Ticker {} has no price data on Stooq", ticker);
            if (cacheManifest != null) {
              cacheManifest.markTickerUnavailable(ticker, "no_data_from_stooq");
            }
            // Reset backoff - confirmed no-data, not rate limiting
            rateLimiter.onSuccess();
            return records;
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

    // If we exhausted retries with mostly empty responses, mark ticker as unavailable
    if (emptyResponseCount > 0 && cacheManifest != null) {
      cacheManifest.markTickerUnavailable(ticker, "no_data_after_retries");
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
    // Stooq uses underscores instead of dashes in ticker symbols (e.g., BAC_PE not BAC-PE)
    // and requires uppercase ticker symbols with .US suffix
    String stooqTicker = ticker.replace("-", "_").toUpperCase();
    url.append("?s=").append(stooqTicker).append(".US");
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
   * Parses CSV response into StockPriceRecord objects without year filtering.
   * Returns all records from the response.
   *
   * @param reader The BufferedReader for the CSV response
   * @return List of all parsed stock price records
   * @throws IOException If an I/O error occurs
   */
  List<StockPriceRecord> parseCsvResponse(BufferedReader reader) throws IOException {
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

      StockPriceRecord record = new StockPriceRecord();
      record.date = parts[0];  // YYYY-MM-DD format
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
   * Parses CSV response into StockPriceRecord objects, filtered by year.
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
    URL url = java.net.URI.create(urlString).toURL();
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
  @SuppressWarnings("UnusedMethod")
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
  private void writeToParquet(String path, List<StockPriceRecord> prices,
      String cik, String ticker, int year) throws IOException {
    List<GenericRecord> records = new ArrayList<GenericRecord>();

    for (StockPriceRecord price : prices) {
      GenericRecord record = new GenericData.Record(priceSchema);
      record.put("cik", cik);
      record.put("ticker", ticker);
      record.put("date", price.date);
      record.put("year", year);
      record.put("open", price.open);
      record.put("high", price.high);
      record.put("low", price.low);
      record.put("close", price.close);
      record.put("volume", price.volume);
      record.put("adjusted_close", price.adjClose);
      record.put("split_coefficient", null);  // Not available from Stooq
      record.put("dividend", null);  // Not available from Stooq
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
   * Set the cache manifest for tracking unavailable tickers.
   * When set, tickers that return no data from Stooq will be marked as unavailable
   * and skipped on subsequent runs.
   *
   * @param cacheManifest The SEC cache manifest
   */
  public void setCacheManifest(SecCacheManifest cacheManifest) {
    this.cacheManifest = cacheManifest;
  }

  /**
   * Ticker and CIK pair for stock price downloads.
   */
  public static class TickerCikPair {
    public final String ticker;
    public final String cik;

    public TickerCikPair(String ticker, String cik) {
      this.ticker = ticker;
      this.cik = cik;
    }
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
