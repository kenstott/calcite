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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Downloads historical stock price data from Alpha Vantage API and stores it in Parquet format.
 * Implements rate limiting and caching to avoid redundant downloads.
 */
public class AlphaVantageDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlphaVantageDownloader.class);

  private static final String ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query";
  private static final String USER_AGENT = "Apache Calcite SEC Adapter";

  // Rate limiting: Free tier allows 5 API requests per minute and 500 per day
  private static final long RATE_LIMIT_MS = 12500; // 12.5 seconds between requests (to stay under 5/min)
  private static final int MAX_PARALLEL_DOWNLOADS = 1; // Sequential to respect rate limit

  private final String apiKey;
  private final StorageProvider storageProvider;
  private final ExecutorService downloadExecutor = Executors.newFixedThreadPool(MAX_PARALLEL_DOWNLOADS);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private long lastRequestTime = 0;
  private volatile boolean dailyLimitReached = false; // Track if we've hit daily API limit

  // Avro schema for stock price records (same as YahooFinanceDownloader for compatibility)
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

  /**
   * Ticker-CIK pair for downloading stock prices.
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
   * Creates a new AlphaVantageDownloader with the specified API key and storage provider.
   */
  public AlphaVantageDownloader(String apiKey, StorageProvider storageProvider) {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalArgumentException("Alpha Vantage API key is required");
    }
    if (storageProvider == null) {
      throw new IllegalArgumentException("StorageProvider is required");
    }
    this.apiKey = apiKey;
    this.storageProvider = storageProvider;
  }

  /**
   * Downloads stock prices for multiple tickers and CIKs.
   */
  public void downloadStockPrices(String basePath, List<TickerCikPair> tickerCikPairs,
      int startYear, int endYear) {
    LOGGER.info("Starting Alpha Vantage stock price downloads for {} tickers from {} to {}",
        tickerCikPairs.size(), startYear, endYear);

    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (TickerCikPair pair : tickerCikPairs) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          downloadTickerData(basePath, pair, startYear, endYear);
        } catch (Exception e) {
          LOGGER.error("Failed to download data for {}: {}", pair.ticker, e.getMessage());
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
    }

    LOGGER.info("Completed stock price downloads");
  }

  private void downloadTickerData(String basePath, TickerCikPair pair, int startYear, int endYear) {
    LOGGER.debug("downloadTickerData called with basePath: {}", basePath);

    // If we've already hit the daily API limit, skip all downloads for this ticker
    if (dailyLimitReached) {
      LOGGER.warn("Skipping stock price download for {} - daily API limit already reached", pair.ticker);
      return;
    }

    // Get current time in EST (market timezone)
    java.time.ZoneId estZone = java.time.ZoneId.of("America/New_York");
    java.time.ZonedDateTime nowEst = java.time.ZonedDateTime.now(estZone);
    int currentYear = nowEst.getYear();

    // Market close is 4:30 PM EST (16:30)
    java.time.ZonedDateTime todayMarketClose = nowEst.toLocalDate()
        .atTime(16, 30)
        .atZone(estZone);

    for (int year = startYear; year <= endYear; year++) {
      // Build the path using StorageProvider.resolvePath for S3 compatibility
      String stockPricesDir = storageProvider.resolvePath(basePath, "stock_prices");
      LOGGER.debug("stockPricesDir: {}", stockPricesDir);
      String tickerDir = storageProvider.resolvePath(stockPricesDir, String.format("ticker=%s", pair.ticker.toUpperCase()));
      String yearDir = storageProvider.resolvePath(tickerDir, String.format("year=%d", year));
      LOGGER.debug("yearDir: {}", yearDir);

      // StorageProvider automatically creates parent directories when writing files
      String fullPath = storageProvider.resolvePath(yearDir, String.format("%s_prices.parquet", pair.ticker.toLowerCase()));
      LOGGER.debug("parquetFile fullPath: {}", fullPath);

      // Check if data already exists and determine if refresh is needed
      boolean needsDownload = true;
      try {
        if (storageProvider.exists(fullPath)) {
          // For historical years (not current year), use cached data
          if (year < currentYear) {
            LOGGER.info("Using cached stock prices for {} year {} (historical data)", pair.ticker, year);
            needsDownload = false;
          } else {
            // For current year, check if we need to refresh after market close
            StorageProvider.FileMetadata metadata = storageProvider.getMetadata(fullPath);
            long fileModifiedTime = metadata.getLastModified();
            java.time.Instant fileModifiedInstant = java.time.Instant.ofEpochMilli(fileModifiedTime);
            java.time.ZonedDateTime fileModifiedEst = fileModifiedInstant.atZone(estZone);

            // Refresh if: current time is after today's market close AND file was modified before today's market close
            if (nowEst.isAfter(todayMarketClose) && fileModifiedEst.isBefore(todayMarketClose)) {
              LOGGER.info("Stock prices for {} year {} need refresh (modified {}, market closed at {})",
                  pair.ticker, year, fileModifiedEst, todayMarketClose);
              needsDownload = true;
            } else {
              LOGGER.info("Using cached stock prices for {} year {} (current year, up to date)", pair.ticker, year);
              needsDownload = false;
            }
          }
        }
      } catch (IOException e) {
        LOGGER.debug("Error checking if file exists: {}", e.getMessage());
        needsDownload = true;
      }

      if (!needsDownload) {
        continue;
      }

      try {
        // Enforce rate limit
        synchronized (this) {
          long timeSinceLastRequest = System.currentTimeMillis() - lastRequestTime;
          if (timeSinceLastRequest < RATE_LIMIT_MS) {
            Thread.sleep(RATE_LIMIT_MS - timeSinceLastRequest);
          }
          lastRequestTime = System.currentTimeMillis();
        }

        // Fetch data from Alpha Vantage
        List<StockPriceRecord> prices = fetchAlphaVantageData(pair.ticker, year);

        if (prices.isEmpty()) {
          LOGGER.warn("No price data available for {} in year {}", pair.ticker, year);
          continue;
        }

        // Write to Parquet using StorageProvider
        writeToParquet(fullPath, prices, pair.cik);

        LOGGER.info("Downloaded {} price records for {} year {}",
            prices.size(), pair.ticker, year);

      } catch (Exception e) {
        String errorMsg = e.getMessage();
        // Check if this is a daily rate limit error
        if (errorMsg != null && (errorMsg.contains("daily rate limit") || errorMsg.contains("25 requests per day"))) {
          LOGGER.warn("Daily API rate limit reached for Alpha Vantage. Stopping all remaining stock price downloads.");
          LOGGER.warn("Error message: {}", errorMsg);
          dailyLimitReached = true;
          return; // Exit immediately, don't try remaining years for this ticker or other tickers
        }
        LOGGER.warn("Failed to download {} for year {}: {}",
            pair.ticker, year, errorMsg);
      }
    }
  }

  /**
   * Fetches stock price data from Alpha Vantage API.
   */
  @SuppressWarnings("deprecation")
  private List<StockPriceRecord> fetchAlphaVantageData(String ticker, int year) throws IOException {
    // Use TIME_SERIES_DAILY for free tier access (not ADJUSTED which is premium)
    String urlString =
        String.format("%s?function=TIME_SERIES_DAILY&symbol=%s&apikey=%s&outputsize=full&datatype=json", ALPHA_VANTAGE_BASE_URL, ticker.toUpperCase(), apiKey);

    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      throw new IOException("HTTP error code: " + responseCode);
    }

    // Parse JSON response
    JsonNode root;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      root = objectMapper.readTree(reader);
    }

    // Check for API errors
    if (root.has("Error Message")) {
      throw new IOException("API error: " + root.get("Error Message").asText());
    }
    if (root.has("Note")) {
      // Rate limit message
      throw new IOException("API rate limit: " + root.get("Note").asText());
    }

    // Extract time series data
    JsonNode timeSeries = root.get("Time Series (Daily)");
    if (timeSeries == null) {
      // Check for error message
      JsonNode errorMsg = root.get("Error Message");
      if (errorMsg != null) {
        throw new IOException("API Error: " + errorMsg.asText());
      }
      JsonNode infoMsg = root.get("Information");
      if (infoMsg != null) {
        throw new IOException("API Info: " + infoMsg.asText());
      }
      JsonNode noteMsg = root.get("Note");
      if (noteMsg != null) {
        throw new IOException("API Note (likely rate limit): " + noteMsg.asText());
      }
      // Log the response to understand what we're getting
      LOGGER.warn("Unexpected API response format. Keys in response: {}", root.fieldNames());
      throw new IOException("No time series data in response");
    }

    List<StockPriceRecord> prices = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> iter = timeSeries.fields();

    while (iter.hasNext()) {
      Map.Entry<String, JsonNode> entry = iter.next();
      String dateStr = entry.getKey();

      // Filter by year
      if (!dateStr.startsWith(String.valueOf(year))) {
        continue;
      }

      JsonNode dayData = entry.getValue();
      StockPriceRecord record = new StockPriceRecord();
      record.date = dateStr;
      record.open = parseDouble(dayData.get("1. open"));
      record.high = parseDouble(dayData.get("2. high"));
      record.low = parseDouble(dayData.get("3. low"));
      record.close = parseDouble(dayData.get("4. close"));
      // Free tier doesn't provide adjusted close, use regular close
      record.adjClose = record.close;
      record.volume = parseLong(dayData.get("5. volume"));

      prices.add(record);
    }

    return prices;
  }

  private Double parseDouble(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asDouble();
  }

  private Long parseLong(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asLong();
  }

  /**
   * Writes stock price data to a Parquet file using StorageProvider.
   */
  private void writeToParquet(String path, List<StockPriceRecord> prices, String cik)
      throws IOException {
    List<GenericRecord> records = new ArrayList<>();

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
   * Internal class for stock price records.
   */
  private static class StockPriceRecord {
    String date;
    Double open;
    Double high;
    Double low;
    Double close;
    Double adjClose;
    Long volume;
  }
}
