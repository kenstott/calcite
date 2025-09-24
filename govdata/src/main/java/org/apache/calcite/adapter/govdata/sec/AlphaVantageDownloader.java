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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
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
    for (int year = startYear; year <= endYear; year++) {
      // Build the path - ensure it's absolute
      File baseDir = new File(basePath);
      LOGGER.debug("baseDir: {}, absolute: {}", baseDir, baseDir.getAbsolutePath());
      File stockPricesDir = new File(baseDir, "stock_prices");
      LOGGER.debug("stockPricesDir: {}, absolute: {}", stockPricesDir, stockPricesDir.getAbsolutePath());
      File tickerDir = new File(stockPricesDir, String.format("ticker=%s", pair.ticker.toUpperCase()));
      File yearDir = new File(tickerDir, String.format("year=%d", year));
      LOGGER.debug("yearDir: {}, absolute: {}", yearDir, yearDir.getAbsolutePath());
      yearDir.mkdirs();

      File parquetFile = new File(yearDir, String.format("%s_prices.parquet", pair.ticker.toLowerCase()));
      String fullPath = parquetFile.getAbsolutePath();
      LOGGER.debug("parquetFile fullPath: {}", fullPath);

      // Check if data already exists
      try {
        if (storageProvider.exists(fullPath)) {
          LOGGER.debug("Stock prices already exist for {} year {}", pair.ticker, year);
          continue;
        }
      } catch (IOException e) {
        LOGGER.debug("Error checking if file exists: {}", e.getMessage());
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
        LOGGER.warn("Failed to download {} for year {}: {}",
            pair.ticker, year, e.getMessage());
      }
    }
  }

  /**
   * Fetches stock price data from Alpha Vantage API.
   */
  @SuppressWarnings("deprecation")
  private List<StockPriceRecord> fetchAlphaVantageData(String ticker, int year) throws IOException {
    // Use TIME_SERIES_DAILY for free tier access (not ADJUSTED which is premium)
    String urlString = String.format("%s?function=TIME_SERIES_DAILY&symbol=%s&apikey=%s&outputsize=full&datatype=json",
        ALPHA_VANTAGE_BASE_URL, ticker.toUpperCase(), apiKey);

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