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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Fetches the recent-price top-up from Alpha Vantage (JSON {@code TIME_SERIES_DAILY}) for a small
 * set of SEC filers, covering the gap between the bulk Stooq snapshot's max date and today.
 *
 * <p>The bulk Stooq zip ({@link StooqBulkProxy}) ingests the full US daily-price history for every
 * ticker but is only refreshed periodically, so its max date trails "today". This downloader tops
 * up the last few trading days for the configured filers by calling Alpha Vantage once per ticker
 * (the free tier is capped at 25 requests/day, which is enough for the DQ sample; premium keys lift
 * the cap for production filer sets).
 *
 * <p>Top-up rows are written as Parquet into the SAME {@code stock_prices__staging} directory the
 * bulk pass wrote to, under {@code year=YYYY/}, with the IDENTICAL column projection
 * ({@code cik, ticker, date, open, high, low, close, volume, adjusted_close}) so the file-passthrough
 * Iceberg materializer commits bulk and top-up files under one name-mapping.
 */
public class AlphaVantageDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlphaVantageDownloader.class);

  private static final String ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query";
  private static final String USER_AGENT = "Apache Calcite SEC Adapter";

  private final String apiKey;
  private final StorageProvider storageProvider;
  private final ObjectMapper objectMapper = new ObjectMapper();
  // Delay between requests; modest by default since the gap top-up is a handful of tickers.
  private final long rateLimitMs;
  private long lastRequestTime = 0;

  public AlphaVantageDownloader(String apiKey, StorageProvider storageProvider) {
    this.apiKey = apiKey;
    this.storageProvider = storageProvider;
    long ms = 1500L;
    String env = System.getenv("ALPHA_VANTAGE_RATE_LIMIT_MS");
    if (env != null && !env.isEmpty()) {
      try {
        ms = Long.parseLong(env.trim());
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ALPHA_VANTAGE_RATE_LIMIT_MS={}, using {}", env, ms);
      }
    }
    this.rateLimitMs = ms;
  }

  /**
   * Tops up {@code stagingDir} with rows dated strictly after {@code afterDate} for the given
   * filers, writing year-partitioned Parquet that matches the bulk schema.
   *
   * @param stagingDir object-storage staging directory the bulk pass wrote to (no trailing slash)
   * @param filers     SEC filers to top up (ticker + cik); one Alpha Vantage call each
   * @param afterDate  bulk snapshot's max date (YYYY-MM-DD); only later rows are written
   * @return number of top-up rows written across all tickers
   */
  public int topUpStockPrices(String topupDir, List<StooqDownloader.TickerCikPair> filers,
                              String afterDate) throws IOException {
    if (filers == null || filers.isEmpty() || afterDate == null || afterDate.isEmpty()) {
      return 0;
    }
    Path tmpCsv = Files.createTempFile("av_topup_", ".csv");
    int fetched = 0;
    boolean dailyLimitReached = false;
    try {
      try (BufferedWriter w = Files.newBufferedWriter(tmpCsv, StandardCharsets.UTF_8)) {
        w.write("cik,ticker,date,open,high,low,close,volume\n");
        for (StooqDownloader.TickerCikPair pair : filers) {
          if (dailyLimitReached) {
            break;
          }
          try {
            rateLimit();
            List<String[]> rows = fetchCompact(pair.ticker, afterDate);
            for (String[] r : rows) {
              // r = {date, open, high, low, close, volume}
              w.write(csv(pair.cik) + "," + csv(pair.ticker.toUpperCase()) + "," + csv(r[0]) + ","
                  + csv(r[1]) + "," + csv(r[2]) + "," + csv(r[3]) + "," + csv(r[4]) + "," + csv(r[5])
                  + "\n");
              fetched++;
            }
            LOGGER.debug("Alpha Vantage top-up: {} rows for {} after {}", rows.size(), pair.ticker,
                afterDate);
          } catch (RateLimitException e) {
            LOGGER.warn("Alpha Vantage daily limit reached after {} fetched rows; topping up no "
                + "further (prior top-up retained): {}", fetched, e.getMessage());
            dailyLimitReached = true;
          } catch (IOException e) {
            LOGGER.warn("Alpha Vantage top-up failed for {}: {}", pair.ticker, e.getMessage());
          }
        }
      }

      // Rewrite the persistent __topup dir as (freshly fetched ∪ previously persisted) rows
      // strictly after the bulk max date, deduped by (ticker, date) preferring fresh. The __topup
      // dir is a sibling the bulk COPY never clears, so a rate-limited/empty fetch retains the
      // prior top-up. Filtering to > afterDate keeps it disjoint from the bulk, so the no-dedup
      // file-passthrough materializer never double-counts a date.
      int written = rewriteTopup(tmpCsv, topupDir, afterDate);
      LOGGER.info("Alpha Vantage top-up: {} fetched, {} rows persisted in {} after {}",
          fetched, written, topupDir, afterDate);
      return written;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Alpha Vantage top-up failed: " + e.getMessage(), e);
    } finally {
      Files.deleteIfExists(tmpCsv);
    }
  }

  /**
   * Merges the freshly fetched CSV with any previously persisted {@code __topup} parquet, filters
   * to rows strictly after {@code afterDate}, dedups by (ticker, date) preferring fresh, and
   * rewrites the year-partitioned {@code __topup} dir. Returns the number of rows persisted.
   */
  private int rewriteTopup(Path freshCsv, String topupDir, String afterDate) throws Exception {
    Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);
    try (Statement stmt = conn.createStatement()) {
      // Explicit column types (auto_detect=false) so cik keeps leading zeros and date stays VARCHAR
      // — matching StooqBulkProxy's bulk parquet schema exactly.
      String freshSel = "SELECT 0 AS src, cik, ticker, date, "
          + "CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, "
          + "CAST(low AS DOUBLE) AS low, CAST(close AS DOUBLE) AS \"close\", "
          + "CAST(volume AS BIGINT) AS volume, CAST(close AS DOUBLE) AS adjusted_close "
          + "FROM read_csv('" + freshCsv.toAbsolutePath() + "', header=true, auto_detect=false, "
          + "columns={'cik':'VARCHAR','ticker':'VARCHAR','date':'VARCHAR',"
          + "'open':'DOUBLE','high':'DOUBLE','low':'DOUBLE','close':'DOUBLE','volume':'BIGINT'})";

      boolean hasExisting = false;
      try (ResultSet rs = stmt.executeQuery(
          "SELECT count(*) FROM glob('" + topupDir + "/**/*.parquet')")) {
        hasExisting = rs.next() && rs.getLong(1) > 0;
      }
      String src = freshSel;
      if (hasExisting) {
        src = freshSel + " UNION ALL SELECT 1 AS src, cik, ticker, date, open, high, low, "
            + "\"close\", volume, adjusted_close FROM read_parquet('" + topupDir
            + "/**/*.parquet', union_by_name=true)";
      }

      // Materialize the merged snapshot BEFORE clearing the dir (it reads the existing files).
      stmt.execute("CREATE TEMP TABLE av_merged AS "
          + "SELECT cik, ticker, date, open, high, low, \"close\", volume, adjusted_close, "
          + "CAST(date[1:4] AS INTEGER) AS year FROM ("
          + "  SELECT *, row_number() OVER (PARTITION BY ticker, date ORDER BY src) AS rn FROM ("
          + src + ") u WHERE date > '" + afterDate + "') r WHERE rn = 1");
      long n;
      try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM av_merged")) {
        rs.next();
        n = rs.getLong(1);
      }

      // Clear then rewrite, so a year whose rows all aged past the bulk max date does not linger.
      clearDir(topupDir);
      if (n > 0) {
        // FILENAME_PATTERN 'topup_{uuid}' gives the top-up parquet a distinct name from the bulk's
        // data_0.parquet — both partition year=2026, and the materializer moves staged files into
        // one data/ dir by relative path, so identical names would collide and overwrite.
        stmt.execute("COPY (SELECT cik, ticker, date, open, high, low, \"close\", volume, "
            + "adjusted_close, year FROM av_merged) TO '" + topupDir
            + "' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE, "
            + "FILENAME_PATTERN 'topup_{uuid}')");
      }
      stmt.execute("DROP TABLE IF EXISTS av_merged");
      return (int) n;
    } finally {
      conn.close();
    }
  }

  /** Deletes all files under a (possibly absent) object-storage directory. */
  private void clearDir(String dir) {
    try {
      for (StorageProvider.FileEntry e : storageProvider.listFiles(dir, true)) {
        if (!e.isDirectory()) {
          storageProvider.delete(e.getPath());
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Nothing to clear at {} ({})", dir, e.getMessage());
    }
  }

  /** Marks an Alpha Vantage daily-cap / rate-limit response so the caller stops early. */
  private static class RateLimitException extends IOException {
    RateLimitException(String message) {
      super(message);
    }
  }

  /**
   * Fetches the compact (last ~100 trading days) daily series for one ticker and returns the rows
   * dated strictly after {@code afterDate} as {date, open, high, low, close, volume} string arrays.
   */
  private List<String[]> fetchCompact(String ticker, String afterDate) throws IOException {
    String urlString = String.format(
        "%s?function=TIME_SERIES_DAILY&symbol=%s&apikey=%s&outputsize=compact&datatype=json",
        ALPHA_VANTAGE_BASE_URL, ticker.toUpperCase(), apiKey);

    URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      throw new IOException("HTTP error code: " + responseCode);
    }

    JsonNode root;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      root = objectMapper.readTree(reader);
    }

    if (root.has("Error Message")) {
      throw new IOException("API error: " + root.get("Error Message").asText());
    }
    // "Note" and "Information" both signal throttling / daily-cap on Alpha Vantage.
    if (root.has("Note")) {
      throw new RateLimitException(root.get("Note").asText());
    }
    if (root.has("Information")) {
      throw new RateLimitException(root.get("Information").asText());
    }

    JsonNode timeSeries = root.get("Time Series (Daily)");
    if (timeSeries == null) {
      throw new IOException("No time series data in response");
    }

    List<String[]> rows = new ArrayList<String[]>();
    Iterator<Map.Entry<String, JsonNode>> iter = timeSeries.fields();
    while (iter.hasNext()) {
      Map.Entry<String, JsonNode> entry = iter.next();
      String dateStr = entry.getKey();
      if (dateStr.compareTo(afterDate) <= 0) {
        continue;
      }
      JsonNode d = entry.getValue();
      rows.add(new String[] {
          dateStr,
          asText(d.get("1. open")),
          asText(d.get("2. high")),
          asText(d.get("3. low")),
          asText(d.get("4. close")),
          asText(d.get("5. volume"))
      });
    }
    return rows;
  }

  private void rateLimit() {
    long now = System.currentTimeMillis();
    long wait = rateLimitMs - (now - lastRequestTime);
    if (wait > 0) {
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    lastRequestTime = System.currentTimeMillis();
  }

  private static String asText(JsonNode node) {
    return (node == null || node.isNull()) ? "" : node.asText();
  }

  /** CSV-escapes a value (ticker/cik/numerics here never contain commas, but be safe). */
  private static String csv(String v) {
    if (v == null) {
      return "";
    }
    if (v.indexOf(',') >= 0 || v.indexOf('"') >= 0) {
      return "\"" + v.replace("\"", "\"\"") + "\"";
    }
    return v;
  }
}
