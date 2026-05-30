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
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Proxy for looking up stock prices in a bulk Stooq zip file before falling back to the HTTP API.
 *
 * <p>The bulk zip contains historical prices for thousands of tickers across 6 US exchanges/categories
 * (nasdaq stocks, nasdaq etfs, nyse stocks, nyse etfs, nysemkt stocks, nysemkt etfs),
 * each as a separate CSV file under data/daily/us/{category}/{ticker}.us.txt.
 *
 * <p>Strategy:
 * <ul>
 *   <li>Lazy download: Downloads from S3 on first use, caches locally</li>
 *   <li>Index scan: Builds a ticker → zip entry path map once</li>
 *   <li>DuckDB zipfs: Queries via DuckDB's zipfs:// extension for performance</li>
 *   <li>Fallback: Returns empty list if ticker not found; caller then tries Stooq API</li>
 * </ul>
 */
public class StooqBulkProxy {
  private static final Logger LOGGER = LoggerFactory.getLogger(StooqBulkProxy.class);

  private final String bulkZipS3Path;
  private final String localCacheDir;
  private final StorageProvider storageProvider;

  private String localZipPath;
  private Map<String, String> tickerIndex;  // ticker (lowercase) → zip entry path

  /**
   * Creates a proxy for the given bulk zip S3 path.
   *
   * @param bulkZipS3Path S3 path to the zip (e.g. s3://bucket/d_us_txt.zip)
   * @param localCacheDir Local directory to cache the downloaded zip
   * @param storageProvider Storage provider for reading from S3
   */
  public StooqBulkProxy(String bulkZipS3Path, String localCacheDir,
                        StorageProvider storageProvider) {
    this.bulkZipS3Path = bulkZipS3Path;
    this.localCacheDir = localCacheDir;
    this.storageProvider = storageProvider;
    this.tickerIndex = null;  // Lazy initialization
  }

  /**
   * Looks up a ticker in the bulk zip and returns all historical price records.
   * Returns empty list if ticker not found.
   *
   * @param ticker Stock ticker symbol (case-insensitive, will be normalized to lowercase)
   * @return List of historical stock price records, or empty list if not found
   */
  public List<StooqDownloader.StockPriceRecord> lookupTicker(String ticker) {
    if (ticker == null || ticker.isEmpty()) {
      return Collections.emptyList();
    }

    try {
      // Ensure zip is downloaded and indexed
      ensureLocalZipAndIndex();

      String tickerKey = ticker.toLowerCase();
      String zipEntryPath = tickerIndex.get(tickerKey);

      if (zipEntryPath == null) {
        LOGGER.debug("Ticker {} not found in bulk zip index", ticker);
        return Collections.emptyList();
      }

      // Query the CSV inside the zip via DuckDB's zipfs extension
      List<StooqDownloader.StockPriceRecord> records = queryZipEntryViaDuckDB(zipEntryPath);
      LOGGER.info("Found ticker {} in bulk zip: {} records", ticker, records.size());
      return records;

    } catch (Exception e) {
      LOGGER.warn("Error looking up ticker {} in bulk zip: {}", ticker, e.getMessage());
      return Collections.emptyList();  // Fallback to HTTP API on any error
    }
  }

  /**
   * Ensures the local zip file exists and the ticker index is built.
   * Downloads from S3 on first call if local cache does not exist.
   */
  private synchronized void ensureLocalZipAndIndex() throws IOException {
    if (tickerIndex != null) {
      return;  // Already initialized
    }

    ensureLocalZip();
    buildTickerIndex();
  }

  /**
   * Ensures the local zip file exists, downloading from S3 if necessary.
   */
  private void ensureLocalZip() throws IOException {
    if (localZipPath != null && Files.exists(Paths.get(localZipPath))) {
      return;  // Already downloaded
    }

    // Create cache directory if it doesn't exist
    Path cachePath = Paths.get(localCacheDir);
    Files.createDirectories(cachePath);

    // Determine local zip filename from S3 path
    String zipFilename = bulkZipS3Path.substring(bulkZipS3Path.lastIndexOf('/') + 1);
    if (zipFilename.isEmpty()) {
      zipFilename = "d_us_txt.zip";
    }
    localZipPath = cachePath.resolve(zipFilename).toString();

    // Check if local copy already exists
    if (Files.exists(Paths.get(localZipPath))) {
      LOGGER.info("Using cached bulk zip at {}", localZipPath);
      return;
    }

    // Download from S3
    LOGGER.info("Downloading bulk stock price zip from S3: {} to {}",
        bulkZipS3Path, localZipPath);
    try (InputStream s3Input = storageProvider.openInputStream(bulkZipS3Path)) {
      Files.copy(s3Input, Paths.get(localZipPath));
      LOGGER.info("Successfully downloaded bulk zip to {}", localZipPath);
    }
  }

  /**
   * Builds an index of all tickers in the zip by scanning the central directory.
   * Maps lowercase ticker → zip entry path.
   */
  private void buildTickerIndex() throws IOException {
    if (localZipPath == null || !Files.exists(Paths.get(localZipPath))) {
      throw new IOException("Local zip file not found: " + localZipPath);
    }

    tickerIndex = new HashMap<>();

    try (ZipFile zipFile = new ZipFile(localZipPath)) {
      for (ZipEntry entry : Collections.list(zipFile.entries())) {
        String path = entry.getName();

        // Match data/daily/us/{category}/{ticker}.us.txt
        // Examples: data/daily/us/nasdaq stocks/aapl.us.txt
        if (!path.startsWith("data/daily/us/") || entry.isDirectory()) {
          continue;
        }

        String filename = path.substring(path.lastIndexOf('/') + 1);
        if (!filename.endsWith(".us.txt")) {
          continue;
        }

        // Extract ticker: "aapl.us.txt" → "aapl"
        String ticker = filename.substring(0, filename.length() - ".us.txt".length());
        String tickerKey = ticker.toLowerCase();

        // Store mapping
        if (!tickerIndex.containsKey(tickerKey)) {
          tickerIndex.put(tickerKey, path);
        } else {
          // Ticker exists in multiple categories (e.g., both nasdaq stocks and nasdaq etfs)
          // Keep the first one found (order: nasdaq etfs, nasdaq stocks, nyse etfs, nyse stocks, nysemkt etfs, nysemkt stocks)
          LOGGER.debug("Ticker {} found in multiple categories, using first: {}",
              tickerKey, tickerIndex.get(tickerKey));
        }
      }
    }

    LOGGER.info("Built ticker index with {} entries from bulk zip", tickerIndex.size());
  }

  /**
   * Queries a CSV file inside the zip via DuckDB's zipfs extension.
   * Converts the bulk CSV format (angle-bracket headers, YYYYMMDD dates) to StockPriceRecord list.
   */
  private List<StooqDownloader.StockPriceRecord> queryZipEntryViaDuckDB(String zipEntryPath) throws IOException {
    if (localZipPath == null) {
      throw new IOException("Local zip path not initialized");
    }

    List<StooqDownloader.StockPriceRecord> records = new ArrayList<>();

    try {
      // Get DuckDB connection with zipfs extension already loaded
      Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);

      // Build the zipfs URL: zipfs:///absolute/path/to/d_us_txt.zip/data/daily/us/...
      String zipfsUrl = String.format("zipfs://%s/%s", localZipPath, zipEntryPath);

      // Query format: read the CSV, parse columns, convert date format from YYYYMMDD to YYYY-MM-DD
      String query = String.format(
          "SELECT "
          + "strftime(strptime(\"<DATE>\"::VARCHAR, '%%Y%%m%%d'), '%%Y-%%m-%%d') AS date, "
          + "TRY_CAST(\"<OPEN>\" AS DOUBLE) AS open, "
          + "TRY_CAST(\"<HIGH>\" AS DOUBLE) AS high, "
          + "TRY_CAST(\"<LOW>\" AS DOUBLE) AS low, "
          + "TRY_CAST(\"<CLOSE>\" AS DOUBLE) AS close, "
          + "TRY_CAST(\"<VOL>\" AS BIGINT) AS volume "
          + "FROM read_csv('%s', header=true, auto_detect=false, "
          + "columns={TICKER: VARCHAR, PER: VARCHAR, DATE: VARCHAR, TIME: VARCHAR, "
          + "OPEN: VARCHAR, HIGH: VARCHAR, LOW: VARCHAR, CLOSE: VARCHAR, "
          + "VOL: VARCHAR, OPENINT: VARCHAR})",
          zipfsUrl);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {

        while (rs.next()) {
          StooqDownloader.StockPriceRecord record = new StooqDownloader.StockPriceRecord();
          record.date = rs.getString("date");
          record.open = rs.getObject("open") != null ? rs.getDouble("open") : null;
          record.high = rs.getObject("high") != null ? rs.getDouble("high") : null;
          record.low = rs.getObject("low") != null ? rs.getDouble("low") : null;
          record.close = rs.getObject("close") != null ? rs.getDouble("close") : null;
          record.adjClose = record.close;  // Bulk data is already split-adjusted
          record.volume = rs.getObject("volume") != null ? rs.getLong("volume") : null;

          records.add(record);
        }
      } finally {
        conn.close();
      }

      return records;

    } catch (Exception e) {
      LOGGER.warn("Error querying bulk zip entry {}: {}", zipEntryPath, e.getMessage());
      throw new IOException("Failed to query bulk zip entry: " + e.getMessage(), e);
    }
  }


}
