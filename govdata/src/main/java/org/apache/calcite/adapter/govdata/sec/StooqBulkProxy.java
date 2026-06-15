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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
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
    // The bulk zip is downloaded, unzipped and queried via DuckDB's zipfs extension, all of
    // which require a real local file. When the configured cache dir is an object-store URI
    // (e.g. s3://govdata-raw-v1/sec/sec/stock_price_seed) map it to a local temp path so the
    // download does not create a literal "s3:/" directory under the working dir.
    this.localCacheDir = org.apache.calcite.adapter.govdata.LocalCacheDirs.toLocal(localCacheDir);
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
    Path localPath = Paths.get(localZipPath);

    // Reuse the local cache only when it matches the current S3 object size; a same-name bulk
    // refresh (new date/size) on object storage must invalidate the stale local copy so the new
    // prices flow through. Size is the reliable signal (MinIO ETags are multipart-mangled).
    if (Files.exists(localPath)) {
      long localSize = Files.size(localPath);
      long remoteSize = -1L;
      try {
        remoteSize = storageProvider.getMetadata(bulkZipS3Path).getSize();
      } catch (IOException e) {
        LOGGER.warn("Could not read S3 metadata for {} to check bulk-zip freshness; "
            + "using cached copy: {}", bulkZipS3Path, e.getMessage());
      }
      if (remoteSize < 0L || remoteSize == localSize) {
        LOGGER.info("Using cached bulk zip at {} ({} bytes)", localZipPath, localSize);
        return;
      }
      LOGGER.info("Bulk zip changed on S3 (local {} bytes != remote {} bytes); re-downloading {}",
          localSize, remoteSize, bulkZipS3Path);
      Files.delete(localPath);
    }

    // Download from S3
    LOGGER.info("Downloading bulk stock price zip from S3: {} to {}",
        bulkZipS3Path, localZipPath);
    try (InputStream s3Input = storageProvider.openInputStream(bulkZipS3Path)) {
      Files.copy(s3Input, localPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
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

  /** The bulk Stooq CSV header (angle-bracket column names), declared for read_csv. */
  private static final String BULK_CSV_COLUMNS =
      "columns={'<TICKER>':'VARCHAR','<PER>':'VARCHAR','<DATE>':'VARCHAR','<TIME>':'VARCHAR',"
      + "'<OPEN>':'VARCHAR','<HIGH>':'VARCHAR','<LOW>':'VARCHAR','<CLOSE>':'VARCHAR',"
      + "'<VOL>':'VARCHAR','<OPENINT>':'VARCHAR'}";

  /**
   * Bulk-ingests EVERY ticker in the zip into year-partitioned stock_prices parquet on object
   * storage. NOT cik-scoped — the whole US daily-prices snapshot is written. A CIK is resolved
   * per ticker from {@code tickerToCik} (empty string when unknown). One DuckDB pass extracts,
   * transforms and writes {@code <stockPricesDir>/year=<y>/...parquet} (ticker stays a column).
   *
   * @return the max trading date present (YYYY-MM-DD), for the caller's top-up gap, or "" if none
   */
  public String ingestAllToStockPrices(String stockPricesDir, Map<String, String> tickerToCik,
      int startYear) throws IOException {
    // Write to a staging sibling of the table location so the file-passthrough materializer can
    // move the parquet into Iceberg without a recursive walk hitting the table's own metadata.
    String stagingDir = stockPricesDir + "__staging";

    // Freshness gate: the bulk zip is the same object every run unless a newer snapshot is staged.
    // When its S3 size is unchanged since the last successful ingest AND the stock_prices table
    // still holds data, skip the expensive DuckDB COPY (the ~23M-row extract+transform) and reuse
    // the recorded max trading date. The data-existence check defeats the dq-rebuild skip-forever
    // trap: a rebuild that clears the table directory forces a full re-ingest even if the marker
    // survives. The marker is a sibling of the table location: "<remoteSize>|<maxDate>".
    String markerPath = stockPricesDir + "__bulk.marker";
    long remoteSize = -1L;
    try {
      remoteSize = storageProvider.getMetadata(bulkZipS3Path).getSize();
    } catch (IOException e) {
      LOGGER.warn("Could not read S3 metadata for {} to gate bulk ingest; proceeding with full "
          + "ingest: {}", bulkZipS3Path, e.getMessage());
    }
    if (remoteSize > 0L) {
      String marker = readMarker(markerPath);
      if (marker != null) {
        int sep = marker.indexOf('|');
        if (sep > 0) {
          long markedSize = -1L;
          try {
            markedSize = Long.parseLong(marker.substring(0, sep).trim());
          } catch (NumberFormatException nfe) {
            markedSize = -1L;  // unreadable marker → fall through to full ingest
          }
          String markedMaxDate = marker.substring(sep + 1);
          if (markedSize == remoteSize && stockPricesHasData(stockPricesDir)) {
            LOGGER.info("Bulk zip unchanged ({} bytes) and stock_prices already materialized — "
                + "skipping bulk ingest; reusing max date {}", remoteSize, markedMaxDate);
            return markedMaxDate;
          }
        }
      }
    }

    ensureLocalZip();

    // zipfs glob is unreliable across DuckDB builds; extract to local disk and read_csv real files.
    Path extractDir = Paths.get(localCacheDir, "extracted");
    deleteDir(extractDir);
    extractZip(extractDir);

    // Write the ticker -> cik map as a CSV for the DuckDB LEFT JOIN.
    Path cikCsv = Paths.get(localCacheDir, "ticker_cik.csv");
    try (BufferedWriter w = Files.newBufferedWriter(cikCsv, StandardCharsets.UTF_8)) {
      w.write("ticker,cik\n");
      for (Map.Entry<String, String> e : tickerToCik.entrySet()) {
        if (e.getKey() != null && e.getValue() != null) {
          w.write(e.getKey().toUpperCase() + "," + e.getValue() + "\n");
        }
      }
    }

    String glob = extractDir.toAbsolutePath() + "/data/daily/us/**/*.txt";
    String srcCsv = "read_csv('" + glob + "', header=true, auto_detect=false, " + BULK_CSV_COLUMNS + ")";
    String maxDate = "";
    try {
      Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("SET preserve_insertion_order=false");
        // cik resolved via LEFT JOIN on the de-suffixed, uppercased ticker; adjusted_close=close
        // (Stooq bulk is already split-adjusted). Partition by year only to keep the file count low.
        String copySql = "COPY ( SELECT "
            + "COALESCE(m.cik, '') AS cik, "
            + "t.\"<TICKER>\" AS ticker, "
            + "strftime(strptime(t.\"<DATE>\", '%Y%m%d'), '%Y-%m-%d') AS date, "
            + "TRY_CAST(t.\"<OPEN>\" AS DOUBLE) AS open, "
            + "TRY_CAST(t.\"<HIGH>\" AS DOUBLE) AS high, "
            + "TRY_CAST(t.\"<LOW>\" AS DOUBLE) AS low, "
            + "TRY_CAST(t.\"<CLOSE>\" AS DOUBLE) AS \"close\", "
            + "TRY_CAST(t.\"<VOL>\" AS BIGINT) AS volume, "
            + "TRY_CAST(t.\"<CLOSE>\" AS DOUBLE) AS adjusted_close, "
            + "CAST(t.\"<DATE>\"[1:4] AS INTEGER) AS year "
            + "FROM " + srcCsv + " t "
            + "LEFT JOIN read_csv('" + cikCsv.toAbsolutePath() + "', header=true, auto_detect=true) m "
            + "  ON upper(replace(t.\"<TICKER>\", '.US', '')) = m.ticker "
            + "WHERE t.\"<DATE>\" >= '" + startYear + "0101' ) "
            + "TO '" + stagingDir + "' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE)";
        LOGGER.info("Bulk-ingesting all tickers from {} into {} (year-partitioned staging)",
            localZipPath, stagingDir);
        long t0 = System.currentTimeMillis();
        stmt.execute(copySql);
        LOGGER.info("Bulk stock-price ingest wrote {} in {}ms", stagingDir,
            System.currentTimeMillis() - t0);

        try (ResultSet rs = stmt.executeQuery(
            "SELECT MAX(strftime(strptime(\"<DATE>\", '%Y%m%d'), '%Y-%m-%d')) FROM " + srcCsv)) {
          if (rs.next() && rs.getString(1) != null) {
            maxDate = rs.getString(1);
          }
        }
      } finally {
        conn.close();
      }
    } catch (Exception e) {
      throw new IOException("Bulk stock-price ingest failed: " + e.getMessage(), e);
    } finally {
      deleteDir(extractDir);
    }
    LOGGER.info("Bulk stock-price ingest complete, max date = {}", maxDate);
    // Record the ingested zip size + max date so an unchanged next run can skip the COPY.
    if (remoteSize > 0L) {
      writeMarker(markerPath, remoteSize + "|" + maxDate);
    }
    return maxDate;
  }

  /** Reads the bulk-ingest marker, or null if absent/unreadable. */
  private String readMarker(String markerPath) {
    try {
      if (!storageProvider.exists(markerPath)) {
        return null;
      }
      try (InputStream in = storageProvider.openInputStream(markerPath)) {
        byte[] bytes = readAllBytes(in);
        return new String(bytes, StandardCharsets.UTF_8).trim();
      }
    } catch (IOException e) {
      LOGGER.warn("Could not read bulk-ingest marker {}: {}", markerPath, e.getMessage());
      return null;
    }
  }

  /** Writes the bulk-ingest marker. */
  private void writeMarker(String markerPath, String value) {
    try {
      storageProvider.writeFile(markerPath, value.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOGGER.warn("Could not write bulk-ingest marker {}: {}", markerPath, e.getMessage());
    }
  }

  /** True when the stock_prices table directory exists and contains at least one parquet file. */
  private boolean stockPricesHasData(String stockPricesDir) {
    try {
      if (!storageProvider.isDirectory(stockPricesDir)) {
        return false;
      }
      for (StorageProvider.FileEntry entry : storageProvider.listFiles(stockPricesDir, true)) {
        if (!entry.isDirectory() && entry.getPath().endsWith(".parquet")) {
          return true;
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Could not list {} to confirm stock_prices data; treating as empty: {}",
          stockPricesDir, e.getMessage());
    }
    return false;
  }

  /** Reads an input stream fully into a byte array (Java 8; no InputStream.readAllBytes). */
  private static byte[] readAllBytes(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  /** Extracts all entries of the local bulk zip under {@code targetDir}. */
  private void extractZip(Path targetDir) throws IOException {
    Files.createDirectories(targetDir);
    try (ZipFile zip = new ZipFile(localZipPath)) {
      for (ZipEntry entry : Collections.list(zip.entries())) {
        if (entry.isDirectory()) {
          continue;
        }
        Path out = targetDir.resolve(entry.getName());
        Files.createDirectories(out.getParent());
        try (InputStream in = zip.getInputStream(entry)) {
          Files.copy(in, out, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  /** Recursively deletes a directory tree (no-op if absent). */
  private void deleteDir(Path dir) {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.delete(p);
        } catch (IOException ignore) {
          // best-effort cleanup
        }
      });
    } catch (IOException e) {
      LOGGER.warn("Failed to clean up {}: {}", dir, e.getMessage());
    }
  }
}
