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
import org.apache.calcite.adapter.govdata.DuckDbExtensionInstaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>DuckDB read_csv: Extracts the matched entry to a local temp file and reads it directly</li>
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
    // The bulk zip is downloaded, unzipped and read via DuckDB read_csv, all of
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
   * Queries a single CSV entry inside the bulk zip via DuckDB.
   * Converts the bulk CSV format (angle-bracket headers, YYYYMMDD dates) to StockPriceRecord list.
   *
   * <p>The entry is extracted to a real local temp file and read via {@code read_csv}, rather than
   * DuckDB's {@code zipfs} extension. zipfs is a community extension that is not available in the
   * core DuckDB repo we bundle for air-gapped operation, and its glob/read is unreliable across
   * DuckDB builds (see {@link #ingestAllToStockPrices}).
   */
  private List<StooqDownloader.StockPriceRecord> queryZipEntryViaDuckDB(String zipEntryPath) throws IOException {
    if (localZipPath == null) {
      throw new IOException("Local zip path not initialized");
    }

    List<StooqDownloader.StockPriceRecord> records = new ArrayList<>();

    // Extract the single CSV entry to a real local file so DuckDB's read_csv can read it directly.
    Path tempCsv = Files.createTempFile(Paths.get(localCacheDir), "stooq_entry_", ".txt");
    try (ZipFile zip = new ZipFile(localZipPath)) {
      ZipEntry entry = zip.getEntry(zipEntryPath);
      if (entry == null) {
        throw new IOException("Zip entry not found: " + zipEntryPath);
      }
      try (InputStream in = zip.getInputStream(entry)) {
        Files.copy(in, tempCsv, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }

      Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);

      // Read the CSV, parse columns, convert date format from YYYYMMDD to YYYY-MM-DD. The bulk
      // files use angle-bracket header names, declared via the shared BULK_CSV_COLUMNS spec.
      String query = String.format(
          "SELECT "
          + "strftime(strptime(\"<DATE>\"::VARCHAR, '%%Y%%m%%d'), '%%Y-%%m-%%d') AS date, "
          + "TRY_CAST(\"<OPEN>\" AS DOUBLE) AS open, "
          + "TRY_CAST(\"<HIGH>\" AS DOUBLE) AS high, "
          + "TRY_CAST(\"<LOW>\" AS DOUBLE) AS low, "
          + "TRY_CAST(\"<CLOSE>\" AS DOUBLE) AS close, "
          + "TRY_CAST(\"<VOL>\" AS BIGINT) AS volume "
          + "FROM read_csv('%s', header=true, auto_detect=false, " + BULK_CSV_COLUMNS + ")",
          tempCsv.toAbsolutePath());

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
    } finally {
      Files.deleteIfExists(tempCsv);
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
  public String ingestAllToStockPrices(String stockPricesDir, int startYear) throws IOException {
    // Write to a staging sibling of the table location so the file-passthrough materializer can
    // move the parquet into Iceberg without a recursive walk hitting the table's own metadata.
    String stagingDir = stockPricesDir + "__staging";
    String markerPath = stockPricesDir + "__bulk.marker";
    // filing_metadata is the sibling of stock_prices; it is the source of the point-in-time
    // (ticker -> cik) tenure joined into every price row (input 2 alongside the bulk zip).
    String secDir = stockPricesDir.substring(0, stockPricesDir.lastIndexOf('/'));
    // Read via iceberg_scan (atomic snapshot) rather than a raw parquet glob — the pool may compact
    // filing_metadata concurrently, and a glob transiently sees deleted/mid-rewrite files.
    String filingMetadataTable = secDir + "/filing_metadata";

    long remoteSize = -1L;
    try {
      remoteSize = storageProvider.getMetadata(bulkZipS3Path).getSize();
    } catch (IOException e) {
      LOGGER.warn("Could not read S3 metadata for {} to gate bulk ingest; proceeding with full "
          + "ingest: {}", bulkZipS3Path, e.getMessage());
    }

    String maxDate = "";
    long fmCount = 0L;  // filing_metadata row count — the freshness fingerprint of input 2.
    try (Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider)) {
      // getDuckDBConnection's default extension set omits iceberg; load the bundled one so
      // iceberg_scan can read filing_metadata's current snapshot atomically.
      try (Statement extStmt = conn.createStatement()) {
        extStmt.execute("LOAD '" + DuckDbExtensionInstaller.getLocalExtensionPath("iceberg") + "'");
      }
      // Point-in-time (ticker -> cik) tenure from filing_metadata: the [first, last]
      // period_of_report each cik reported under a ticker. A price row is later stamped with the
      // cik whose window contains its trade date (correct-or-null — the LEFT JOIN yields '' for a
      // date no filer covered). This is input 2 of the COPY; its row count is the freshness
      // fingerprint (fmCount). iceberg_scan reads filing_metadata's committed snapshot atomically
      // (immune to concurrent compaction). No fallback: if filing_metadata is absent (sec filings
      // not yet ingested) this throws and the bulk fails loudly — cik enrichment is a hard
      // dependency on filing_metadata, and an all-null fallback would ship wrong data silently.
      // Orchestration must ingest sec filings before stock_prices. acceptance_datetime is
      // unpopulated in filing_metadata, so period_of_report is the tenure date.
      try (Statement tStmt = conn.createStatement()) {
        tStmt.execute(
            "CREATE OR REPLACE TEMP TABLE ticker_cik_tenure AS "
            + "SELECT upper(trim(ticker)) AS ticker, cik, "
            + "  min(TRY_CAST(period_of_report AS DATE)) AS from_date, "
            + "  max(TRY_CAST(period_of_report AS DATE)) AS to_date "
            + "FROM iceberg_scan('" + filingMetadataTable + "', allow_moved_paths = true) "
            + "WHERE ticker IS NOT NULL AND ticker <> '' AND cik IS NOT NULL "
            + "  AND TRY_CAST(period_of_report AS DATE) IS NOT NULL "
            + "GROUP BY upper(trim(ticker)), cik");
      }

      // Tenure fingerprint (own Statement — a closed ResultSet closes its Statement in DuckDB JDBC,
      // so this must not share the Statement used for the COPY below).
      try (Statement cStmt = conn.createStatement();
          ResultSet rs = cStmt.executeQuery("SELECT COUNT(*) FROM ticker_cik_tenure")) {
        if (rs.next()) {
          fmCount = rs.getLong(1);
        }
      }

      // Composite freshness gate. Skip the ~23M-row COPY only when the zip size AND the tenure
      // fingerprint are both unchanged and the table still holds data. The data-existence check
      // defeats the dq-rebuild skip-forever trap. Marker: "<zipSize>|<fmCount>|<maxDate>"; a stale
      // 2-field marker fails the parse and re-ingests.
      if (remoteSize > 0L) {
        String marker = readMarker(markerPath);
        String[] parts = marker != null ? marker.split("\\|", 3) : null;
        if (parts != null && parts.length == 3) {
          try {
            long markedSize = Long.parseLong(parts[0].trim());
            long markedFm = Long.parseLong(parts[1].trim());
            if (markedSize == remoteSize && markedFm == fmCount && stockPricesHasData(stockPricesDir)) {
              LOGGER.info("Bulk zip ({} bytes) and cik tenure ({} rows) both unchanged and "
                  + "stock_prices already materialized — skipping bulk ingest; reusing max date {}",
                  remoteSize, fmCount, parts[2]);
              return parts[2];
            }
          } catch (NumberFormatException nfe) {
            // stale/old-format marker → fall through to a full ingest
          }
        }
      }

      ensureLocalZip();
      // zipfs glob is unreliable across DuckDB builds; extract to local disk and read_csv real files.
      Path extractDir = Paths.get(localCacheDir, "extracted");
      deleteDir(extractDir);
      extractZip(extractDir);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("SET preserve_insertion_order=false");
        String glob = extractDir.toAbsolutePath() + "/data/daily/us/**/*.txt";
        String srcCsv = "read_csv('" + glob + "', header=true, auto_detect=false, " + BULK_CSV_COLUMNS + ")";
        // Range-join every price row to the tenure windows that contain its trade date, then keep
        // exactly one (the latest-starting window — the most-recently-active filer at that date) via
        // QUALIFY. adjusted_close=close (Stooq bulk is already split-adjusted). Partition by year.
        String copySql = "COPY ( SELECT "
            + "COALESCE(tn.cik, '') AS cik, "
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
            + "LEFT JOIN ticker_cik_tenure tn "
            + "  ON upper(replace(t.\"<TICKER>\", '.US', '')) = tn.ticker "
            + "  AND strptime(t.\"<DATE>\", '%Y%m%d') BETWEEN tn.from_date AND tn.to_date "
            + "WHERE t.\"<DATE>\" >= '" + startYear + "0101' "
            + "QUALIFY ROW_NUMBER() OVER ("
            + "  PARTITION BY t.\"<TICKER>\", t.\"<DATE>\" ORDER BY tn.from_date DESC NULLS LAST) = 1 ) "
            + "TO '" + stagingDir + "' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE)";
        LOGGER.info("Bulk-ingesting all tickers from {} into {} (year-partitioned staging; "
            + "point-in-time cik from filing_metadata, {} tenure rows)", localZipPath, stagingDir, fmCount);
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
        deleteDir(extractDir);
      }
    } catch (Exception e) {
      throw new IOException("Bulk stock-price ingest failed: " + e.getMessage(), e);
    }
    LOGGER.info("Bulk stock-price ingest complete, max date = {}", maxDate);
    // Record zip size + filing_metadata fingerprint + max date so an unchanged next run can skip.
    if (remoteSize > 0L) {
      writeMarker(markerPath, remoteSize + "|" + fmCount + "|" + maxDate);
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
