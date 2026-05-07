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
package org.apache.calcite.adapter.govdata.cyber;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Lightweight base class for cyber schema downloaders.
 *
 * <p>Provides: HTTP fetch with retry, ZIP decompression, and DuckDB-based
 * JSON-to-parquet conversion with StorageProvider upload.
 *
 * <p>Subclasses should override {@link #download()} to implement the fetch →
 * transform → write pipeline for their specific data source.
 */
public abstract class AbstractCyberDownloader {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCyberDownloader.class);

  /** DuckDB JDBC URL for in-memory conversion. */
  private static final String DUCKDB_URL = "jdbc:duckdb:";

  protected final StorageProvider storageProvider;

  /** Absolute path to the table's parquet directory (e.g. {@code /data/cyber_vuln}). */
  protected final String parquetDirectory;

  protected final HttpClient httpClient;

  protected AbstractCyberDownloader(StorageProvider storageProvider, String parquetDirectory) {
    this.storageProvider = storageProvider;
    this.parquetDirectory = parquetDirectory;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
  }

  /**
   * Execute the download: fetch remote data, transform, write parquet.
   */
  public abstract void download() throws IOException, InterruptedException;

  /**
   * Fetches a URL and returns the body as raw bytes.
   *
   * @param url     HTTP URL to fetch
   * @param headers alternating name/value pairs, e.g. "Accept", "application/json"
   */
  protected byte[] fetchBytes(String url, String... headers) throws IOException, InterruptedException {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(120))
        .GET();
    for (int i = 0; i + 1 < headers.length; i += 2) {
      builder.header(headers[i], headers[i + 1]);
    }
    HttpResponse<byte[]> response =
        httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    if (response.statusCode() != 200) {
      throw new IOException("HTTP " + response.statusCode() + " fetching " + url);
    }
    return response.body();
  }

  /**
   * Fetches a URL and returns the UTF-8 body as a string.
   *
   * @param url     HTTP URL to fetch
   * @param headers alternating name/value pairs
   */
  protected String fetchString(String url, String... headers) throws IOException, InterruptedException {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(120))
        .GET();
    for (int i = 0; i + 1 < headers.length; i += 2) {
      builder.header(headers[i], headers[i + 1]);
    }
    HttpResponse<String> response =
        httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (response.statusCode() != 200) {
      throw new IOException("HTTP " + response.statusCode() + " fetching " + url);
    }
    return response.body();
  }

  /**
   * Decompresses a ZIP byte array and returns the content of the first entry whose
   * name ends with {@code entryNameSuffix}.
   */
  protected String extractXmlFromZip(byte[] zipBytes, String entryNameSuffix) throws IOException {
    try (ZipInputStream zis =
        new ZipInputStream(new java.io.ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (!entry.isDirectory() && entry.getName().endsWith(entryNameSuffix)) {
          return new String(readAllBytes(zis), StandardCharsets.UTF_8);
        }
        zis.closeEntry();
      }
    }
    throw new IOException("No ZIP entry matching '*" + entryNameSuffix + "'");
  }

  /**
   * Converts a JSON array string to parquet and writes it via the StorageProvider.
   *
   * <p>Steps: write JSON to a local temp file → DuckDB {@code COPY … TO} a local temp
   * parquet → read bytes → {@link StorageProvider#writeFile}.
   *
   * @param jsonArray       JSON array string produced by a transformer
   * @param tableName       human-readable table name used in log messages
   * @param parquetRelPath  path relative to {@link #parquetDirectory}, e.g.
   *                        {@code "type=cwe_catalog.parquet"}
   */
  protected void writeJsonToParquet(String jsonArray, String tableName, String parquetRelPath)
      throws IOException {
    if (jsonArray == null || jsonArray.trim().equals("[]")) {
      LOGGER.warn("{}: empty JSON array — skipping parquet write", tableName);
      return;
    }

    Path tempJson = Files.createTempFile("cyber_" + tableName + "_", ".json");
    Path tempParquet = Files.createTempFile("cyber_" + tableName + "_", ".parquet");
    try {
      Files.write(tempJson, jsonArray.getBytes(StandardCharsets.UTF_8));

      String jsonPath = tempJson.toAbsolutePath().toString().replace("'", "''");
      String parquetPath = tempParquet.toAbsolutePath().toString().replace("'", "''");
      String sql = String.format(
          "COPY (SELECT * FROM read_json('%s', format='array', auto_detect=true)) "
              + "TO '%s' (FORMAT PARQUET, COMPRESSION 'SNAPPY')",
          jsonPath, parquetPath);

      try (Connection conn = DriverManager.getConnection(DUCKDB_URL)) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("SET memory_limit='512MB'");
          stmt.execute("SET temp_directory='" + System.getProperty("java.io.tmpdir").replace("'", "''") + "'");
          stmt.execute(sql);
        }
      } catch (Exception e) {
        throw new IOException("DuckDB JSON→parquet failed for " + tableName + ": " + e.getMessage(), e);
      }

      byte[] parquetBytes = Files.readAllBytes(tempParquet);
      String fullPath = storageProvider.resolvePath(parquetDirectory, parquetRelPath);
      storageProvider.writeFile(fullPath, parquetBytes);
      LOGGER.info("{}: wrote {} bytes → {}", tableName, parquetBytes.length, fullPath);

    } finally {
      Files.deleteIfExists(tempJson);
      Files.deleteIfExists(tempParquet);
    }
  }

  /**
   * Builds a minimal {@link RequestContext} for a given URL.
   */
  protected RequestContext buildContext(String url) {
    return new RequestContext.Builder().url(url).build();
  }

  /**
   * Builds a {@link RequestContext} carrying HTTP headers (for API key pass-through).
   */
  protected RequestContext buildContext(String url, java.util.Map<String, String> headers) {
    return new RequestContext.Builder().url(url).headers(headers).build();
  }

  /**
   * Builds a {@link RequestContext} carrying both headers and query parameters.
   */
  protected RequestContext buildContext(String url,
      java.util.Map<String, String> headers,
      java.util.Map<String, String> parameters) {
    return new RequestContext.Builder().url(url).headers(headers).parameters(parameters).build();
  }

  private static byte[] readAllBytes(InputStream in) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(65536);
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      bos.write(buf, 0, n);
    }
    return bos.toByteArray();
  }
}
