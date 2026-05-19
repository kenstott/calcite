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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Data provider for USGS Watershed Boundary Dataset (WBD).
 *
 * <p>Downloads and processes the National WBD geodatabase containing hydrologic
 * unit boundaries at multiple levels (HUC2, HUC4, HUC6, HUC8, HUC10, HUC12).
 *
 * <p>Since full GDB parsing requires GDAL/OGR which may not be available, this
 * provider uses a simplified approach of extracting available metadata and
 * boundary information from the GDB structure.
 *
 * <p>For production use with full geometry support, consider using DuckDB's
 * spatial extension with ST_Read() to parse the GDB files directly.
 */
public class WatershedDataProvider implements DataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(WatershedDataProvider.class);

  private static final String WBD_URL =
      "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.zip";

  // Cache extracted data to avoid re-downloading
  private static volatile Map<String, List<Map<String, Object>>> cachedData;
  private static volatile long cacheTimestamp = 0;
  private static final long CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

  @Override
  public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {

    String tableName = config.getName();
    LOGGER.info("Fetching watershed data for table: {}", tableName);

    // Determine which HUC level to extract
    String hucLevel = getHucLevelFromTableName(tableName);
    if (hucLevel == null) {
      LOGGER.warn("Unknown watershed table: {}", tableName);
      return Collections.emptyIterator();
    }

    // Check cache
    if (cachedData != null && System.currentTimeMillis() - cacheTimestamp < CACHE_TTL_MS) {
      List<Map<String, Object>> data = cachedData.get(hucLevel);
      if (data != null) {
        LOGGER.info("Using cached watershed data for HUC{}: {} records", hucLevel, data.size());
        return data.iterator();
      }
    }

    // Download and extract WBD data — retry up to 3 times on transient failures
    Exception lastError = null;
    for (int attempt = 1; attempt <= 3; attempt++) {
      try {
        Map<String, List<Map<String, Object>>> allData = downloadAndExtractWbd();
        cachedData = allData;
        cacheTimestamp = System.currentTimeMillis();

        List<Map<String, Object>> data = allData.get(hucLevel);
        if (data != null) {
          LOGGER.info("Extracted {} HUC{} watershed records", data.size(), hucLevel);
          return data.iterator();
        }
        break;
      } catch (Exception e) {
        lastError = e;
        LOGGER.warn("WBD download/extract attempt {}/3 failed: {}", attempt, e.getMessage());
      }
    }

    if (lastError != null) {
      LOGGER.error("Failed to download/extract WBD data after 3 attempts: {}",
          lastError.getMessage(), lastError);
      // Populate cache with empty data so subsequent batches skip re-download
      Map<String, List<Map<String, Object>>> empty = new HashMap<>();
      empty.put("2", new ArrayList<Map<String, Object>>());
      empty.put("4", new ArrayList<Map<String, Object>>());
      empty.put("8", new ArrayList<Map<String, Object>>());
      empty.put("12", new ArrayList<Map<String, Object>>());
      cachedData = empty;
      cacheTimestamp = System.currentTimeMillis();
    }

    return Collections.emptyIterator();
  }

  private String getHucLevelFromTableName(String tableName) {
    if (tableName.contains("huc2")) {
      return "2";
    }
    if (tableName.contains("huc4")) {
      return "4";
    }
    if (tableName.contains("huc8")) {
      return "8";
    }
    if (tableName.contains("huc12")) {
      return "12";
    }
    return null;
  }

  private Map<String, List<Map<String, Object>>> downloadAndExtractWbd() throws IOException {
    Map<String, List<Map<String, Object>>> result = new HashMap<>();
    result.put("2", new ArrayList<>());
    result.put("4", new ArrayList<>());
    result.put("8", new ArrayList<>());
    result.put("12", new ArrayList<>());

    // Create temp directory for extraction
    Path tempDir = Files.createTempDirectory("wbd_");
    try {
      // Download ZIP file
      LOGGER.info("Downloading WBD data from USGS...");
      Path zipFile = tempDir.resolve("wbd.zip");
      downloadFile(WBD_URL, zipFile.toFile());

      // Extract and parse
      LOGGER.info("Extracting WBD geodatabase...");
      extractAndParseGdb(zipFile, tempDir, result);

    } finally {
      // Cleanup temp directory
      deleteDirectory(tempDir.toFile());
    }

    return result;
  }

  private void downloadFile(String urlString, File destination) throws IOException {
    URL url = URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData-Adapter");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000); // 5 minutes for large file

    try (InputStream in = new BufferedInputStream(conn.getInputStream());
         FileOutputStream out = new FileOutputStream(destination)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      long totalBytes = 0;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        totalBytes += bytesRead;
        if (totalBytes % (10 * 1024 * 1024) == 0) {
          LOGGER.debug("Downloaded {} MB", totalBytes / (1024 * 1024));
        }
      }
      LOGGER.info("Download complete: {} MB", totalBytes / (1024 * 1024));
    }
  }

  private void extractAndParseGdb(Path zipFile, Path tempDir,
      Map<String, List<Map<String, Object>>> result) throws IOException {

    Path gdbDir = extractGdbFromZip(zipFile, tempDir);
    if (gdbDir == null) {
      LOGGER.error("No .gdb directory found in WBD ZIP — cannot parse watershed data");
      return;
    }
    LOGGER.info("Extracted GDB to: {}", gdbDir);

    // layer name, result key, HUC field name (lowercase in output record)
    String[][] levels = {
        {"WBDHU2",  "2",  "huc2"},
        {"WBDHU4",  "4",  "huc4"},
        {"WBDHU8",  "8",  "huc8"},
        {"WBDHU12", "12", "huc12"}
    };

    for (String[] level : levels) {
      String layer    = level[0];
      String key      = level[1];
      String hucField = level[2];
      List<Map<String, Object>> records = queryHucLevel(gdbDir, layer, hucField);
      result.get(key).addAll(records);
      LOGGER.info("Parsed {} {} records from WBD GDB", records.size(), layer);
    }
  }

  private List<Map<String, Object>> queryHucLevel(Path gdbDir, String layer,
      String hucField) throws IOException {

    String hucFieldUpper = hucField.toUpperCase();
    Path outFile = gdbDir.getParent().resolve("wbd_" + hucField + ".tsv");

    String gdbPath = gdbDir.toAbsolutePath().toString().replace("'", "\\'");
    String outPath = outFile.toAbsolutePath().toString().replace("'", "\\'");

    String sql = String.format(
        "LOAD spatial; "
        + "COPY (SELECT lower(%s) AS huc_code, name, areasqkm "
        + "FROM ST_Read('%s', layer='%s')) "
        + "TO '%s' (FORMAT CSV, DELIMITER '\t', HEADER);",
        hucFieldUpper, gdbPath, layer, outPath);

    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.environment().put("HOME", System.getProperty("user.home"));
    pb.redirectErrorStream(true);
    Process proc = pb.start();

    StringBuilder procOutput = new StringBuilder();
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(proc.getInputStream()))) {
      String line;
      while ((line = br.readLine()) != null) {
        procOutput.append(line).append('\n');
      }
    }

    int exitCode;
    try {
      exitCode = proc.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("DuckDB interrupted while querying " + layer, e);
    }

    if (exitCode != 0) {
      LOGGER.error("DuckDB failed for layer {} (exit {}): {}", layer, exitCode, procOutput);
      return new ArrayList<>();
    }

    List<Map<String, Object>> records = new ArrayList<>();
    if (!Files.exists(outFile)) {
      return records;
    }

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(Files.newInputStream(outFile)))) {
      br.readLine(); // skip TSV header
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split("\t", 3);
        if (parts.length < 3) {
          continue;
        }
        String hucCode = parts[0].trim();
        String name    = parts[1].trim();
        double areaSqKm = 0.0;
        try {
          areaSqKm = Double.parseDouble(parts[2].trim());
        } catch (NumberFormatException e) {
          LOGGER.debug("Invalid areasqkm for {}: {}", hucCode, parts[2]);
        }

        Map<String, Object> record = new HashMap<>();
        record.put(hucField, hucCode);
        if ("huc4".equals(hucField) && hucCode.length() >= 2) {
          record.put("huc2", hucCode.substring(0, 2));
        } else if ("huc8".equals(hucField) && hucCode.length() >= 4) {
          record.put("huc4", hucCode.substring(0, 4));
        } else if ("huc12".equals(hucField) && hucCode.length() >= 8) {
          record.put("huc8", hucCode.substring(0, 8));
        }
        record.put("name", name);
        record.put("area_sq_km", areaSqKm);
        record.put("geometry", null);
        records.add(record);
      }
    }

    Files.deleteIfExists(outFile);
    return records;
  }

  private Path extractGdbFromZip(Path zipFile, Path tempDir) throws IOException {
    Path gdbDir = null;
    String gdbPrefix = null;

    try (ZipInputStream zis = new ZipInputStream(
        new BufferedInputStream(Files.newInputStream(zipFile)))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        int gdbIdx = name.indexOf(".gdb");
        if (gdbIdx < 0) {
          zis.closeEntry();
          continue;
        }

        // Determine the prefix before the .gdb segment on the first GDB entry
        if (gdbPrefix == null) {
          String upToGdb = name.substring(0, gdbIdx + 4);
          int lastSlash  = upToGdb.lastIndexOf('/');
          String gdbSegment  = upToGdb.substring(lastSlash + 1);
          gdbPrefix = upToGdb.substring(0, lastSlash + 1);
          gdbDir = tempDir.resolve(gdbSegment);
          Files.createDirectories(gdbDir);
        }

        // Strip prefix so the path is relative to tempDir
        String relative = name.substring(gdbPrefix.length());
        Path entryPath  = tempDir.resolve(relative);

        if (entry.isDirectory()) {
          Files.createDirectories(entryPath);
        } else {
          Files.createDirectories(entryPath.getParent());
          try (FileOutputStream out = new FileOutputStream(entryPath.toFile())) {
            byte[] buf = new byte[65536];
            int n;
            while ((n = zis.read(buf)) != -1) {
              out.write(buf, 0, n);
            }
          }
        }
        zis.closeEntry();
      }
    }
    return gdbDir;
  }

  private void deleteDirectory(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    dir.delete();
  }
}
