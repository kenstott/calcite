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
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Custom DataProvider for Census Bureau Gazetteer data.
 *
 * <p>Downloads Gazetteer ZIP files, extracts TSV content, and maps columns
 * to the expected schema.
 *
 * <p>Gazetteer files contain authoritative names, codes, area measurements,
 * and representative coordinates for geographic entities.
 */
public class GazetteerDataProvider implements DataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GazetteerDataProvider.class);
  private static final String GAZETTEER_BASE_URL =
      "https://www2.census.gov/geo/docs/maps-data/data/gazetteer";

  @Override
  public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config, Map<String, String> variables)
      throws IOException {

    String tableName = config.getName();
    String year = variables.get("year");

    LOGGER.info("GazetteerDataProvider: Fetching {} for year={}", tableName, year);

    String url = buildDownloadUrl(tableName, year);
    if (url == null) {
      LOGGER.warn("Could not build download URL for table {} year={}", tableName, year);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    Path tempDir = null;
    try {
      tempDir = Files.createTempDirectory("gazetteer_" + tableName + "_");
      File zipFile = new File(tempDir.toFile(), "download.zip");

      LOGGER.info("Downloading Gazetteer data from: {}", url);
      downloadFile(url, zipFile);

      // Extract .txt file from ZIP
      File extractedFile = extractTxtFromZip(zipFile, tempDir.toFile());
      if (extractedFile == null) {
        LOGGER.error("No .txt file found in ZIP for table {}", tableName);
        return new ArrayList<Map<String, Object>>().iterator();
      }

      // Parse TSV and map columns
      List<Map<String, Object>> result = parseTsvFile(extractedFile, tableName, year);

      LOGGER.info("Parsed {} records from Gazetteer for table {}",
          result.size(), tableName);

      // Cleanup temp directory
      final Path tempDirToClean = tempDir;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          Files.walk(tempDirToClean)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException e) {
          LOGGER.debug("Failed to cleanup temp directory: {}", e.getMessage());
        }
      }));

      return result.iterator();

    } catch (IOException e) {
      if (tempDir != null) {
        try {
          Files.walk(tempDir)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException cleanupError) {
          LOGGER.debug("Failed to cleanup temp directory: {}", cleanupError.getMessage());
        }
      }
      throw e;
    }
  }

  private String buildDownloadUrl(String tableName, String year) {
    switch (tableName) {
    case "gazetteer_counties":
      return String.format("%s/%s_Gazetteer/%s_Gaz_counties_national.zip",
          GAZETTEER_BASE_URL, year, year);
    case "gazetteer_places":
      return String.format("%s/%s_Gazetteer/%s_Gaz_place_national.zip",
          GAZETTEER_BASE_URL, year, year);
    case "gazetteer_zctas":
      return String.format("%s/%s_Gazetteer/%s_Gaz_zcta_national.zip",
          GAZETTEER_BASE_URL, year, year);
    default:
      return null;
    }
  }

  private void downloadFile(String urlString, File outputFile) throws IOException {
    URI uri = URI.create(urlString);
    URL url = uri.toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData-Adapter/1.0");
    conn.setRequestProperty("Accept", "application/zip, application/octet-stream, */*");

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + responseCode + " for URL: " + urlString);
    }

    try (BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
         FileOutputStream out = new FileOutputStream(outputFile)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
      }
    }
  }

  private File extractTxtFromZip(File zipFile, File destDir) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile.toPath()))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().endsWith(".txt")) {
          File destFile = new File(destDir, entry.getName());
          destFile.getParentFile().mkdirs();
          try (FileOutputStream fos = new FileOutputStream(destFile)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = zis.read(buffer)) > 0) {
              fos.write(buffer, 0, len);
            }
          }
          return destFile;
        }
      }
    }
    return null;
  }

  private List<Map<String, Object>> parseTsvFile(File tsvFile, String tableName, String year)
      throws IOException {
    List<Map<String, Object>> result = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(Files.newInputStream(tsvFile.toPath()), StandardCharsets.UTF_8))) {

      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }

      // Parse tab-delimited header
      String[] headers = headerLine.split("\t");
      for (int i = 0; i < headers.length; i++) {
        headers[i] = headers[i].trim();
      }

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }

        String[] values = line.split("\t", -1);
        Map<String, Object> record = transformRecord(headers, values, tableName, year);
        if (record != null) {
          result.add(record);
        }
      }
    }

    return result;
  }

  private Map<String, Object> transformRecord(String[] headers, String[] values,
      String tableName, String year) {
    Map<String, Object> record = new HashMap<>();

    // Add partition columns
    record.put("type", "gazetteer");
    record.put("year", Integer.parseInt(year));

    switch (tableName) {
    case "gazetteer_counties":
      String countyGeoid = getStringValue(headers, values, "GEOID");
      record.put("county_fips", countyGeoid);
      if (countyGeoid != null && countyGeoid.length() >= 2) {
        record.put("state_fips", countyGeoid.substring(0, 2));
      }
      record.put("county_name", getStringValue(headers, values, "NAME"));
      record.put("land_area_sqmi", getDoubleValue(headers, values, "ALAND_SQMI", "ALAND"));
      record.put("water_area_sqmi", getDoubleValue(headers, values, "AWATER_SQMI", "AWATER"));
      record.put("latitude", getDoubleValue(headers, values, "INTPTLAT"));
      record.put("longitude", getDoubleValue(headers, values, "INTPTLONG"));
      break;

    case "gazetteer_places":
      String placeGeoid = getStringValue(headers, values, "GEOID");
      record.put("place_fips", placeGeoid);
      if (placeGeoid != null && placeGeoid.length() >= 2) {
        record.put("state_fips", placeGeoid.substring(0, 2));
      }
      record.put("place_name", getStringValue(headers, values, "NAME"));
      record.put("place_type", getStringValue(headers, values, "LSAD", "CLASSFP"));
      record.put("land_area_sqmi", getDoubleValue(headers, values, "ALAND_SQMI", "ALAND"));
      record.put("water_area_sqmi", getDoubleValue(headers, values, "AWATER_SQMI", "AWATER"));
      record.put("latitude", getDoubleValue(headers, values, "INTPTLAT"));
      record.put("longitude", getDoubleValue(headers, values, "INTPTLONG"));
      Integer placePop = getIntValue(headers, values, "POP", "POPULATION", "POP10");
      if (placePop != null) {
        record.put("population", placePop);
      }
      Integer placeHu = getIntValue(headers, values, "HU", "HOUSING", "HU10");
      if (placeHu != null) {
        record.put("housing_units", placeHu);
      }
      break;

    case "gazetteer_zctas":
      record.put("zcta", getStringValue(headers, values, "GEOID", "ZCTA5"));
      record.put("land_area_sqmi", getDoubleValue(headers, values, "ALAND_SQMI", "ALAND"));
      record.put("water_area_sqmi", getDoubleValue(headers, values, "AWATER_SQMI", "AWATER"));
      record.put("latitude", getDoubleValue(headers, values, "INTPTLAT"));
      record.put("longitude", getDoubleValue(headers, values, "INTPTLONG"));
      Integer zctaPop = getIntValue(headers, values, "POP", "POPULATION", "POP10");
      if (zctaPop != null) {
        record.put("population", zctaPop);
      }
      Integer zctaHu = getIntValue(headers, values, "HU", "HOUSING", "HU10");
      if (zctaHu != null) {
        record.put("housing_units", zctaHu);
      }
      break;

    default:
      // Generic - copy all columns with lowercase names
      for (int i = 0; i < headers.length && i < values.length; i++) {
        String value = values[i].trim();
        if (!value.isEmpty()) {
          record.put(headers[i].toLowerCase(), value);
        }
      }
    }

    return record;
  }

  private int findColumnIndex(String[] headers, String... names) {
    for (String name : names) {
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase(name)) {
          return i;
        }
      }
    }
    return -1;
  }

  private String getStringValue(String[] headers, String[] values, String... columnNames) {
    int idx = findColumnIndex(headers, columnNames);
    if (idx >= 0 && idx < values.length) {
      String value = values[idx].trim();
      return value.isEmpty() ? null : value;
    }
    return null;
  }

  private Double getDoubleValue(String[] headers, String[] values, String... columnNames) {
    String strValue = getStringValue(headers, values, columnNames);
    if (strValue != null) {
      try {
        double value = Double.parseDouble(strValue);
        // If column is ALAND/AWATER (square meters), convert to square miles
        int idx = findColumnIndex(headers, columnNames);
        if (idx >= 0 && (headers[idx].equals("ALAND") || headers[idx].equals("AWATER"))) {
          return value / 2589988.11; // square meters to square miles
        }
        return value;
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Integer getIntValue(String[] headers, String[] values, String... columnNames) {
    String strValue = getStringValue(headers, values, columnNames);
    if (strValue != null) {
      try {
        return Integer.parseInt(strValue.replace(",", ""));
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
