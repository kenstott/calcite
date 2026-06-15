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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Custom DataProvider for Census Bureau Gazetteer data.
 *
 * <p>Downloads Gazetteer ZIP files, extracts TSV content, and maps columns
 * to the expected schema.
 *
 * <p>Gazetteer files contain authoritative names, codes, area measurements,
 * and representative coordinates for geographic entities.
 */
public class GazetteerDataProvider implements StorageAwareDataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GazetteerDataProvider.class);
  private static final String GAZETTEER_BASE_URL =
      "https://www2.census.gov/geo/docs/maps-data/data/gazetteer";

  private StorageProvider storageProvider;
  private String cacheBaseDir;

  @Override public void setStorageProvider(StorageProvider sp, String cacheDir) {
    this.storageProvider = sp;
    this.cacheBaseDir = cacheDir;
  }

  private StorageProvider storageProvider() {
    if (storageProvider == null) {
      storageProvider = StorageProviderFactory.createForGovDataCache();
      cacheBaseDir = StorageProviderFactory.getGovDataCacheDir();
    }
    return storageProvider;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config, Map<String, String> variables)
      throws IOException {

    String tableName = config.getName();
    String year = variables.get("year");

    LOGGER.info("GazetteerDataProvider: Fetching {} for year={}", tableName, year);

    String url = buildDownloadUrl(tableName, year);
    if (url == null) {
      LOGGER.warn("Could not build download URL for table {} year={}", tableName, year);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    String cachePath = storageProvider().resolvePath(cacheBaseDir, "geo/gazetteer/year=" + year + "/" + tableName);
    File tempDir = null;
    try {
      try {
        tempDir = ZipDownloadUtils.downloadZipToTempDirCached(url, null, "gazetteer-" + tableName, cachePath, storageProvider());
      } catch (IOException e) {
        if (e.getMessage() != null && e.getMessage().contains("HTTP 404")) {
          LOGGER.info("Gazetteer data not yet published for year={} table={} — skipping", year, tableName);
          return new ArrayList<Map<String, Object>>().iterator();
        }
        throw e;
      }

      // Find .txt file in extracted temp dir
      File extractedFile = findFileByExtension(tempDir, ".txt");
      if (extractedFile == null) {
        LOGGER.error("No .txt file found in ZIP for table {}", tableName);
        return new ArrayList<Map<String, Object>>().iterator();
      }

      List<Map<String, Object>> result = parseTsvFile(extractedFile, tableName, year);
      LOGGER.info("Parsed {} records from Gazetteer for table {}", result.size(), tableName);
      return result.iterator();

    } finally {
      if (tempDir != null) {
        deleteDirectory(tempDir);
      }
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

  private static File findFileByExtension(File dir, String ext) {
    File[] files = dir.listFiles();
    if (files == null) return null;
    for (File f : files) {
      if (f.isFile() && f.getName().endsWith(ext)) return f;
      if (f.isDirectory()) {
        File found = findFileByExtension(f, ext);
        if (found != null) return found;
      }
    }
    return null;
  }

  private static void deleteDirectory(File dir) {
    ZipDownloadUtils.deleteDirectory(dir);
  }

  private List<Map<String, Object>> parseTsvFile(File tsvFile, String tableName, String year)
      throws IOException {
    List<Map<String, Object>> result = new ArrayList<>();

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(Files.newInputStream(tsvFile.toPath()), StandardCharsets.UTF_8))) {

      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return result;
      }

      // Auto-detect delimiter: Census switched from tab to pipe in 2025
      char delimiter = headerLine.indexOf('|') >= 0 ? '|' : '\t';
      List<String> headerList = CsvRecordReader.splitFields(headerLine, delimiter);
      String[] headers = new String[headerList.size()];
      for (int i = 0; i < headerList.size(); i++) {
        headers[i] = headerList.get(i).trim();
      }

      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }

        List<String> valueList = CsvRecordReader.splitFields(line, delimiter);
        String[] values = valueList.toArray(new String[0]);
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
