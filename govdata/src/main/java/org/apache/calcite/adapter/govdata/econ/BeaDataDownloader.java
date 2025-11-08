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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Downloads and converts Bureau of Economic Analysis (BEA) data to Parquet format.
 * Provides detailed GDP components, personal income, trade statistics, and regional data.
 *
 * <p>Requires a BEA API key from <a href="https://apps.bea.gov/api/signup/">...</a>
 */
public class BeaDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BeaDataDownloader.class);

  private final String parquetDir;

  public BeaDataDownloader(String cacheDir, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDir, cacheDir, parquetDir, cacheStorageProvider, storageProvider, null);
  }

  public BeaDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest);
    this.parquetDir = parquetDir;
  }

  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BeaDataDownloader(String cacheDir) {
    super(cacheDir,
        org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir),
        org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir));
    this.parquetDir = cacheDir; // For compatibility, use same dir
  }

  @Override protected String getTableName() {
    return "national_accounts";
  }

  // ===== METADATA-DRIVEN DOWNLOAD/CONVERSION METHODS =====
  // These methods use the download configurations from econ-schema.json
  // and the executeDownload() infrastructure from AbstractGovDataDownloader

  /**
   * Downloads national accounts (NIPA) data using metadata-driven pattern.
   *
   * @param startYear        First year to download
   * @param endYear          Last year to download
   * @param nipaTablesList   List of NIPA table IDs to download
   * @param tableFrequencies Map of table names to their available frequencies (A=Annual,
   *                         Q=Quarterly)
   */
  public void downloadNationalAccountsMetadata(int startYear, int endYear,
      List<String> nipaTablesList, Map<String, Set<String>> tableFrequencies) {
    if (nipaTablesList == null || nipaTablesList.isEmpty()) {
      LOGGER.warn("No NIPA tables provided for download");
      return;
    }

    LOGGER.info("Downloading {} NIPA tables for years {}-{}", nipaTablesList.size(), startYear,
        endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "national_accounts";

    for (String nipaTable : nipaTablesList) {
      // Get frequencies for this table (default to Annual if not found)
      Set<String> frequencies = tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));

      for (String frequency : frequencies) {
        for (int year = startYear; year <= endYear; year++) {
          // Build variables map with frequency
          Map<String, String> variables =
              ImmutableMap.of(
                  "year", String.valueOf(year),
                  "TableName", nipaTable,
                  "frequency", frequency);

          if (isCachedOrExists(tableName, year, variables)) {
            skippedCount++;
            continue;
          }

          // Download via metadata-driven executeDownload()
          try {
            String cachedPath =
                cacheStorageProvider.resolvePath(cacheDirectory, executeDownload(tableName, variables));

            // Mark as downloaded in cache manifest
            try {
              long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
              cacheManifest.markCached(tableName, year, variables, cachedPath, fileSize);
            } catch (Exception ex) {
              LOGGER.warn("Failed to mark NIPA data set {} as cached in manifest: {}", cachedPath
                  , ex.getMessage());
            }

            downloadedCount++;

            if (downloadedCount % 10 == 0) {
              LOGGER.info("Downloaded {}/{} NIPA table-frequency-years (skipped {} cached)",
                  downloadedCount,
                  nipaTablesList.size() * frequencies.size() * (endYear - startYear + 1),
                  skippedCount);
            }
          } catch (Exception e) {
            LOGGER.error("Error downloading NIPA table {} frequency {} for year {}: {}",
                nipaTable, frequency, year, e.getMessage());
          }
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("National accounts download complete: downloaded {} table-frequency-years, skipped " +
            "{} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts national accounts (NIPA) data using metadata-driven pattern.
   *
   * @param startYear        First year to convert
   * @param endYear          Last year to convert
   * @param nipaTablesList   List of NIPA table IDs to convert
   * @param tableFrequencies Map of table names to their available frequencies (A=Annual,
   *                         Q=Quarterly)
   */
  public void convertNationalAccountsMetadata(int startYear, int endYear,
      List<String> nipaTablesList, Map<String, Set<String>> tableFrequencies) {
    if (nipaTablesList == null || nipaTablesList.isEmpty()) {
      LOGGER.warn("No NIPA tables provided for conversion");
      return;
    }

    LOGGER.info("Converting {} NIPA tables to Parquet for years {}-{}", nipaTablesList.size(),
        startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "national_accounts";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String nipaTable : nipaTablesList) {
      // Get frequencies for this table (default to Annual if not found)
      Set<String> frequencies = tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));

      for (String frequency : frequencies) {
        for (int year = startYear; year <= endYear; year++) {
          // Build variables map
          Map<String, String> variables = new HashMap<>();
          variables.put("year", String.valueOf(year));
          variables.put("frequency", frequency);
          variables.put("TableName", nipaTable);

          // Resolve paths using pattern
          String jsonPath = resolveJsonPath(pattern, variables);
          String parquetPath = resolveParquetPath(pattern, variables);
          String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
          String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

          // Check if conversion needed
          Map<String, String> params = new HashMap<>();
          params.put("TableName", nipaTable);

          if (isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath)) {
            skippedCount++;
            continue;
          }

          // Convert via metadata-driven approach
          try {
            convertCachedJsonToParquet(tableName, variables);
            cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
            convertedCount++;

            if (convertedCount % 10 == 0) {
              LOGGER.info("Converted {}/{} table-frequency-years (skipped {} up-to-date)",
                  convertedCount,
                  nipaTablesList.size() * frequencies.size() * (endYear - startYear + 1),
                  skippedCount);
            }
          } catch (Exception e) {
            LOGGER.error("Error converting NIPA table {} frequency {} for year {}: {}", nipaTable
                , frequency, year, e.getMessage());
          }
        }
      }
    }

    LOGGER.info("National accounts conversion complete: converted {} table-frequency-years, skipped " +
            "{} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads regional income data using metadata-driven pattern.
   *
   * @param startYear     First year to download
   * @param endYear       Last year to download
   * @param lineCodesList List of line codes to download
   */
  public void downloadRegionalIncomeMetadata(int startYear, int endYear,
      List<String> lineCodesList) {
    if (lineCodesList == null || lineCodesList.isEmpty()) {
      LOGGER.warn("No line codes provided for download");
      return;
    }

    LOGGER.info("Downloading {} line codes for years {}-{}", lineCodesList.size(), startYear,
        endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "regional_income";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String lineCode : lineCodesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        Map<String, String> variables = new HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("LineCode", lineCode);

        // Resolve path using pattern
        String relativePath = resolveJsonPath(pattern, variables);

        // Check if already cached
        Map<String, String> params = new HashMap<>();
        params.put("LineCode", lineCode);

        if (isCachedOrExists(tableName, year, params)) {
          skippedCount++;
          continue;
        }

        // Download via metadata-driven executeDownload()
        try {
          String cachedPath = executeDownload(tableName, variables);

          // Mark as downloaded in cache manifest
          try {
            String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
            long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
            cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
          } catch (Exception ex) {
            LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath,
                ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} line codes (skipped {} cached)", downloadedCount,
                lineCodesList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading line code {} for year {}: {}", lineCode, year,
              e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Regional income download complete: downloaded {} line-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts regional income data using metadata-driven pattern.
   */
  public void convertRegionalIncomeMetadata(int startYear, int endYear,
      List<String> lineCodesList) {
    if (lineCodesList == null || lineCodesList.isEmpty()) {
      LOGGER.warn("No line codes provided for conversion");
      return;
    }

    LOGGER.info("Converting {} line codes to Parquet for years {}-{}", lineCodesList.size(),
        startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "regional_income";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String lineCode : lineCodesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        Map<String, String> variables = new HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("LineCode", lineCode);

        // Resolve paths using pattern
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        Map<String, String> params = new HashMap<>();
        params.put("LineCode", lineCode);

        if (isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
          convertedCount++;
        } catch (Exception e) {
          LOGGER.error("Error converting line code {} for year {}: {}", lineCode, year,
              e.getMessage());
        }
      }
    }

    LOGGER.info("Regional income conversion complete: converted {} line-years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads trade statistics data using metadata-driven pattern.
   * This table has no iteration (single table T40205B).
   */
  public void downloadTradeStatisticsMetadata(int startYear, int endYear) {
    LOGGER.info("Downloading trade statistics for years {}-{}", startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "trade_statistics";

    for (int year = startYear; year <= endYear; year++) {
      // Build variables map
      Map<String, String> variables = ImmutableMap.of("year", String.valueOf(year));

      if (isCachedOrExists(tableName, year, variables)) {
        skippedCount++;
        continue;
      }

      try {
        String cachedPath =
            cacheStorageProvider.resolvePath(cacheDirectory, executeDownload(tableName, variables));

        // Mark as downloaded in cache manifest
        try {
          long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
          cacheManifest.markCached(tableName, year, variables, cachedPath, fileSize);
        } catch (Exception ex) {
          LOGGER.warn("Failed to mark {} as cached in manifest: {}", cachedPath, ex.getMessage());
        }

        downloadedCount++;
      } catch (Exception e) {
        LOGGER.error("Error downloading trade statistics for year {}: {}", year, e.getMessage());
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Trade statistics download complete: downloaded {} years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts trade statistics data using metadata-driven pattern.
   */
  public void convertTradeStatisticsMetadata(int startYear, int endYear) {
    LOGGER.info("Converting trade statistics to Parquet for years {}-{}", startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "trade_statistics";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (int year = startYear; year <= endYear; year++) {
      Map<String, String> variables = ImmutableMap.of("year", String.valueOf(year));

      // Resolve paths using pattern
      String jsonPath =
          cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, variables));
      String parquetPath =
          storageProvider.resolvePath(parquetDir, resolveParquetPath(pattern, variables));

      if (isParquetConvertedOrExists(tableName, year, variables, jsonPath, parquetPath)) {
        skippedCount++;
        continue;
      }

      try {
        convertCachedJsonToParquet(tableName, variables);
        cacheManifest.markParquetConverted(tableName, year, variables, parquetPath);
        convertedCount++;
      } catch (Exception e) {
        LOGGER.error("Error converting trade statistics for year {}: {}", year, e.getMessage());
      }
    }

    LOGGER.info("Trade statistics conversion complete: converted {} years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads ITA data using metadata-driven pattern.
   *
   * @param startYear         First year to download
   * @param endYear           Last year to download
   * @param itaIndicatorsList List of ITA indicator codes to download
   */
  public void downloadItaDataMetadata(int startYear, int endYear, List<String> itaIndicatorsList) {
    if (itaIndicatorsList == null || itaIndicatorsList.isEmpty()) {
      LOGGER.warn("No ITA indicators provided for download");
      return;
    }

    LOGGER.info("Downloading {} ITA indicators for years {}-{}", itaIndicatorsList.size(),
        startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "ita_data";

    for (String indicator : itaIndicatorsList) {
      for (int year = startYear; year <= endYear; year++) {

        Map<String, String> params = Map.of("year", String.valueOf(year), "indicator", indicator);

        if (isCachedOrExists(tableName, year, params)) {
          skippedCount++;
          continue;
        }

        try {
          String cachedPath =
              cacheStorageProvider.resolvePath(cacheDirectory, executeDownload(tableName, params));

          // Mark as downloaded in cache manifest
          try {
            long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
            cacheManifest.markCached(tableName, year, params, cachedPath, fileSize);
          } catch (Exception ex) {
            LOGGER.warn("Failed to mark ITA data set {} as cached in manifest: {}", cachedPath,
                ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} ITA indicators (skipped {} cached)", downloadedCount,
                itaIndicatorsList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading ITA indicator {} for year {}: {}", indicator, year,
              e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save ITA data cache manifest: {}", e.getMessage());
    }

    LOGGER.info("ITA data download complete: downloaded {} indicator-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts ITA data using metadata-driven pattern.
   */
  public void convertItaDataMetadata(int startYear, int endYear, List<String> itaIndicatorsList) {
    if (itaIndicatorsList == null || itaIndicatorsList.isEmpty()) {
      LOGGER.warn("No ITA indicators provided for conversion");
      return;
    }

    LOGGER.info("Converting {} ITA indicators to Parquet for years {}-{}",
        itaIndicatorsList.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "ita_data";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String indicator : itaIndicatorsList) {
      for (int year = startYear; year <= endYear; year++) {
        Map<String, String> variables =
            Map.of("year", String.valueOf(year), "indicator", indicator);

        // Resolve paths using pattern
        String rawPath =
            cacheStorageProvider.resolvePath(
                cacheDirectory, resolveJsonPath(pattern
            , variables));
        String fullParquetPath =
            storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));

        if (isParquetConvertedOrExists(tableName, year, variables, rawPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, variables, fullParquetPath);
          convertedCount++;
        } catch (Exception e) {
          LOGGER.error("Error converting ITA indicator {} for year {}: {}", indicator, year,
              e.getMessage());
        }
      }
    }

    LOGGER.info("ITA data conversion complete: converted {} indicator-years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads industry GDP data using metadata-driven pattern.
   *
   * @param startYear         First year to download
   * @param endYear           Last year to download
   * @param keyIndustriesList List of industry codes to download
   */
  public void downloadIndustryGdpMetadata(int startYear, int endYear,
      List<String> keyIndustriesList) {
    if (keyIndustriesList == null || keyIndustriesList.isEmpty()) {
      LOGGER.warn("No industries provided for download");
      return;
    }

    LOGGER.info("Downloading {} industries for years {}-{}", keyIndustriesList.size(), startYear,
        endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "industry_gdp";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String industry : keyIndustriesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        Map<String, String> variables = new HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Industry", industry);

        // Resolve path using pattern
        String relativePath = resolveJsonPath(pattern, variables);

        Map<String, String> params = new HashMap<>();
        params.put("Industry", industry);

        if (isCachedOrExists(tableName, year, params)) {
          skippedCount++;
          continue;
        }

        try {
          String cachedPath = executeDownload(tableName, variables);

          // Mark as downloaded in cache manifest
          try {
            String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
            long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
            cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
          } catch (Exception ex) {
            LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath,
                ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} industries (skipped {} cached)", downloadedCount,
                keyIndustriesList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading industry {} for year {}: {}", industry, year,
              e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Industry GDP download complete: downloaded {} industry-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts industry GDP data using metadata-driven pattern.
   */
  public void convertIndustryGdpMetadata(int startYear, int endYear,
      List<String> keyIndustriesList) {
    if (keyIndustriesList == null || keyIndustriesList.isEmpty()) {
      LOGGER.warn("No industries provided for conversion");
      return;
    }

    LOGGER.info("Converting {} industries to Parquet for years {}-{}", keyIndustriesList.size(),
        startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "industry_gdp";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String industry : keyIndustriesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        Map<String, String> variables = new HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Industry", industry);

        // Resolve paths using pattern
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        Map<String, String> params = new HashMap<>();
        params.put("Industry", industry);

        if (isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
          convertedCount++;
        } catch (Exception e) {
          LOGGER.error("Error converting industry {} for year {}: {}", industry, year,
              e.getMessage());
        }
      }
    }

    LOGGER.info("Industry GDP conversion complete: converted {} industry-years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Public wrapper to download reference_nipa_tables using metadata-driven pattern.
   *
   * @return Path to downloaded JSON cache file
   * @throws IOException          if download fails
   * @throws InterruptedException if download is interrupted
   */
  public String downloadReferenceNipaTables() throws IOException, InterruptedException {
    LOGGER.info("Downloading reference_nipa_tables catalog");
    return executeDownload("reference_nipa_tables", new HashMap<>());
  }

  /**
   * Loads NIPA tables from reference_nipa_tables catalog and extracts frequency information.
   *
   * <p>Parses the Description field using regex to determine which frequencies (Annual, Quarterly)
   * are available for each table based on (A) and (Q) indicators.</p>
   *
   * @return Map of table name to Set of available frequency codes ("A", "Q")
   * @throws IOException if cache file cannot be read or parsed
   */
  public Map<String, Set<String>> loadNipaTableFrequencies() throws IOException {
    LOGGER.info("Loading NIPA table frequencies from reference_nipa_tables catalog");

    // Resolve path to cached JSON file
    String jsonPath = "type=reference/nipa_tables.json";
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    if (!cacheStorageProvider.exists(fullJsonPath)) {
      LOGGER.warn("reference_nipa_tables.json not found at {}, returning empty map", fullJsonPath);
      return new HashMap<>();
    }

    try {
      // Read JSON file
      com.fasterxml.jackson.databind.JsonNode root;
      try (java.io.InputStream is = cacheStorageProvider.openInputStream(fullJsonPath)) {
        root = MAPPER.readTree(is);
      }

      if (!root.isArray()) {
        LOGGER.warn("reference_nipa_tables.json is not an array, returning empty map");
        return new HashMap<>();
      }

      // Extract table codes and frequencies from ParamValue array
      // BEA API response format: [{Key: "T10101", Desc: "Table 1.1.1... (A) (Q)"}, ...]
      Map<String, Set<String>> tableFrequencies = new HashMap<>();

      // Regex pattern to match frequency indicators: (A) or (Q)
      Pattern frequencyPattern = Pattern.compile("\\((A|Q)\\)");

      for (JsonNode item : root) {
        JsonNode keyNode = item.get("TableName");
        JsonNode descNode = item.get("Description");

        if (keyNode != null && !keyNode.isNull() && descNode != null && !descNode.isNull()) {
          String tableName = keyNode.asText();
          String description = descNode.asText();

          // Extract frequency indicators using regex
          Set<String> frequencies = new HashSet<>();
          Matcher matcher = frequencyPattern.matcher(description);
          while (matcher.find()) {
            String freq = matcher.group(1); // Captures "A" or "Q"
            frequencies.add(freq);
          }

          // If no frequency indicators found, default to Annual
          if (frequencies.isEmpty()) {
            frequencies.add("A");
            LOGGER.debug("No frequency indicators found for table {}, defaulting to Annual",
                tableName);
          }

          tableFrequencies.put(tableName, frequencies);
        }
      }

      LOGGER.info("Loaded {} NIPA tables with frequency information from catalog",
          tableFrequencies.size());

      // Log summary of frequency distribution
      long annualOnly = tableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("A")).count();
      long quarterlyOnly = tableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("Q")).count();
      long both = tableFrequencies.values().stream()
          .filter(f -> f.size() == 2).count();
      LOGGER.info("Frequency distribution: {} annual-only, {} quarterly-only, {} both",
          annualOnly, quarterlyOnly, both);

      return tableFrequencies;
    } catch (Exception e) {
      LOGGER.error("Failed to load NIPA table frequencies from catalog: {}", e.getMessage(), e);
      throw new IOException("Failed to load NIPA table frequencies: " + e.getMessage(), e);
    }
  }

  /**
   * Maps NIPA section number to descriptive section name.
   *
   * <p>NIPA tables are organized into 8 sections based on the first digit after 'T':
   * <ul>
   *   <li>1 = Domestic Product and Income (GDP, national income, value added)</li>
   *   <li>2 = Personal Income and Outlays (wages, consumer spending, disposable income)</li>
   *   <li>3 = Government (current receipts, expenditures, benefits)</li>
   *   <li>4 = Foreign Transactions (exports, imports, international transactions)</li>
   *   <li>5 = Saving and Investment (fixed investment, inventories, capital formation)</li>
   *   <li>6 = Income and Employment by Industry (compensation, profits by sector)</li>
   *   <li>7 = Supplemental Tables (per capita, pension plans, additional details)</li>
   *   <li>8 = Not Seasonally Adjusted (unadjusted versions of other tables)</li>
   * </ul>
   *
   * @param section Section number as string (1-8)
   * @return Descriptive section name (e.g., "domestic_product_income")
   */
  private static String getSectionName(String section) {
    switch (section) {
      case "1":
        return "domestic_product_income";
      case "2":
        return "personal_income_outlays";
      case "3":
        return "government";
      case "4":
        return "foreign_transactions";
      case "5":
        return "saving_investment";
      case "6":
        return "industry";
      case "7":
        return "supplemental";
      case "8":
        return "not_seasonally_adjusted";
      default:
        return "unknown";
    }
  }

  /**
   * Converts reference_nipa_tables JSON to Parquet with dynamic frequency columns and section categorization.
   * Parses Description field to add annual and quarterly boolean flags.
   * Parses TableName to extract section, family, metric, and table_number.
   *
   * @throws IOException if conversion fails
   */
  public void convertReferenceNipaTablesWithFrequencies() throws IOException {
    LOGGER.info("Converting reference_nipa_tables to parquet with frequency columns");

    String jsonPath = "type=reference/nipa_tables.json";
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    // Read JSON
    com.fasterxml.jackson.databind.JsonNode root;
    try (java.io.InputStream is = cacheStorageProvider.openInputStream(fullJsonPath)) {
      root = MAPPER.readTree(is);
    }

    if (!root.isArray()) {
      throw new IOException("reference_nipa_tables.json is not an array");
    }

    // Regex pattern to match frequency indicators
    Pattern frequencyPattern = Pattern.compile("\\((A|Q)\\)");

    List<Map<String, Object>> enrichedRecords = new ArrayList<>();
    for (com.fasterxml.jackson.databind.JsonNode item : root) {
      com.fasterxml.jackson.databind.JsonNode keyNode = item.get("Key");
      com.fasterxml.jackson.databind.JsonNode descNode = item.get("Desc");

      if (keyNode != null && descNode != null) {
        String tableName = keyNode.asText();
        String description = descNode.asText();

        // Parse frequency indicators from description
        boolean hasAnnual = description.contains("(A)");
        boolean hasQuarterly = description.contains("(Q)");

        // Parse TableName structure: TXYYZZ where X=section, YY=family, ZZ=metric
        String section = null;
        String sectionName = null;
        String family = null;
        String metric = null;
        String tableNumber = null;

        if (tableName != null && tableName.startsWith("T") && tableName.length() >= 4) {
          try {
            section = tableName.substring(1, 2);           // Character at position 1
            family = tableName.substring(2, 4);            // Characters at positions 2-3
            metric = tableName.substring(4);               // Remaining characters (may include letters like 'B')
            sectionName = getSectionName(section);
            tableNumber = section + "." + family + "." + metric;  // e.g., "1.01.05" or "4.02.05B"
          } catch (Exception e) {
            LOGGER.warn("Failed to parse TableName '{}': {}", tableName, e.getMessage());
          }
        }

        Map<String, Object> record = new HashMap<>();
        record.put("TableName", tableName);
        record.put("Description", description);
        record.put("section", section);
        record.put("section_name", sectionName);
        record.put("family", family);
        record.put("metric", metric);
        record.put("table_number", tableNumber);
        record.put("annual", hasAnnual);
        record.put("quarterly", hasQuarterly);

        enrichedRecords.add(record);
      }
    }

    LOGGER.info("Enriched {} NIPA tables with frequency and section columns", enrichedRecords.size());

    // Group records by section for partitioned writing
    Map<String, List<Map<String, Object>>> recordsBySection = new HashMap<>();
    for (Map<String, Object> record : enrichedRecords) {
      String section = (String) record.get("section");
      if (section == null) {
        section = "unknown";
      }
      recordsBySection.computeIfAbsent(section, k -> new ArrayList<>()).add(record);
    }

    // Write each section partition separately
    List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumnsFromMetadata("reference_nipa_tables");

    for (Map.Entry<String, List<Map<String, Object>>> entry : recordsBySection.entrySet()) {
      String section = entry.getKey();
      List<Map<String, Object>> sectionRecords = entry.getValue();

      String parquetPath = "type=reference/section=" + section + "/nipa_tables.parquet";
      String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

      storageProvider.writeAvroParquet(fullParquetPath, columns, sectionRecords,
          "reference_nipa_tables", "reference_nipa_tables");

      LOGGER.info("Wrote {} tables for section {} to {}", sectionRecords.size(), section, parquetPath);
    }

    LOGGER.info("Converted reference_nipa_tables to parquet with frequency and section columns across {} sections",
        recordsBySection.size());
  }

}
