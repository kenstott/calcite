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
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Schema factory for U.S. economic data sources.
 *
 * <p>Provides access to economic data from:
 * <ul>
 *   <li>Bureau of Labor Statistics (BLS) - Employment, inflation, wages</li>
 *   <li>Federal Reserve (FRED) - Interest rates, GDP, economic indicators</li>
 *   <li>U.S. Treasury - Treasury yields, auction results, debt statistics</li>
 *   <li>Bureau of Economic Analysis (BEA) - GDP components, trade data</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>
 * {
 *   "schemas": [{
 *     "name": "ECON",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.econ.EconSchemaFactory",
 *     "operand": {
 *       "blsApiKey": "${BLS_API_KEY}",
 *       "fredApiKey": "${FRED_API_KEY}",
 *       "updateFrequency": "daily",
 *       "historicalDepth": "10 years",
 *       "enabledSources": ["bls", "fred", "treasury"],
 *       "cacheDirectory": "${ECON_CACHE_DIR:/tmp/econ-cache}"
 *     }
 *   }]
 * }
 * </pre>
 */
public class EconSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconSchemaFactory.class);

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private Map<String, Map<String, Object>> tableConstraints;

  /**
   * Build the operand configuration for ECON schema without creating the FileSchema.
   * This method is called by GovDataSchemaFactory to collect table definitions.
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand, StorageProvider storageProvider) {
    LOGGER.info("Building ECON schema operand configuration");

    // Read environment variables at runtime (not static initialization)
    // Check both actual environment variables and system properties (for tests)
    String govdataCacheDir = System.getenv("GOVDATA_CACHE_DIR");
    if (govdataCacheDir == null) {
      govdataCacheDir = System.getProperty("GOVDATA_CACHE_DIR");
    }
    String govdataParquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (govdataParquetDir == null) {
      govdataParquetDir = System.getProperty("GOVDATA_PARQUET_DIR");
    }

    // Check required environment variables
    if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
    }
    if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
    }

    // ECON data directories
    String econRawDir = govdataCacheDir + "/econ";
    String econParquetDir = govdataParquetDir + "/source=econ";

    // Use unified govdata directory structure (matching GEO pattern)
    LOGGER.info("Using unified govdata directories - cache: {}, parquet: {}",
        econRawDir, econParquetDir);

    // Get year range from unified environment variables
    Integer startYear = getConfiguredStartYear(operand);
    Integer endYear = getConfiguredEndYear(operand);

    LOGGER.info("Economic data configuration:");
    LOGGER.info("  Cache directory: {}", econRawDir);
    LOGGER.info("  Parquet directory: {}", econParquetDir);
    LOGGER.info("  Year range: {} - {}", startYear, endYear);

    // Extract API keys from environment variables or system properties
    String blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) {
      blsApiKey = System.getProperty("BLS_API_KEY");
    }

    String fredApiKey = System.getenv("FRED_API_KEY");
    if (fredApiKey == null) {
      fredApiKey = System.getProperty("FRED_API_KEY");
    }

    String beaApiKey = System.getenv("BEA_API_KEY");
    if (beaApiKey == null) {
      beaApiKey = System.getProperty("BEA_API_KEY");
    }

    // Check for operand overrides (model can override environment)
    if (operand.get("blsApiKey") != null) {
      blsApiKey = (String) operand.get("blsApiKey");
    }
    if (operand.get("fredApiKey") != null) {
      fredApiKey = (String) operand.get("fredApiKey");
    }
    if (operand.get("beaApiKey") != null) {
      beaApiKey = (String) operand.get("beaApiKey");
    }

    // Get enabled sources
    @SuppressWarnings("unchecked")
    List<String> enabledSources = (List<String>) operand.get("enabledSources");
    if (enabledSources == null) {
      enabledSources = java.util.Arrays.asList("bls", "fred", "treasury", "bea", "worldbank");
    }

    LOGGER.info("  Enabled sources: {}", enabledSources);
    LOGGER.info("  BLS API key: {}", blsApiKey != null ? "configured" : "not configured");
    LOGGER.info("  FRED API key: {}", fredApiKey != null ? "configured" : "not configured");
    LOGGER.info("  BEA API key: {}", beaApiKey != null ? "configured" : "not configured");

    // Check auto-download setting (default true like GEO)
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    if (autoDownload == null) {
      autoDownload = true;
    }

    // Create mutable operand for modifications
    Map<String, Object> mutableOperand = new java.util.HashMap<>(operand);

    // Download data if auto-download is enabled
    if (autoDownload) {
      LOGGER.info("Auto-download enabled for ECON data");
      try {
        downloadEconData(mutableOperand, econRawDir, econParquetDir,
            blsApiKey, fredApiKey, beaApiKey, enabledSources, startYear, endYear, storageProvider);
      } catch (Exception e) {
        LOGGER.error("Error downloading ECON data", e);
        // Continue even if download fails - existing data may be available
      }
    }

    // Set the directory for FileSchemaFactory to use
    mutableOperand.put("directory", econParquetDir);
    mutableOperand.put("partitionedTables", loadEconTableDefinitions());
    Map<String, Map<String, Object>> econConstraints = loadEconTableConstraints();
    if (tableConstraints != null) {
      econConstraints.putAll(tableConstraints);
    }

    mutableOperand.put("comment", "U.S. and international economic data including employment statistics, inflation metrics (CPI/PPI), "
        + "Federal Reserve interest rates, GDP components, Treasury yields, federal debt, and global economic indicators. "
        + "Data sources include Bureau of Labor Statistics (BLS), U.S. Treasury, World Bank, "
        + "Federal Reserve Economic Data (FRED), and Bureau of Economic Analysis (BEA). "
        + "Enables macroeconomic analysis, market research, and correlation with financial data.");

    // Add constraint metadata to operand so FileSchemaFactory can pass it to FileSchema
    mutableOperand.put("tableConstraints", econConstraints);

    return mutableOperand;
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    // This method should not be called directly anymore
    // GovDataSchemaFactory should call buildOperand() instead
    throw new UnsupportedOperationException(
        "EconSchemaFactory.create() should not be called directly. " +
        "Use GovDataSchemaFactory to create a unified schema.");
  }

  /**
   * Get configured start year from operand or environment.
   */
  private Integer getConfiguredStartYear(Map<String, Object> operand) {
    Integer year = (Integer) operand.get("startYear");
    if (year != null) return year;

    String envYear = System.getenv("GOVDATA_START_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", envYear);
      }
    }

    // Default to 5 years ago
    return java.time.Year.now().getValue() - 5;
  }

  /**
   * Get configured end year from operand or environment.
   */
  private Integer getConfiguredEndYear(Map<String, Object> operand) {
    Integer year = (Integer) operand.get("endYear");
    if (year != null) return year;

    String envYear = System.getenv("GOVDATA_END_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", envYear);
      }
    }

    // Default to current year
    return java.time.Year.now().getValue();
  }

  /**
   * Download economic data from various sources following GEO pattern.
   */
  private void downloadEconData(Map<String, Object> operand, String cacheDir, String parquetDir,
      String blsApiKey, String fredApiKey, String beaApiKey, List<String> enabledSources,
      int startYear, int endYear, StorageProvider storageProvider) throws IOException {

    LOGGER.info("Downloading economic data to: {}", cacheDir);

    // Create cache and parquet directories
    File cacheDirFile = new File(cacheDir);
    File parquetDirFile = new File(parquetDir);
    if (!cacheDirFile.exists()) {
      cacheDirFile.mkdirs();
    }
    if (!parquetDirFile.exists()) {
      parquetDirFile.mkdirs();
    }

    // Create hive-partitioned subdirectories
    File timeSeriesDir = new File(parquetDir, "type=timeseries");
    File indicatorsDir = new File(parquetDir, "type=indicators");
    File regionalDir = new File(parquetDir, "type=regional");
    timeSeriesDir.mkdirs();
    indicatorsDir.mkdirs();
    regionalDir.mkdirs();


    // Download BLS data if enabled
    if (enabledSources.contains("bls") && !blsApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading BLS data for years {}-{}", startYear, endYear);
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir, storageProvider);

        // Download all BLS data for the year range
        blsDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          // Convert employment statistics
          String employmentParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/employment_statistics.parquet");
          blsDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), employmentParquetPath);

          // Convert inflation metrics
          String inflationParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/inflation_metrics.parquet");
          blsDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), inflationParquetPath);

          // Convert wage growth
          String wageParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/wage_growth.parquet");
          blsDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), wageParquetPath);

          // Convert regional employment
          String regionalParquetPath = storageProvider.resolvePath(parquetDir, "type=regional/year=" + year + "/regional_employment.parquet");
          blsDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), regionalParquetPath);
        }

        LOGGER.info("BLS data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BLS data", e);
      }
    }

    // Download FRED data if enabled
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading FRED data for years {}-{}", startYear, endYear);
        FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, fredApiKey, storageProvider);

        // Download all FRED data for the year range
        fredDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String fredParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/fred_indicators.parquet");
          fredDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), fredParquetPath);
        }

        LOGGER.info("FRED data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading FRED data", e);
      }
    }

    // Download Treasury data if enabled (no API key required)
    if (enabledSources.contains("treasury")) {
      try {
        LOGGER.info("Downloading Treasury data for years {}-{}", startYear, endYear);
        TreasuryDataDownloader treasuryDownloader = new TreasuryDataDownloader(cacheDir, storageProvider);

        // Download all Treasury data for the year range
        treasuryDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String yieldsParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/year=" + year + "/treasury_yields.parquet");
          treasuryDownloader.convertToParquet(new File(cacheDir, "source=econ/type=timeseries/year=" + year), yieldsParquetPath);

          // Convert federal debt data to parquet
          String debtParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/year=" + year + "/federal_debt.parquet");
          treasuryDownloader.convertFederalDebtToParquet(new File(cacheDir, "source=econ/type=timeseries/year=" + year), debtParquetPath);
        }

        LOGGER.info("Treasury data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Treasury data", e);
      }
    }

    // Download BEA data if enabled
    if (enabledSources.contains("bea") && beaApiKey != null && !beaApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading BEA data for years {}-{}", startYear, endYear);
        BeaDataDownloader beaDownloader = new BeaDataDownloader(cacheDir, beaApiKey, storageProvider);

        // Download all BEA data for the year range
        beaDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          // Convert GDP components
          String gdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_components.parquet");
          beaDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), gdpParquetPath);

          // Create gdp_statistics table (GDP growth rates from BEA)
          String gdpStatsParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_statistics.parquet");
          beaDownloader.convertGdpStatisticsToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), gdpStatsParquetPath);

          // Convert regional income - use the specific converter method
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/regional_income.parquet");
          beaDownloader.convertRegionalIncomeToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), regionalIncomeParquetPath);

          // Convert State GDP data
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/state_gdp.parquet");
          beaDownloader.convertStateGdpToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), stateGdpParquetPath);

          // Convert BEA trade statistics, ITA data, and industry GDP data
          String tradeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/trade_statistics.parquet");
          beaDownloader.convertTradeStatisticsToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), tradeParquetPath);

          String itaParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/ita_data.parquet");
          beaDownloader.convertItaDataToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), itaParquetPath);

          String industryGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/industry_gdp.parquet");
          beaDownloader.convertIndustryGdpToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), industryGdpParquetPath);
        }

        LOGGER.info("BEA data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BEA data", e);
      }
    }

    // Download World Bank data if enabled (no API key required)
    if (enabledSources.contains("worldbank")) {
      try {
        LOGGER.info("Downloading World Bank data for years {}-{}", startYear, endYear);
        WorldBankDataDownloader worldBankDownloader = new WorldBankDataDownloader(cacheDir, storageProvider);

        // Download all World Bank data for the year range
        worldBankDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String worldParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/world_indicators.parquet");
          worldBankDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), worldParquetPath);
        }

        LOGGER.info("World Bank data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading World Bank data", e);
      }
    }

    LOGGER.info("ECON data download completed");
  }

  /**
   * Load table definitions from JSON resource file.
   */
  private static List<Map<String, Object>> loadEconTableDefinitions() {
    try (InputStream is = EconSchemaFactory.class.getResourceAsStream("/econ-schema.json")) {
      if (is == null) {
        throw new IllegalStateException("Could not find econ-schema.json resource file");
      }

      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      List<Map<String, Object>> tables = (List<Map<String, Object>>) schema.get("partitionedTables");
      if (tables == null) {
        throw new IllegalStateException("No 'partitionedTables' field found in econ-schema.json");
      }
      LOGGER.info("Loaded {} table definitions from econ-schema.json", tables.size());
      for (Map<String, Object> table : tables) {
        LOGGER.debug("  - Table: {} with pattern: {}", table.get("name"), table.get("pattern"));
      }
      return tables;
    } catch (IOException e) {
      throw new RuntimeException("Error loading econ-schema.json", e);
    }
  }

  /**
   * Load constraint definitions from JSON resource file.
   */
  private static Map<String, Map<String, Object>> loadEconTableConstraints() {
    try (InputStream is = EconSchemaFactory.class.getResourceAsStream("/econ-schema.json")) {
      if (is == null) {
        throw new IllegalStateException("Could not find econ-schema.json resource file");
      }

      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      Map<String, Map<String, Object>> constraints = (Map<String, Map<String, Object>>) schema.get("constraints");
      if (constraints == null) {
        throw new IllegalStateException("No 'constraints' field found in econ-schema.json");
      }
      LOGGER.info("Loaded constraints for {} tables from econ-schema.json", constraints.size());
      return constraints;
    } catch (IOException e) {
      throw new RuntimeException("Error loading econ-schema.json", e);
    }
  }

  @Override public boolean supportsConstraints() {
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    // EconSchemaFactory doesn't use tableDefinitions
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}
