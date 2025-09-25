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
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.model.JsonTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
public class EconSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconSchemaFactory.class);

  private Map<String, Map<String, Object>> tableConstraints;

/**
   * Build the operand configuration for ECON schema without creating the FileSchema.
   * This method is called by GovDataSchemaFactory to collect table definitions.
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand, StorageProvider storageProvider) {
    LOGGER.info("Building ECON schema operand configuration");

    // Read environment variables at runtime (not static initialization)
    String govdataCacheDir = getGovDataCacheDir();
    String govdataParquetDir = getGovDataParquetDir();

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
    mutableOperand.put("partitionedTables", loadTableDefinitions());

    // Pass through executionEngine if specified (critical for DuckDB vs PARQUET)
    if (operand.containsKey("executionEngine")) {
      mutableOperand.put("executionEngine", operand.get("executionEngine"));
    }
    Map<String, Map<String, Object>> econConstraints = loadTableConstraints();
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

  @Override
  public String getSchemaResourceName() {
    return "/econ-schema.json";
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
          String cacheYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          blsDownloader.convertToParquet(new File(cacheYearPath), employmentParquetPath);

          // Convert inflation metrics
          String inflationParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/inflation_metrics.parquet");
          blsDownloader.convertToParquet(new File(cacheYearPath), inflationParquetPath);

          // Convert wage growth
          String wageParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/wage_growth.parquet");
          blsDownloader.convertToParquet(new File(cacheYearPath), wageParquetPath);

          // Convert regional employment
          String regionalParquetPath = storageProvider.resolvePath(parquetDir, "type=regional/year=" + year + "/regional_employment.parquet");
          blsDownloader.convertToParquet(new File(cacheYearPath), regionalParquetPath);
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
          String cacheFredYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          fredDownloader.convertToParquet(new File(cacheFredYearPath), fredParquetPath);
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
          String cacheTimeseriesYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=timeseries/year=" + year);
          treasuryDownloader.convertToParquet(new File(cacheTimeseriesYearPath), yieldsParquetPath);

          // Convert federal debt data to parquet
          String debtParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/year=" + year + "/federal_debt.parquet");
          treasuryDownloader.convertFederalDebtToParquet(new File(cacheTimeseriesYearPath), debtParquetPath);
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

        // Convert to parquet files for each year using StorageProvider
        for (int year = startYear; year <= endYear; year++) {
          String cacheIndicatorsYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);

          // Convert GDP components using StorageProvider
          String gdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_components.parquet");
          beaDownloader.convertToParquet(new File(cacheIndicatorsYearPath), gdpParquetPath);

          // Create gdp_statistics table (GDP growth rates from BEA) using StorageProvider
          String gdpStatsParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_statistics.parquet");
          beaDownloader.convertGdpStatisticsToParquet(new File(cacheIndicatorsYearPath), gdpStatsParquetPath);

          // Convert regional income using StorageProvider
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/regional_income.parquet");
          beaDownloader.convertRegionalIncomeToParquet(new File(cacheIndicatorsYearPath), regionalIncomeParquetPath);

          // Convert State GDP data using StorageProvider
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/state_gdp.parquet");
          beaDownloader.convertStateGdpToParquet(new File(cacheIndicatorsYearPath), stateGdpParquetPath);

          // Convert BEA trade statistics, ITA data, and industry GDP data using StorageProvider
          String tradeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/trade_statistics.parquet");
          beaDownloader.convertTradeStatisticsToParquet(new File(cacheIndicatorsYearPath), tradeParquetPath);

          String itaParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/ita_data.parquet");
          beaDownloader.convertItaDataToParquet(new File(cacheIndicatorsYearPath), itaParquetPath);

          String industryGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/industry_gdp.parquet");
          beaDownloader.convertIndustryGdpToParquet(new File(cacheIndicatorsYearPath), industryGdpParquetPath);
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
          String cacheWorldBankYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          worldBankDownloader.convertToParquet(new File(cacheWorldBankYearPath), worldParquetPath);
        }

        LOGGER.info("World Bank data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading World Bank data", e);
      }
    }

    LOGGER.info("ECON data download completed");
  }



  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    // EconSchemaFactory doesn't use tableDefinitions
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}
