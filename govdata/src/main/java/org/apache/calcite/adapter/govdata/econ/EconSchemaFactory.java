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
import org.apache.calcite.adapter.govdata.BulkDownloadConfig;
import org.apache.calcite.adapter.govdata.GovDataSchemaFactory;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.model.JsonTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;

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
  private Set<String> enabledBlsTables;

/**
   * Build the operand configuration for ECON schema without creating the FileSchema.
   * This method is called by GovDataSchemaFactory to collect table definitions.
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand, GovDataSchemaFactory parent) {
    LOGGER.debug("Building ECON schema operand configuration");

    // Access shared services from parent
    StorageProvider storageProvider = parent.getStorageProvider();
    StorageProvider cacheStorageProvider = parent.getCacheStorageProvider();

    // Use directory from operand if provided (contains full S3 URI or local path)
    // Otherwise fall back to environment variables
    String baseDirectory = (String) operand.get("directory");
    String cacheDirectory = (String) operand.get("cacheDirectory");

    if (baseDirectory == null) {
      // Fallback to environment variables
      String govdataParquetDir = getGovDataParquetDir(operand);
      if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
        throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
      }
      baseDirectory = govdataParquetDir;
    }

    if (cacheDirectory == null) {
      String govdataCacheDir = getGovDataCacheDir(operand);
      if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
        throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
      }
      cacheDirectory = govdataCacheDir;
    }

    // ECON data directories
    // Add source=econ prefix ONCE here when reading from operand
    // All downstream code uses these variables with relative paths (e.g., "type=indicators/year=2020")
    // Final paths: baseDirectory/source=econ + type=indicators/year=2020 = baseDirectory/source=econ/type=indicators/year=2020
    String econRawDir = cacheStorageProvider.resolvePath(cacheDirectory, "source=econ");
    String econParquetDir = storageProvider.resolvePath(baseDirectory, "source=econ");

    // Use unified govdata directory structure (matching GEO pattern)
    LOGGER.debug("Using unified govdata directories - cache: {}, parquet: {}",
        econRawDir, econParquetDir);

    // Get year range from unified environment variables
    Integer startYear = getConfiguredStartYear(operand);
    Integer endYear = getConfiguredEndYear(operand);

    LOGGER.debug("Economic data configuration:");
    LOGGER.debug("  Cache directory: {}", econRawDir);
    LOGGER.debug("  Parquet directory: {}", econParquetDir);
    LOGGER.debug("  Year range: {} - {}", startYear, endYear);

    // Get API keys: try operand first (allows model.json to specify key directly),
    // then fall back to environment variable (supports ${ENV_VAR} syntax in model.json)
    // If key comes from operand, set it as system property so metadata-driven downloaders can access it
    String blsApiKey = (String) operand.get("blsApiKey");
    if (blsApiKey == null) {
      blsApiKey = System.getenv("BLS_API_KEY");
    } else {
      // Set as system property for metadata-driven downloaders to access
      System.setProperty("BLS_API_KEY", blsApiKey);
    }

    String fredApiKey = (String) operand.get("fredApiKey");
    if (fredApiKey == null) {
      fredApiKey = System.getenv("FRED_API_KEY");
    } else {
      // Set as system property for metadata-driven downloaders to access
      System.setProperty("FRED_API_KEY", fredApiKey);
    }

    String beaApiKey = (String) operand.get("beaApiKey");
    if (beaApiKey == null) {
      beaApiKey = System.getenv("BEA_API_KEY");
    } else {
      // Set as system property for metadata-driven downloaders to access
      System.setProperty("BEA_API_KEY", beaApiKey);
    }

    // Get enabled sources
    @SuppressWarnings("unchecked")
    List<String> enabledSources = (List<String>) operand.get("enabledSources");
    if (enabledSources == null) {
      enabledSources = Arrays.asList("bls", "fred", "treasury", "bea", "worldbank");
    }

    LOGGER.debug("  Enabled sources: {}", enabledSources);
    LOGGER.debug("  BLS API key: {}", blsApiKey != null ? "configured" : "not configured");
    LOGGER.debug("  FRED API key: {}", fredApiKey != null ? "configured" : "not configured");
    LOGGER.debug("  BEA API key: {}", beaApiKey != null ? "configured" : "not configured");

    // Parse custom FRED series configuration
    @SuppressWarnings("unchecked")
    List<String> customFredSeries = (List<String>) operand.get("customFredSeries");

    // Parse BLS table filtering configuration (needed for both download and schema filtering)
    this.enabledBlsTables = parseBlsTableFilter(operand);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("  Custom FRED series: {}", customFredSeries != null ? customFredSeries.size() + " series configured" : "none");
    }

    // Parse FRED minimum popularity threshold for catalog-based series filtering
    Integer fredMinPopularity = (Integer) operand.get("fredMinPopularity");
    if (fredMinPopularity == null) {
      fredMinPopularity = 50;  // Default: only moderately popular series
    }
    LOGGER.debug("  FRED minimum popularity threshold: {}", fredMinPopularity);

    // Parse FRED catalog cache force refresh flag (default false)
    Boolean fredCatalogForceRefresh = (Boolean) operand.get("fredCatalogForceRefresh");
    if (fredCatalogForceRefresh == null) {
      fredCatalogForceRefresh = false;
    }

    // Check auto-download setting (default true like GEO)
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    if (autoDownload == null) {
      autoDownload = true;
    }

    // Create mutable operand for modifications
    Map<String, Object> mutableOperand = new HashMap<>(operand);

    // Download data if auto-download is enabled
    if (autoDownload) {
      LOGGER.debug("Auto-download enabled for ECON data");
      try {
        downloadEconData(mutableOperand, econRawDir, econParquetDir,
            blsApiKey, fredApiKey, beaApiKey, enabledSources, startYear, endYear, storageProvider,
            customFredSeries, cacheStorageProvider, fredMinPopularity, fredCatalogForceRefresh);
      } catch (Exception e) {
        LOGGER.error("Error downloading ECON data", e);
        // Continue even if download fails - existing data may be available
      }
    }

    // Set the directory for FileSchemaFactory to use
    mutableOperand.put("directory", econParquetDir);

    // Separate table definitions into partitioned tables and views
    List<Map<String, Object>> allTables = loadTableDefinitions();

    // Rewrite FK schema names if actual schema name is different from declared name
    String actualSchemaName = (String) operand.get("actualSchemaName");
    if (actualSchemaName != null) {
      rewriteForeignKeySchemaNames(allTables, actualSchemaName);
    }

    // Add declared schema name to operand for view SQL rewriting in DuckDB
    // This allows DuckDB to rewrite view definitions from "econ.table" to "ECON.table"
    String declaredSchemaName = loadDeclaredSchemaName();
    mutableOperand.put("declaredSchemaName", declaredSchemaName);
    LOGGER.debug("Added declaredSchemaName='{}' to operand for view rewriting", declaredSchemaName);

    List<Map<String, Object>> partitionedTables = new ArrayList<>();
    List<Map<String, Object>> regularTables = new ArrayList<>();

    for (Map<String, Object> table : allTables) {
      String tableType = (String) table.get("type");
      if ("view".equals(tableType)) {
        // This is a view definition - add to regular tables
        regularTables.add(table);
        LOGGER.debug("Adding view to tables array: {}", table.get("name"));
      } else {
        // This is a partitioned table - add to partitioned tables
        partitionedTables.add(table);
      }
    }

    mutableOperand.put("partitionedTables", partitionedTables);
    if (!regularTables.isEmpty()) {
      mutableOperand.put("tables", regularTables);
      LOGGER.info("Added {} views to 'tables' operand for FileSchema", regularTables.size());
    }

    // Generate materializations from trend tables for automatic optimizer substitution
    List<Map<String, Object>> materializations = generateMaterializations(allTables);
    if (!materializations.isEmpty()) {
      mutableOperand.put("materializations", materializations);
      LOGGER.info("Generated {} materializations from trend patterns for automatic substitution",
          materializations.size());
    }

    // Pass through executionEngine if specified (critical for DuckDB vs PARQUET)
    if (operand.containsKey("executionEngine")) {
      mutableOperand.put("executionEngine", operand.get("executionEngine"));
    }
    Map<String, Map<String, Object>> econConstraints = loadTableConstraints();

    // Rewrite FK schema names in constraints if actual schema name is different from declared name
    if (actualSchemaName != null) {
      rewriteConstraintForeignKeySchemaNames(econConstraints, actualSchemaName);
    }

    if (tableConstraints != null) {
      econConstraints.putAll(tableConstraints);
    }

    // Add constraint metadata to operand so FileSchemaFactory can pass it to FileSchema
    mutableOperand.put("tableConstraints", econConstraints);

    // NOTE: We cannot create and register the RawToParquetConverter here because:
    // 1. EconSchemaFactory only builds operand configuration, doesn't create FileSchema
    // 2. The downloaders are created inside downloadEconData() which already finished
    // 3. GovDataSchemaFactory creates the FileSchema after this method returns
    //
    // SOLUTION: GovDataSchemaFactory will need to create the converter and downloaders
    // after schema creation, then cast FileSchema and call registerRawToParquetConverter()
    // This requires downloaders to be recreated in GovDataSchemaFactory.buildEconOperand()

    // Add schema-level comment from JSON metadata
    String schemaComment = loadSchemaComment();
    if (schemaComment != null) {
      mutableOperand.put("comment", schemaComment);
    }

    return mutableOperand;
  }

  @Override public String getSchemaResourceName() {
    return "/econ/econ-schema.json";
  }

  /**
   * Loads bulk download configurations from the econ-schema.json resource file.
   * Bulk downloads are large source files that feed multiple tables (e.g., QCEW ZIP file).
   *
   * @return Map of bulk download name to BulkDownloadConfig
   */
  protected Map<String, BulkDownloadConfig> loadBulkDownloads() {
    try (InputStream is = getClass().getResourceAsStream(getSchemaResourceName())) {
      if (is == null) {
        throw new IllegalStateException(
            "Could not find " + getSchemaResourceName() + " resource file");
      }

      com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      Map<String, Object> schema = mapper.readValue(is, Map.class);

      Map<String, Map<String, Object>> bulkDownloadsJson =
          (Map<String, Map<String, Object>>) schema.get("bulkDownloads");
      if (bulkDownloadsJson == null) {
        LOGGER.debug("No 'bulkDownloads' section found in schema, returning empty map");
        return new HashMap<>();
      }

      Map<String, BulkDownloadConfig> bulkDownloads = new HashMap<>();
      for (Map.Entry<String, Map<String, Object>> entry : bulkDownloadsJson.entrySet()) {
        String name = entry.getKey();
        Map<String, Object> config = entry.getValue();

        String cachePattern = (String) config.get("cachePattern");
        String url = (String) config.get("url");
        List<String> variables = (List<String>) config.get("variables");
        String comment = (String) config.get("comment");

        BulkDownloadConfig bulkDownload =
            new BulkDownloadConfig(name, cachePattern, url, variables, comment);
        bulkDownloads.put(name, bulkDownload);

        LOGGER.debug("Loaded bulk download config: {}", bulkDownload);
      }

      LOGGER.info("Loaded {} bulk download configurations from {}", bulkDownloads.size(), getSchemaResourceName());
      return bulkDownloads;

    } catch (Exception e) {
      throw new RuntimeException("Error loading bulk downloads from " + getSchemaResourceName(), e);
    }
  }

  /**
   * Download economic data from various sources using unified downloader pattern.
   * This method creates all downloaders and orchestrates the standard 3-phase download pattern:
   * Phase 0: Download reference data
   * Phase 1: Download all time-series data
   * Phase 2: Convert all data to Parquet
   */
  private void downloadEconData(Map<String, Object> operand, String cacheDir, String parquetDir,
      String blsApiKey, String fredApiKey, String beaApiKey, List<String> enabledSources,
      int startYear, int endYear, StorageProvider storageProvider,
      List<String> customFredSeries, StorageProvider cacheStorageProvider, int fredMinPopularity,
      boolean fredCatalogForceRefresh) {

    // Operating directory for metadata (.aperio/econ/)
    // This is passed from GovDataSchemaFactory which establishes it centrally
    // The .aperio directory is ALWAYS on local filesystem (working directory), even if parquet data is on S3
    String econOperatingDirectory = (String) operand.get("operatingDirectory");
    if (econOperatingDirectory == null) {
      throw new IllegalStateException("Operating directory must be established by GovDataSchemaFactory");
    }
    LOGGER.debug("Received operating directory from parent: {}", econOperatingDirectory);

    // Directory creation handled automatically by StorageProvider when writing files
    // No need to create cache or parquet directories explicitly

    // Load or create cache manifest for tracking parquet conversions from operating directory
    CacheManifest cacheManifest = CacheManifest.load(econOperatingDirectory);
    LOGGER.debug("Loaded ECON cache manifest from {}", econOperatingDirectory);

    // cacheStorageProvider is passed as parameter (created by GovDataSchemaFactory)
    LOGGER.debug("Using shared cache storage provider for cache directory: {}", cacheDir);

    // Create list of all downloaders (only for enabled sources)
    List<AbstractEconDataDownloader> downloaders = new ArrayList<>();

    // Create BLS downloader if enabled
    if (enabledSources.contains("bls") && blsApiKey != null && !blsApiKey.isEmpty()) {
      BlsDataDownloader blsDownloader =
          new BlsDataDownloader(blsApiKey, cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
          cacheManifest, enabledBlsTables);
      downloaders.add(blsDownloader);
      LOGGER.debug("Added BLS downloader to orchestration list");
    }

    // Create FRED downloader if enabled
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
        // Get catalog cache TTL from operand (default 365 days)
        Integer catalogCacheTtlDays = (Integer) operand.get("fredCatalogCacheTtlDays");

        // FredDataDownloader handles all FRED-specific initialization internally
        FredDataDownloader fredDownloader =
            new FredDataDownloader(fredApiKey, cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
            cacheManifest, customFredSeries, fredMinPopularity, fredCatalogForceRefresh,
            catalogCacheTtlDays);
        downloaders.add(fredDownloader);
        LOGGER.debug("Added FRED downloader to orchestration list");
      } catch (Exception e) {
        LOGGER.error("Error creating FRED downloader", e);
      }
    }

    // Create Treasury downloader if enabled (no API key required)
    if (enabledSources.contains("treasury")) {
      TreasuryDataDownloader treasuryDownloader =
          new TreasuryDataDownloader(cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);
      downloaders.add(treasuryDownloader);
      LOGGER.debug("Added Treasury downloader to orchestration list");
    }

    // Create BEA downloader if enabled
    if (enabledSources.contains("bea") && beaApiKey != null && !beaApiKey.isEmpty()) {
      try {
        // BeaDataDownloader handles all BEA-specific initialization internally
        BeaDataDownloader beaDownloader =
            new BeaDataDownloader(cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);
        downloaders.add(beaDownloader);
        LOGGER.debug("Added BEA downloader to orchestration list");
      } catch (Exception e) {
        LOGGER.error("Error creating BEA downloader", e);
      }
    }

    // Create WorldBank downloader if enabled (no API key required)
    if (enabledSources.contains("worldbank")) {
      WorldBankDataDownloader worldBankDownloader =
          new WorldBankDataDownloader(cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);
      downloaders.add(worldBankDownloader);
      LOGGER.debug("Added WorldBank downloader to orchestration list");
    }

    // PHASE 0: Download all reference data
    LOGGER.info("=== PHASE 0: Downloading all reference data ===");
    for (AbstractEconDataDownloader downloader : downloaders) {
      try {
        String downloaderName = downloader.getClass().getSimpleName();
        LOGGER.info("Downloading reference data using {}", downloaderName);
        downloader.downloadReferenceData();
      } catch (Exception e) {
        LOGGER.error("Error during reference data download for {}: {}",
            downloader.getClass().getSimpleName(), e.getMessage(), e);
      }
    }

    // PHASE 1: Download all time-series data
    LOGGER.info("=== PHASE 1: Downloading all time-series data ===");
    for (AbstractEconDataDownloader downloader : downloaders) {
      try {
        String downloaderName = downloader.getClass().getSimpleName();
        LOGGER.info("Downloading data using {}", downloaderName);
        downloader.downloadAll(startYear, endYear);
      } catch (Exception e) {
        LOGGER.error("Error during download phase for {}: {}",
            downloader.getClass().getSimpleName(), e.getMessage(), e);
      }
    }

    // PHASE 2: Convert all data to Parquet
    LOGGER.info("=== PHASE 2: Converting all data to Parquet ===");
    for (AbstractEconDataDownloader downloader : downloaders) {
      try {
        String downloaderName = downloader.getClass().getSimpleName();
        LOGGER.info("Converting data using {}", downloaderName);
        downloader.convertAll(startYear, endYear);
      } catch (Exception e) {
        LOGGER.error("Error during conversion phase for {}: {}",
            downloader.getClass().getSimpleName(), e.getMessage(), e);
      }
    }

    // PHASE 3: Consolidate trend tables
    LOGGER.info("=== PHASE 3: Consolidating trend tables ===");
    for (AbstractEconDataDownloader downloader : downloaders) {
      try {
        String downloaderName = downloader.getClass().getSimpleName();
        LOGGER.info("Consolidating trends using {}", downloaderName);
        downloader.consolidateAll();
      } catch (Exception e) {
        LOGGER.error("Error during trend consolidation for {}: {}",
            downloader.getClass().getSimpleName(), e.getMessage(), e);
      }
    }

    // Save cache manifest after all downloads to operating directory
    cacheManifest.save(econOperatingDirectory);
    LOGGER.info("=== All ECON data download, conversion, and consolidation complete ===");
  }

  /**
   * Override loadTableDefinitions to add custom FRED table definitions dynamically.
   * This combines static table definitions from econ-schema.json with dynamically
   * generated table definitions for custom FRED series.
   */
  @Override public List<Map<String, Object>> loadTableDefinitions() {
    // Start with base table definitions from econ-schema.json
    List<Map<String, Object>> baseTables = GovDataSubSchemaFactory.super.loadTableDefinitions();
    List<Map<String, Object>> tables = new ArrayList<>();

    LOGGER.info("[DEBUG] Loaded {} base table definitions from econ-schema.json", baseTables.size());

    // Filter BLS tables based on enabledBlsTables configuration
    for (Map<String, Object> table : baseTables) {
      String tableName = (String) table.get("name");

      // Check if this is a BLS table
      boolean isBlsTable = tableName.equals(BlsDataDownloader.BLS.tableNames.employmentStatistics)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.inflationMetrics)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.regionalCpi)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.metroCpi)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.stateIndustry)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.stateWages)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.countyWages)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.countyQcew)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.metroIndustry)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.metroWages)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.joltsRegional)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.wageGrowth)
          || tableName.equals(BlsDataDownloader.BLS.tableNames.regionalEmployment);

      if (isBlsTable) {
        // Check if table is enabled (null means all tables enabled)
        if (enabledBlsTables == null || enabledBlsTables.contains(tableName)) {
          tables.add(table);
          LOGGER.debug("[DEBUG] Including BLS table: {} with pattern: {}", tableName, table.get("pattern"));
        } else {
          LOGGER.info("Filtering out BLS table '{}' from schema (not in enabled tables)", tableName);
        }
      } else {
        // Non-BLS table, always include
        tables.add(table);
        LOGGER.debug("[DEBUG] Including non-BLS table: {} with pattern: {}", tableName, table.get("pattern"));
      }
    }

    // Expand trend_patterns into additional table definitions
    expandTrendPatterns(tables);

    LOGGER.info("[DEBUG] Total ECON table definitions: {}", tables.size());
    return tables;
  }

  /**
   * Generalized include/exclude filter parser for any data source.
   * Supports whitelist (include) or blacklist (exclude) patterns, but not both.
   *
   * @param operand  Schema configuration operand
   * @param allItems Complete set of all available items
   * @return Filtered set of items, or null for no filtering
   */
  private Set<String> parseIncludeExcludeFilter(
      Map<String, Object> operand,
      Set<String> allItems) {

    @SuppressWarnings("unchecked")
    Map<String, Object> config = (Map<String, Object>) operand.get("blsConfig");
    if (config == null) {
      return null; // No filtering
    }

    @SuppressWarnings("unchecked")
    List<String> includeItems = (List<String>) config.get("includeTables");
    @SuppressWarnings("unchecked")
    List<String> excludeItems = (List<String>) config.get("excludeTables");

    // Validate mutual exclusivity
    if (includeItems != null && excludeItems != null) {
      throw new IllegalArgumentException(
          String.format("Cannot specify both '%s' and '%s' in %s. " +
              "Use %s for whitelist or %s for blacklist, but not both.",
              "includeTables", "excludeTables", "blsConfig", "includeTables", "excludeTables"));
    }

    if (includeItems != null) {
      Set<String> filtered = new HashSet<>(includeItems);
      LOGGER.info("{} filter: including {} {}: {}", "BLS", filtered.size(), "tables", includeItems);
      return filtered;
    }

    if (excludeItems != null) {
      Set<String> filtered = new HashSet<>(allItems);
      excludeItems.forEach(filtered::remove);
      LOGGER.info("{} filter: excluding {} {}, downloading {} {}",
          "BLS", excludeItems.size(), "tables", filtered.size(), "tables");
      return filtered;
    }

    return null; // No filtering
  }

  /**
   * Parse BLS table filtering configuration from operand.
   * Delegates to generalized parseIncludeExcludeFilter method.
   *
   * @param operand Schema configuration operand
   * @return Set of enabled table names, or null to download all tables
   */
  private Set<String> parseBlsTableFilter(Map<String, Object> operand) {
    Set<String> allBlsTables =
        new HashSet<>(
            Arrays.asList(
            BlsDataDownloader.BLS.tableNames.employmentStatistics,
            BlsDataDownloader.BLS.tableNames.inflationMetrics,
            BlsDataDownloader.BLS.tableNames.regionalCpi,
            BlsDataDownloader.BLS.tableNames.metroCpi,
            BlsDataDownloader.BLS.tableNames.stateIndustry,
            BlsDataDownloader.BLS.tableNames.stateWages,
            BlsDataDownloader.BLS.tableNames.metroIndustry,
            BlsDataDownloader.BLS.tableNames.metroWages,
            BlsDataDownloader.BLS.tableNames.joltsRegional,
            BlsDataDownloader.BLS.tableNames.wageGrowth,
            BlsDataDownloader.BLS.tableNames.regionalEmployment));

    return parseIncludeExcludeFilter(operand,
        allBlsTables);
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    // EconSchemaFactory doesn't use tableDefinitions
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }

  /**
   * Generate materialization definitions from trend tables.
   *
   * <p>For each trend table (marked with _isTrendTable=true), creates a materialization
   * that tells Calcite's optimizer that the trend table is equivalent to:
   * SELECT * FROM detail_table
   *
   * <p>This allows the optimizer to automatically substitute trend tables when queries
   * don't use all partition keys, reducing S3 API calls.
   *
   * @param tables List of all table definitions (including trend tables)
   * @return List of materialization definitions for FileSchema
   */
  private List<Map<String, Object>> generateMaterializations(List<Map<String, Object>> tables) {
    List<Map<String, Object>> materializations = new ArrayList<>();

    for (Map<String, Object> table : tables) {
      Boolean isTrendTable = (Boolean) table.get("_isTrendTable");
      if (isTrendTable != null && isTrendTable) {
        String trendTableName = (String) table.get("name");
        String detailTableName = (String) table.get("_detailTableName");

        if (trendTableName == null || detailTableName == null) {
          LOGGER.warn("Skipping materialization for trend table with missing metadata");
          continue;
        }

        // Create materialization definition
        // Format expected by FileSchema:
        // {
        //   "table": "trend_table_name",
        //   "sql": "SELECT * FROM detail_table",
        //   "viewSchemaPath": ["ECON"],
        //   "existing": true
        // }
        Map<String, Object> materialization = new HashMap<>();
        materialization.put("table", trendTableName);
        materialization.put("sql", "SELECT * FROM " + detailTableName);

        // Set the schema path for the materialized view (ECON schema)
        materialization.put("viewSchemaPath", Collections.singletonList("ECON"));

        // Mark as existing (trend data files already exist, don't need to be generated)
        materialization.put("existing", true);

        materializations.add(materialization);
        LOGGER.debug("Created materialization: trend table '{}' for detail table '{}'",
            trendTableName, detailTableName);
      }
    }

    return materializations;
  }

}
