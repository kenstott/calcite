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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.duckdb.DuckDBJdbcSchema;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.census.CensusSchemaFactory;
import org.apache.calcite.adapter.govdata.econ.BeaDataDownloader;
import org.apache.calcite.adapter.govdata.econ.BlsDataDownloader;
import org.apache.calcite.adapter.govdata.econ.CacheManifest;
import org.apache.calcite.adapter.govdata.econ.EconRawToParquetConverter;
import org.apache.calcite.adapter.govdata.econ.EconSchemaFactory;
import org.apache.calcite.adapter.govdata.econ.FredDataDownloader;
import org.apache.calcite.adapter.govdata.econ.TreasuryDataDownloader;
import org.apache.calcite.adapter.govdata.econ.WorldBankDataDownloader;
import org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Government Data Schema Factory - Uber factory for government data sources.
 *
 * <p>This factory routes to specialized factories based on the 'dataSource'
 * parameter. Supported data sources:
 * <ul>
 *   <li>sec - Securities and Exchange Commission (EDGAR filings)</li>
 *   <li>geo - Geographic data (Census TIGER, HUD crosswalk, demographics)</li>
 *   <li>econ - Economic data (BLS employment, FRED indicators, Treasury yields)</li>
 *   <li>safety - Public safety data (FBI crime, NHTSA traffic, FEMA disasters)</li>
 *   <li>pub - Public data (Wikipedia, OpenStreetMap, Wikidata, academic research)</li>
 *   <li>census - U.S. Census Bureau demographic and socioeconomic data</li>
 *   <li>irs - Internal Revenue Service data (future)</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "GOV",
 *   "schemas": [{
 *     "name": "GOV",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "dataSource": "sec",
 *       "ciks": ["AAPL", "MSFT"],
 *       "startYear": 2020,
 *       "endYear": 2023
 *     }
 *   }]
 *
 * </pre>
 */
public class GovDataSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSchemaFactory.class);

  // Unified storage provider for all government data sources
  private StorageProvider storageProvider;

  // Cache storage provider for raw data (separate from parquet storage)
  private StorageProvider cacheStorageProvider;

  // Enriched storage configuration with endpoint overrides
  private Map<String, Object> storageConfig;

  // Store constraint metadata to pass to sub-factories
  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  // Track schemas created in this model for cross-domain constraint detection
  private final Map<String, Schema> createdSchemas = new HashMap<>();
  private final Map<String, String> schemaDataSources = new HashMap<>();

  @Override @NonNull public Schema create(@Nullable SchemaPlus parentSchema, @NonNull String name,
      @NonNull Map<String, Object> operand) {

    // Initialize storage provider based on configuration
    initializeStorageProvider(operand);

    // Get the data source
    String dataSource = (String) operand.get("dataSource");

    if (dataSource == null) {
      dataSource = "sec";
      LOGGER.info("No dataSource specified, defaulting to 'sec'");
    }

    LOGGER.info("Creating unified government data schema for source: {}", dataSource);

    // Establish operating cache directory (.aperio/<dataSource>/) ONCE for all sub-schemas
    // This is ALWAYS on local filesystem (working directory), even if parquet data is on S3
    // The .aperio directory requires file locking which doesn't work on S3
    String operatingDirectory = establishOperatingDirectory(dataSource);
    LOGGER.debug("Established operating directory for {}: {}", dataSource, operatingDirectory);

    // Add operating directory to operand so sub-schemas can use it
    Map<String, Object> enrichedOperand = new HashMap<>(operand);
    enrichedOperand.put("operatingDirectory", operatingDirectory);
    enrichedOperand.put("actualSchemaName", name);

    // Build the unified operand for FileSchemaFactory
    Map<String, Object> unifiedOperand = buildUnifiedOperand(dataSource, enrichedOperand);

    // Add storageType and storageConfig to unifiedOperand if storage provider was initialized
    // This ensures FileSchemaFactory can properly configure DuckDB with S3 endpoint and credentials
    if (operand.containsKey("storageType")) {
      unifiedOperand.put("storageType", operand.get("storageType"));
    }
    if (operand.containsKey("storageConfig")) {
      unifiedOperand.put("storageConfig", operand.get("storageConfig"));
    }

    // Pass through database_filename for shared DuckDB database support
    if (operand.containsKey("database_filename")) {
      unifiedOperand.put("database_filename", operand.get("database_filename"));
      LOGGER.info("Passing database_filename to FileSchema: {}", operand.get("database_filename"));
    }

    // Track this schema for cross-domain constraint detection
    schemaDataSources.put(name.toUpperCase(), dataSource.toUpperCase());

    // Create schema using FileSchemaFactory which properly handles StorageProvider and path resolution
    LOGGER.info("Creating schema with unified operand for {} data", dataSource);
    assert parentSchema != null;
    Schema schema = org.apache.calcite.adapter.file.FileSchemaFactory.INSTANCE.create(parentSchema, name, unifiedOperand);

    // Get the operating cache directory from FileSchema (.aperio/<schema_name>/)
    // Use file schema instance to extract operating directory
    File operatingCacheDirectory = null;
    if (schema instanceof FileSchema) {
      operatingCacheDirectory = ((FileSchema) schema).getOperatingCacheDirectory();
    } else if (schema instanceof DuckDBJdbcSchema) {
      FileSchema fileSchema = ((DuckDBJdbcSchema) schema).getFileSchema();
      if (fileSchema != null) {
        operatingCacheDirectory = fileSchema.getOperatingCacheDirectory();
      }
    }

    if (operatingCacheDirectory != null) {
      LOGGER.debug("Operating cache directory established by FileSchema: {}", operatingCacheDirectory);
      // Store this for potential future use by sub-schemas if they need to access it post-creation
      // However, by this point buildOperand() has already been called, so sub-schemas already
      // established their own directories. This architectural issue requires deeper refactoring.
    }

    // Register custom ECON converter if this is ECON data
    if (unifiedOperand.containsKey("_econData") && Boolean.TRUE.equals(unifiedOperand.get("_econData"))) {
      registerEconConverter(schema, operand);
    }

    createdSchemas.put(name.toUpperCase(), schema);
    return schema;
  }

  /**
   * Build unified operand by delegating to the appropriate sub-schema factory.
   * The sub-schema factory only builds configuration, doesn't create a schema.
   */
  private Map<String, Object> buildUnifiedOperand(String dataSource, Map<String, Object> operand) {
    switch (dataSource.toLowerCase()) {
      case "sec":
      case "edgar":
        return buildSecOperand(operand);

      case "geo":
      case "geographic":
        return buildGeoOperand(operand);

      case "econ":
      case "economic":
      case "economy":
        return buildEconOperand(operand);

      case "safety":
      case "crime":
      case "publicsafety":
      case "public_safety":
        return buildSafetyOperand(operand);

      case "pub":
      case "public":
      case "wikipedia":
      case "osm":
      case "openstreetmap":
        return buildPubOperand(operand);

      case "census":
        return buildCensusOperand(operand);

      case "irs":
        throw new UnsupportedOperationException(
            "IRS data source not yet implemented. Coming soon!");

      default:
        throw new IllegalArgumentException(
            "Unsupported government data source: '" + dataSource + "'. " +
            "Supported sources: sec, geo, econ, safety, pub, census, irs (future)");
    }
  }

  /**
   * Build operand for SEC/EDGAR data using the specialized SEC factory.
   */
  private Map<String, Object> buildSecOperand(Map<String, Object> operand) {
    LOGGER.debug("Building operand for SEC/EDGAR data");

    GovDataSubSchemaFactory secFactory = new org.apache.calcite.adapter.govdata.sec.SecSchemaFactory();

    // Get the operand configuration from SecSchemaFactory (parent provides services)
    return secFactory.buildOperand(operand, this);
  }

  /**
   * Build operand for Geographic data using the specialized Geo factory.
   */
  private Map<String, Object> buildGeoOperand(Map<String, Object> operand) {
    LOGGER.debug("Building operand for Geographic data");

    GovDataSubSchemaFactory geoFactory = new GeoSchemaFactory();

    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }

    // Add cross-domain constraints if other schemas exist
    if (schemaDataSources.containsValue("ECON")) {
      LOGGER.debug("ECON schema exists - regional economic analysis available");
    }
    if (schemaDataSources.containsValue("SEC")) {
      LOGGER.debug("SEC schema exists - geographic business analysis available");
    }

    if (!allConstraints.isEmpty() && tableDefinitions != null && geoFactory.supportsConstraints()) {
      geoFactory.setTableConstraints(allConstraints, tableDefinitions);
    }

    // Get the operand configuration from GeoSchemaFactory (parent provides services)
    return geoFactory.buildOperand(operand, this);
  }

  /**
   * Build operand for Public Safety data using the specialized Safety factory.
   */
  private Map<String, Object> buildSafetyOperand(Map<String, Object> operand) {
    LOGGER.debug("Building operand for Public Safety data");

    // TODO: Refactor SafetySchemaFactory to have buildOperand method
    // For now, throw an exception to indicate this needs implementation
    throw new UnsupportedOperationException(
        "SafetySchemaFactory needs to be refactored to support buildOperand pattern");
  }

  /**
   * Build operand for Public data using the specialized Pub factory.
   */
  private Map<String, Object> buildPubOperand(Map<String, Object> operand) {
    LOGGER.debug("Building operand for Public data");

    // TODO: Refactor PubSchemaFactory to have buildOperand method
    // For now, throw an exception to indicate this needs implementation
    throw new UnsupportedOperationException(
        "PubSchemaFactory needs to be refactored to support buildOperand pattern");
  }

  /**
   * Initialize the storage provider based on operand configuration.
   * Uses the same auto-detection logic as FileSchemaFactory.
   */
  private void initializeStorageProvider(Map<String, Object> operand) {
    if (storageProvider != null) {
      return; // Already initialized
    }

    String storageType = null;

    @SuppressWarnings("unchecked")
    Map<String, Object> tempStorageConfig = (Map<String, Object>) operand.get("storageConfig");
    if (tempStorageConfig == null) {
      tempStorageConfig = (Map<String, Object>) operand.get("s3Config");
      if (tempStorageConfig != null) {
        storageType = "s3";
      }
    }

    // Initialize instance variable for storageConfig
    if (storageConfig == null) {
      storageConfig = tempStorageConfig != null ? new HashMap<>(tempStorageConfig) : new HashMap<>();
    }

    // Auto-detect storage type from directory path (same logic as FileSchemaFactory)
    if (storageType == null) {
      String directory = (String) operand.get("directory");
      if (directory != null) {
        if (directory.startsWith("s3://")) {
          storageType = "s3";
          LOGGER.info("Auto-detected S3 storage from directory path: {}", directory);
        } else if (directory.startsWith("http://") || directory.startsWith("https://")) {
          storageType = "http";
          LOGGER.info("Auto-detected HTTP storage from directory path: {}", directory);
        } else if (directory.startsWith("hdfs://")) {
          storageType = "hdfs";
          LOGGER.info("Auto-detected HDFS storage from directory path: {}", directory);
        }
      }
    }

    // Add directory to storageConfig for all storage types
    String directory = (String) operand.get("directory");
    // Resolve environment variable placeholders (${VAR:default} syntax)
    // This uses the same logic as GovDataSubSchemaFactory.resolveEnvVar()
    if (directory != null && directory.contains("${")) {
      java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?}");
      java.util.regex.Matcher matcher = pattern.matcher(directory);
      if (matcher.find()) {
        String varName = matcher.group(1);
        String defaultValue = matcher.group(2);
        String resolvedValue = System.getenv(varName);
        if (resolvedValue == null) {
          resolvedValue = System.getProperty(varName);
        }
        if (resolvedValue == null) {
          resolvedValue = defaultValue;
        }
        directory = resolvedValue;
      }
    }
    if (directory != null && !storageConfig.containsKey("directory")) {
      storageConfig.put("directory", directory);
    }

    // Add AWS endpoint override to storageConfig if present (for MinIO, Wasabi, etc.)
    String endpointOverride = System.getenv("AWS_ENDPOINT_OVERRIDE");
    if (endpointOverride == null) {
      endpointOverride = System.getProperty("AWS_ENDPOINT_OVERRIDE");
    }
    if (endpointOverride != null && !storageConfig.containsKey("endpoint")) {
      storageConfig.put("endpoint", endpointOverride);
      LOGGER.info("Added AWS_ENDPOINT_OVERRIDE to storageConfig: {}", endpointOverride);
    }

    // Create the appropriate storage provider using the factory
    if (storageType != null) {
      storageProvider = StorageProviderFactory.createFromType(storageType, storageConfig);
      LOGGER.debug("Initialized {} StorageProvider with directory: {}", storageType, directory);
    } else {
      // Default to local file storage
      storageProvider = StorageProviderFactory.createFromType("local", storageConfig);
      LOGGER.debug("Initialized default LocalFileStorageProvider with directory: {}", directory);
    }

    // Add storageType and enriched storageConfig back to operand
    // This ensures FileSchemaFactory and DuckDB can access the complete storage configuration
    if (storageType != null) {
      operand.put("storageType", storageType);
    }
    operand.put("storageConfig", storageConfig);

    // Initialize cache storage provider (for raw data)
    // Cache directory can be S3 or local - get from operand
    String cacheDirectory = (String) operand.get("cacheDirectory");
    if (cacheDirectory == null) {
      cacheDirectory = System.getenv("GOVDATA_CACHE_DIR");
      if (cacheDirectory == null) {
        cacheDirectory = System.getProperty("GOVDATA_CACHE_DIR");
      }
    }

    if (cacheDirectory != null) {
      // Resolve environment variable placeholders in cache directory
      if (cacheDirectory.contains("${")) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?}");
        java.util.regex.Matcher matcher = pattern.matcher(cacheDirectory);
        if (matcher.find()) {
          String varName = matcher.group(1);
          String defaultValue = matcher.group(2);
          String resolvedValue = System.getenv(varName);
          if (resolvedValue == null) {
            resolvedValue = System.getProperty(varName);
          }
          if (resolvedValue == null) {
            resolvedValue = defaultValue;
          }
          cacheDirectory = resolvedValue;
        }
      }

      // Create cache storage provider using enriched storageConfig
      if (cacheDirectory.startsWith("s3://")) {
        // Use enriched S3 config from operand (includes endpoint override)
        Map<String, Object> cacheS3Config = new HashMap<>(storageConfig);
        cacheS3Config.put("directory", cacheDirectory);
        cacheStorageProvider = StorageProviderFactory.createFromType("s3", cacheS3Config);
        LOGGER.info("Initialized S3 cache storage provider with endpoint: {}", storageConfig.get("endpoint"));
      } else {
        cacheStorageProvider = StorageProviderFactory.createFromUrl(cacheDirectory);
        LOGGER.debug("Initialized local cache storage provider: {}", cacheDirectory);
      }

      // NOTE: cacheStorageProvider is NO LONGER added to operand Map
      // Sub-factories access it via parent.getCacheStorageProvider() instead
    }
  }

  // Deprecated create methods removed - now using buildOperand pattern with unified FileSchema creation

  /**
   * Build operand for Census data using the specialized Census factory.
   */
  private Map<String, Object> buildCensusOperand(Map<String, Object> operand) {
    LOGGER.debug("Building operand for Census demographic and socioeconomic data");

    GovDataSubSchemaFactory censusFactory = new CensusSchemaFactory();

    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }

    // Add cross-domain constraints if other schemas exist
    if (schemaDataSources.containsValue("GEO")) {
      LOGGER.debug("GEO schema exists - geographic demographic joins available");
    }
    if (schemaDataSources.containsValue("ECON")) {
      LOGGER.debug("ECON schema exists - economic demographic correlations available");
    }
    if (schemaDataSources.containsValue("SEC")) {
      LOGGER.debug("SEC schema exists - corporate demographic analysis available");
    }

    if (!allConstraints.isEmpty() && tableDefinitions != null && censusFactory.supportsConstraints()) {
      censusFactory.setTableConstraints(allConstraints, tableDefinitions);
    }

    // Get the operand configuration from CensusSchemaFactory (parent provides services)
    return censusFactory.buildOperand(operand, this);
  }

  /**
   * Build operand for Economic data using the specialized Econ factory.
   */
  private Map<String, Object> buildEconOperand(Map<String, Object> operand) {
    LOGGER.debug("Building operand from EconSchemaFactory for economic data");

    GovDataSubSchemaFactory factory = new EconSchemaFactory();

    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }

    // Add cross-domain constraints if SEC or GEO schemas exist
    if (schemaDataSources.containsValue("SEC")) {
      LOGGER.debug("SEC schema exists - economic/financial correlations available");
    }
    if (schemaDataSources.containsValue("GEO")) {
      LOGGER.debug("GEO schema exists - regional economic analysis available");
      Map<String, Map<String, Object>> crossDomainConstraints = defineCrossDomainConstraintsForEcon();
      allConstraints.putAll(crossDomainConstraints);
    }

    if (!allConstraints.isEmpty() && tableDefinitions != null && factory.supportsConstraints()) {
      factory.setTableConstraints(allConstraints, tableDefinitions);
    }

    // Get the operand configuration from EconSchemaFactory (parent provides services)
    // EconSchemaFactory will add source=econ prefix when reading directories from operand
    Map<String, Object> builtOperand = factory.buildOperand(operand, this);

    // Mark that this is ECON data so we can register the custom converter after FileSchema creation
    builtOperand.put("_econData", true);

    return builtOperand;
  }

  /**
   * Register the EconRawToParquetConverter with the FileSchema after it's been created.
   * This allows ECON data sources (BLS, FRED, Treasury, BEA, World Bank) to properly
   * register their parquet conversions in FileSchema's conversion registry.
   *
   * <p>When using executionEngine: "DUCKDB", the schema will be a DuckDBJdbcSchema
   * which wraps an internal FileSchema. This method extracts the internal FileSchema
   * and registers the converter on it.
   *
   * @param schema The created Schema instance (FileSchema or DuckDBJdbcSchema)
   * @param operand The original ECON operand configuration
   */
  private void registerEconConverter(Schema schema, Map<String, Object> operand) {
    LOGGER.info("registerEconConverter called with schema type: {}", schema.getClass().getName());
    FileSchema fileSchema = null;

    // Handle ConstraintAwareJdbcSchema wrapper (unwrap to get delegate)
    Schema unwrappedSchema = schema;
    if (schema instanceof org.apache.calcite.adapter.file.ConstraintAwareJdbcSchema) {
      unwrappedSchema = ((org.apache.calcite.adapter.file.ConstraintAwareJdbcSchema) schema).getDelegate();
      LOGGER.debug("Unwrapped ConstraintAwareJdbcSchema to get delegate: {}", unwrappedSchema.getClass().getName());
    }

    // Handle DuckDB execution engine case
    if (unwrappedSchema instanceof DuckDBJdbcSchema) {
      fileSchema = ((DuckDBJdbcSchema) unwrappedSchema).getFileSchema();
      LOGGER.debug("Extracted internal FileSchema from DuckDBJdbcSchema for converter registration");
    } else if (unwrappedSchema instanceof FileSchema) {
      fileSchema = (FileSchema) unwrappedSchema;
      LOGGER.debug("Using FileSchema directly for converter registration");
    }

    if (fileSchema == null) {
      LOGGER.warn("Schema is not a FileSchema or DuckDBJdbcSchema (actual type: {}), cannot register ECON converter",
          unwrappedSchema.getClass().getName());
      return;
    }

    // Extract cache directory from operand
    String cacheDirectory = (String) operand.get("cacheDirectory");
    if (cacheDirectory == null) {
      // Try environment variable fallback
      cacheDirectory = System.getenv("GOVDATA_CACHE_DIR");
      if (cacheDirectory == null) {
        cacheDirectory = System.getProperty("GOVDATA_CACHE_DIR");
      }
    }

    if (cacheDirectory == null) {
      LOGGER.warn("Cannot register ECON converter: cacheDirectory not found in operand or environment");
      return;
    }

    String econCacheDir = cacheStorageProvider.resolvePath(cacheDirectory, "source=econ");

    // Get .aperio directory for metadata (always local filesystem)
    File aperioDir = fileSchema.getOperatingCacheDirectory();
    String econOperatingDirectory = aperioDir.getAbsolutePath();

    // Load cache manifest from .aperio directory (not raw cache directory)
    CacheManifest cacheManifest = CacheManifest.load(econOperatingDirectory);

    // Extract API keys from environment variables or operand
    String blsApiKey = extractApiKey(operand, "blsApiKey", "BLS_API_KEY");
    String fredApiKey = extractApiKey(operand, "fredApiKey", "FRED_API_KEY");
    String beaApiKey = extractApiKey(operand, "beaApiKey", "BEA_API_KEY");

    // Get directory for parquet files
    String directory = (String) operand.get("directory");
    if (directory == null) {
      directory = System.getenv("GOVDATA_PARQUET_DIR");
      if (directory == null) {
        directory = System.getProperty("GOVDATA_PARQUET_DIR");
      }
    }

    String econParquetDir = directory != null ? storageProvider.resolvePath(directory, "source=econ") : null;

    // Use instance variable directly - no need to extract from operand
    if (cacheStorageProvider == null) {
      LOGGER.warn("Cache storage provider not initialized, converter registration may fail");
      return;
    }

    // Create downloaders (lightweight - just configuration, no download occurs)
    // Note: Pass null for config fields since these downloaders are only used for raw-to-parquet conversion registration
    // Pass placeholder years (0, 0) since this instance is only for converter registration, not actual downloading
    BlsDataDownloader blsDownloader = blsApiKey != null
        ? new BlsDataDownloader(blsApiKey, econCacheDir, econOperatingDirectory, econParquetDir, cacheStorageProvider, storageProvider, cacheManifest, null, 0, 0)
        : null;

    FredDataDownloader fredDownloader = fredApiKey != null
        ? new FredDataDownloader(fredApiKey, econCacheDir, econOperatingDirectory, econParquetDir, cacheStorageProvider, storageProvider, cacheManifest, null, 50, false, 365)
        : null;

    TreasuryDataDownloader treasuryDownloader =
        new TreasuryDataDownloader(econCacheDir, econOperatingDirectory, econParquetDir, cacheStorageProvider, storageProvider, cacheManifest);

    BeaDataDownloader beaDownloader = beaApiKey != null && econParquetDir != null
        ? new BeaDataDownloader(econCacheDir, econOperatingDirectory, econParquetDir, cacheStorageProvider, storageProvider, cacheManifest)
        : null;

    WorldBankDataDownloader worldBankDownloader =
        new WorldBankDataDownloader(econCacheDir, econOperatingDirectory, econParquetDir, cacheStorageProvider, storageProvider, cacheManifest);

    // Create and register the ECON converter
    EconRawToParquetConverter econConverter =
        new EconRawToParquetConverter(blsDownloader, fredDownloader);

    fileSchema.registerRawToParquetConverter(econConverter);

    LOGGER.info("Registered EconRawToParquetConverter with FileSchema (raw cache: {}, manifest: {})", econCacheDir, econOperatingDirectory);
  }

  /**
   * Extract API key from operand or environment variables.
   */
  private String extractApiKey(Map<String, Object> operand, String operandKey, String envKey) {
    // Check operand first
    String apiKey = (String) operand.get(operandKey);
    if (apiKey != null) {
      return apiKey;
    }

    // Fallback to environment variable
    apiKey = System.getenv(envKey);
    if (apiKey != null) {
      return apiKey;
    }

    // Fallback to system property
    return System.getProperty(envKey);
  }

  /**
   * Establish the operating directory (.aperio/<dataSource>/) ONCE for all sub-schemas.
   * This is ALWAYS on local filesystem (working directory), even if parquet data is on S3.
   * The .aperio directory requires file locking which doesn't work on S3.
   *
   * @param dataSource The data source name (sec, geo, econ, etc.)
   * @return The absolute path to the operating directory
   */
  private String establishOperatingDirectory(String dataSource) {
    String workingDir = System.getProperty("user.dir");
    if ("/".equals(workingDir) || workingDir == null || workingDir.isEmpty()) {
      LOGGER.warn("Working directory is root or invalid ('{}'), falling back to temp directory", workingDir);
      workingDir = System.getProperty("java.io.tmpdir");
    }

    String operatingDirectory = workingDir + "/.aperio/" + dataSource.toLowerCase();
    File operatingDir = new File(operatingDirectory);
    if (!operatingDir.exists()) {
      operatingDir.mkdirs();
    }

    return operatingDirectory;
  }

  /**
   * Get the unified storage provider for parquet data.
   * Sub-factories can use this to access parquet storage operations.
   *
   * @return Storage provider for parquet data (S3, local, etc.)
   */
  public StorageProvider getStorageProvider() {
    return storageProvider;
  }

  /**
   * Get the cache storage provider for raw data.
   * Sub-factories can use this to access raw data cache operations.
   *
   * @return Storage provider for cache operations (S3, local, etc.)
   */
  public StorageProvider getCacheStorageProvider() {
    return cacheStorageProvider;
  }

  /**
   * Get the enriched storage configuration with endpoint overrides.
   * Sub-factories can use this to create additional storage providers with same configuration.
   *
   * @return Enriched storage configuration map (includes AWS endpoint override, etc.)
   */
  public Map<String, Object> getStorageConfig() {
    return storageConfig != null ? new HashMap<>(storageConfig) : new HashMap<>();
  }

  /**
   * Get the operating directory for a data source.
   * Operating directories are always on local filesystem (.aperio/<dataSource>/).
   *
   * @param dataSource The data source name (sec, geo, econ, etc.)
   * @return Absolute path to operating directory
   */
  public String getOperatingDirectory(String dataSource) {
    return establishOperatingDirectory(dataSource);
  }

  // Deprecated create methods removed - now using buildOperand pattern with unified FileSchema creation

  @Override public boolean supportsConstraints() {
    // Enable constraint support for all government data sources
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints.size());
  }

  /**
   * Defines cross-domain foreign key constraints from SEC tables to GEO tables.
   * These are automatically added when both SEC and GEO schemas are present in the model.
   *
   * @return Map of table names to their cross-domain constraint definitions
   */
  private Map<String, Map<String, Object>> defineCrossDomainConstraintsForSec() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();

    // Find the GEO schema name
    String geoSchemaName = null;
    for (Map.Entry<String, String> entry : schemaDataSources.entrySet()) {
      if ("GEO".equals(entry.getValue())) {
        geoSchemaName = entry.getKey();
        break;
      }
    }

    if (geoSchemaName == null) {
      return constraints;
    }

    // Define FK from filing_metadata.state_of_incorporation to tiger_states.state_code
    Map<String, Object> filingMetadataConstraints = new HashMap<>();
    Map<String, Object> stateIncorpFK = new HashMap<>();
    stateIncorpFK.put("columns", List.of("state_of_incorporation"));
    stateIncorpFK.put("targetTable", Arrays.asList(geoSchemaName, "tiger_states"));
    stateIncorpFK.put("targetColumns", List.of("state_code"));

    filingMetadataConstraints.put("foreignKeys", List.of(stateIncorpFK));
    constraints.put("filing_metadata", filingMetadataConstraints);

    LOGGER.info("Added cross-domain FK constraint: filing_metadata.state_of_incorporation -> {}.tiger_states.state_code",
        geoSchemaName);

    // Future: Add more cross-domain constraints as needed
    // e.g., insider_transactions.insider_state -> tiger_states.state_code
    // e.g., company locations -> census_places

    return constraints;
  }

  /**
   * Defines cross-domain foreign key constraints from ECON tables to GEO tables.
   * These are automatically added when both ECON and GEO schemas are present in the model.
   *
   * @return Map of table names to their cross-domain constraint definitions
   */
  private Map<String, Map<String, Object>> defineCrossDomainConstraintsForEcon() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();

    // Find the GEO schema name
    String geoSchemaName = null;
    for (Map.Entry<String, String> entry : schemaDataSources.entrySet()) {
      if ("GEO".equals(entry.getValue())) {
        geoSchemaName = entry.getKey();
        break;
      }
    }

    if (geoSchemaName == null) {
      return constraints;
    }

    // regional_employment.state_code -> tiger_states.state_code (2-letter codes)
    Map<String, Object> regionalEmploymentConstraints = new HashMap<>();
    List<Map<String, Object>> regionalEmploymentFks = new ArrayList<>();

    Map<String, Object> regionalEmploymentToStatesFK = new HashMap<>();
    regionalEmploymentToStatesFK.put("columns", List.of("state_code"));
    regionalEmploymentToStatesFK.put("targetTable", Arrays.asList(geoSchemaName, "tiger_states"));
    regionalEmploymentToStatesFK.put("targetColumns", List.of("state_code"));
    regionalEmploymentFks.add(regionalEmploymentToStatesFK);

    regionalEmploymentConstraints.put("foreignKeys", regionalEmploymentFks);
    constraints.put("regional_employment", regionalEmploymentConstraints);

    LOGGER.info("Added cross-domain FK constraint: regional_employment.state_code -> {}.tiger_states.state_code",
        geoSchemaName);

    // regional_income.geo_fips -> tiger_states.state_fips (FIPS codes)
    // NOTE: This is a partial FK - only valid when geo_fips contains 2-digit state codes
    Map<String, Object> regionalIncomeConstraints = new HashMap<>();
    List<Map<String, Object>> regionalIncomeFks = new ArrayList<>();

    Map<String, Object> regionalIncomeToStatesFK = new HashMap<>();
    regionalIncomeToStatesFK.put("columns", List.of("geo_fips"));
    regionalIncomeToStatesFK.put("targetTable", Arrays.asList(geoSchemaName, "tiger_states"));
    regionalIncomeToStatesFK.put("targetColumns", List.of("state_fips"));
    regionalIncomeFks.add(regionalIncomeToStatesFK);

    regionalIncomeConstraints.put("foreignKeys", regionalIncomeFks);
    constraints.put("regional_income", regionalIncomeConstraints);

    LOGGER.info("Added cross-domain FK constraint: regional_income.geo_fips -> {}.tiger_states.state_fips (partial FK for state-level data)",
        geoSchemaName);

    // state_gdp.geo_fips -> tiger_states.state_fips (FIPS codes)
    Map<String, Object> stateGdpConstraints = new HashMap<>();
    List<Map<String, Object>> stateGdpFks = new ArrayList<>();

    Map<String, Object> stateGdpToStatesFK = new HashMap<>();
    stateGdpToStatesFK.put("columns", List.of("geo_fips"));
    stateGdpToStatesFK.put("targetSchema", geoSchemaName);
    stateGdpToStatesFK.put("targetTable", "tiger_states");
    stateGdpToStatesFK.put("targetColumns", List.of("state_fips"));
    stateGdpFks.add(stateGdpToStatesFK);

    stateGdpConstraints.put("foreignKeys", stateGdpFks);
    constraints.put("state_gdp", stateGdpConstraints);

    LOGGER.info("Added cross-domain FK constraint: state_gdp.geo_fips -> {}.tiger_states.state_fips",
        geoSchemaName);

    return constraints;
  }

}
