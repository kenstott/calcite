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

import org.apache.calcite.adapter.file.ModelLifecycleProcessor;
import org.apache.calcite.adapter.file.SubSchemaFactory;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.DuckDBPartitionStatusStore;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.census.CensusSchemaFactory;
import org.apache.calcite.adapter.govdata.econ.EconReferenceSchemaFactory;
import org.apache.calcite.adapter.govdata.econ.EconSchemaFactory;
import org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory;
import org.apache.calcite.adapter.govdata.sec.SecSchemaFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Government Data Schema Factory - Uber factory for government data sources.
 *
 * <p>Uses {@link ModelLifecycleProcessor} to orchestrate ETL and schema creation
 * with shared storage providers and incremental tracking.
 *
 * <p>Supported data sources:
 * <ul>
 *   <li>sec - Securities and Exchange Commission (EDGAR filings)</li>
 *   <li>geo - Geographic data (Census TIGER, HUD crosswalk, demographics)</li>
 *   <li>econ - Economic data (BLS employment, FRED indicators, Treasury yields)</li>
 *   <li>econ_reference - Reference/dimension tables for economic data</li>
 *   <li>census - U.S. Census Bureau demographic and socioeconomic data</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "ECON",
 *   "schemas": [{
 *     "name": "ECON",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "dataSource": "econ",
 *       "directory": "s3://bucket/parquet/",
 *       "cacheDirectory": "s3://bucket/raw/"
 *     }
 *   }]
 * }
 * </pre>
 */
public class GovDataSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSchemaFactory.class);

  // Shared storage providers (created once per model)
  private StorageProvider sourceStorage;
  private StorageProvider materializedStorage;

  // Store constraint metadata to pass to sub-factories
  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  @Override @NonNull public Schema create(@Nullable SchemaPlus parentSchema, @NonNull String name,
      @NonNull Map<String, Object> operand) {

    String dataSource = (String) operand.get("dataSource");
    if (dataSource == null) {
      dataSource = "sec";
      LOGGER.info("No dataSource specified, defaulting to 'sec'");
    }

    LOGGER.info("Creating government data schema '{}' for source: {}", name, dataSource);

    // Initialize storage providers
    initializeStorageProviders(operand);

    // Set cross-schema system properties for YAML variable substitution
    setCrossSchemaProperties(dataSource, operand);

    // Get the appropriate sub-schema factory
    SubSchemaFactory factory = getFactoryForDataSource(dataSource);

    // Build processor with dependencies first, then the requested schema
    ModelLifecycleProcessor.Builder processorBuilder = ModelLifecycleProcessor.builder()
        .sourceStorage(sourceStorage)
        .materializedStorage(materializedStorage);

    // Process dependencies first (in order)
    for (String depDataSource : factory.getDependencies()) {
      if (!processedDependencies.contains(depDataSource)) {
        LOGGER.info("Processing dependency '{}' for schema '{}'", depDataSource, name);

        String depOperatingDir = establishOperatingDirectory(depDataSource);
        IncrementalTracker depTracker = createIncrementalTracker(depOperatingDir, depDataSource);
        SubSchemaFactory depFactory = getFactoryForDataSource(depDataSource);
        Map<String, Object> depOperand = enrichOperand(operand, depDataSource, depDataSource.toUpperCase());

        processorBuilder
            .operatingDirectory(depOperatingDir)
            .incrementalTracker(depTracker)
            .addSchema(depDataSource.toUpperCase(), depFactory, depOperand);

        processedDependencies.add(depDataSource);
      }
    }

    // Now add the main schema
    String operatingDirectory = establishOperatingDirectory(dataSource);
    IncrementalTracker tracker = createIncrementalTracker(operatingDirectory, name);

    // Check for freshStart option - clears all completion tracking to force re-download
    Boolean freshStart = (Boolean) operand.get("freshStart");
    if (Boolean.TRUE.equals(freshStart)) {
      LOGGER.info("freshStart=true: Clearing all completion tracking for '{}'", name);
      tracker.clearAllCompletions();
    }

    Map<String, Object> enrichedOperand = enrichOperand(operand, dataSource, name);

    processorBuilder
        .operatingDirectory(operatingDirectory)
        .incrementalTracker(tracker)
        .addSchema(name, factory, enrichedOperand);

    // Run ETL for all schemas
    ModelLifecycleProcessor.ProcessResult result = processorBuilder.build().process();

    // Return the created schema directly (not the SchemaPlus wrapper)
    // This is essential because CachingCalciteSchema.snapshot() fails
    // with SchemaPlus wrappers (SchemaPlusImpl.snapshot() throws UnsupportedOperationException)
    Schema schema = result.getSchema(name);
    if (schema == null) {
      throw new IllegalStateException("Failed to create schema: " + name);
    }

    LOGGER.info("Schema '{}' created successfully", name);
    return schema;
  }

  // Track processed dependencies to avoid duplicates
  private final java.util.Set<String> processedDependencies = new java.util.HashSet<>();

  /**
   * Get the sub-schema factory for the given data source.
   */
  private SubSchemaFactory getFactoryForDataSource(String dataSource) {
    switch (dataSource.toLowerCase()) {
      case "sec":
      case "edgar":
        return new SecSchemaFactory();

      case "geo":
      case "geographic":
        return new GeoSchemaFactory();

      case "econ_reference":
      case "econ_ref":
        return new EconReferenceSchemaFactory();

      case "econ":
      case "economic":
      case "economy":
        return new EconSchemaFactory();

      case "census":
        return new CensusSchemaFactory();

      default:
        throw new IllegalArgumentException(
            "Unsupported government data source: '" + dataSource + "'. " +
            "Supported sources: sec, geo, econ_reference, econ, census");
    }
  }

  /**
   * Initialize storage providers based on operand configuration.
   */
  @SuppressWarnings("unchecked")
  private void initializeStorageProviders(Map<String, Object> operand) {
    // Check for S3 configuration in operand (for R2, MinIO, custom S3)
    // Check for both "s3Config" (legacy) and "s3" (new) keys
    Map<String, Object> s3Config = (Map<String, Object>) operand.get("s3Config");
    if (s3Config == null) {
      s3Config = (Map<String, Object>) operand.get("s3");
    }
    if (s3Config != null) {
      // Resolve env vars in nested s3 config
      s3Config = resolveS3Config(s3Config);
      LOGGER.debug("Resolved S3 config with {} keys", s3Config.size());
    } else {
      // Also check for individual S3 fields in operand
      if (operand.containsKey("accessKeyId") || operand.containsKey("secretAccessKey")
          || operand.containsKey("endpoint") || operand.containsKey("region")) {
        s3Config = new HashMap<>();
        if (operand.containsKey("accessKeyId")) {
          s3Config.put("accessKeyId", resolveEnvVar(operand.get("accessKeyId")));
        }
        if (operand.containsKey("secretAccessKey")) {
          s3Config.put("secretAccessKey", resolveEnvVar(operand.get("secretAccessKey")));
        }
        if (operand.containsKey("endpoint")) {
          s3Config.put("endpoint", resolveEnvVar(operand.get("endpoint")));
        }
        if (operand.containsKey("region")) {
          s3Config.put("region", resolveEnvVar(operand.get("region")));
        }
      }
    }

    // Materialized storage (parquet/iceberg output)
    String directory = resolveDirectory(operand, "directory");
    if (directory != null) {
      if (directory.startsWith("s3://") && s3Config != null && !s3Config.isEmpty()) {
        LOGGER.debug("Creating S3StorageProvider with explicit config for {}", directory);
        // Add directory to config so S3StorageProvider sets baseS3Path for lifecycle rules
        Map<String, Object> storageConfig = new HashMap<>(s3Config);
        storageConfig.put("directory", directory);
        materializedStorage = StorageProviderFactory.createFromType("s3", storageConfig);
      } else {
        materializedStorage = StorageProviderFactory.createFromUrl(directory);
      }
      LOGGER.debug("Initialized materialized storage: {}", directory);
    }

    // Source storage (raw data cache)
    String cacheDirectory = resolveDirectory(operand, "cacheDirectory");
    if (cacheDirectory == null) {
      cacheDirectory = System.getenv("GOVDATA_CACHE_DIR");
    }
    if (cacheDirectory != null) {
      if (cacheDirectory.startsWith("s3://") && s3Config != null && !s3Config.isEmpty()) {
        // Add directory to config so S3StorageProvider sets baseS3Path for lifecycle rules
        Map<String, Object> cacheStorageConfig = new HashMap<>(s3Config);
        cacheStorageConfig.put("directory", cacheDirectory);
        sourceStorage = StorageProviderFactory.createFromType("s3", cacheStorageConfig);
      } else {
        sourceStorage = StorageProviderFactory.createFromUrl(cacheDirectory);
      }
      LOGGER.debug("Initialized source storage: {}", cacheDirectory);
    }
  }

  /**
   * Resolve ${VAR} patterns in S3 config values.
   */
  private Map<String, Object> resolveS3Config(Map<String, Object> config) {
    Map<String, Object> resolved = new HashMap<>();
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      resolved.put(entry.getKey(), resolveEnvVar(entry.getValue()));
    }
    return resolved;
  }

  /**
   * Resolve ${VAR} pattern in a value if it's a string.
   */
  private Object resolveEnvVar(Object value) {
    if (value instanceof String) {
      String str = (String) value;
      if (str.contains("${")) {
        return org.apache.calcite.adapter.file.etl.VariableResolver.resolveEnvVars(str);
      }
    }
    return value;
  }

  /**
   * Resolve directory from operand, handling environment variable substitution.
   */
  private String resolveDirectory(Map<String, Object> operand, String key) {
    String directory = (String) operand.get(key);
    if (directory == null) {
      return null;
    }

    // Resolve ${VAR:default} patterns
    if (directory.contains("${")) {
      java.util.regex.Pattern pattern =
          java.util.regex.Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?}");
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
    return directory;
  }

  /**
   * Establish the operating directory (.aperio/<dataSource>/).
   * Always on local filesystem (for file locking).
   */
  private String establishOperatingDirectory(String dataSource) {
    String workingDir = System.getProperty("user.dir");
    if ("/".equals(workingDir) || workingDir == null || workingDir.isEmpty()) {
      LOGGER.warn("Working directory is root or invalid, falling back to temp directory");
      workingDir = System.getProperty("java.io.tmpdir");
    }

    String operatingDirectory = workingDir + "/.aperio/" + dataSource.toLowerCase();
    File opDir = new File(operatingDirectory);
    if (!opDir.exists()) {
      opDir.mkdirs();
    }

    LOGGER.debug("Operating directory: {}", operatingDirectory);
    return operatingDirectory;
  }

  /**
   * Create incremental tracker for resumable ETL.
   */
  private IncrementalTracker createIncrementalTracker(String operatingDirectory, String schemaName) {
    try {
      return DuckDBPartitionStatusStore.getInstance(operatingDirectory);
    } catch (Exception e) {
      LOGGER.warn("Failed to create DuckDB tracker, using NOOP: {}", e.getMessage());
      return IncrementalTracker.NOOP;
    }
  }

  /**
   * Enrich operand with additional properties needed by sub-factories.
   */
  private Map<String, Object> enrichOperand(Map<String, Object> operand,
      String dataSource, String schemaName) {
    Map<String, Object> enriched = new HashMap<>(operand);
    enriched.put("canonicalSchemaName", dataSource.toLowerCase());
    enriched.put("actualSchemaName", schemaName);
    return enriched;
  }

  /**
   * Set cross-schema system properties for YAML variable substitution.
   */
  private void setCrossSchemaProperties(String dataSource, Map<String, Object> operand) {
    // GEO schema name
    String geoSchemaName = getStringOrDefault(operand, "geoSchemaName", "geo");
    System.setProperty("GEO_SCHEMA_NAME", geoSchemaName);

    // ECON schema name
    String econSchemaName = getStringOrDefault(operand, "econSchemaName", "econ");
    System.setProperty("ECON_SCHEMA_NAME", econSchemaName);

    // ECON_REFERENCE schema name
    String econRefSchemaName = getStringOrDefault(operand, "econReferenceSchemaName", "econ_reference");
    System.setProperty("ECON_REFERENCE_SCHEMA_NAME", econRefSchemaName);

    // CENSUS schema name
    String censusSchemaName = getStringOrDefault(operand, "censusSchemaName", "census");
    System.setProperty("CENSUS_SCHEMA_NAME", censusSchemaName);

    // SEC schema name
    String secSchemaName = getStringOrDefault(operand, "secSchemaName", "sec");
    System.setProperty("SEC_SCHEMA_NAME", secSchemaName);

    // Set parquet directory for cross-schema references (e.g., BeaDimensionResolver)
    // This allows dimension resolvers to find reference tables from other schemas
    String directory = resolveDirectory(operand, "directory");
    if (directory != null) {
      System.setProperty("GOVDATA_PARQUET_DIR", directory);
      LOGGER.debug("Set GOVDATA_PARQUET_DIR={}", directory);
    }

    LOGGER.debug("Set cross-schema properties for {}", dataSource);
  }

  private String getStringOrDefault(Map<String, Object> operand, String key, String defaultValue) {
    Object value = operand.get(key);
    if (value instanceof String && !((String) value).isEmpty()) {
      return (String) value;
    }
    return defaultValue;
  }

  /**
   * Get the storage provider for materialized data.
   *
   * <p>Note: For new schemas using ModelLifecycleProcessor, storage providers
   * are passed directly rather than through this method.
   */
  public StorageProvider getStorageProvider() {
    return materializedStorage;
  }

  /**
   * Get the cache storage provider for raw data.
   *
   * <p>Note: For new schemas using ModelLifecycleProcessor, storage providers
   * are passed directly rather than through this method.
   */
  public StorageProvider getCacheStorageProvider() {
    return sourceStorage;
  }

  @Override public boolean supportsConstraints() {
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables", tableConstraints.size());
  }
}
