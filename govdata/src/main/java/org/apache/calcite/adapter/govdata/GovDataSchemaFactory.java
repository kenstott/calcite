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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.ModelLifecycleProcessor;
import org.apache.calcite.adapter.file.SubSchemaFactory;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PipelineTracker;
import org.apache.calcite.adapter.file.partition.PipelineTrackerFactory;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.census.CensusSchemaFactory;
import org.apache.calcite.adapter.govdata.crime.CrimeSchemaFactory;
import org.apache.calcite.adapter.govdata.cyber.CyberSchemaFactory;
import org.apache.calcite.adapter.govdata.econ.EconReferenceSchemaFactory;
import org.apache.calcite.adapter.govdata.econ.EconSchemaFactory;
import org.apache.calcite.adapter.govdata.fec.FecSchemaFactory;
import org.apache.calcite.adapter.govdata.fedregister.FedRegisterSchemaFactory;
import org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory;
import org.apache.calcite.adapter.govdata.edu.EduSchemaFactory;
import org.apache.calcite.adapter.govdata.energy.EnergySchemaFactory;
import org.apache.calcite.adapter.govdata.health.HealthSchemaFactory;
import org.apache.calcite.adapter.govdata.lands.LandsSchemaFactory;
import org.apache.calcite.adapter.govdata.patents.PatentsSchemaFactory;
import org.apache.calcite.adapter.govdata.ref.RefSchemaFactory;
import org.apache.calcite.adapter.govdata.sec.SecSchemaFactory;
import org.apache.calcite.adapter.govdata.weather.WeatherSchemaFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.adapter.file.etl.VariableResolver;

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
 *   <li>crime - FBI Crime Data Explorer and Bureau of Justice Statistics</li>
 *   <li>weather - NWS weather stations/alerts, NOAA CDO climate data, EPA air quality</li>
 *   <li>ref - Reference data (GLEIF entities, CIK mapping, OpenFIGI instruments)</li>
 *   <li>fec - Federal Election Commission campaign finance data</li>
 *   <li>fedregister - U.S. Federal Register (rules, proposed rules, notices, presidential docs)</li>
 *   <li>cyber_vuln - Cybersecurity vulnerability data (NVD CVEs, CISA KEV, OSV, GitHub SA)</li>
 *   <li>cyber_threat - Cyber threat intelligence (ATT&CK, IOC feeds, exploits, standards)</li>
 *   <li>energy - U.S. energy data (EIA electricity, fossil fuel production, storage, prices)</li>
 *   <li>lands - U.S. federal public lands (USFS, NPS, BLM, ONRR, FIA)</li>
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

    // Check if this schema was already created (e.g., as a dependency of another schema)
    String cacheKey = dataSource.toLowerCase();
    Schema cachedSchema = schemaCache.get(cacheKey);
    if (cachedSchema != null) {
      LOGGER.info("Schema '{}' (dataSource={}) already created as dependency, returning cached",
          name, dataSource);
      return cachedSchema;
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

        String depOperatingDir = establishOperatingDirectory(depDataSource, null);
        IncrementalTracker depTracker = createIncrementalTracker(depOperatingDir, depDataSource, operand);
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
    String operatingDirectory = establishOperatingDirectory(dataSource, operand);
    IncrementalTracker tracker = createIncrementalTracker(operatingDirectory, name, operand);

    // Check for freshStart option - clears all completion tracking to force re-download
    Boolean freshStart = (Boolean) operand.get("freshStart");
    if (Boolean.TRUE.equals(freshStart)) {
      LOGGER.info("freshStart=true: Clearing all completion tracking for '{}'", name);
      tracker.clearAllCompletions();
    }

    Map<String, Object> enrichedOperand = enrichOperand(operand, dataSource, name);
    enrichedOperand.put("operatingDirectory", operatingDirectory);

    processorBuilder
        .operatingDirectory(operatingDirectory)
        .incrementalTracker(tracker)
        .addSchema(name, factory, enrichedOperand);

    // Run ETL for all schemas
    ModelLifecycleProcessor.ProcessResult result = processorBuilder.build().process();

    // Cache dependency schemas so they can be returned if requested later
    for (String depDataSource : factory.getDependencies()) {
      String depSchemaName = depDataSource.toUpperCase();
      Schema depSchema = result.getSchema(depSchemaName);
      if (depSchema != null) {
        schemaCache.put(depDataSource.toLowerCase(), depSchema);
        LOGGER.debug("Cached dependency schema '{}' (key={})", depSchemaName, depDataSource.toLowerCase());
      }
    }

    // Return the created schema directly (not the SchemaPlus wrapper)
    // This is essential because CachingCalciteSchema.snapshot() fails
    // with SchemaPlus wrappers (SchemaPlusImpl.snapshot() throws UnsupportedOperationException)
    Schema schema = result.getSchema(name);
    if (schema == null) {
      throw new IllegalStateException("Failed to create schema: " + name);
    }

    // Cache the main schema as well
    schemaCache.put(cacheKey, schema);

    LOGGER.info("Schema '{}' created successfully", name);
    return schema;
  }

  // Track processed dependencies to avoid duplicates across factory instances
  // Static because Calcite creates new factory instances per schema
  private static final Set<String> processedDependencies =
      Collections.synchronizedSet(new HashSet<>());

  // Cache schemas that were created as dependencies so we can return them
  // when they are requested as main schemas (avoids double processing)
  private static final Map<String, Schema> schemaCache =
      Collections.synchronizedMap(new HashMap<>());

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

      case "crime":
      case "fbi_crime":
      case "public_safety":
        return new CrimeSchemaFactory();

      case "weather":
      case "climate":
        return new WeatherSchemaFactory();

      case "ref":
      case "reference":
        return new RefSchemaFactory();

      case "fec":
      case "campaign_finance":
        return new FecSchemaFactory();

      case "fedregister":
      case "federal_register":
      case "fr":
        return new FedRegisterSchemaFactory();

      case "cyber_vuln":
      case "cybervuln":
        return new CyberSchemaFactory("cyber_vuln");

      case "cyber_vuln_smoke":
      case "cybervulnsmoke":
        return new CyberSchemaFactory("cyber_vuln_smoke");

      case "cyber_threat":
      case "cyberthreat":
        return new CyberSchemaFactory("cyber_threat");

      case "health":
      case "health_fda":
      case "pharma":
        return new HealthSchemaFactory();

      case "energy":
      case "eia":
        return new EnergySchemaFactory();

      case "edu":
      case "education":
        return new EduSchemaFactory();

      case "patents":
      case "patent":
      case "uspto":
        return new PatentsSchemaFactory();

      case "lands":
        return new LandsSchemaFactory();

      default:
        throw new IllegalArgumentException(
            "Unsupported government data source: '" + dataSource + "'. " +
            "Supported sources: sec, geo, econ_reference, econ, census, crime, weather, ref, fec,"
            + " fedregister, cyber_vuln, cyber_threat, health, energy, edu, patents, lands");
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
      // Resolve env vars in nested s3 config and update operand so downstream
      // consumers (PipelineTrackerFactory, SecSchemaFactory) get resolved values
      s3Config = resolveS3Config(s3Config);
      operand.put("s3Config", s3Config);
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
      if (directory.startsWith("s3://")) {
        // S3 storage requires explicit credentials - fail fast if missing
        if (s3Config == null || s3Config.isEmpty()) {
          throw new IllegalArgumentException(
              "S3 storage configured (directory=" + directory + ") but s3Config is missing. "
              + "Provide s3Config with accessKeyId, secretAccessKey, and endpoint (for S3-compatible) "
              + "or region (for AWS S3). Will not fall back to AWS credential chain.");
        }
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
      cacheDirectory = GovDataUtils.resolveEnvVar("${GOVDATA_CACHE_DIR}");
    }
    if (cacheDirectory != null) {
      if (cacheDirectory.startsWith("s3://")) {
        // S3 storage requires explicit credentials - fail fast if missing
        if (s3Config == null || s3Config.isEmpty()) {
          throw new IllegalArgumentException(
              "S3 storage configured (cacheDirectory=" + cacheDirectory + ") but s3Config is missing. "
              + "Provide s3Config with accessKeyId, secretAccessKey, and endpoint (for S3-compatible) "
              + "or region (for AWS S3). Will not fall back to AWS credential chain.");
        }
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
        return VariableResolver.resolveEnvVars(str);
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
      Pattern pattern = Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?}");
      Matcher matcher = pattern.matcher(directory);
      if (matcher.find()) {
        String varName = matcher.group(1);
        String defaultValue = matcher.group(2);
        String resolvedValue = System.getProperty(varName);
        if (resolvedValue == null) {
          resolvedValue = System.getenv(varName);
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
   *
   * <p>Honors an explicit {@code operatingDirectory} key in the operand when present,
   * allowing tests and callers to isolate tracker state in a temp directory per run.
   */
  private String establishOperatingDirectory(String dataSource, Map<String, Object> operand) {
    // Allow tests/callers to override the operating directory entirely
    Object override = operand != null ? operand.get("operatingDirectory") : null;
    if (override instanceof String && !((String) override).isEmpty()) {
      String opDir = (String) override;
      new File(opDir).mkdirs();
      LOGGER.debug("Operating directory (override): {}", opDir);
      return opDir;
    }

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
   * Uses PipelineTrackerFactory to select backend based on operand or environment.
   */
  private IncrementalTracker createIncrementalTracker(String operatingDirectory,
      String schemaName, Map<String, Object> operand) {
    return PipelineTrackerFactory.createFromOperand(operand, operatingDirectory);
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

    // FEC schema name
    String fecSchemaName = getStringOrDefault(operand, "fecSchemaName", "fec");
    System.setProperty("FEC_SCHEMA_NAME", fecSchemaName);

    // CYBER schema names
    String cyberVulnSchemaName = getStringOrDefault(operand, "cyberVulnSchemaName", "cyber_vuln");
    System.setProperty("CYBER_VULN_SCHEMA_NAME", cyberVulnSchemaName);
    String cyberThreatSchemaName = getStringOrDefault(operand, "cyberThreatSchemaName", "cyber_threat");
    System.setProperty("CYBER_THREAT_SCHEMA_NAME", cyberThreatSchemaName);

    // Set parquet directory for cross-schema references (e.g., BeaDimensionResolver)
    // This allows dimension resolvers to find reference tables from other schemas
    String directory = resolveDirectory(operand, "directory");
    if (directory != null) {
      System.setProperty("GOVDATA_PARQUET_DIR", directory);
      LOGGER.debug("Set GOVDATA_PARQUET_DIR={}", directory);
    }

    // Set year range properties from operand for YAML variable substitution
    // This allows ${GOVDATA_START_YEAR}, ${SEC_START_YEAR}, etc. to resolve from model config
    Object startYearObj = operand.get("startYear");
    if (startYearObj != null) {
      String startYear = String.valueOf(startYearObj);
      System.setProperty("GOVDATA_START_YEAR", startYear);
      if ("sec".equalsIgnoreCase(dataSource)) {
        System.setProperty("SEC_START_YEAR", startYear);
      }
      LOGGER.debug("Set GOVDATA_START_YEAR={}", startYear);
    }
    Object endYearObj = operand.get("endYear");
    if (endYearObj != null) {
      String endYear = String.valueOf(endYearObj);
      System.setProperty("GOVDATA_END_YEAR", endYear);
      if ("sec".equalsIgnoreCase(dataSource)) {
        System.setProperty("SEC_END_YEAR", endYear);
      }
      LOGGER.debug("Set GOVDATA_END_YEAR={}", endYear);
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
