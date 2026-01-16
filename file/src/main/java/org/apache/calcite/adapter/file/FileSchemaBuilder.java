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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.SchemaConfig;
import org.apache.calcite.adapter.file.etl.SchemaLifecycleProcessor;
import org.apache.calcite.adapter.file.etl.SchemaResult;
import org.apache.calcite.adapter.file.etl.TableContext;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Builder for creating FileSchema with ETL lifecycle support.
 *
 * <p>This builder allows configuring ETL hooks before running the ETL pipeline
 * and creating the final FileSchema. The typical flow is:
 *
 * <pre>{@code
 * Schema schema = FileSchemaBuilder.create()
 *     .schemaResource("/econ/econ-schema.yaml")
 *     .resolveDimensions("world_indicators", (ctx, dims) -> resolveWorldBank(dims))
 *     .shouldProcess("employment_stats", ctx -> hasApiKey("BLS_API_KEY"))
 *     .runEtl()
 *     .build(parentSchema, "ECON");
 * }</pre>
 */
public class FileSchemaBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSchemaBuilder.class);

  private Map<String, Object> config;
  private Map<String, Object> operandOverrides = new HashMap<>();
  private SchemaLifecycleProcessor.Builder etlBuilder;
  private java.util.Set<String> excludedTables = new java.util.HashSet<>();
  private boolean autoDownload = false;
  private boolean etlExecuted = false;
  private StorageProvider storageProvider;
  private StorageProvider cacheStorageProvider;
  private IncrementalTracker incrementalTracker;

  private FileSchemaBuilder() {
    this.etlBuilder = SchemaLifecycleProcessor.builder()
        .incrementalTracker(IncrementalTracker.NOOP);
  }

  /**
   * Create a new FileSchemaBuilder.
   */
  public static FileSchemaBuilder create() {
    return new FileSchemaBuilder();
  }

  /**
   * Load schema configuration from a classpath resource (YAML or JSON).
   */
  @SuppressWarnings("unchecked")
  public FileSchemaBuilder schemaResource(String resourcePath) {
    try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IllegalStateException("Schema resource not found: " + resourcePath);
      }
      // Configure LoaderOptions with higher alias limit for complex YAML schemas
      // that use many anchors/aliases (e.g., *county_fips_column repeated in 30+ tables)
      org.yaml.snakeyaml.LoaderOptions loaderOptions = new org.yaml.snakeyaml.LoaderOptions();
      loaderOptions.setMaxAliasesForCollections(500);
      Map<String, Object> rawConfig = new Yaml(loaderOptions).load(is);
      // Resolve all ${ENV_VAR} patterns at load time
      this.config = resolveAllEnvVars(rawConfig);
      LOGGER.debug("Loaded schema config from: {}", resourcePath);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load schema: " + resourcePath, e);
    }
    return this;
  }

  /**
   * Set schema configuration directly from a Map.
   */
  public FileSchemaBuilder schemaConfig(Map<String, Object> config) {
    // Resolve all ${ENV_VAR} patterns at load time
    // Use LinkedHashMap to preserve key order from input
    this.config = resolveAllEnvVars(new LinkedHashMap<>(config));
    return this;
  }

  /**
   * Recursively resolve all ${ENV_VAR} patterns in a config map.
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> resolveAllEnvVars(Map<String, Object> map) {
    // Use LinkedHashMap to preserve YAML key order - critical for dimension ordering
    Map<String, Object> resolved = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof String) {
        resolved.put(entry.getKey(), VariableResolver.resolveEnvVars((String) value));
      } else if (value instanceof Map) {
        resolved.put(entry.getKey(), resolveAllEnvVars((Map<String, Object>) value));
      } else if (value instanceof java.util.List) {
        resolved.put(entry.getKey(), resolveListEnvVars((java.util.List<Object>) value));
      } else {
        resolved.put(entry.getKey(), value);
      }
    }
    return resolved;
  }

  /**
   * Recursively resolve all ${ENV_VAR} patterns in a list.
   */
  @SuppressWarnings("unchecked")
  private java.util.List<Object> resolveListEnvVars(java.util.List<Object> list) {
    java.util.List<Object> resolved = new java.util.ArrayList<>();
    for (Object item : list) {
      if (item instanceof String) {
        resolved.add(VariableResolver.resolveEnvVars((String) item));
      } else if (item instanceof Map) {
        resolved.add(resolveAllEnvVars((Map<String, Object>) item));
      } else if (item instanceof java.util.List) {
        resolved.add(resolveListEnvVars((java.util.List<Object>) item));
      } else {
        resolved.add(item);
      }
    }
    return resolved;
  }

  /**
   * Add operand overrides (merged with config, overrides take precedence).
   */
  public FileSchemaBuilder operand(Map<String, Object> overrides) {
    this.operandOverrides.putAll(overrides);
    return this;
  }

  /**
   * Add a single operand override.
   */
  public FileSchemaBuilder operand(String key, Object value) {
    this.operandOverrides.put(key, value);
    return this;
  }

  /**
   * Set the storage provider for both ETL and final schema.
   *
   * <p>When set, this provider is used instead of creating one from directory config.
   * This allows sharing a single storage provider instance across multiple schemas.
   *
   * @param provider The storage provider instance for parquet data
   */
  public FileSchemaBuilder storageProvider(StorageProvider provider) {
    this.storageProvider = provider;
    // Also set on ETL builder so ETL uses the same provider
    etlBuilder.storageProvider(provider);
    LOGGER.info("FileSchemaBuilder.storageProvider(): Set storage provider {} on etlBuilder",
        provider != null ? provider.getClass().getSimpleName() : "null");
    return this;
  }

  /**
   * Set the cache storage provider for raw data during ETL.
   *
   * <p>This is separate from the main storage provider and is used for
   * downloading and caching source data before materialization.
   *
   * @param provider The storage provider instance for cache data
   */
  public FileSchemaBuilder cacheStorageProvider(StorageProvider provider) {
    this.cacheStorageProvider = provider;
    // Also set on ETL builder so raw cache uses this provider
    etlBuilder.sourceStorageProvider(provider);
    return this;
  }

  /**
   * Set the incremental tracker for resumable ETL.
   *
   * <p>The tracker persists which dimension combinations have been processed,
   * allowing ETL to resume from where it left off after restarts. This is
   * critical for datasets that span decades of data.
   *
   * @param tracker Incremental tracker for resumability
   */
  public FileSchemaBuilder incrementalTracker(IncrementalTracker tracker) {
    this.incrementalTracker = tracker;
    etlBuilder.incrementalTracker(tracker);
    return this;
  }

  /**
   * Enable automatic ETL execution when {@link #getOperand()} or {@link #build} is called.
   *
   * <p>This ensures all hooks are configured before ETL runs. The ETL will execute
   * lazily on first call to getOperand() or build(), and only once.
   *
   * @param enabled true to enable auto-download, false to require explicit {@link #runEtl()}
   */
  public FileSchemaBuilder autoDownload(boolean enabled) {
    this.autoDownload = enabled;
    return this;
  }

  /**
   * Add a dimension resolver hook for ETL.
   * Called during ETL to dynamically resolve dimension values.
   */
  public FileSchemaBuilder resolveDimensions(String tableName,
      BiFunction<TableContext, Map<String, DimensionConfig>, Map<String, DimensionConfig>> resolver) {
    etlBuilder.resolveDimensions(tableName, resolver);
    return this;
  }

  /**
   * Add an isEnabled hook to conditionally enable/disable tables during ETL.
   *
   * <p>When the predicate returns false, the table is:
   * <ul>
   *   <li>Skipped during ETL (no source download or materialization)</li>
   *   <li>Excluded from the final schema metadata</li>
   * </ul>
   *
   * <p>This corresponds to the YAML `enabled` flag in hooks configuration.
   *
   * @param tableName Table name to apply the hook to
   * @param predicate Returns true if table should be enabled, false to disable
   */
  public FileSchemaBuilder isEnabled(String tableName, Predicate<TableContext> predicate) {
    etlBuilder.isEnabled(tableName, predicate);
    return this;
  }

  /**
   * @deprecated Use {@link #isEnabled(String, Predicate)} instead
   */
  @Deprecated
  public FileSchemaBuilder shouldProcess(String tableName, Predicate<TableContext> predicate) {
    return isEnabled(tableName, predicate);
  }

  /**
   * Add a before-source hook for ETL.
   */
  public FileSchemaBuilder beforeSource(String tableName, java.util.function.Consumer<TableContext> hook) {
    etlBuilder.beforeSource(tableName, hook);
    return this;
  }

  /**
   * Add a before-materialize hook for ETL.
   */
  public FileSchemaBuilder beforeMaterialize(String tableName, java.util.function.Consumer<TableContext> hook) {
    etlBuilder.beforeMaterialize(tableName, hook);
    return this;
  }

  /**
   * Run the ETL pipeline with configured hooks.
   *
   * <p>This method can be called explicitly, or ETL will run automatically
   * if {@link #autoDownload(boolean)} is enabled when {@link #getOperand()}
   * or {@link #build} is called.
   */
  public FileSchemaBuilder runEtl() {
    if (etlExecuted) {
      LOGGER.debug("ETL already executed, skipping");
      return this;
    }
    if (config == null) {
      throw new IllegalStateException("Schema config must be set before running ETL");
    }

    // Build SchemaConfig from the loaded config
    SchemaConfig schemaConfig = SchemaConfig.fromMap(config);
    etlBuilder.config(schemaConfig);

    try {
      SchemaResult result = etlBuilder.build().process();
      LOGGER.info("ETL complete: {} tables, {} rows in {}ms",
          result.getTotalTables(), result.getTotalRows(), result.getElapsedMs());

      // Check for failed tables and throw if any failed
      if (result.getFailedTables() > 0) {
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("ETL failed for ").append(result.getFailedTables()).append(" tables:");
        for (Map.Entry<String, org.apache.calcite.adapter.file.etl.EtlResult> entry
            : result.getTableResults().entrySet()) {
          if (entry.getValue().isFailed()) {
            errorMsg.append("\n  - ").append(entry.getKey()).append(": ")
                .append(entry.getValue().getFailureMessage());
          }
        }
        throw new RuntimeException(errorMsg.toString());
      }

      // Track tables that were skipped by isEnabled hook
      // These will be excluded from schema metadata
      // Also collect materialization info for later metadata update
      Map<String, Map<String, String>> materializationInfo = new HashMap<>();
      for (Map.Entry<String, org.apache.calcite.adapter.file.etl.EtlResult> entry
          : result.getTableResults().entrySet()) {
        org.apache.calcite.adapter.file.etl.EtlResult etlResult = entry.getValue();
        if (etlResult.isSkipped() && etlResult.getTableLocation() == null) {
          // Table was skipped by isEnabled hook (not a completed Iceberg table)
          excludedTables.add(entry.getKey());
          LOGGER.debug("Table '{}' excluded from schema (skipped during ETL)", entry.getKey());
        } else if (!etlResult.isFailed() && etlResult.getTableLocation() != null) {
          // Store materialization info for updating ConversionMetadata later
          // This includes both newly materialized tables AND skipped tables that are already complete
          Map<String, String> info = new HashMap<>();
          info.put("tableLocation", etlResult.getTableLocation());
          if (etlResult.getMaterializeFormat() != null) {
            String conversionType = etlResult.getMaterializeFormat() ==
                org.apache.calcite.adapter.file.etl.MaterializeConfig.Format.ICEBERG
                ? "ICEBERG_PARQUET" : "PARQUET";
            info.put("conversionType", conversionType);
          }
          // Include row count for COUNT(*) optimization
          if (etlResult.getTotalRows() > 0) {
            info.put("rowCount", String.valueOf(etlResult.getTotalRows()));
          }
          materializationInfo.put(entry.getKey(), info);
          LOGGER.debug("Table '{}' materialized: location={}, format={}, rows={}, skipped={}",
              entry.getKey(), etlResult.getTableLocation(), etlResult.getMaterializeFormat(),
              etlResult.getTotalRows(), etlResult.isSkipped());
        }
      }

      // Store materialization info in operand overrides for later use
      if (!materializationInfo.isEmpty()) {
        operandOverrides.put("_materializationInfo", materializationInfo);
        LOGGER.info("Stored materialization info for {} tables", materializationInfo.size());
      }
    } catch (Exception e) {
      LOGGER.error("ETL failed", e);
      throw new RuntimeException("ETL pipeline failed", e);
    }

    etlExecuted = true;
    return this;
  }

  /**
   * Run ETL if autoDownload is enabled and ETL hasn't been executed yet.
   */
  private void runEtlIfNeeded() {
    if (autoDownload && !etlExecuted) {
      runEtl();
    }
  }

  /**
   * Build the FileSchema.
   *
   * @param parentSchema Parent schema to register with
   * @param name Schema name
   * @return Created schema
   */
  public Schema build(SchemaPlus parentSchema, String name) {
    if (config == null) {
      throw new IllegalStateException("Schema config must be set before building");
    }

    // Inject schema name into config for ETL processing
    // This ensures SchemaConfig.fromMap() has the required name field
    if (!config.containsKey("name") && !config.containsKey("schemaName")) {
      config.put("name", name);
    }

    // Run ETL if autoDownload is enabled (ensures all hooks are configured first)
    runEtlIfNeeded();

    // Set schema name for variable substitution
    System.setProperty("SCHEMA_NAME", name);

    // Merge config with overrides
    Map<String, Object> operand = new HashMap<>(config);
    operand.putAll(operandOverrides);

    // Schema's materializeDirectory takes precedence over parent's directory
    // The YAML schema's materializeDirectory is more specific (e.g., bucket/source=econ)
    // than the parent operand's directory (e.g., just bucket root)
    // Note: ${ENV_VAR} patterns are already resolved at load time via resolveAllEnvVars()
    if (config.containsKey("materializeDirectory")) {
      operand.put("directory", config.get("materializeDirectory"));
    }

    // Add storage providers so they can be passed to any nested builders
    if (storageProvider != null) {
      operand.put("_storageProvider", storageProvider);
    }
    if (cacheStorageProvider != null) {
      operand.put("_cacheStorageProvider", cacheStorageProvider);
    }
    // Add incremental tracker for cache database sharing across schemas
    if (incrementalTracker != null) {
      operand.put("_incrementalTracker", incrementalTracker);
    }

    // Create schema via FileSchemaFactory
    return FileSchemaFactory.INSTANCE.create(parentSchema, name, operand);
  }

  /**
   * Get the merged operand (config + overrides) without building the schema.
   * Useful for GovData factories that need to return operand to parent.
   *
   * <p>Excluded tables (from isEnabled hook) are marked for filtering by FileSchema.
   */
  public Map<String, Object> getOperand() {
    if (config == null) {
      throw new IllegalStateException("Schema config must be set");
    }

    // Run ETL if autoDownload is enabled (ensures all hooks are configured first)
    runEtlIfNeeded();

    Map<String, Object> operand = new HashMap<>(config);
    operand.putAll(operandOverrides);

    // Schema's materializeDirectory takes precedence over parent's directory
    if (config.containsKey("materializeDirectory")) {
      operand.put("directory", config.get("materializeDirectory"));
    }

    // Add excluded tables (from isEnabled hook) so schema can filter them out
    if (!excludedTables.isEmpty()) {
      operand.put("_excludedTables", new java.util.ArrayList<>(excludedTables));
      LOGGER.debug("Passing {} excluded tables to schema: {}", excludedTables.size(), excludedTables);
    }

    // Add storage provider instances if set (allows sharing across schemas)
    if (storageProvider != null) {
      operand.put("_storageProvider", storageProvider);
    }
    if (cacheStorageProvider != null) {
      operand.put("_cacheStorageProvider", cacheStorageProvider);
    }
    // Add incremental tracker for cache database sharing across schemas
    if (incrementalTracker != null) {
      operand.put("_incrementalTracker", incrementalTracker);
    }

    return operand;
  }

  /**
   * Explicitly exclude a table from the schema.
   * Use this to programmatically disable tables without running ETL.
   */
  public FileSchemaBuilder excludeTable(String tableName) {
    excludedTables.add(tableName);
    return this;
  }

  /**
   * Explicitly exclude multiple tables from the schema.
   */
  public FileSchemaBuilder excludeTables(java.util.Collection<String> tableNames) {
    excludedTables.addAll(tableNames);
    return this;
  }
}
