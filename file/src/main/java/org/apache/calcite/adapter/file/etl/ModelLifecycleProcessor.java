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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Orchestrates processing of a model containing multiple schemas.
 *
 * <p>The ModelLifecycleProcessor handles the top-level lifecycle:
 * <ul>
 *   <li>ModelLifecycle: beforeModel → process schemas → afterModel</li>
 *   <li>SchemaLifecycle: beforeSchema → process tables → afterSchema (per schema)</li>
 *   <li>TableLifecycle: beforeTable → process → afterTable (per table)</li>
 * </ul>
 *
 * <p>Schemas are processed in dependency order - schemas with no dependencies
 * are processed first, then schemas that depend on them.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Directories can be defined in each schema's YAML config:
 * // schema:
 * //   name: econ
 * //   sourceDirectory: "s3://bucket/raw/"
 * //   materializeDirectory: "s3://bucket/parquet/"
 *
 * ModelConfig model = ModelConfig.builder()
 *     .name("govdata_econ")
 *     .schema("econ_reference", "/econ/econ-reference-schema.yaml")
 *     .schema("econ", "/econ/econ-schema.yaml", "econ_reference")
 *     .build();
 *
 * ModelResult result = ModelLifecycleProcessor.builder()
 *     .model(model)
 *     .schemaConfigLoader(resourcePath -> loadYamlConfig(resourcePath))
 *     // Table-level hooks: schema.table → hook
 *     .isEnabled("econ.employment_statistics", ctx -> hasApiKey("BLS_API_KEY"))
 *     .isEnabled("econ_reference.fred_series", ctx -> hasApiKey("FRED_API_KEY"))
 *     .build()
 *     .process();
 * }</pre>
 */
public class ModelLifecycleProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelLifecycleProcessor.class);

  private final ModelConfig model;
  private final StorageProvider storageProvider;
  private final String sourceDirectory;
  private final String materializeDirectory;
  private final IncrementalTracker incrementalTracker;
  private final Function<String, SchemaConfig> schemaConfigLoader;

  // Model-level hooks
  private final Consumer<ModelConfig> beforeModelHook;
  private final BiConsumer<ModelConfig, ModelResult> afterModelHook;

  // Schema-level hooks (keyed by schema name)
  private final Map<String, Consumer<ModelConfig.SchemaRef>> beforeSchemaHooks;
  private final Map<String, BiConsumer<ModelConfig.SchemaRef, SchemaResult>> afterSchemaHooks;

  // Table-level hooks (keyed by "schema.table")
  private final Map<String, Consumer<TableContext>> beforeTableHooks;
  private final Map<String, BiConsumer<TableContext, EtlResult>> afterTableHooks;
  private final Map<String, BiFunction<TableContext, Exception, Boolean>> errorHooks;
  private final Map<String, BiFunction<TableContext, Map<String, DimensionConfig>,
      Map<String, DimensionConfig>>> dimensionHooks;
  private final Map<String, Predicate<TableContext>> filterHooks;

  // Source phase hooks (keyed by "schema.table")
  private final Map<String, Consumer<TableContext>> beforeSourceHooks;
  private final Map<String, BiConsumer<TableContext, SourceResult>> afterSourceHooks;
  private final Map<String, BiFunction<TableContext, Exception, Boolean>> sourceErrorHooks;

  // Materialize phase hooks (keyed by "schema.table")
  private final Map<String, Consumer<TableContext>> beforeMaterializeHooks;
  private final Map<String, BiConsumer<TableContext, MaterializeResult>> afterMaterializeHooks;
  private final Map<String, BiFunction<TableContext, Exception, Boolean>> materializeErrorHooks;

  // Data provider/writer hooks (keyed by "schema.table")
  private final Map<String, BiFunction<TableContext, Map<String, String>,
      java.util.Iterator<Map<String, Object>>>> fetchDataHooks;
  private final Map<String, Builder.WriteDataFunction> writeDataHooks;

  private ModelLifecycleProcessor(Builder builder) {
    this.model = builder.model;
    this.storageProvider = builder.storageProvider;
    this.sourceDirectory = builder.sourceDirectory;
    this.materializeDirectory = builder.materializeDirectory;
    this.incrementalTracker = builder.incrementalTracker;
    this.schemaConfigLoader = builder.schemaConfigLoader;
    this.beforeModelHook = builder.beforeModelHook;
    this.afterModelHook = builder.afterModelHook;
    this.beforeSchemaHooks = new HashMap<>(builder.beforeSchemaHooks);
    this.afterSchemaHooks = new HashMap<>(builder.afterSchemaHooks);
    this.beforeTableHooks = new HashMap<>(builder.beforeTableHooks);
    this.afterTableHooks = new HashMap<>(builder.afterTableHooks);
    this.errorHooks = new HashMap<>(builder.errorHooks);
    this.dimensionHooks = new HashMap<>(builder.dimensionHooks);
    this.filterHooks = new HashMap<>(builder.filterHooks);
    this.beforeSourceHooks = new HashMap<>(builder.beforeSourceHooks);
    this.afterSourceHooks = new HashMap<>(builder.afterSourceHooks);
    this.sourceErrorHooks = new HashMap<>(builder.sourceErrorHooks);
    this.beforeMaterializeHooks = new HashMap<>(builder.beforeMaterializeHooks);
    this.afterMaterializeHooks = new HashMap<>(builder.afterMaterializeHooks);
    this.materializeErrorHooks = new HashMap<>(builder.materializeErrorHooks);
    this.fetchDataHooks = new HashMap<>(builder.fetchDataHooks);
    this.writeDataHooks = new HashMap<>(builder.writeDataHooks);
  }

  /**
   * Process all schemas in the model.
   *
   * @return ModelResult with aggregated statistics
   */
  public ModelResult process() {
    long startTime = System.currentTimeMillis();
    LOGGER.info("=== Starting model processing: {} ===", model.getName());
    LOGGER.info("Schemas to process: {}", model.getSchemas().size());

    // Call beforeModel hook
    if (beforeModelHook != null) {
      beforeModelHook.accept(model);
    }

    // Sort schemas by dependency order
    List<ModelConfig.SchemaRef> orderedSchemas = topologicalSort(model.getSchemas());

    List<SchemaResult> schemaResults = new ArrayList<>();
    int successfulSchemas = 0;
    int failedSchemas = 0;

    for (ModelConfig.SchemaRef schemaRef : orderedSchemas) {
      try {
        LOGGER.info("--- Processing schema: {} ---", schemaRef.getName());

        SchemaResult result = processSchema(schemaRef);
        schemaResults.add(result);

        if (result.getFailedTables() == 0) {
          successfulSchemas++;
        } else {
          failedSchemas++;
        }

        LOGGER.info("--- Schema {} complete: {} tables, {} rows ---",
            schemaRef.getName(), result.getTotalTables(), result.getTotalRows());

      } catch (Exception e) {
        LOGGER.error("Schema {} failed: {}", schemaRef.getName(), e.getMessage(), e);
        failedSchemas++;
        // Create error result
        schemaResults.add(new SchemaResult(schemaRef.getName(), 0, 0, 0, 0,
            System.currentTimeMillis() - startTime, e.getMessage()));
      }
    }

    long elapsed = System.currentTimeMillis() - startTime;
    ModelResult result = new ModelResult(model.getName(), schemaResults,
        successfulSchemas, failedSchemas, elapsed);

    // Call afterModel hook
    if (afterModelHook != null) {
      afterModelHook.accept(model, result);
    }

    LOGGER.info("=== Model processing complete: {} schemas ({} successful, {} failed) in {}ms ===",
        model.getSchemas().size(), successfulSchemas, failedSchemas, elapsed);

    return result;
  }

  /**
   * Process a single schema.
   */
  private SchemaResult processSchema(ModelConfig.SchemaRef schemaRef) throws Exception {
    String schemaName = schemaRef.getName();

    // Call beforeSchema hook
    Consumer<ModelConfig.SchemaRef> beforeHook = beforeSchemaHooks.get(schemaName);
    if (beforeHook != null) {
      beforeHook.accept(schemaRef);
    }

    // Load schema config
    SchemaConfig schemaConfig = schemaConfigLoader.apply(schemaRef.getResourcePath());
    if (schemaConfig == null) {
      throw new IllegalStateException("Failed to load schema config: " + schemaRef.getResourcePath());
    }

    // Build SchemaLifecycleProcessor with table hooks for this schema
    // Model-level directories are used as defaults; schema config can override
    SchemaLifecycleProcessor.Builder processorBuilder = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .incrementalTracker(incrementalTracker);

    // Only pass model-level directory if schema config doesn't have one
    if (schemaConfig.getSourceDirectory() == null && sourceDirectory != null) {
      processorBuilder.sourceDirectory(sourceDirectory);
    }
    if (schemaConfig.getMaterializeDirectory() == null && materializeDirectory != null) {
      processorBuilder.materializeDirectory(materializeDirectory);
    }

    // Register table-level hooks that match this schema
    String schemaPrefix = schemaName + ".";
    for (Map.Entry<String, Consumer<TableContext>> entry : beforeTableHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.beforeTable(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, BiConsumer<TableContext, EtlResult>> entry : afterTableHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.afterTable(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, BiFunction<TableContext, Exception, Boolean>> entry : errorHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.onTableError(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, BiFunction<TableContext, Map<String, DimensionConfig>,
        Map<String, DimensionConfig>>> entry : dimensionHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.resolveDimensions(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, Predicate<TableContext>> entry : filterHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.isEnabled(tableName, entry.getValue());
      }
    }
    // Register source phase hooks
    for (Map.Entry<String, Consumer<TableContext>> entry : beforeSourceHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.beforeSource(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, BiConsumer<TableContext, SourceResult>> entry : afterSourceHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.afterSource(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, BiFunction<TableContext, Exception, Boolean>> entry : sourceErrorHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.onSourceError(tableName, entry.getValue());
      }
    }
    // Register materialize phase hooks
    for (Map.Entry<String, Consumer<TableContext>> entry : beforeMaterializeHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.beforeMaterialize(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, BiConsumer<TableContext, MaterializeResult>> entry : afterMaterializeHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.afterMaterialize(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, BiFunction<TableContext, Exception, Boolean>> entry : materializeErrorHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.onMaterializeError(tableName, entry.getValue());
      }
    }
    // Register data provider/writer hooks
    for (Map.Entry<String, BiFunction<TableContext, Map<String, String>,
        java.util.Iterator<Map<String, Object>>>> entry : fetchDataHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.fetchData(tableName, entry.getValue());
      }
    }
    for (Map.Entry<String, Builder.WriteDataFunction> entry : writeDataHooks.entrySet()) {
      if (entry.getKey().startsWith(schemaPrefix)) {
        String tableName = entry.getKey().substring(schemaPrefix.length());
        processorBuilder.writeData(tableName, (ctx, data, vars) -> entry.getValue().write(ctx, data, vars));
      }
    }

    SchemaLifecycleProcessor processor = processorBuilder.build();
    SchemaResult result = processor.process();

    // Call afterSchema hook
    BiConsumer<ModelConfig.SchemaRef, SchemaResult> afterHook = afterSchemaHooks.get(schemaName);
    if (afterHook != null) {
      afterHook.accept(schemaRef, result);
    }

    return result;
  }

  /**
   * Topologically sort schemas by dependencies.
   * Schemas with no dependencies come first.
   */
  private List<ModelConfig.SchemaRef> topologicalSort(List<ModelConfig.SchemaRef> schemas) {
    // Build dependency graph
    Map<String, ModelConfig.SchemaRef> schemaMap = new HashMap<>();
    Map<String, Set<String>> dependencies = new HashMap<>();

    for (ModelConfig.SchemaRef schema : schemas) {
      schemaMap.put(schema.getName(), schema);
      dependencies.put(schema.getName(), new HashSet<>(schema.getDependsOn()));
    }

    List<ModelConfig.SchemaRef> sorted = new ArrayList<>();
    Set<String> processed = new HashSet<>();

    while (sorted.size() < schemas.size()) {
      boolean progress = false;

      for (ModelConfig.SchemaRef schema : schemas) {
        if (processed.contains(schema.getName())) {
          continue;
        }

        // Check if all dependencies are satisfied
        Set<String> deps = dependencies.get(schema.getName());
        if (processed.containsAll(deps)) {
          sorted.add(schema);
          processed.add(schema.getName());
          progress = true;
        }
      }

      if (!progress) {
        throw new IllegalStateException("Circular dependency detected in schemas: "
            + schemas.stream()
                .filter(s -> !processed.contains(s.getName()))
                .map(ModelConfig.SchemaRef::getName)
                .reduce((a, b) -> a + ", " + b)
                .orElse(""));
      }
    }

    return sorted;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for ModelLifecycleProcessor.
   */
  public static class Builder {
    private ModelConfig model;
    private StorageProvider storageProvider;
    private String sourceDirectory;
    private String materializeDirectory;
    private IncrementalTracker incrementalTracker = IncrementalTracker.NOOP;
    private Function<String, SchemaConfig> schemaConfigLoader;

    // Model-level hooks
    private Consumer<ModelConfig> beforeModelHook;
    private BiConsumer<ModelConfig, ModelResult> afterModelHook;

    // Schema-level hooks
    private final Map<String, Consumer<ModelConfig.SchemaRef>> beforeSchemaHooks = new HashMap<>();
    private final Map<String, BiConsumer<ModelConfig.SchemaRef, SchemaResult>> afterSchemaHooks = new HashMap<>();

    // Table-level hooks (keyed by "schema.table")
    private final Map<String, Consumer<TableContext>> beforeTableHooks = new HashMap<>();
    private final Map<String, BiConsumer<TableContext, EtlResult>> afterTableHooks = new HashMap<>();
    private final Map<String, BiFunction<TableContext, Exception, Boolean>> errorHooks = new HashMap<>();
    private final Map<String, BiFunction<TableContext, Map<String, DimensionConfig>,
        Map<String, DimensionConfig>>> dimensionHooks = new HashMap<>();
    private final Map<String, Predicate<TableContext>> filterHooks = new HashMap<>();

    // Source phase hooks (keyed by "schema.table")
    private final Map<String, Consumer<TableContext>> beforeSourceHooks = new HashMap<>();
    private final Map<String, BiConsumer<TableContext, SourceResult>> afterSourceHooks = new HashMap<>();
    private final Map<String, BiFunction<TableContext, Exception, Boolean>> sourceErrorHooks = new HashMap<>();

    // Materialize phase hooks (keyed by "schema.table")
    private final Map<String, Consumer<TableContext>> beforeMaterializeHooks = new HashMap<>();
    private final Map<String, BiConsumer<TableContext, MaterializeResult>> afterMaterializeHooks = new HashMap<>();
    private final Map<String, BiFunction<TableContext, Exception, Boolean>> materializeErrorHooks = new HashMap<>();

    // Data provider/writer hooks (keyed by "schema.table")
    private final Map<String, BiFunction<TableContext, Map<String, String>,
        java.util.Iterator<Map<String, Object>>>> fetchDataHooks = new HashMap<>();
    private final Map<String, WriteDataFunction> writeDataHooks = new HashMap<>();

    /** Functional interface for writeData hook. */
    @FunctionalInterface
    public interface WriteDataFunction {
      long write(TableContext context, java.util.Iterator<Map<String, Object>> data,
          Map<String, String> variables);
    }

    public Builder model(ModelConfig model) {
      this.model = model;
      return this;
    }

    /**
     * Sets the storage provider directly (for advanced use cases).
     * Usually not needed - the processor auto-creates from materializeDirectory.
     */
    public Builder storageProvider(StorageProvider storageProvider) {
      this.storageProvider = storageProvider;
      return this;
    }

    /**
     * Sets the default source directory for all schemas.
     *
     * <p>Individual schemas can override with their own sourceDirectory in config.
     *
     * @param directory The source directory URL or path
     * @return this builder
     */
    public Builder sourceDirectory(String directory) {
      this.sourceDirectory = directory;
      return this;
    }

    /**
     * Sets the default materialize directory for all schemas.
     *
     * <p>Individual schemas can override with their own materializeDirectory in config.
     *
     * <p>Supports various URL schemes that auto-create the appropriate StorageProvider:
     * <ul>
     *   <li>{@code s3://bucket/path} - S3 storage</li>
     *   <li>{@code /local/path} or {@code file:///path} - Local filesystem</li>
     *   <li>{@code hdfs://host/path} - HDFS storage</li>
     * </ul>
     *
     * @param directory The output directory URL or path
     * @return this builder
     */
    public Builder materializeDirectory(String directory) {
      this.materializeDirectory = directory;
      return this;
    }

    /**
     * @deprecated Use {@link #materializeDirectory(String)} instead
     */
    @Deprecated
    public Builder baseDirectory(String baseDirectory) {
      this.materializeDirectory = baseDirectory;
      return this;
    }

    public Builder incrementalTracker(IncrementalTracker tracker) {
      this.incrementalTracker = tracker;
      return this;
    }

    public Builder schemaConfigLoader(Function<String, SchemaConfig> loader) {
      this.schemaConfigLoader = loader;
      return this;
    }

    // Model-level hooks
    public Builder beforeModel(Consumer<ModelConfig> hook) {
      this.beforeModelHook = hook;
      return this;
    }

    public Builder afterModel(BiConsumer<ModelConfig, ModelResult> hook) {
      this.afterModelHook = hook;
      return this;
    }

    // Schema-level hooks
    public Builder beforeSchema(String schemaName, Consumer<ModelConfig.SchemaRef> hook) {
      this.beforeSchemaHooks.put(schemaName, hook);
      return this;
    }

    public Builder afterSchema(String schemaName, BiConsumer<ModelConfig.SchemaRef, SchemaResult> hook) {
      this.afterSchemaHooks.put(schemaName, hook);
      return this;
    }

    // Table-level hooks (use "schema.table" format)
    public Builder beforeTable(String schemaTable, Consumer<TableContext> hook) {
      this.beforeTableHooks.put(schemaTable, hook);
      return this;
    }

    public Builder afterTable(String schemaTable, BiConsumer<TableContext, EtlResult> hook) {
      this.afterTableHooks.put(schemaTable, hook);
      return this;
    }

    public Builder onTableError(String schemaTable, BiFunction<TableContext, Exception, Boolean> hook) {
      this.errorHooks.put(schemaTable, hook);
      return this;
    }

    public Builder resolveDimensions(String schemaTable,
        BiFunction<TableContext, Map<String, DimensionConfig>, Map<String, DimensionConfig>> hook) {
      this.dimensionHooks.put(schemaTable, hook);
      return this;
    }

    public Builder isEnabled(String schemaTable, Predicate<TableContext> hook) {
      this.filterHooks.put(schemaTable, hook);
      return this;
    }

    /**
     * @deprecated Use {@link #isEnabled(String, Predicate)} instead
     */
    @Deprecated
    public Builder shouldProcess(String schemaTable, Predicate<TableContext> hook) {
      return isEnabled(schemaTable, hook);
    }

    // Source phase hooks (use "schema.table" format)
    public Builder beforeSource(String schemaTable, Consumer<TableContext> hook) {
      this.beforeSourceHooks.put(schemaTable, hook);
      return this;
    }

    public Builder afterSource(String schemaTable, BiConsumer<TableContext, SourceResult> hook) {
      this.afterSourceHooks.put(schemaTable, hook);
      return this;
    }

    public Builder onSourceError(String schemaTable, BiFunction<TableContext, Exception, Boolean> hook) {
      this.sourceErrorHooks.put(schemaTable, hook);
      return this;
    }

    // Materialize phase hooks (use "schema.table" format)
    public Builder beforeMaterialize(String schemaTable, Consumer<TableContext> hook) {
      this.beforeMaterializeHooks.put(schemaTable, hook);
      return this;
    }

    public Builder afterMaterialize(String schemaTable, BiConsumer<TableContext, MaterializeResult> hook) {
      this.afterMaterializeHooks.put(schemaTable, hook);
      return this;
    }

    public Builder onMaterializeError(String schemaTable, BiFunction<TableContext, Exception, Boolean> hook) {
      this.materializeErrorHooks.put(schemaTable, hook);
      return this;
    }

    // Data provider/writer hooks (use "schema.table" format)

    /**
     * Registers a custom data fetcher for a specific table.
     * If the hook returns non-null, the built-in HttpSource is skipped.
     */
    public Builder fetchData(String schemaTable,
        BiFunction<TableContext, Map<String, String>, java.util.Iterator<Map<String, Object>>> hook) {
      this.fetchDataHooks.put(schemaTable, hook);
      return this;
    }

    /**
     * Registers a custom data writer for a specific table.
     * If the hook returns >= 0, the built-in MaterializationWriter is skipped.
     */
    public Builder writeData(String schemaTable, WriteDataFunction hook) {
      this.writeDataHooks.put(schemaTable, hook);
      return this;
    }

    public ModelLifecycleProcessor build() {
      if (model == null) {
        throw new IllegalStateException("Model is required");
      }
      // materializeDirectory is optional at model level if schemas provide their own
      // Auto-create storage provider from directory URL if both are set
      if (storageProvider == null && materializeDirectory != null) {
        storageProvider = StorageProviderFactory.createFromUrl(materializeDirectory);
      }
      // Use default YAML loader if not provided
      if (schemaConfigLoader == null) {
        schemaConfigLoader = ModelLifecycleProcessor::loadSchemaConfigFromYaml;
      }
      return new ModelLifecycleProcessor(this);
    }
  }

  /**
   * Default schema config loader that loads from classpath YAML resources.
   *
   * <p>Understands the file adapter's standard YAML format:
   * <ul>
   *   <li>{@code schemaName} - maps to SchemaConfig.name</li>
   *   <li>{@code partitionedTables} - maps to SchemaConfig.tables</li>
   *   <li>{@code materializeDirectory} - with ${ENV_VAR} expansion</li>
   *   <li>{@code sourceDirectory} - with ${ENV_VAR} expansion</li>
   * </ul>
   *
   * @param resourcePath Classpath resource path (e.g., "/econ/econ-schema.yaml")
   * @return SchemaConfig or null if resource not found
   */
  public static SchemaConfig loadSchemaConfigFromYaml(String resourcePath) {
    try (java.io.InputStream is =
             ModelLifecycleProcessor.class.getResourceAsStream(resourcePath)) {
      if (is == null) {
        LOGGER.warn("Could not find schema resource: {}", resourcePath);
        return null;
      }
      // Configure LoaderOptions with higher alias limit for complex YAML schemas
      // (e.g., *county_fips_column repeated in 30+ census tables)
      org.yaml.snakeyaml.LoaderOptions loaderOptions = new org.yaml.snakeyaml.LoaderOptions();
      loaderOptions.setMaxAliasesForCollections(500);
      org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml(loaderOptions);
      Map<String, Object> schemaMap = yaml.load(is);

      // Convert file adapter YAML format to SchemaConfig format
      Map<String, Object> configMap = new java.util.HashMap<>();
      configMap.put("name", schemaMap.get("schemaName"));
      configMap.put("tables", schemaMap.get("partitionedTables"));
      configMap.put("metadata", schemaMap);

      // Copy directory settings with env var expansion
      Object sourceDir = schemaMap.get("sourceDirectory");
      if (sourceDir instanceof String) {
        configMap.put("sourceDirectory",
            org.apache.calcite.util.EnvironmentVariableSubstitutor.substitute((String) sourceDir));
      }
      Object materializeDir = schemaMap.get("materializeDirectory");
      if (materializeDir instanceof String) {
        configMap.put("materializeDirectory",
            org.apache.calcite.util.EnvironmentVariableSubstitutor.substitute((String) materializeDir));
      }

      return SchemaConfig.fromMap(configMap);
    } catch (Exception e) {
      LOGGER.error("Error loading schema config from {}", resourcePath, e);
      return null;
    }
  }
}
