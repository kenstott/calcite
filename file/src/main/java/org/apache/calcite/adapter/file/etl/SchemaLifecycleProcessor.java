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
// storage-provider-guard:allow-scheme - storage-dispatch layer: inspecting a URI scheme here is the legitimate job (provider dispatch / S3 path handling / endpoint SSL config), not a consumer branching local-vs-remote.
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.etl.cache.BundleArchiver;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Orchestrates schema processing with lifecycle hooks.
 *
 * <p>SchemaLifecycleProcessor is the main entry point for processing a schema
 * configuration. It manages the complete lifecycle:
 *
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │                    Schema Lifecycle                          │
 * ├─────────────────────────────────────────────────────────────┤
 * │  1. LOAD           → Load schema config                      │
 * │  2. beforeSchema() → Schema pre-processing hook              │
 * │       │                                                      │
 * │       ├──► FOR EACH TABLE:                                   │
 * │       │    beforeTable()  → Table pre-processing hook        │
 * │       │    RESOLVE        → Resolve dimensions               │
 * │       │    FILTER         → Skip already-processed batches   │
 * │       │    FETCH          → Fetch data from source           │
 * │       │    MATERIALIZE    → Write to output format           │
 * │       │    afterTable()   → Table post-processing hook       │
 * │       │                                                      │
 * │  3. afterSchema()  → Schema post-processing hook             │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * // Directories are defined in the config YAML:
 * // schema:
 * //   name: econ
 * //   sourceDirectory: "s3://bucket/raw/"
 * //   materializeDirectory: "s3://bucket/parquet/"
 * //   tables: [...]
 *
 * SchemaConfig config = SchemaConfig.fromMap(yamlMap);
 *
 * SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
 *     .config(config)
 *     .incrementalTracker(tracker)
 *     .build();
 *
 * SchemaResult result = processor.process();
 * }</pre>
 *
 * @see SchemaConfig
 * @see SchemaLifecycleListener
 * @see TableLifecycleListener
 */
public class SchemaLifecycleProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLifecycleProcessor.class);

  private final SchemaConfig config;
  private final StorageProvider storageProvider;
  private final StorageProvider sourceStorageProvider;  // For raw/source data
  private final String sourceDirectory;
  private final String materializeDirectory;
  private final String operatingDirectory;
  private final IncrementalTracker incrementalTracker;
  private final SchemaLifecycleListener schemaListener;
  private final TableLifecycleListener defaultTableListener;

  private SchemaLifecycleProcessor(Builder builder) {
    this.config = builder.config;
    this.storageProvider = builder.storageProvider;
    // Default to main storageProvider if sourceStorageProvider not set
    this.sourceStorageProvider = builder.sourceStorageProvider != null
        ? builder.sourceStorageProvider : builder.storageProvider;
    // Use builder override or fall back to config
    this.sourceDirectory = builder.sourceDirectory != null
        ? builder.sourceDirectory : config.getSourceDirectory();
    this.materializeDirectory = builder.materializeDirectory != null
        ? builder.materializeDirectory : config.getMaterializeDirectory();
    this.operatingDirectory = builder.operatingDirectory;
    this.incrementalTracker = builder.incrementalTracker != null
        ? builder.incrementalTracker : IncrementalTracker.NOOP;
    this.schemaListener = builder.schemaListener != null
        ? builder.schemaListener : loadSchemaListener(config);
    this.defaultTableListener = builder.defaultTableListener != null
        ? builder.defaultTableListener : loadDefaultTableListener(config);
  }

  /**
   * Processes the schema configuration.
   *
   * <p>Executes the full lifecycle: beforeSchema, process each table, afterSchema.
   *
   * @return Aggregated results from all tables
   * @throws IOException If processing fails fatally
   */
  public SchemaResult process() throws IOException {
    String schemaName = config.getName();
    LOGGER.info("Starting schema lifecycle processing: {}", schemaName);
    long startTime = System.currentTimeMillis();

    SchemaResult.Builder resultBuilder = SchemaResult.builder().schemaName(schemaName);

    // Create schema context
    SchemaContext schemaContext = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .sourceStorageProvider(sourceStorageProvider)
        .sourceDirectory(sourceDirectory)
        .materializeDirectory(materializeDirectory)
        .operatingDirectory(operatingDirectory)
        .incrementalTracker(incrementalTracker)
        .build();

    try {
      // Phase 1: Schema pre-processing
      LOGGER.info("Phase 1: Schema pre-processing");
      schemaListener.beforeSchema(schemaContext);

      // Phase 2: Bulk downloads (download once, use many)
      processBulkDownloads(schemaContext);

      // Phase 2b: Preload table completion markers (single S3 query)
      incrementalTracker.preloadAllCompletions();

      // Phase 3: Process each table
      List<EtlPipelineConfig> tables = config.getTables();
      int totalTables = tables.size();
      LOGGER.info("Phase 3: Processing {} tables", totalTables);

      for (int i = 0; i < totalTables; i++) {
        EtlPipelineConfig tableConfig = tables.get(i);
        String tableName = tableConfig.getName();

        // Create table context
        TableContext tableContext = TableContext.builder()
            .tableConfig(tableConfig)
            .schemaContext(schemaContext)
            .tableIndex(i)
            .totalTables(totalTables)
            .build();

        // Get table-specific listener or use default
        TableLifecycleListener tableListener = loadTableListener(tableConfig, defaultTableListener);

        try {
          // Check if table is enabled (both YAML flag and callback hook)
          // Use table-level enabled flag (supports env var interpolation like ${VAR:default})
          boolean yamlEnabled = tableConfig.isEnabled();
          boolean hookEnabled = tableListener.isTableEnabled(tableContext);

          if (!yamlEnabled || !hookEnabled) {
            String reason = !yamlEnabled ? "YAML enabled=false" : "isEnabled hook returned false";
            LOGGER.info("Skipping table {}/{}: {} ({})",
                i + 1, totalTables, tableName, reason);
            EtlResult skippedResult = EtlResult.skipped(tableName, 0);
            resultBuilder.addTableResult(tableName, skippedResult);
            continue;
          }

          // Table pre-processing
          LOGGER.info("Processing table {}/{}: {} (source: {})",
              i + 1, totalTables, tableName, tableContext.detectSource());
          tableListener.beforeTable(tableContext);

          // Resolve dimensions (callback) - allows dynamic dimension building
          Map<String, DimensionConfig> resolvedDimensions =
              tableListener.resolveDimensions(tableContext, tableConfig.getDimensions());

          // Check if table has source config - tables without source only need hook processing
          boolean hasSource = tableConfig.getSource() != null
              || tableConfig.getRawSourceConfig() != null;

          long sourceStart = System.currentTimeMillis();
          EtlResult tableResult;

          if (!hasSource) {
            // No source config - skip ETL pipeline, run hooks only (e.g., for post-materialization)
            LOGGER.info("Table '{}' has no source - running hooks only", tableName);
            tableResult = EtlResult.builder()
                .pipelineName(tableName)
                .failed(false)
                .elapsedMs(0)
                .build();
          } else {
            // Source phase hooks - called before ETL pipeline starts
            tableListener.beforeSource(tableContext);

            // Execute the ETL pipeline for this table (source + materialize are interleaved)
            try {
              // Materialize phase hooks - called when pipeline starts writing
              tableListener.beforeMaterialize(tableContext);
              tableResult = processTable(tableContext, tableListener, resolvedDimensions);
            } catch (Exception sourceEx) {
              // Source or materialize error
              boolean continueAfterSourceError = tableListener.onSourceError(tableContext, sourceEx);
              boolean continueAfterMaterializeError = tableListener.onMaterializeError(tableContext, sourceEx);
              if (!continueAfterSourceError || !continueAfterMaterializeError) {
                throw sourceEx;
              }
              // Create failed result
              tableResult = EtlResult.builder()
                  .pipelineName(tableName)
                  .failed(true)
                  .failureMessage(sourceEx.getMessage())
                  .elapsedMs(System.currentTimeMillis() - sourceStart)
                  .build();
            }
          }
          long sourceDuration = System.currentTimeMillis() - sourceStart;

          // Source phase complete - create result with stats from EtlResult
          SourceResult sourceResult = tableResult.isFailed()
              ? SourceResult.error(tableResult.getFailureMessage(), sourceDuration, null)
              : SourceResult.success(tableResult.getTotalRows(), 0, sourceDuration, null);
          if (hasSource) {
            tableListener.afterSource(tableContext, sourceResult);
          }

          // Materialize phase complete - create result with stats from EtlResult
          MaterializeResult materializeResult = tableResult.isFailed()
              ? MaterializeResult.error(tableResult.getFailureMessage(), sourceDuration)
              : MaterializeResult.success(tableResult.getTotalRows(), -1, sourceDuration);
          if (hasSource) {
            tableListener.afterMaterialize(tableContext, materializeResult);
          }

          // Table post-processing - always run (handles bulkGenerator, etc.)
          tableListener.afterTable(tableContext, tableResult);

          // Execute table-level postProcess hooks if configured
          HooksConfig hooks = tableConfig.getHooks();
          if (hooks != null && !hooks.getPostProcess().isEmpty()) {
            executeTablePostProcess(tableContext, hooks.getPostProcess(), tableResult);
          }

          resultBuilder.addTableResult(tableName, tableResult);
          LOGGER.info("Table '{}' complete: {}", tableName, tableResult);

          // Force GC between tables to prevent memory accumulation from large geometry data
          // This is especially important for TIGER shapefiles with WKT strings
          Runtime runtime = Runtime.getRuntime();
          long beforeGc = runtime.totalMemory() - runtime.freeMemory();
          System.gc();
          long afterGc = runtime.totalMemory() - runtime.freeMemory();
          long freedMb = (beforeGc - afterGc) / (1024 * 1024);
          if (freedMb > 50) {
            LOGGER.info("Memory cleanup after '{}': freed {}MB ({}MB -> {}MB used)",
                tableName, freedMb, beforeGc / (1024 * 1024), afterGc / (1024 * 1024));
          }

        } catch (Exception e) {
          LOGGER.error("Table '{}' failed: {}", tableName, e.getMessage(), e);

          // Let table listener decide whether to continue
          boolean continueProcessing = tableListener.onTableError(tableContext, e);

          // Record failure
          EtlResult failedResult = EtlResult.builder()
              .pipelineName(tableName)
              .failed(true)
              .failureMessage(e.getMessage())
              .build();
          resultBuilder.addTableResult(tableName, failedResult);

          if (!continueProcessing) {
            throw new IOException("Table processing aborted by listener: " + tableName, e);
          }
        }
      }

      // Phase 4: Execute schema-level post-processing scripts
      executeSchemaPostProcessing(schemaContext);

      // Phase 4b: Archive raw cache to S3 bundles
      // Run if new data was written OR if a prior archive is incomplete
      if (resultBuilder.getTotalRows() > 0) {
        archiveRawCache(schemaContext);
      } else if (needsArchiveRetry(schemaContext)) {
        LOGGER.info("Retrying raw cache archive — prior archive was incomplete");
        archiveRawCache(schemaContext);
      } else {
        LOGGER.info("Skipping raw cache archive — no new data and archive is complete");
      }

      // Phase 5: Schema post-processing hooks
      long elapsed = System.currentTimeMillis() - startTime;
      resultBuilder.elapsedMs(elapsed);
      SchemaResult result = resultBuilder.build();

      LOGGER.info("Phase 5: Schema post-processing hooks");
      schemaListener.afterSchema(schemaContext, result);

      LOGGER.info("Schema '{}' processing complete: {}", schemaName, result);
      return result;

    } catch (Exception e) {
      LOGGER.error("Schema '{}' processing failed: {}", schemaName, e.getMessage(), e);
      schemaListener.onSchemaError(schemaContext, e);

      long elapsed = System.currentTimeMillis() - startTime;
      resultBuilder.elapsedMs(elapsed);
      resultBuilder.addError("Schema processing failed: " + e.getMessage());

      throw new IOException("Schema processing failed: " + schemaName, e);
    }
  }

  /**
   * Processes a single table using the EtlPipeline.
   *
   * @param context Table context
   * @param listener Table listener for callbacks
   * @param resolvedDimensions Dimensions resolved by listener, or null to use config dimensions
   */
  private EtlResult processTable(TableContext context, TableLifecycleListener listener,
      Map<String, DimensionConfig> resolvedDimensions) throws IOException {
    EtlPipelineConfig tableConfig = context.getTableConfig();

    // If dimensions were resolved by listener, create a new config with them
    EtlPipelineConfig effectiveConfig = tableConfig;
    if (resolvedDimensions != null && !resolvedDimensions.isEmpty()) {
      LOGGER.debug("Using {} resolved dimensions for table '{}'",
          resolvedDimensions.size(), tableConfig.getName());
      // Faithful copy with only dimensions overridden — must carry EVERY field, or
      // resolved-dimension tables silently lose config. Dropping freshness/datasetType/
      // backfillPeriod here is why the freshness gate never fired and snapshot/delta
      // semantics were lost for any table whose dimensions get resolved by a listener.
      effectiveConfig = EtlPipelineConfig.builder()
          .name(tableConfig.getName())
          .enabled(tableConfig.isEnabled())
          .sourceType(tableConfig.getSourceType())
          .source(tableConfig.getSource())
          .rawSourceConfig(tableConfig.getRawSourceConfig())
          .dimensions(resolvedDimensions)
          .columns(tableConfig.getColumns())
          .materialize(tableConfig.getMaterialize())
          .errorHandling(tableConfig.getErrorHandling())
          .hooks(tableConfig.getHooks())
          .freshness(tableConfig.getFreshness())
          .datasetType(tableConfig.getDatasetType())
          .backfillPeriod(tableConfig.getBackfillPeriod())
          .dqRowLimit(tableConfig.getDqRowLimit())
          .build();
    }

    // Create DataProvider: check for explicit class first, then use listener's fetchData hook
    DataProvider dataProvider = createDataProvider(effectiveConfig, context, listener);

    // Create DataWriter from listener's writeData hook
    DataWriter dataWriter = (config, data, variables) -> listener.writeData(context, data, variables);

    // Build schema-prefixed output directory: materializeDirectory/schemaName/
    String baseMaterializeDir = context.getMaterializeDirectory();
    String schemaName = context.getSchemaContext().getConfig().getName();
    String schemaMaterializeDir = baseMaterializeDir;
    if (baseMaterializeDir != null && schemaName != null && !schemaName.isEmpty()) {
      if (!baseMaterializeDir.endsWith("/")) {
        schemaMaterializeDir = baseMaterializeDir + "/" + schemaName;
      } else {
        schemaMaterializeDir = baseMaterializeDir + schemaName;
      }
    }

    // Create and execute the ETL pipeline
    // sourceStorageProvider handles raw cache (has its base path configured)
    // storageProvider handles parquet output
    EtlPipeline pipeline =
        new EtlPipeline(effectiveConfig,
        context.getStorageProvider(),                       // For parquet output
        context.getSchemaContext().getSourceStorageProvider(),  // For raw cache
        schemaMaterializeDir,
        new EtlPipeline.LoggingProgressListener(),
        context.getIncrementalTracker(),
        dataProvider,
        dataWriter,
        context.getSchemaContext().getOperatingDirectory());

    return pipeline.execute();
  }

  /**
   * Creates a DataProvider for the table.
   *
   * <p>Checks for explicit dataProviderClass in hooks config first,
   * then falls back to the listener's fetchData hook.
   *
   * @param config Pipeline config
   * @param context Table context
   * @param listener Table listener
   * @return DataProvider instance
   */
  private DataProvider createDataProvider(EtlPipelineConfig config, TableContext context,
      TableLifecycleListener listener) {
    HooksConfig hooks = config.getHooks();
    if (hooks != null && hooks.getDataProviderClass() != null) {
      String className = hooks.getDataProviderClass();
      try {
        LOGGER.debug("Creating DataProvider from class: {}", className);
        Class<?> clazz = Class.forName(className);
        Object instance = clazz.getDeclaredConstructor().newInstance();
        if (instance instanceof DataProvider) {
          if (instance instanceof StorageAwareDataProvider) {
            // When no sourceDirectory is configured, the raw-cache root must still be scoped to
            // the schema (<raw>/<schema>/<table>/…) to match the ${GOVDATA_CACHE_DIR}/<schema>
            // operand convention. getGovDataCacheDir() is the UNSCOPED bucket root; passing it
            // bare let StorageAwareDataProviders (e.g. geo's TigerDataProvider) write at the root
            // (<raw>/tiger instead of <raw>/geo/tiger).
            String cacheDir = sourceDirectory != null ? sourceDirectory
                : sourceStorageProvider.resolvePath(
                    org.apache.calcite.adapter.file.storage.StorageProviderFactory
                        .getGovDataCacheDir(),
                    this.config.getName());
            ((StorageAwareDataProvider) instance)
                .setStorageProvider(sourceStorageProvider, cacheDir);
          }
          return (DataProvider) instance;
        } else {
          LOGGER.error("Class {} does not implement DataProvider", className);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to instantiate DataProvider class '{}': {}",
            className, e.getMessage());
      }
    }

    // Fall back to listener's fetchData hook
    return (cfg, variables) -> listener.fetchData(context, variables);
  }

  /**
   * Executes schema-level post-processing scripts.
   *
   * <p>Post-processing scripts are executed after all tables have been materialized.
   * They run in order, respecting any dependencies defined between them.
   * Common use cases include GPU-based embedding generation and VSS index rebuilding.
   *
   * @param schemaContext Schema context
   */
  /**
   * Checks if the raw cache archive needs a retry.
   * Returns true if local cache files exist but no complete archive is in S3.
   */
  private boolean needsArchiveRetry(SchemaContext schemaContext) {
    String opDir = schemaContext.getOperatingDirectory();
    if (opDir == null) {
      return false;
    }
    java.io.File cacheDir = new java.io.File(opDir + "/cache/raw");
    if (!cacheDir.exists() || !cacheDir.isDirectory()) {
      return false;
    }
    StorageProvider source = sourceStorageProvider;
    if (source == null || "local".equals(source.getStorageType())) {
      return false;
    }
    return !BundleArchiver.hasCompleteArchive(source, config.getName());
  }

  private void archiveRawCache(SchemaContext schemaContext) {
    String opDir = schemaContext.getOperatingDirectory();
    if (opDir == null) {
      return;
    }
    String localCacheDir = opDir + "/cache/raw";
    java.io.File cacheDir = new java.io.File(localCacheDir);
    if (!cacheDir.exists() || !cacheDir.isDirectory()) {
      return;
    }
    StorageProvider source = sourceStorageProvider;
    if (source == null || "local".equals(source.getStorageType())) {
      LOGGER.debug("Skipping raw cache archive — no remote storage provider");
      return;
    }
    String schemaName = config.getName();
    String bundleId = "run-" + java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmm")
        .format(java.time.LocalDateTime.now(java.time.ZoneOffset.UTC));
    try {
      BundleArchiver.archive(localCacheDir, source, schemaName, bundleId);
    } catch (Exception e) {
      LOGGER.warn("Raw cache archive failed (non-fatal): {}", e.getMessage());
    }
  }

  /**
   * Executes schema-level post-processing scripts.
   *
   * @param schemaContext Schema context
   */
  @SuppressWarnings("UnusedVariable")
  private void executeSchemaPostProcessing(SchemaContext schemaContext) {
    List<PostProcessConfig> postProcessConfigs = config.getPostProcess();
    if (postProcessConfigs == null || postProcessConfigs.isEmpty()) {
      LOGGER.debug("No schema-level post-processing scripts configured");
      return;
    }

    LOGGER.info("Phase 4: Executing {} schema-level post-processing scripts",
        postProcessConfigs.size());

    // Determine base directory for script execution
    // Use the govdata root directory (parent of materialize directory)
    java.nio.file.Path baseDir;
    if (materializeDirectory != null) {
      // If materializeDirectory is like /root/calcite/govdata/.aperio/sec or s3://bucket/source=sec
      // We want the govdata root for script execution
      String matDir = materializeDirectory;
      if (matDir.startsWith("s3://")) {
        // For S3, use current working directory as script base
        baseDir = java.nio.file.Paths.get(System.getProperty("user.dir"));
      } else {
        // For local paths, go up from .aperio/sec to govdata
        java.nio.file.Path matPath = java.nio.file.Paths.get(matDir);
        // Check if path contains .aperio and navigate up
        if (matPath.toString().contains(".aperio")) {
          java.nio.file.Path parent = matPath;
          while (parent != null && !parent.endsWith("govdata")) {
            parent = parent.getParent();
          }
          baseDir = parent != null ? parent : matPath.getParent();
        } else {
          baseDir = matPath.getParent();
        }
      }
    } else {
      baseDir = java.nio.file.Paths.get(System.getProperty("user.dir"));
    }

    LOGGER.info("Post-processing base directory: {}", baseDir);

    // Create executor
    PostProcessExecutor executor = new PostProcessExecutor(baseDir);

    // Build variables map for substitution
    Map<String, String> variables = new java.util.HashMap<>();
    variables.put("schema", config.getName());

    // Track completed post-processes for dependency resolution
    java.util.Set<String> completed = new java.util.HashSet<>();

    for (PostProcessConfig ppConfig : postProcessConfigs) {
      String name = ppConfig.getName();

      // Check dependencies
      List<String> dependsOn = ppConfig.getDependsOn();
      if (dependsOn != null && !dependsOn.isEmpty()) {
        boolean dependenciesMet = true;
        for (String dep : dependsOn) {
          if (!completed.contains(dep)) {
            LOGGER.warn("Post-process '{}' depends on '{}' which has not completed",
                name, dep);
            dependenciesMet = false;
          }
        }
        if (!dependenciesMet) {
          LOGGER.warn("Skipping post-process '{}' due to unmet dependencies", name);
          continue;
        }
      }

      try {
        boolean success = executor.execute(ppConfig, variables);
        if (success) {
          completed.add(name);
        }
      } catch (PostProcessExecutor.PostProcessException e) {
        // If onFailure=ERROR, the exception is thrown
        LOGGER.error("Post-process '{}' failed with error: {}", name, e.getMessage());
        throw new RuntimeException("Schema post-processing failed: " + name, e);
      }
    }

    LOGGER.info("Phase 4 complete: {} of {} post-processing scripts executed",
        completed.size(), postProcessConfigs.size());
  }

  /**
   * Executes table-level post-processing scripts.
   *
   * <p>Post-processing scripts are executed after the table has been materialized
   * or after hooks-only processing completes. Common use cases include GPU-based
   * embedding generation for vectorized_chunks tables.
   *
   * @param tableContext Table context
   * @param postProcessConfigs List of post-process configurations from hooks
   * @param tableResult Result from table processing
   */
  private void executeTablePostProcess(TableContext tableContext,
      List<PostProcessConfig> postProcessConfigs, EtlResult tableResult) {
    String tableName = tableContext.getTableName();

    if (postProcessConfigs == null || postProcessConfigs.isEmpty()) {
      LOGGER.debug("No table-level post-processing scripts for '{}'", tableName);
      return;
    }

    LOGGER.info("Executing {} post-processing scripts for table '{}'",
        postProcessConfigs.size(), tableName);

    // Determine base directory for script execution
    java.nio.file.Path baseDir;
    if (materializeDirectory != null) {
      String matDir = materializeDirectory;
      if (matDir.startsWith("s3://")) {
        baseDir = java.nio.file.Paths.get(System.getProperty("user.dir"));
      } else {
        java.nio.file.Path matPath = java.nio.file.Paths.get(matDir);
        if (matPath.toString().contains(".aperio")) {
          java.nio.file.Path parent = matPath;
          while (parent != null && !parent.endsWith("govdata")) {
            parent = parent.getParent();
          }
          baseDir = parent != null ? parent : matPath.getParent();
        } else {
          baseDir = matPath.getParent();
        }
      }
    } else {
      baseDir = java.nio.file.Paths.get(System.getProperty("user.dir"));
    }

    PostProcessExecutor executor = new PostProcessExecutor(baseDir);

    // Build variables map for substitution
    Map<String, String> variables = new java.util.HashMap<>();
    variables.put("schema", config.getName());
    variables.put("table", tableName);
    variables.put("rows", String.valueOf(tableResult.getTotalRows()));

    // Track completed post-processes for dependency resolution
    java.util.Set<String> completed = new java.util.HashSet<>();

    for (PostProcessConfig ppConfig : postProcessConfigs) {
      String name = ppConfig.getName();

      // Check dependencies
      List<String> dependsOn = ppConfig.getDependsOn();
      if (dependsOn != null && !dependsOn.isEmpty()) {
        boolean dependenciesMet = true;
        for (String dep : dependsOn) {
          if (!completed.contains(dep)) {
            LOGGER.warn("Table '{}' post-process '{}' depends on '{}' which has not completed",
                tableName, name, dep);
            dependenciesMet = false;
          }
        }
        if (!dependenciesMet) {
          LOGGER.warn("Skipping table '{}' post-process '{}' due to unmet dependencies",
              tableName, name);
          continue;
        }
      }

      try {
        boolean success = executor.execute(ppConfig, variables);
        if (success) {
          completed.add(name);
          LOGGER.info("Table '{}' post-process '{}' completed successfully", tableName, name);
        }
      } catch (PostProcessExecutor.PostProcessException e) {
        LOGGER.error("Table '{}' post-process '{}' failed: {}", tableName, name, e.getMessage());
        // Re-throw if onFailure=ERROR (the execute method throws in that case)
        throw new RuntimeException("Table post-processing failed: " + tableName + "/" + name, e);
      }
    }

    LOGGER.info("Table '{}' post-processing complete: {} of {} scripts executed",
        tableName, completed.size(), postProcessConfigs.size());
  }

  /**
   * Processes bulk downloads for the schema.
   *
   * <p>Downloads large files that are shared by multiple tables. Each bulk download
   * is downloaded once and cached in the source directory. Tables can then reference
   * the cached file instead of downloading separately.
   *
   * @param schemaContext Schema context
   */
  private void processBulkDownloads(SchemaContext schemaContext) {
    Map<String, BulkDownloadConfig> bulkDownloads = config.getBulkDownloads();
    if (bulkDownloads == null || bulkDownloads.isEmpty()) {
      LOGGER.debug("No bulk downloads configured, skipping phase 2");
      return;
    }

    LOGGER.info("Phase 2: Processing {} bulk downloads", bulkDownloads.size());

    for (Map.Entry<String, BulkDownloadConfig> entry : bulkDownloads.entrySet()) {
      String name = entry.getKey();
      BulkDownloadConfig bulkConfig = entry.getValue();

      LOGGER.info("Processing bulk download: {}", name);

      // Get dimensions from bulk download config
      Map<String, DimensionConfig> dimensions = bulkConfig.getDimensions();
      if (dimensions == null || dimensions.isEmpty()) {
        // No dimensions - single download
        processSingleBulkDownload(schemaContext, name, bulkConfig, java.util.Collections.emptyMap());
      } else {
        // Expand dimensions and download for each combination
        DimensionIterator dimIterator = new DimensionIterator();
        List<Map<String, String>> combinations = dimIterator.expand(dimensions);

        LOGGER.info("Bulk download '{}' has {} dimension combinations", name, combinations.size());

        for (Map<String, String> variables : combinations) {
          processSingleBulkDownload(schemaContext, name, bulkConfig, variables);
        }
      }
    }

    LOGGER.info("Phase 2 complete: bulk downloads processed");
  }

  /**
   * Downloads a single bulk file for a specific dimension combination.
   *
   * @param schemaContext Schema context
   * @param name Bulk download name
   * @param bulkConfig Bulk download configuration
   * @param variables Dimension variable values
   */
  private void processSingleBulkDownload(SchemaContext schemaContext, String name,
      BulkDownloadConfig bulkConfig, Map<String, String> variables) {
    // Resolve cache path and URL
    String cachePath = bulkConfig.resolveCachePath(variables);
    String url = bulkConfig.resolveUrl(variables);

    // Skip if URL or cache path still has unreplaced placeholders
    // This happens when a required dimension (like year) resolves to empty
    if (url.contains("{") || cachePath.contains("{")) {
      LOGGER.warn("Skipping bulk download '{}' - unresolved placeholders in URL or cache path. "
          + "URL: {}, cachePath: {}, variables: {}",
          name, url, cachePath, variables);
      return;
    }

    // Full path in source directory (fall back to materialize directory if source not set)
    String baseDir = sourceDirectory != null ? sourceDirectory : materializeDirectory;
    String fullCachePath = baseDir != null
        ? baseDir + "/" + cachePath
        : cachePath;

    // Create variable key for storing in context
    String variableKey = createVariableKey(variables);

    // Check if already downloaded
    StorageProvider storage = schemaContext.getStorageProvider();
    try {
      if (storage != null && storage.exists(fullCachePath)) {
        LOGGER.info("Bulk download '{}' [{}] already exists: {}", name, variableKey, fullCachePath);
        schemaContext.setBulkDownloadPath(name, variableKey, fullCachePath);
        return;
      }
    } catch (IOException e) {
      LOGGER.debug("Could not check if bulk download exists: {}", e.getMessage());
      // Continue to download
    }

    // Download the file - try custom hook first, then fallback to HTTP
    LOGGER.info("Downloading bulk file '{}' [{}]: {} -> {}", name, variableKey, url, fullCachePath);
    try {
      // Try custom download hook (for SharePoint, S3, FTP, etc.)
      String customPath = schemaListener.downloadBulkFile(schemaContext, bulkConfig, variables, fullCachePath);
      if (customPath != null) {
        LOGGER.info("Bulk download '{}' [{}] complete via custom hook: {}", name, variableKey, customPath);
        schemaContext.setBulkDownloadPath(name, variableKey, customPath);
        return;
      }

      // Default: HTTP download
      downloadBulkFile(url, fullCachePath, storage);
      schemaContext.setBulkDownloadPath(name, variableKey, fullCachePath);
      LOGGER.info("Bulk download '{}' [{}] complete: {}", name, variableKey, fullCachePath);
    } catch (Exception e) {
      LOGGER.error("Failed to download bulk file '{}' [{}]: {}", name, variableKey, e.getMessage());
      // Continue with other downloads - don't fail the whole schema
    }
  }

  /**
   * Creates a variable key string from dimension values.
   */
  private String createVariableKey(Map<String, String> variables) {
    if (variables == null || variables.isEmpty()) {
      return "default";
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return sb.toString();
  }

  /**
   * Downloads a file from URL to the cache path using the storage provider.
   */
  private void downloadBulkFile(String url, String cachePath, StorageProvider storage)
      throws IOException {
    final int maxAttempts = 4;
    IOException last = null;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        downloadBulkFileOnce(url, cachePath, storage);
        return;
      } catch (IOException e) {
        last = e;
        if (attempt < maxAttempts) {
          long backoffMs = 2000L * attempt;
          LOGGER.warn("Bulk download attempt {}/{} failed for {} ({}); retrying in {}ms",
              attempt, maxAttempts, url, e.getMessage(), backoffMs);
          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during bulk download retry backoff for " + url, ie);
          }
        }
      }
    }
    throw new IOException("Bulk download failed after " + maxAttempts + " attempts for " + url, last);
  }

  /** Single bulk-download attempt — see {@link #downloadBulkFile} for the retrying wrapper. */
  private void downloadBulkFileOnce(String url, String cachePath, StorageProvider storage)
      throws IOException {
    // Fetch the raw bytes from URL
    java.net.URL urlObj = java.net.URI.create(url).toURL();
    java.net.HttpURLConnection conn = (java.net.HttpURLConnection) urlObj.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    // Between-bytes read timeout: a healthy stream never idles this long, so 2 minutes of silence
    // means the upstream stalled — fail fast and let the retry wrapper re-request rather than
    // hanging for minutes (seen on FEC / FIA / TIGER / NOAA bulk downloads).
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent",
        "Apache-Calcite-DataAdapter/1.0 (https://calcite.apache.org; data-analysis-tool)");

    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      throw new IOException("HTTP " + responseCode + " for " + url);
    }

    // Use storage provider to write file (works for local, S3, etc.)
    try (java.io.InputStream in = conn.getInputStream()) {
      storage.writeFile(cachePath, in);
      LOGGER.debug("Downloaded to {} via {}", cachePath, storage.getStorageType());
    }
  }

  /**
   * Loads schema listener from configuration or returns NOOP.
   */
  private static SchemaLifecycleListener loadSchemaListener(SchemaConfig config) {
    if (config.getHooks() == null) {
      return SchemaLifecycleListener.NOOP;
    }

    String className = config.getHooks().getSchemaLifecycleListenerClass();
    if (className == null || className.isEmpty()) {
      return SchemaLifecycleListener.NOOP;
    }

    return loadInstance(className, SchemaLifecycleListener.class);
  }

  /**
   * Loads default table listener from schema configuration.
   */
  private static TableLifecycleListener loadDefaultTableListener(SchemaConfig config) {
    if (config.getHooks() == null) {
      return TableLifecycleListener.NOOP;
    }

    String className = config.getHooks().getTableLifecycleListenerClass();
    if (className == null || className.isEmpty()) {
      return TableLifecycleListener.NOOP;
    }

    return loadInstance(className, TableLifecycleListener.class);
  }

  /**
   * Loads table-specific listener or falls back to default.
   */
  private static TableLifecycleListener loadTableListener(EtlPipelineConfig tableConfig,
      TableLifecycleListener defaultListener) {
    if (tableConfig.getHooks() == null) {
      return defaultListener;
    }

    String className = tableConfig.getHooks().getTableLifecycleListenerClass();
    if (className == null || className.isEmpty()) {
      return defaultListener;
    }

    return loadInstance(className, TableLifecycleListener.class);
  }

  /**
   * Loads a class instance by name.
   */
  @SuppressWarnings("unchecked")
  private static <T> T loadInstance(String className, Class<T> expectedType) {
    try {
      Class<?> clazz = Class.forName(className);
      if (!expectedType.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + className + " does not implement " + expectedType.getName());
      }
      return (T) clazz.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found: " + className, e);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to instantiate: " + className, e);
    }
  }

  /**
   * Creates a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for SchemaLifecycleProcessor.
   *
   * <p>Supports direct hook registration without requiring listener classes:
   * <pre>{@code
   * SchemaLifecycleProcessor.builder()
   *     .config(schemaConfig)
   *     .storageProvider(storageProvider)
   *     .beforeTable("table1", ctx -> log.info("Starting table1"))
   *     .afterTable("table2", (ctx, result) -> log.info("Done"))
   *     .onTableError("table3", (ctx, ex) -> true)
   *     .resolveDimensions("table4", (ctx, dims) -> resolveDynamicDims(ctx, dims))
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private SchemaConfig config;
    private StorageProvider storageProvider;
    private StorageProvider sourceStorageProvider;  // For raw/source data
    private String sourceDirectory;
    private String materializeDirectory;
    private String operatingDirectory;
    private IncrementalTracker incrementalTracker;
    private SchemaLifecycleListener schemaListener;
    private TableLifecycleListener defaultTableListener;

    // Hook registries - table level
    private final Map<String, java.util.function.Consumer<TableContext>> beforeTableHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.BiConsumer<TableContext, EtlResult>> afterTableHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> errorHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.BiFunction<TableContext,
        Map<String, DimensionConfig>, Map<String, DimensionConfig>>> dimensionHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.Predicate<TableContext>> filterHooks =
        new java.util.HashMap<>();

    // Hook registries - source phase
    private final Map<String, java.util.function.Consumer<TableContext>> beforeSourceHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.BiConsumer<TableContext, SourceResult>> afterSourceHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> sourceErrorHooks =
        new java.util.HashMap<>();

    // Hook registries - materialize phase
    private final Map<String, java.util.function.Consumer<TableContext>> beforeMaterializeHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.BiConsumer<TableContext, MaterializeResult>> afterMaterializeHooks =
        new java.util.HashMap<>();
    private final Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> materializeErrorHooks =
        new java.util.HashMap<>();

    // Hook registries - data provider/writer (custom source/sink)
    private final Map<String, java.util.function.BiFunction<TableContext, java.util.Map<String, String>,
        java.util.Iterator<java.util.Map<String, Object>>>> fetchDataHooks = new java.util.HashMap<>();
    private final Map<String, FetchDataWriteFunction> writeDataHooks = new java.util.HashMap<>();

    /** Functional interface for writeData hook (takes context, data iterator, and variables). */
    @FunctionalInterface
    public interface FetchDataWriteFunction {
      long write(TableContext context, java.util.Iterator<java.util.Map<String, Object>> data,
          java.util.Map<String, String> variables);
    }

    public Builder config(SchemaConfig config) {
      this.config = config;
      return this;
    }

    /**
     * Sets the storage provider for materialized output (parquet).
     */
    public Builder storageProvider(StorageProvider storageProvider) {
      this.storageProvider = storageProvider;
      return this;
    }

    /**
     * Sets the storage provider for source/raw data (cache).
     *
     * <p>If not set, falls back to main storageProvider.
     */
    public Builder sourceStorageProvider(StorageProvider sourceStorageProvider) {
      this.sourceStorageProvider = sourceStorageProvider;
      return this;
    }

    /**
     * Overrides the source directory from config.
     *
     * <p>Normally the source directory is read from {@code SchemaConfig.getSourceDirectory()}.
     * Use this method to override for testing or dynamic configuration.
     *
     * @param directory The source directory URL or path
     * @return this builder
     */
    public Builder sourceDirectory(String directory) {
      this.sourceDirectory = directory;
      return this;
    }

    /**
     * Overrides the materialize directory from config.
     *
     * <p>Normally the materialize directory is read from {@code SchemaConfig.getMaterializeDirectory()}.
     * Use this method to override for testing or dynamic configuration.
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
     * Sets the operating directory for local caching (e.g., {@code .aperio/<schema>}).
     *
     * <p>Used to derive the local raw cache path: {@code <operatingDirectory>/cache/raw}.
     *
     * @param directory The operating directory path
     * @return this builder
     */
    public Builder operatingDirectory(String directory) {
      this.operatingDirectory = directory;
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

    public Builder incrementalTracker(IncrementalTracker incrementalTracker) {
      this.incrementalTracker = incrementalTracker;
      return this;
    }

    /**
     * Sets a programmatic schema listener (overrides config).
     */
    public Builder schemaListener(SchemaLifecycleListener schemaListener) {
      this.schemaListener = schemaListener;
      return this;
    }

    /**
     * Sets a default table listener (overrides config).
     */
    public Builder defaultTableListener(TableLifecycleListener defaultTableListener) {
      this.defaultTableListener = defaultTableListener;
      return this;
    }

    /**
     * Registers a beforeTable hook for a specific table.
     */
    public Builder beforeTable(String tableName, java.util.function.Consumer<TableContext> hook) {
      beforeTableHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers an afterTable hook for a specific table.
     */
    public Builder afterTable(String tableName,
        java.util.function.BiConsumer<TableContext, EtlResult> hook) {
      afterTableHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers an error hook for a specific table.
     * Returns true to continue, false to abort.
     */
    public Builder onTableError(String tableName,
        java.util.function.BiFunction<TableContext, Exception, Boolean> hook) {
      errorHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers a dimension resolver hook for a specific table.
     */
    public Builder resolveDimensions(String tableName,
        java.util.function.BiFunction<TableContext,
            Map<String, DimensionConfig>, Map<String, DimensionConfig>> hook) {
      dimensionHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers an enabled hook for a specific table.
     *
     * <p>When the predicate returns false, the table is skipped during ETL
     * and excluded from the final schema metadata.
     *
     * @param tableName Table name to apply the hook to
     * @param hook Returns true if table should be enabled, false to disable
     */
    public Builder isEnabled(String tableName,
        java.util.function.Predicate<TableContext> hook) {
      filterHooks.put(tableName, hook);
      return this;
    }

    /**
     * @deprecated Use {@link #isEnabled(String, java.util.function.Predicate)} instead
     */
    @Deprecated
    public Builder shouldProcess(String tableName,
        java.util.function.Predicate<TableContext> hook) {
      return isEnabled(tableName, hook);
    }

    // ========== SOURCE PHASE HOOKS ==========

    /**
     * Registers a beforeSource hook for a specific table.
     */
    public Builder beforeSource(String tableName, java.util.function.Consumer<TableContext> hook) {
      beforeSourceHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers an afterSource hook for a specific table.
     */
    public Builder afterSource(String tableName,
        java.util.function.BiConsumer<TableContext, SourceResult> hook) {
      afterSourceHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers a source error hook for a specific table.
     * Returns true to continue, false to abort.
     */
    public Builder onSourceError(String tableName,
        java.util.function.BiFunction<TableContext, Exception, Boolean> hook) {
      sourceErrorHooks.put(tableName, hook);
      return this;
    }

    // ========== MATERIALIZE PHASE HOOKS ==========

    /**
     * Registers a beforeMaterialize hook for a specific table.
     */
    public Builder beforeMaterialize(String tableName, java.util.function.Consumer<TableContext> hook) {
      beforeMaterializeHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers an afterMaterialize hook for a specific table.
     */
    public Builder afterMaterialize(String tableName,
        java.util.function.BiConsumer<TableContext, MaterializeResult> hook) {
      afterMaterializeHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers a materialize error hook for a specific table.
     * Returns true to continue, false to abort.
     */
    public Builder onMaterializeError(String tableName,
        java.util.function.BiFunction<TableContext, Exception, Boolean> hook) {
      materializeErrorHooks.put(tableName, hook);
      return this;
    }

    // ========== DATA PROVIDER/WRITER HOOKS ==========

    /**
     * Registers a custom data fetcher for a specific table.
     * If the hook returns non-null, the built-in HttpSource is skipped.
     *
     * <p>Example:
     * <pre>{@code
     * .fetchData("ftp_inventory", (ctx, vars) -> {
     *   return ftpClient.downloadAndParse(ctx.getTableConfig().getSource(), vars);
     * })
     * }</pre>
     */
    public Builder fetchData(String tableName,
        java.util.function.BiFunction<TableContext, java.util.Map<String, String>,
            java.util.Iterator<java.util.Map<String, Object>>> hook) {
      fetchDataHooks.put(tableName, hook);
      return this;
    }

    /**
     * Registers a custom data writer for a specific table.
     * If the hook returns >= 0, the built-in MaterializationWriter is skipped.
     *
     * <p>Example:
     * <pre>{@code
     * .writeData("realtime_prices", (ctx, data, vars) -> {
     *   long count = 0;
     *   while (data.hasNext()) {
     *     kafkaProducer.send(data.next());
     *     count++;
     *   }
     *   return count;
     * })
     * }</pre>
     */
    public Builder writeData(String tableName, FetchDataWriteFunction hook) {
      writeDataHooks.put(tableName, hook);
      return this;
    }

    public SchemaLifecycleProcessor build() {
      if (config == null) {
        throw new IllegalArgumentException("Schema config is required");
      }

      // Resolve effective materialize directory (builder override or config)
      String effectiveMaterializeDir = materializeDirectory != null
          ? materializeDirectory : config.getMaterializeDirectory();

      if (effectiveMaterializeDir == null) {
        throw new IllegalArgumentException(
            "Materialize directory is required (set in config or via builder)");
      }

      // Storage provider must be explicitly set
      if (storageProvider == null) {
        throw new IllegalArgumentException("Storage provider is required");
      }

      // If any hooks were registered, create a delegating table listener
      boolean hasTableHooks = !beforeTableHooks.isEmpty() || !afterTableHooks.isEmpty()
          || !errorHooks.isEmpty() || !dimensionHooks.isEmpty() || !filterHooks.isEmpty();
      boolean hasSourceHooks = !beforeSourceHooks.isEmpty() || !afterSourceHooks.isEmpty()
          || !sourceErrorHooks.isEmpty();
      boolean hasMaterializeHooks = !beforeMaterializeHooks.isEmpty() || !afterMaterializeHooks.isEmpty()
          || !materializeErrorHooks.isEmpty();
      boolean hasDataHooks = !fetchDataHooks.isEmpty() || !writeDataHooks.isEmpty();

      if (hasTableHooks || hasSourceHooks || hasMaterializeHooks || hasDataHooks) {
        this.defaultTableListener =
            new DelegatingTableListener(beforeTableHooks, afterTableHooks, errorHooks, dimensionHooks, filterHooks,
            beforeSourceHooks, afterSourceHooks, sourceErrorHooks,
            beforeMaterializeHooks, afterMaterializeHooks, materializeErrorHooks,
            fetchDataHooks, writeDataHooks,
            this.defaultTableListener);
      }

      return new SchemaLifecycleProcessor(this);
    }
  }

  /**
   * Internal listener that delegates to registered hooks.
   */
  private static class DelegatingTableListener implements TableLifecycleListener {
    // Table-level hooks
    private final Map<String, java.util.function.Consumer<TableContext>> beforeHooks;
    private final Map<String, java.util.function.BiConsumer<TableContext, EtlResult>> afterHooks;
    private final Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> errorHooks;
    private final Map<String, java.util.function.BiFunction<TableContext,
        Map<String, DimensionConfig>, Map<String, DimensionConfig>>> dimensionHooks;
    private final Map<String, java.util.function.Predicate<TableContext>> filterHooks;

    // Source phase hooks
    private final Map<String, java.util.function.Consumer<TableContext>> beforeSourceHooks;
    private final Map<String, java.util.function.BiConsumer<TableContext, SourceResult>> afterSourceHooks;
    private final Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> sourceErrorHooks;

    // Materialize phase hooks
    private final Map<String, java.util.function.Consumer<TableContext>> beforeMaterializeHooks;
    private final Map<String, java.util.function.BiConsumer<TableContext, MaterializeResult>> afterMaterializeHooks;
    private final Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> materializeErrorHooks;

    // Data provider/writer hooks
    private final Map<String, java.util.function.BiFunction<TableContext, java.util.Map<String, String>,
        java.util.Iterator<java.util.Map<String, Object>>>> fetchDataHooks;
    private final Map<String, Builder.FetchDataWriteFunction> writeDataHooks;

    private final TableLifecycleListener delegate;

    DelegatingTableListener(
        Map<String, java.util.function.Consumer<TableContext>> beforeHooks,
        Map<String, java.util.function.BiConsumer<TableContext, EtlResult>> afterHooks,
        Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> errorHooks,
        Map<String, java.util.function.BiFunction<TableContext,
            Map<String, DimensionConfig>, Map<String, DimensionConfig>>> dimensionHooks,
        Map<String, java.util.function.Predicate<TableContext>> filterHooks,
        Map<String, java.util.function.Consumer<TableContext>> beforeSourceHooks,
        Map<String, java.util.function.BiConsumer<TableContext, SourceResult>> afterSourceHooks,
        Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> sourceErrorHooks,
        Map<String, java.util.function.Consumer<TableContext>> beforeMaterializeHooks,
        Map<String, java.util.function.BiConsumer<TableContext, MaterializeResult>> afterMaterializeHooks,
        Map<String, java.util.function.BiFunction<TableContext, Exception, Boolean>> materializeErrorHooks,
        Map<String, java.util.function.BiFunction<TableContext, java.util.Map<String, String>,
            java.util.Iterator<java.util.Map<String, Object>>>> fetchDataHooks,
        Map<String, Builder.FetchDataWriteFunction> writeDataHooks,
        TableLifecycleListener delegate) {
      this.beforeHooks = beforeHooks;
      this.afterHooks = afterHooks;
      this.errorHooks = errorHooks;
      this.dimensionHooks = dimensionHooks;
      this.filterHooks = filterHooks;
      this.beforeSourceHooks = beforeSourceHooks;
      this.afterSourceHooks = afterSourceHooks;
      this.sourceErrorHooks = sourceErrorHooks;
      this.beforeMaterializeHooks = beforeMaterializeHooks;
      this.afterMaterializeHooks = afterMaterializeHooks;
      this.materializeErrorHooks = materializeErrorHooks;
      this.fetchDataHooks = fetchDataHooks;
      this.writeDataHooks = writeDataHooks;
      this.delegate = delegate;
    }

    @Override public void beforeTable(TableContext context) throws Exception {
      java.util.function.Consumer<TableContext> hook = beforeHooks.get(context.getTableName());
      if (hook != null) {
        hook.accept(context);
      } else if (delegate != null) {
        delegate.beforeTable(context);
      }
    }

    @Override public void afterTable(TableContext context, EtlResult result) {
      java.util.function.BiConsumer<TableContext, EtlResult> hook =
          afterHooks.get(context.getTableName());
      if (hook != null) {
        hook.accept(context, result);
      } else if (delegate != null) {
        delegate.afterTable(context, result);
      }
    }

    @Override public boolean onTableError(TableContext context, Exception error) {
      java.util.function.BiFunction<TableContext, Exception, Boolean> hook =
          errorHooks.get(context.getTableName());
      if (hook != null) {
        return hook.apply(context, error);
      } else if (delegate != null) {
        return delegate.onTableError(context, error);
      }
      return true; // Continue by default
    }

    @Override public Map<String, DimensionConfig> resolveDimensions(TableContext context,
        Map<String, DimensionConfig> staticDimensions) {
      java.util.function.BiFunction<TableContext,
          Map<String, DimensionConfig>, Map<String, DimensionConfig>> hook =
          dimensionHooks.get(context.getTableName());
      if (hook != null) {
        return hook.apply(context, staticDimensions);
      } else if (delegate != null) {
        return delegate.resolveDimensions(context, staticDimensions);
      }
      return staticDimensions;
    }

    @Override public boolean isTableEnabled(TableContext context) {
      // Check for exact table name match first
      java.util.function.Predicate<TableContext> hook =
          filterHooks.get(context.getTableName());
      if (hook != null) {
        return hook.test(context);
      }
      // Check for wildcard "*" match as fallback
      hook = filterHooks.get("*");
      if (hook != null) {
        return hook.test(context);
      }
      if (delegate != null) {
        return delegate.isTableEnabled(context);
      }
      return true;
    }

    @Override public String resolveApiKey(TableContext context, String keyName) {
      if (delegate != null) {
        return delegate.resolveApiKey(context, keyName);
      }
      return null;
    }

    // ========== SOURCE PHASE HOOKS ==========

    @Override public void beforeSource(TableContext context) {
      java.util.function.Consumer<TableContext> hook =
          beforeSourceHooks.get(context.getTableName());
      if (hook != null) {
        hook.accept(context);
      } else if (delegate != null) {
        delegate.beforeSource(context);
      }
    }

    @Override public void afterSource(TableContext context, SourceResult result) {
      java.util.function.BiConsumer<TableContext, SourceResult> hook =
          afterSourceHooks.get(context.getTableName());
      if (hook != null) {
        hook.accept(context, result);
      } else if (delegate != null) {
        delegate.afterSource(context, result);
      }
    }

    @Override public boolean onSourceError(TableContext context, Exception error) {
      java.util.function.BiFunction<TableContext, Exception, Boolean> hook =
          sourceErrorHooks.get(context.getTableName());
      if (hook != null) {
        return hook.apply(context, error);
      } else if (delegate != null) {
        return delegate.onSourceError(context, error);
      }
      return true; // Continue by default
    }

    // ========== MATERIALIZE PHASE HOOKS ==========

    @Override public void beforeMaterialize(TableContext context) {
      java.util.function.Consumer<TableContext> hook =
          beforeMaterializeHooks.get(context.getTableName());
      if (hook != null) {
        hook.accept(context);
      } else if (delegate != null) {
        delegate.beforeMaterialize(context);
      }
    }

    @Override public void afterMaterialize(TableContext context, MaterializeResult result) {
      java.util.function.BiConsumer<TableContext, MaterializeResult> hook =
          afterMaterializeHooks.get(context.getTableName());
      if (hook != null) {
        hook.accept(context, result);
      } else if (delegate != null) {
        delegate.afterMaterialize(context, result);
      }
    }

    @Override public boolean onMaterializeError(TableContext context, Exception error) {
      java.util.function.BiFunction<TableContext, Exception, Boolean> hook =
          materializeErrorHooks.get(context.getTableName());
      if (hook != null) {
        return hook.apply(context, error);
      } else if (delegate != null) {
        return delegate.onMaterializeError(context, error);
      }
      return true; // Continue by default
    }

    // ========== DATA PROVIDER/WRITER HOOKS ==========

    @Override public java.util.Iterator<java.util.Map<String, Object>> fetchData(
        TableContext context, java.util.Map<String, String> variables) {
      java.util.function.BiFunction<TableContext, java.util.Map<String, String>,
          java.util.Iterator<java.util.Map<String, Object>>> hook =
          fetchDataHooks.get(context.getTableName());
      if (hook != null) {
        return hook.apply(context, variables);
      } else if (delegate != null) {
        return delegate.fetchData(context, variables);
      }
      return null; // Use default HttpSource
    }

    @Override public long writeData(
        TableContext context,
        java.util.Iterator<java.util.Map<String, Object>> data,
        java.util.Map<String, String> variables) {
      Builder.FetchDataWriteFunction hook = writeDataHooks.get(context.getTableName());
      if (hook != null) {
        return hook.write(context, data, variables);
      } else if (delegate != null) {
        return delegate.writeData(context, data, variables);
      }
      return -1; // Use default MaterializationWriter
    }
  }
}
