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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Orchestrates ETL pipeline execution from HTTP sources to Iceberg or Parquet tables.
 *
 * <p>EtlPipeline coordinates the full ETL process:
 * <ol>
 *   <li>Dimension Expansion - Generate all dimension value combinations</li>
 *   <li>Data Fetching - Fetch data from HTTP source for each combination</li>
 *   <li>Materialization - Write to Iceberg tables (default) or hive-partitioned Parquet</li>
 *   <li>Progress Reporting - Track and report pipeline progress</li>
 * </ol>
 *
 * <p>Output format is controlled by {@link MaterializeConfig.Format}:
 * <ul>
 *   <li>{@code ICEBERG} (default) - Uses {@link IcebergMaterializer} with atomic commits</li>
 *   <li>{@code PARQUET} - Uses {@link HiveParquetWriter} for hive-partitioned files</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * EtlPipelineConfig config = EtlPipelineConfig.builder()
 *     .name("sales_data")
 *     .source(httpSourceConfig)
 *     .dimensions(dimensionConfigs)
 *     .materialize(materializeConfig)
 *     .build();
 *
 * EtlPipeline pipeline = new EtlPipeline(config, storageProvider, "/data");
 * EtlResult result = pipeline.execute();
 *
 * System.out.println("Processed " + result.getTotalRows() + " rows");
 * }</pre>
 *
 * <h3>Error Handling</h3>
 * <p>The pipeline handles errors according to the configured error handling policy:
 * <ul>
 *   <li>Transient errors (429, 503) - Retry with exponential backoff</li>
 *   <li>Not Found (404) - Skip and mark unavailable</li>
 *   <li>API errors - Skip or fail based on configuration</li>
 *   <li>Auth errors (401, 403) - Fail immediately</li>
 * </ul>
 *
 * @see EtlPipelineConfig
 * @see EtlResult
 */
public class EtlPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(EtlPipeline.class);

  private final EtlPipelineConfig config;
  private final StorageProvider storageProvider;
  private final StorageProvider sourceStorageProvider;  // For raw cache (separate from parquet)
  private final String baseDirectory;
  private final ProgressListener progressListener;
  private final IncrementalTracker incrementalTracker;
  private final DataProvider dataProvider;
  private final DataWriter dataWriter;

  /**
   * Creates a new ETL pipeline.
   *
   * @param config Pipeline configuration
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output
   */
  public EtlPipeline(EtlPipelineConfig config, StorageProvider storageProvider,
      String baseDirectory) {
    this(config, storageProvider, baseDirectory, null, IncrementalTracker.NOOP, null, null);
  }

  /**
   * Creates a new ETL pipeline with progress listener.
   *
   * @param config Pipeline configuration
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output
   * @param progressListener Listener for progress updates
   */
  public EtlPipeline(EtlPipelineConfig config, StorageProvider storageProvider,
      String baseDirectory, ProgressListener progressListener) {
    this(config, storageProvider, baseDirectory, progressListener, IncrementalTracker.NOOP, null, null);
  }

  /**
   * Creates a new ETL pipeline with progress listener and incremental tracking.
   *
   * @param config Pipeline configuration
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output
   * @param progressListener Listener for progress updates
   * @param incrementalTracker Tracker for incremental processing
   */
  public EtlPipeline(EtlPipelineConfig config, StorageProvider storageProvider,
      String baseDirectory, ProgressListener progressListener,
      IncrementalTracker incrementalTracker) {
    this(config, storageProvider, baseDirectory, progressListener, incrementalTracker, null, null);
  }

  /**
   * Creates a new ETL pipeline with all options including custom data provider/writer.
   *
   * @param config Pipeline configuration
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output (parquet/materialized data)
   * @param progressListener Listener for progress updates
   * @param incrementalTracker Tracker for incremental processing
   * @param dataProvider Custom data provider (if null, uses built-in HttpSource)
   * @param dataWriter Custom data writer (if null, uses built-in MaterializationWriter)
   */
  public EtlPipeline(EtlPipelineConfig config, StorageProvider storageProvider,
      String baseDirectory, ProgressListener progressListener,
      IncrementalTracker incrementalTracker, DataProvider dataProvider, DataWriter dataWriter) {
    this(config, storageProvider, null, baseDirectory, progressListener, incrementalTracker,
        dataProvider, dataWriter);
  }

  /**
   * Creates a new ETL pipeline with separate source storage provider for raw cache.
   *
   * @param config Pipeline configuration
   * @param storageProvider Storage provider for materialized output (parquet)
   * @param sourceStorageProvider Storage provider for raw cache; if null, uses storageProvider
   * @param baseDirectory Base directory for output (parquet/materialized data)
   * @param progressListener Listener for progress updates
   * @param incrementalTracker Tracker for incremental processing
   * @param dataProvider Custom data provider (if null, uses built-in HttpSource)
   * @param dataWriter Custom data writer (if null, uses built-in MaterializationWriter)
   */
  public EtlPipeline(EtlPipelineConfig config, StorageProvider storageProvider,
      StorageProvider sourceStorageProvider, String baseDirectory,
      ProgressListener progressListener, IncrementalTracker incrementalTracker,
      DataProvider dataProvider, DataWriter dataWriter) {
    this.config = config;
    this.storageProvider = storageProvider;
    // Default to main storageProvider if sourceStorageProvider not specified
    this.sourceStorageProvider = sourceStorageProvider != null ? sourceStorageProvider : storageProvider;
    this.baseDirectory = baseDirectory;
    this.progressListener = progressListener;
    this.incrementalTracker = incrementalTracker != null ? incrementalTracker : IncrementalTracker.NOOP;
    this.dataProvider = dataProvider;
    this.dataWriter = dataWriter;
  }

  /**
   * Executes the ETL pipeline.
   *
   * <p>Uses optimized bulk filtering and table completion tracking:
   * <ul>
   *   <li>Fast-path: Skip entire pipeline if table is complete with same dimension signature</li>
   *   <li>Bulk filtering: Check all combinations in one database call instead of per-batch</li>
   *   <li>Table completion: Mark pipeline complete after successful processing</li>
   * </ul>
   *
   * @return Execution result with statistics
   * @throws IOException If pipeline execution fails
   */
  public EtlResult execute() throws IOException {
    String pipelineName = config.getName();
    LOGGER.info("Starting ETL pipeline: {}", pipelineName);
    long startTime = System.currentTimeMillis();

    // Track execution statistics
    long totalRows = 0;
    int successfulBatches = 0;
    int failedBatches = 0;
    int skippedBatches = 0;
    List<String> errors = new ArrayList<String>();

    MaterializationWriter writer = null;

    try {
      // Fast-path: Check cached completion from DuckDB to skip dimension expansion entirely
      // This works for both Parquet and Iceberg formats
      MaterializeConfig materializeConfig = config.getMaterialize();
      String configHash = IncrementalTracker.computeConfigHash(config.getDimensions());

      IncrementalTracker.CachedCompletion cached = incrementalTracker.getCachedCompletion(pipelineName);
      if (cached != null && configHash.equals(cached.configHash)) {
        // Config hash matches - verify data still exists
        if (verifyDataExists(pipelineName, config)) {
          long elapsed = System.currentTimeMillis() - startTime;

          // Check empty result TTL
          if (cached.rowCount == 0 && materializeConfig != null && materializeConfig.getOptions() != null) {
            long emptyResultTtlMillis = materializeConfig.getOptions().getEmptyResultTtlMillis();
            if (emptyResultTtlMillis > 0) {
              LOGGER.info("Pipeline '{}' complete but has 0 rows - invalidating to retry ({}ms TTL)",
                  pipelineName, emptyResultTtlMillis);
              incrementalTracker.invalidateTableCompletion(pipelineName);
              // Fall through to normal dimension expansion
            } else {
              LOGGER.info("Pipeline '{}' complete (fast-path, no dimension expansion) - "
                  + "skipping ({}ms, 0 rows)", pipelineName, elapsed);
              String tableLocation = baseDirectory + "/" + pipelineName;
              return EtlResult.builder()
                  .pipelineName(pipelineName)
                  .skippedEntirePipeline(true)
                  .elapsedMs(elapsed)
                  .tableLocation(tableLocation)
                  .materializeFormat(materializeConfig != null ? materializeConfig.getFormat() : null)
                  .totalRows(0)
                  .build();
            }
          } else {
            LOGGER.info("Pipeline '{}' complete (fast-path, no dimension expansion) - "
                + "skipping ({}ms, {} rows)", pipelineName, elapsed, cached.rowCount);
            String tableLocation = baseDirectory + "/" + pipelineName;
            return EtlResult.builder()
                .pipelineName(pipelineName)
                .skippedEntirePipeline(true)
                .elapsedMs(elapsed)
                .tableLocation(tableLocation)
                .materializeFormat(materializeConfig != null ? materializeConfig.getFormat() : null)
                .totalRows(cached.rowCount)
                .build();
          }
        } else {
          // Data doesn't exist - invalidate and fall through
          LOGGER.warn("Pipeline '{}' marked complete but no data found - invalidating",
              pipelineName);
          incrementalTracker.invalidateTableCompletion(pipelineName);
          incrementalTracker.invalidateAll(pipelineName);
        }
      } else if (cached != null) {
        LOGGER.debug("Config hash mismatch (cached: {}, current: {}) - will expand dimensions",
            cached.configHash, configHash);
      }

      // Phase 1: Expand dimensions (with optional custom DimensionResolver from hooks)
      LOGGER.info("Phase 1: Expanding dimensions for pipeline '{}'", pipelineName);
      DimensionResolver dimensionResolver = loadDimensionResolver(config.getHooks());
      DimensionIterator dimensionIterator = dimensionResolver != null
          ? new DimensionIterator(dimensionResolver, storageProvider)
          : new DimensionIterator();
      List<Map<String, String>> combinations = dimensionIterator.expand(config.getDimensions());
      int totalBatches = combinations.size();
      LOGGER.info("Expanded to {} dimension combinations", totalBatches);

      // Compute dimension signature for table-level completion tracking
      String dimensionSignature = IncrementalTracker.computeDimensionSignature(combinations);

      // Fast-path: Check if entire pipeline was already completed with same dimensions
      if (incrementalTracker.isTableComplete(pipelineName, dimensionSignature)) {
        // Verify data actually exists - completion marker may be stale if bucket was cleared
        if (verifyDataExists(pipelineName, config)) {
          long elapsed = System.currentTimeMillis() - startTime;
          // Include materialization info for skipped tables so DuckDB knows it's Iceberg
          if (materializeConfig != null && materializeConfig.isEnabled()) {
            String tableLocation = baseDirectory + "/" + pipelineName;
            // Read row count from Iceberg metadata for COUNT(*) optimization
            long rowCount = 0;
            if (materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG) {
              rowCount = readRowCountFromIceberg(tableLocation);
            }
            // Store config hash in DuckDB for fast-path skip on subsequent connections
            incrementalTracker.markTableCompleteWithConfig(pipelineName, configHash,
                dimensionSignature, rowCount);
            // If table has 0 rows and empty result TTL is configured, check if we should retry
            if (rowCount == 0 && materializeConfig.getOptions() != null) {
              long emptyResultTtlMillis = materializeConfig.getOptions().getEmptyResultTtlMillis();
              if (emptyResultTtlMillis > 0) {
                LOGGER.info("Pipeline '{}' complete but has 0 rows - invalidating to retry after TTL ({}ms)",
                    pipelineName, emptyResultTtlMillis);
                incrementalTracker.invalidateTableCompletion(pipelineName);
                // Continue to Phase 2 to check individual partition TTLs
              } else {
                LOGGER.info("Pipeline '{}' is complete with {} combinations - skipping (signature: {}, {}ms, 0 rows)",
                    pipelineName, totalBatches, dimensionSignature, elapsed);
                return EtlResult.builder()
                    .pipelineName(pipelineName)
                    .skippedEntirePipeline(true)
                    .elapsedMs(elapsed)
                    .tableLocation(tableLocation)
                    .materializeFormat(materializeConfig.getFormat())
                    .totalRows(rowCount)
                    .build();
              }
            } else {
              LOGGER.info("Pipeline '{}' is complete with {} combinations - skipping (signature: {}, {}ms)",
                  pipelineName, totalBatches, dimensionSignature, elapsed);
              return EtlResult.builder()
                  .pipelineName(pipelineName)
                  .skippedEntirePipeline(true)
                  .elapsedMs(elapsed)
                  .tableLocation(tableLocation)
                  .materializeFormat(materializeConfig.getFormat())
                  .totalRows(rowCount)
                  .build();
            }
          } else {
            LOGGER.info("Pipeline '{}' is complete with {} combinations - skipping (signature: {}, {}ms)",
                pipelineName, totalBatches, dimensionSignature, elapsed);
            return EtlResult.skipped(pipelineName, elapsed);
          }
        } else {
          // Data doesn't exist despite completion marker - invalidate and reprocess
          LOGGER.warn("Pipeline '{}' marked complete but no data found - invalidating stale marker",
              pipelineName);
          incrementalTracker.invalidateTableCompletion(pipelineName);
          incrementalTracker.invalidateAll(pipelineName);
        }
      }

      if (progressListener != null) {
        progressListener.onPhaseStart("dimension_expansion", totalBatches);
        progressListener.onPhaseComplete("dimension_expansion", totalBatches);
      }

      // Phase 1.5: Self-healing - rebuild cache from existing Iceberg data if needed
      // This handles the case where cache DB was deleted but Iceberg table has data
      if (materializeConfig != null
          && materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG
          && materializeConfig.isEnabled()) {
        rebuildCacheFromIceberg(pipelineName, config, combinations);
      }

      // Phase 2: Bulk filter to find unprocessed combinations
      // Use TTL-aware filtering to requery empty results after configured interval
      LOGGER.info("Phase 2: Bulk filtering {} combinations", totalBatches);
      long filterStartMs = System.currentTimeMillis();
      long emptyResultTtlMillis = materializeConfig != null && materializeConfig.getOptions() != null
          ? materializeConfig.getOptions().getEmptyResultTtlMillis()
          : MaterializeOptionsConfig.defaults().getEmptyResultTtlMillis();
      Set<Integer> unprocessedIndices =
          incrementalTracker.filterUnprocessedWithEmptyTtl(
              pipelineName, pipelineName, combinations, emptyResultTtlMillis);
      long filterElapsedMs = System.currentTimeMillis() - filterStartMs;

      int neededCount = unprocessedIndices.size();
      skippedBatches = totalBatches - neededCount;
      LOGGER.info("Bulk filtering: {} unprocessed of {} total ({}ms, {}% cached)",
          neededCount, totalBatches, filterElapsedMs,
          totalBatches > 0 ? (skippedBatches * 100 / totalBatches) : 0);

      // If all combinations are already processed, mark complete and return
      // But only if there were actual combinations - empty dimensions is a config error
      if (neededCount == 0 && totalBatches > 0) {
        // Try to get row count from Iceberg metadata for COUNT(*) optimization
        long cachedRowCount = 0;
        if (materializeConfig != null
            && materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG) {
          String tableLocation = baseDirectory + "/" + pipelineName;
          cachedRowCount = readRowCountFromIceberg(tableLocation);
        }
        incrementalTracker.markTableCompleteWithConfig(pipelineName, configHash,
            dimensionSignature, cachedRowCount);
        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("All {} combinations already processed - marking complete ({}ms, {} rows)",
            totalBatches, elapsed, cachedRowCount);
        return EtlResult.builder()
            .pipelineName(pipelineName)
            .totalRows(cachedRowCount)
            .successfulBatches(0)
            .skippedBatches(totalBatches)
            .elapsedMs(elapsed)
            .build();
      } else if (totalBatches == 0) {
        // No dimension combinations - likely a configuration error
        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.warn("Pipeline '{}' has no dimension combinations - check dimensions config", pipelineName);
        return EtlResult.builder()
            .pipelineName(pipelineName)
            .totalRows(0)
            .successfulBatches(0)
            .failedBatches(0)
            .skippedBatches(0)
            .elapsedMs(elapsed)
            .errors(Collections.singletonList("No dimension combinations - check dimensions config"))
            .build();
      }

      // Phase 3: Create data source based on type
      LOGGER.info("Phase 3: Creating data source (type={})", config.getSourceType());
      DataSource dataSource = createDataSource(config);

      // Phase 4: Create and initialize materialization writer
      // Reuse materializeConfig from Phase 1.5 - already fetched above
      MaterializeConfig.Format format = materializeConfig != null
          ? materializeConfig.getFormat() : MaterializeConfig.Format.ICEBERG;
      LOGGER.info("Phase 4: Creating MaterializationWriter (format={})", format);

      // Merge table-level config into materialize config:
      // 1. Default name to pipeline name for Iceberg table ID
      // 2. Default columns to table-level columns if not defined in materialize section
      if (materializeConfig != null) {
        boolean needsName = (materializeConfig.getName() == null || materializeConfig.getName().isEmpty())
            && (materializeConfig.getTargetTableId() == null || materializeConfig.getTargetTableId().isEmpty());
        boolean needsColumns = (materializeConfig.getColumns() == null || materializeConfig.getColumns().isEmpty())
            && config.getColumns() != null && !config.getColumns().isEmpty();

        if (needsName || needsColumns) {
          materializeConfig = MaterializeConfig.builder()
              .enabled(materializeConfig.isEnabled())
              .format(materializeConfig.getFormat())
              .targetTableId(materializeConfig.getTargetTableId())
              .output(materializeConfig.getOutput())
              .partition(materializeConfig.getPartition())
              .columns(needsColumns ? config.getColumns() : materializeConfig.getColumns())
              .options(materializeConfig.getOptions())
              .name(needsName ? config.getName() : materializeConfig.getName())
              .iceberg(materializeConfig.getIceberg())
              .build();
          LOGGER.debug("Merged table config: name={}, columns={}",
              needsName, needsColumns ? config.getColumns().size() : 0);
        }
      }

      // Append table name to base directory: schema/tableName/
      // Skip for Iceberg format - Iceberg catalog manages table location using warehousePath/tableName
      String tableName = config.getName();
      String tableDirectory = baseDirectory;
      if (format != MaterializeConfig.Format.ICEBERG
          && tableName != null && !tableName.isEmpty()) {
        if (!baseDirectory.endsWith("/")) {
          tableDirectory = baseDirectory + "/" + tableName;
        } else {
          tableDirectory = baseDirectory + tableName;
        }
      }

      writer =
          MaterializationWriterFactory.createFromConfig(materializeConfig, storageProvider, tableDirectory, incrementalTracker);
      writer.initialize(materializeConfig);
      LOGGER.info("Initialized {} writer for table {}", format, tableName);

      // Phase 5: Process only unprocessed dimension combinations
      LOGGER.info("Phase 5: Processing {} unprocessed batches (of {} total)", neededCount, totalBatches);
      if (progressListener != null) {
        progressListener.onPhaseStart("data_processing", neededCount);
      }

      int processedCount = 0;
      for (int idx = 0; idx < combinations.size(); idx++) {
        // Skip already-processed combinations (from bulk filter)
        if (!unprocessedIndices.contains(idx)) {
          continue;
        }

        Map<String, String> variables = combinations.get(idx);
        processedCount++;

        if (progressListener != null) {
          progressListener.onBatchStart(processedCount, neededCount, variables);
        }

        try {
          LOGGER.info("Processing batch {}/{}: {}", processedCount, neededCount, variables);

          // Fetch data - use custom provider if available, otherwise built-in HttpSource
          Iterator<Map<String, Object>> data = null;
          if (dataProvider != null) {
            data = dataProvider.fetch(config, variables);
            if (data != null) {
              LOGGER.debug("Using custom DataProvider for batch {}", processedCount);
            }
          }
          if (data == null) {
            // Fall back to built-in DataSource
            data = dataSource.fetch(variables);
          }

          // Check if response partitioning is enabled
          HttpSourceConfig sourceConfig = config.getSource();
          boolean hasResponsePartitioning = sourceConfig != null
              && sourceConfig.hasResponsePartitioning();

          long batchRows;
          if (hasResponsePartitioning) {
            // Response partitioning: group rows by partition fields and write each group
            batchRows = writeWithResponsePartitioning(
                data, variables, sourceConfig.getResponsePartitioning(),
                writer, dataWriter, incrementalTracker, pipelineName);
          } else {
            // Standard path: write all rows with the URL dimension variables
            if (dataWriter != null) {
              batchRows = dataWriter.write(config, data, variables);
              if (batchRows >= 0) {
                LOGGER.debug("Used custom DataWriter for batch {}: {} rows", processedCount, batchRows);
              } else {
                // Custom writer returned -1, use built-in writer
                batchRows = writer.writeBatch(data, variables);
              }
            } else {
              // Use built-in MaterializationWriter
              batchRows = writer.writeBatch(data, variables);
            }
            // Mark batch as successfully processed for incremental tracking
            // Track row count so empty results can be requeried after TTL
            incrementalTracker.markProcessedWithRowCount(
                pipelineName, pipelineName, variables, null, batchRows);
          }
          totalRows += batchRows;

          LOGGER.debug("Wrote {} rows for batch {}", batchRows, processedCount);

          successfulBatches++;

          if (progressListener != null) {
            progressListener.onBatchComplete(processedCount, neededCount, (int) batchRows, null);
          }

          // Periodic GC every 10 batches to prevent memory buildup
          // Critical for large batch pipelines like VTDs (78 batches with geometry data)
          if (processedCount % 10 == 0) {
            System.gc();
          }

        } catch (IOException e) {
          String errorMsg =
              String.format("Batch %d/%d failed: %s", processedCount, neededCount, e.getMessage());
          LOGGER.error(errorMsg, e);
          errors.add(errorMsg);

          // Invalidate table completion since we have an error
          incrementalTracker.invalidateTableCompletion(pipelineName);

          // Handle error according to policy
          EtlPipelineConfig.ErrorHandlingConfig.ErrorAction action =
              determineErrorAction(e, config.getErrorHandling());

          switch (action) {
            case FAIL:
              // Mark as error before failing - allows TTL-based retry on next run
              incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                  variables, null, e.getMessage());
              throw new IOException("Pipeline failed at batch " + processedCount, e);
            case SKIP:
              skippedBatches++;
              // Mark as error so it will be retried after error TTL expires
              incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                  variables, null, e.getMessage());
              LOGGER.warn("Skipping batch {} due to error (will retry after TTL): {}", processedCount, e.getMessage());
              break;
            case WARN:
              failedBatches++;
              // Mark as error so it will be retried after error TTL expires
              incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                  variables, null, e.getMessage());
              LOGGER.warn("Batch {} failed (will retry after TTL): {}", processedCount, e.getMessage());
              break;
            default:
              failedBatches++;
              incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                  variables, null, e.getMessage());
          }

          if (progressListener != null) {
            progressListener.onBatchComplete(processedCount, neededCount, 0, e);
          }
        }
      }

      if (progressListener != null) {
        progressListener.onPhaseComplete("data_processing", successfulBatches);
      }

      // Phase 6: Commit writes
      LOGGER.info("Phase 6: Committing writes");
      writer.commit();

      // Capture table location and format for metadata update
      String tableLocation = writer.getTableLocation();
      MaterializeConfig.Format writerFormat = writer.getFormat();
      LOGGER.info("Materialization complete: format={}, location={}", writerFormat, tableLocation);

      // Close resources
      dataSource.close();

      // Mark table as complete if all batches succeeded without errors
      if (failedBatches == 0 && errors.isEmpty()) {
        incrementalTracker.markTableCompleteWithConfig(pipelineName, configHash, dimensionSignature, totalRows);
        LOGGER.info("Marked pipeline '{}' as complete with configHash={}, signature={}, rows={}",
            pipelineName, configHash, dimensionSignature, totalRows);
      }

      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("ETL pipeline '{}' complete: {} rows, {} successful, {} failed, {} skipped in {}ms",
          pipelineName, totalRows, successfulBatches, failedBatches, skippedBatches, elapsed);

      return EtlResult.builder()
          .pipelineName(pipelineName)
          .totalRows(totalRows)
          .successfulBatches(successfulBatches)
          .failedBatches(failedBatches)
          .skippedBatches(skippedBatches)
          .elapsedMs(elapsed)
          .errors(errors)
          .tableLocation(tableLocation)
          .materializeFormat(writerFormat)
          .build();

    } catch (Exception e) {
      long elapsed = System.currentTimeMillis() - startTime;
      String errorMsg =
          String.format("ETL pipeline '%s' failed after %dms: %s", pipelineName, elapsed, e.getMessage());
      LOGGER.error(errorMsg, e);

      // Invalidate table completion on failure
      incrementalTracker.invalidateTableCompletion(pipelineName);

      return EtlResult.builder()
          .pipelineName(pipelineName)
          .totalRows(totalRows)
          .successfulBatches(successfulBatches)
          .failedBatches(failedBatches + 1)
          .skippedBatches(skippedBatches)
          .elapsedMs(elapsed)
          .errors(errors)
          .failed(true)
          .failureMessage(e.getMessage())
          .build();
    } finally {
      // Ensure writer is closed
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOGGER.warn("Error closing writer: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Writes data with response partitioning.
   *
   * <p>Groups rows by partition field values extracted from the response,
   * then writes each group separately with merged partition variables.
   *
   * @param data Iterator of rows from the API response
   * @param urlVariables Variables from URL dimension expansion
   * @param partitionConfig Response partitioning configuration
   * @param writer Materialization writer
   * @param dataWriter Optional custom data writer
   * @param tracker Incremental tracker for marking processed partitions
   * @param pipelineName Name of the pipeline
   * @return Total rows written across all partitions
   * @throws IOException If writing fails
   */
  private long writeWithResponsePartitioning(
      Iterator<Map<String, Object>> data,
      Map<String, String> urlVariables,
      HttpSourceConfig.ResponsePartitioningConfig partitionConfig,
      MaterializationWriter writer,
      DataWriter dataWriter,
      IncrementalTracker tracker,
      String pipelineName) throws IOException {

    Map<String, String> fieldMappings = partitionConfig.getFields();
    LOGGER.info("Response partitioning enabled with fields: {}", fieldMappings);

    // Check for year filtering
    boolean hasYearFilter = partitionConfig.hasYearFilter();
    String yearField = partitionConfig.getYearField();
    if (hasYearFilter) {
      LOGGER.info("Year filter enabled: field={}, range={}-{}",
          yearField, partitionConfig.getYearStart(), partitionConfig.getYearEnd());
    }

    // Collect all rows (with year filtering), let DuckDB PARTITION_BY handle partitioning
    List<Map<String, Object>> allRows = new ArrayList<Map<String, Object>>();
    int totalRows = 0;
    int filteredRows = 0;

    while (data.hasNext()) {
      Map<String, Object> row = data.next();
      totalRows++;

      // Apply year filter if configured
      if (hasYearFilter) {
        Object yearValue = row.get(yearField);
        if (!partitionConfig.isYearInRange(yearValue)) {
          filteredRows++;
          continue;  // Skip rows outside year range
        }
      }

      allRows.add(row);
    }

    if (filteredRows > 0) {
      LOGGER.info("Filtered {} of {} rows by year range, writing {} rows in single batch",
          filteredRows, totalRows, allRows.size());
    } else {
      LOGGER.info("Writing {} rows in single batch (DuckDB PARTITION_BY handles partitioning)",
          allRows.size());
    }

    if (allRows.isEmpty()) {
      LOGGER.info("No rows to write after filtering");
      // Mark as empty (0 rows) - will be requeried after TTL
      tracker.markProcessedWithRowCount(pipelineName, pipelineName, urlVariables, null, 0);
      return 0;
    }

    // Write ALL rows in ONE batch - DuckDB's PARTITION_BY handles the physical partitioning
    // This is O(1) COPY operations instead of O(partitions)
    long writtenRows = writer.writeBatch(allRows.iterator(), urlVariables);

    // Track at URL dimension level (e.g., indicator), not every partition combination
    tracker.markProcessedWithRowCount(pipelineName, pipelineName, urlVariables, null, writtenRows);

    LOGGER.info("Response partitioning complete: {} rows written in single batch",
        writtenRows);

    return writtenRows;
  }

  /**
   * Determines the error action based on the exception type.
   */
  private EtlPipelineConfig.ErrorHandlingConfig.ErrorAction determineErrorAction(
      IOException e, EtlPipelineConfig.ErrorHandlingConfig errorHandling) {

    String message = e.getMessage();
    if (message == null) {
      return errorHandling.getApiErrorAction();
    }

    if (message.contains("HTTP 401") || message.contains("HTTP 403")) {
      return errorHandling.getAuthErrorAction();
    }
    if (message.contains("HTTP 404")) {
      return errorHandling.getNotFoundAction();
    }
    if (message.contains("HTTP 429") || message.contains("HTTP 503")) {
      return errorHandling.getTransientErrorAction();
    }
    if (message.contains("HTTP 5")) {
      return errorHandling.getApiErrorAction();
    }

    return errorHandling.getApiErrorAction();
  }

  /**
   * Creates a DataSource based on the source type in the configuration.
   *
   * @param config Pipeline configuration
   * @return DataSource instance (HttpSource or ConstantsSource)
   */
  private DataSource createDataSource(EtlPipelineConfig config) {
    String sourceType = config.getSourceType();

    if (EtlPipelineConfig.SOURCE_TYPE_CONSTANTS.equals(sourceType)) {
      LOGGER.info("Creating ConstantsSource for type: {}", sourceType);
      return ConstantsSource.fromMap(config.getRawSourceConfig());
    }

    if (EtlPipelineConfig.SOURCE_TYPE_FILE.equals(sourceType)) {
      LOGGER.info("Creating FileSource for type: {}", sourceType);
      FileSourceConfig fileConfig = FileSourceConfig.fromMap(config.getRawSourceConfig());
      return new FileSource(fileConfig);
    }

    // Default to HTTP source
    // Pass sourceStorageProvider and rawCachePath for persistent response caching
    HttpSourceConfig sourceConfig = config.getSource();
    String rawCachePath = null;
    HttpSourceConfig.RawCacheConfig rawCacheConfig = sourceConfig.getRawCache();
    if (rawCacheConfig.isEnabled()) {
      // Build raw cache path: just use table name as relative path
      // The sourceStorageProvider has its base path configured (e.g., s3://bucket/raw/)
      // so files go to {baseS3Path}/{tableName}/{partitionKey}/response.json
      rawCachePath = config.getName();
      LOGGER.info("Creating HttpSource with raw cache: {} (via {})",
          rawCachePath, sourceStorageProvider.getStorageType());
    } else {
      LOGGER.info("Creating HttpSource for type: {}", sourceType);
    }
    // Use sourceStorageProvider for raw cache (not the materialized storage provider)
    return new HttpSource(sourceConfig, config.getHooks(), sourceStorageProvider, rawCachePath);
  }

  /**
   * Loads a DimensionResolver from HooksConfig if configured.
   *
   * @param hooksConfig Hooks configuration
   * @return DimensionResolver instance, or null if not configured
   */
  private DimensionResolver loadDimensionResolver(HooksConfig hooksConfig) {
    if (hooksConfig == null || hooksConfig.getDimensionResolverClass() == null) {
      return null;
    }

    String className = hooksConfig.getDimensionResolverClass();
    try {
      Class<?> clazz = Class.forName(className);
      if (!DimensionResolver.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + className + " does not implement DimensionResolver");
      }
      DimensionResolver resolver = (DimensionResolver) clazz.getDeclaredConstructor().newInstance();
      LOGGER.info("Loaded DimensionResolver: {}", className);
      return resolver;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("DimensionResolver class not found: " + className, e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to instantiate DimensionResolver: " + className, e);
    }
  }

  /**
   * Creates an EtlPipeline from configuration.
   */
  public static EtlPipeline create(EtlPipelineConfig config, StorageProvider storageProvider,
      String baseDirectory) {
    return new EtlPipeline(config, storageProvider, baseDirectory);
  }

  /**
   * Rebuilds the incremental tracker cache from existing Iceberg table metadata.
   *
   * <p>This enables "self-healing" when the cache database is deleted but Iceberg
   * data still exists. Instead of re-downloading all data, we query the Iceberg
   * table's partition metadata and mark those partitions as processed.
   *
   * <p>The method only acts when:
   * <ol>
   *   <li>Cache is empty (all combinations show as unprocessed)</li>
   *   <li>Iceberg table exists with partition data</li>
   *   <li>Existing partitions are a subset of expected combinations (not stale)</li>
   * </ol>
   *
   * @param pipelineName Pipeline name for cache key
   * @param config Pipeline configuration
   * @param combinations Expected dimension combinations
   */
  private void rebuildCacheFromIceberg(String pipelineName, EtlPipelineConfig config,
      List<Map<String, String>> combinations) {

    // Quick check: if cache already has data, skip rebuild
    Set<Integer> unprocessed = incrementalTracker.filterUnprocessed(
        pipelineName, pipelineName, combinations);
    if (unprocessed.size() < combinations.size()) {
      LOGGER.debug("Cache has {} processed entries, skipping Iceberg rebuild",
          combinations.size() - unprocessed.size());
      return;
    }

    // Cache is empty - check if Iceberg table has data we can use
    MaterializeConfig materializeConfig = config.getMaterialize();
    if (materializeConfig == null || !materializeConfig.isEnabled()) {
      return;
    }

    // Get target table ID
    String targetTableId = materializeConfig.getTargetTableId();
    if (targetTableId == null || targetTableId.isEmpty()) {
      targetTableId = materializeConfig.getName();
    }
    if (targetTableId == null || targetTableId.isEmpty()) {
      targetTableId = config.getName();
    }

    // Get partition columns
    MaterializePartitionConfig partitionConfig = materializeConfig.getPartition();
    List<String> partitionColumns = partitionConfig != null
        ? partitionConfig.getColumns()
        : Collections.<String>emptyList();

    if (partitionColumns.isEmpty()) {
      LOGGER.debug("No partition columns configured, skipping Iceberg rebuild");
      return;
    }

    // Build catalog config for querying Iceberg metadata
    Map<String, Object> catalogConfig = buildIcebergCatalogConfig(materializeConfig);

    // Query existing partitions from Iceberg table
    java.util.Set<Map<String, String>> existingPartitions =
        IcebergMaterializationWriter.getExistingPartitions(catalogConfig, targetTableId, partitionColumns);

    if (existingPartitions.isEmpty()) {
      LOGGER.debug("No existing partitions in Iceberg table '{}', no cache rebuild needed",
          targetTableId);
      return;
    }

    // Verify existing partitions are not stale - they should be a subset of expected combinations
    // Convert combinations to a set for efficient lookup
    java.util.Set<Map<String, String>> expectedSet =
        new java.util.HashSet<Map<String, String>>(combinations);
    java.util.Set<Map<String, String>> stalePartitions =
        new java.util.HashSet<Map<String, String>>();

    for (Map<String, String> existing : existingPartitions) {
      // Extract only the partition columns we care about for comparison
      Map<String, String> partitionOnly = new java.util.LinkedHashMap<String, String>();
      for (String col : partitionColumns) {
        String val = existing.get(col);
        if (val != null) {
          partitionOnly.put(col, val);
        }
      }
      if (!expectedSet.contains(partitionOnly)) {
        stalePartitions.add(partitionOnly);
      }
    }

    // Find partitions that ARE in expected set (can be rebuilt)
    java.util.Set<Map<String, String>> rebuildable =
        new java.util.HashSet<Map<String, String>>();
    for (Map<String, String> existing : existingPartitions) {
      Map<String, String> partitionOnly = new java.util.LinkedHashMap<String, String>();
      for (String col : partitionColumns) {
        String val = existing.get(col);
        if (val != null) {
          partitionOnly.put(col, val);
        }
      }
      if (expectedSet.contains(partitionOnly)) {
        rebuildable.add(partitionOnly);
      }
    }

    if (!stalePartitions.isEmpty()) {
      LOGGER.info("Found {} stale partitions in Iceberg table '{}' not in current dimensions "
          + "(Example: {}). These will be ignored - only {} current partitions will be rebuilt.",
          stalePartitions.size(), targetTableId,
          stalePartitions.iterator().next(), rebuildable.size());
    }

    if (rebuildable.isEmpty()) {
      LOGGER.debug("No rebuildable partitions found for table '{}', skipping cache rebuild",
          targetTableId);
      return;
    }

    // Rebuild cache from existing partitions that match current dimensions
    LOGGER.info("Self-healing: Rebuilding cache from {} existing Iceberg partitions for table '{}'",
        rebuildable.size(), targetTableId);

    int rebuilt = 0;
    for (Map<String, String> partition : rebuildable) {
      // Mark with -1 (unknown row count but has data) to indicate non-empty
      incrementalTracker.markProcessedWithRowCount(
          pipelineName, pipelineName, partition, null, -1);
      rebuilt++;
    }

    LOGGER.info("Self-healing complete: Rebuilt cache with {} partition entries from Iceberg metadata",
        rebuilt);
  }

  /**
   * Builds Iceberg catalog configuration from MaterializeConfig.
   */
  private Map<String, Object> buildIcebergCatalogConfig(MaterializeConfig materializeConfig) {
    Map<String, Object> catalogConfig = new java.util.HashMap<String, Object>();

    MaterializeConfig.IcebergConfig icebergConfig = materializeConfig.getIceberg();
    if (icebergConfig != null) {
      MaterializeConfig.IcebergConfig.CatalogType catalogType = icebergConfig.getCatalogType();
      switch (catalogType) {
        case REST:
          catalogConfig.put("catalog", "rest");
          if (icebergConfig.getRestUri() != null) {
            catalogConfig.put("uri", icebergConfig.getRestUri());
          }
          break;
        case HIVE:
          catalogConfig.put("catalog", "hive");
          break;
        case HADOOP:
        default:
          catalogConfig.put("catalog", "hadoop");
          break;
      }

      String warehousePath = icebergConfig.getWarehousePath();
      if (warehousePath == null || warehousePath.isEmpty()) {
        warehousePath = baseDirectory;
      }
      // Convert s3:// to s3a:// for Hadoop S3A FileSystem compatibility
      if (warehousePath != null && warehousePath.startsWith("s3://")) {
        warehousePath = "s3a://" + warehousePath.substring(5);
      }
      catalogConfig.put("warehousePath", warehousePath);
    } else {
      catalogConfig.put("catalog", "hadoop");
      String warehousePath = baseDirectory;
      if (warehousePath != null && warehousePath.startsWith("s3://")) {
        warehousePath = "s3a://" + warehousePath.substring(5);
      }
      catalogConfig.put("warehousePath", warehousePath);
    }

    // Add S3 credentials from storage provider
    Map<String, String> s3Config = storageProvider != null ? storageProvider.getS3Config() : null;
    if (s3Config != null && !s3Config.isEmpty()) {
      Map<String, String> hadoopConfig = new java.util.HashMap<String, String>();
      hadoopConfig.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

      String accessKey = s3Config.get("accessKeyId");
      String secretKey = s3Config.get("secretAccessKey");
      if (accessKey != null) {
        hadoopConfig.put("fs.s3a.access.key", accessKey);
      }
      if (secretKey != null) {
        hadoopConfig.put("fs.s3a.secret.key", secretKey);
      }

      String endpoint = s3Config.get("endpoint");
      if (endpoint != null) {
        hadoopConfig.put("fs.s3a.endpoint", endpoint);
        hadoopConfig.put("fs.s3a.path.style.access", "true");
      }

      String region = s3Config.get("region");
      if (region != null) {
        hadoopConfig.put("fs.s3a.endpoint.region", region);
      }

      catalogConfig.put("hadoopConfig", hadoopConfig);
    }

    return catalogConfig;
  }

  /**
   * Verify that data files actually exist for a pipeline.
   *
   * <p>This prevents stale completion markers from causing skipped processing
   * when the underlying data has been deleted (e.g., bucket cleared).
   *
   * @param pipelineName Pipeline name (also table name)
   * @param config Pipeline configuration
   * @return true if data files exist, false otherwise
   */
  private boolean verifyDataExists(String pipelineName, EtlPipelineConfig config) {
    MaterializeConfig materializeConfig = config.getMaterialize();
    if (materializeConfig == null || !materializeConfig.isEnabled()) {
      // No materialization configured - can't verify data exists
      return true;
    }

    try {
      if (storageProvider == null) {
        // No storage provider - can't verify, assume exists
        return true;
      }

      if (materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG) {
        // For Iceberg, check if metadata directory has files (indicates table was created)
        // Use isDirectory which does a list check - works for S3 where directories don't exist as objects
        String metadataPath = baseDirectory + "/" + pipelineName + "/metadata";
        if (storageProvider.isDirectory(metadataPath)) {
          LOGGER.debug("Verified Iceberg metadata exists at {}", metadataPath);
          return true;
        }
      } else {
        // For Parquet format, check if data directory has files
        String dataPath = baseDirectory + "/" + pipelineName;
        if (storageProvider.isDirectory(dataPath)) {
          LOGGER.debug("Verified data exists at {}", dataPath);
          return true;
        }
      }

      LOGGER.debug("No data found for pipeline '{}' at base directory '{}'",
          pipelineName, baseDirectory);
      return false;

    } catch (IOException e) {
      // If we can't verify, assume data exists to avoid unnecessary reprocessing
      LOGGER.warn("Could not verify data existence for '{}': {} - assuming exists",
          pipelineName, e.getMessage());
      return true;
    }
  }

  /**
   * Read row count from Iceberg metadata for COUNT(*) optimization.
   * This is called for skipped tables that are already materialized.
   *
   * @param tableLocation The Iceberg table location
   * @return Row count from metadata, or 0 if unable to read
   */
  private long readRowCountFromIceberg(String tableLocation) {
    try {
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

      // Configure S3 filesystem if using S3 storage
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        org.apache.calcite.adapter.file.storage.S3StorageProvider s3Provider =
            (org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider;
        java.util.Map<String, String> s3Config = s3Provider.getS3Config();
        if (s3Config != null) {
          // S3A FileSystem implementation - required for Hadoop to find the FS
          hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
          hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

          String endpoint = s3Config.get("endpoint");
          String accessKey = s3Config.get("accessKeyId");
          String secretKey = s3Config.get("secretAccessKey");
          String region = s3Config.get("region");

          if (endpoint != null) {
            hadoopConf.set("fs.s3a.endpoint", endpoint);
            hadoopConf.set("fs.s3a.path.style.access", "true");
          }
          if (accessKey != null) {
            hadoopConf.set("fs.s3a.access.key", accessKey);
          }
          if (secretKey != null) {
            hadoopConf.set("fs.s3a.secret.key", secretKey);
          }
          if (region != null) {
            hadoopConf.set("fs.s3a.endpoint.region", region);
          }
        }
      }

      // Extract warehouse path from table location
      String warehousePath = tableLocation;
      int lastSlash = warehousePath.lastIndexOf('/');
      if (lastSlash > 0) {
        warehousePath = warehousePath.substring(0, lastSlash);
      }

      org.apache.iceberg.hadoop.HadoopCatalog catalog = null;
      try {
        catalog = new org.apache.iceberg.hadoop.HadoopCatalog(hadoopConf, warehousePath);

        String tableName = tableLocation.substring(lastSlash + 1);
        org.apache.iceberg.catalog.TableIdentifier tableId =
            org.apache.iceberg.catalog.TableIdentifier.of(tableName);

        org.apache.iceberg.Table table = catalog.loadTable(tableId);
        org.apache.iceberg.Snapshot snapshot = table.currentSnapshot();

        if (snapshot == null) {
          LOGGER.debug("Iceberg table '{}' has no snapshot, returning 0 row count", tableLocation);
          return 0;
        }

        long totalRecords = 0;
        for (org.apache.iceberg.ManifestFile manifest : snapshot.allManifests(table.io())) {
          Long addedRows = manifest.addedRowsCount();
          if (addedRows != null) {
            totalRecords += addedRows;
          }
        }

        LOGGER.info("Read row count {} from Iceberg metadata for skipped table: {}",
            totalRecords, tableLocation);
        return totalRecords;

      } finally {
        if (catalog != null) {
          catalog.close();
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to read row count from Iceberg for '{}': {}", tableLocation, e.getMessage());
      return 0;
    }
  }

  /**
   * Cached ETL properties from Iceberg table.
   * Used for fast-path skip check to avoid dimension expansion.
   */
  private static class CachedEtlProperties {
    final String configHash;
    final String signature;
    final long rowCount;

    CachedEtlProperties(String configHash, String signature, long rowCount) {
      this.configHash = configHash;
      this.signature = signature;
      this.rowCount = rowCount;
    }
  }

  /**
   * Reads ETL properties from Iceberg table metadata.
   * Returns cached config hash, signature, and row count if available.
   *
   * @param tableLocation The Iceberg table location
   * @return CachedEtlProperties, or null if properties not found or table doesn't exist
   */
  private CachedEtlProperties readEtlPropertiesFromIceberg(String tableLocation) {
    try {
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

      // Configure S3 filesystem if using S3 storage
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        org.apache.calcite.adapter.file.storage.S3StorageProvider s3Provider =
            (org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider;
        java.util.Map<String, String> s3Config = s3Provider.getS3Config();
        if (s3Config != null) {
          hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
          hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

          String endpoint = s3Config.get("endpoint");
          String accessKey = s3Config.get("accessKeyId");
          String secretKey = s3Config.get("secretAccessKey");
          String region = s3Config.get("region");

          if (endpoint != null) {
            hadoopConf.set("fs.s3a.endpoint", endpoint);
            hadoopConf.set("fs.s3a.path.style.access", "true");
          }
          if (accessKey != null) {
            hadoopConf.set("fs.s3a.access.key", accessKey);
          }
          if (secretKey != null) {
            hadoopConf.set("fs.s3a.secret.key", secretKey);
          }
          if (region != null) {
            hadoopConf.set("fs.s3a.endpoint.region", region);
          }
        }
      }

      // Extract warehouse path from table location
      String warehousePath = tableLocation;
      int lastSlash = warehousePath.lastIndexOf('/');
      if (lastSlash > 0) {
        warehousePath = warehousePath.substring(0, lastSlash);
      }

      org.apache.iceberg.hadoop.HadoopCatalog catalog = null;
      try {
        catalog = new org.apache.iceberg.hadoop.HadoopCatalog(hadoopConf, warehousePath);

        String tableName = tableLocation.substring(lastSlash + 1);
        org.apache.iceberg.catalog.TableIdentifier tableId =
            org.apache.iceberg.catalog.TableIdentifier.of(tableName);

        if (!catalog.tableExists(tableId)) {
          return null;
        }

        org.apache.iceberg.Table table = catalog.loadTable(tableId);

        // Read ETL properties
        String configHash = table.properties().get("etl.config-hash");
        String signature = table.properties().get("etl.signature");
        String rowCountStr = table.properties().get("etl.row-count");

        if (configHash == null || signature == null) {
          LOGGER.debug("Iceberg table '{}' has no cached ETL properties", tableLocation);
          return null;
        }

        long rowCount = 0;
        if (rowCountStr != null) {
          try {
            rowCount = Long.parseLong(rowCountStr);
          } catch (NumberFormatException e) {
            // Fall back to reading from manifest
            org.apache.iceberg.Snapshot snapshot = table.currentSnapshot();
            if (snapshot != null) {
              for (org.apache.iceberg.ManifestFile manifest : snapshot.allManifests(table.io())) {
                Long addedRows = manifest.addedRowsCount();
                if (addedRows != null) {
                  rowCount += addedRows;
                }
              }
            }
          }
        }

        LOGGER.debug("Read cached ETL properties from '{}': configHash={}, signature={}, rows={}",
            tableLocation, configHash, signature, rowCount);
        return new CachedEtlProperties(configHash, signature, rowCount);

      } finally {
        if (catalog != null) {
          catalog.close();
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to read ETL properties from Iceberg for '{}': {}",
          tableLocation, e.getMessage());
      return null;
    }
  }

  /**
   * Stores ETL properties to an existing Iceberg table.
   * Used to cache dimension config hash and signature for fast-path skip on subsequent connections.
   *
   * @param tableLocation The Iceberg table location
   * @param configHash Hash of dimension configuration
   * @param signature Dimension signature
   * @param rowCount Total row count
   */
  private void storeEtlPropertiesToIceberg(String tableLocation, String configHash,
      String signature, long rowCount) {
    try {
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

      // Configure S3 filesystem if using S3 storage
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        org.apache.calcite.adapter.file.storage.S3StorageProvider s3Provider =
            (org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider;
        java.util.Map<String, String> s3Config = s3Provider.getS3Config();
        if (s3Config != null) {
          hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
          hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

          String endpoint = s3Config.get("endpoint");
          String accessKey = s3Config.get("accessKeyId");
          String secretKey = s3Config.get("secretAccessKey");
          String region = s3Config.get("region");

          if (endpoint != null) {
            hadoopConf.set("fs.s3a.endpoint", endpoint);
            hadoopConf.set("fs.s3a.path.style.access", "true");
          }
          if (accessKey != null) {
            hadoopConf.set("fs.s3a.access.key", accessKey);
          }
          if (secretKey != null) {
            hadoopConf.set("fs.s3a.secret.key", secretKey);
          }
          if (region != null) {
            hadoopConf.set("fs.s3a.endpoint.region", region);
          }
        }
      }

      // Extract warehouse path from table location
      String warehousePath = tableLocation;
      int lastSlash = warehousePath.lastIndexOf('/');
      if (lastSlash > 0) {
        warehousePath = warehousePath.substring(0, lastSlash);
      }

      org.apache.iceberg.hadoop.HadoopCatalog catalog = null;
      try {
        catalog = new org.apache.iceberg.hadoop.HadoopCatalog(hadoopConf, warehousePath);

        String tableName = tableLocation.substring(lastSlash + 1);
        org.apache.iceberg.catalog.TableIdentifier tableId =
            org.apache.iceberg.catalog.TableIdentifier.of(tableName);

        if (!catalog.tableExists(tableId)) {
          LOGGER.debug("Cannot store ETL properties: table doesn't exist at {}", tableLocation);
          return;
        }

        org.apache.iceberg.Table table = catalog.loadTable(tableId);

        // Store ETL properties
        table.updateProperties()
            .set("etl.config-hash", configHash)
            .set("etl.signature", signature)
            .set("etl.row-count", String.valueOf(rowCount))
            .set("etl.completed-timestamp", String.valueOf(System.currentTimeMillis()))
            .commit();

        LOGGER.info("Stored ETL properties to Iceberg table '{}' for fast-path skip: configHash={}",
            tableLocation, configHash);

      } finally {
        if (catalog != null) {
          catalog.close();
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to store ETL properties to Iceberg for '{}': {}",
          tableLocation, e.getMessage());
    }
  }

  /**
   * Listener for pipeline progress updates.
   */
  public interface ProgressListener {
    /**
     * Called when a phase starts.
     *
     * @param phase Phase name
     * @param totalItems Total items in this phase
     */
    void onPhaseStart(String phase, int totalItems);

    /**
     * Called when a phase completes.
     *
     * @param phase Phase name
     * @param processedItems Number of items processed
     */
    void onPhaseComplete(String phase, int processedItems);

    /**
     * Called when a batch starts.
     *
     * @param batchNum Current batch number
     * @param totalBatches Total number of batches
     * @param variables Dimension values for this batch
     */
    void onBatchStart(int batchNum, int totalBatches, Map<String, String> variables);

    /**
     * Called when a batch completes.
     *
     * @param batchNum Current batch number
     * @param totalBatches Total number of batches
     * @param rowCount Rows processed in this batch
     * @param error Error if batch failed, null otherwise
     */
    void onBatchComplete(int batchNum, int totalBatches, int rowCount, Exception error);
  }

  /**
   * Default progress listener that logs to SLF4J.
   */
  public static class LoggingProgressListener implements ProgressListener {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingProgressListener.class);

    @Override public void onPhaseStart(String phase, int totalItems) {
      LOG.info("Starting phase '{}' with {} items", phase, totalItems);
    }

    @Override public void onPhaseComplete(String phase, int processedItems) {
      LOG.info("Completed phase '{}': {} items processed", phase, processedItems);
    }

    @Override public void onBatchStart(int batchNum, int totalBatches, Map<String, String> variables) {
      LOG.debug("Starting batch {}/{}: {}", batchNum, totalBatches, variables);
    }

    @Override public void onBatchComplete(int batchNum, int totalBatches, int rowCount, Exception error) {
      if (error != null) {
        LOG.warn("Batch {}/{} failed: {}", batchNum, totalBatches, error.getMessage());
      } else {
        LOG.debug("Batch {}/{} complete: {} rows", batchNum, totalBatches, rowCount);
      }
    }
  }
}
