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
   * @param baseDirectory Base directory for output
   * @param progressListener Listener for progress updates
   * @param incrementalTracker Tracker for incremental processing
   * @param dataProvider Custom data provider (if null, uses built-in HttpSource)
   * @param dataWriter Custom data writer (if null, uses built-in MaterializationWriter)
   */
  public EtlPipeline(EtlPipelineConfig config, StorageProvider storageProvider,
      String baseDirectory, ProgressListener progressListener,
      IncrementalTracker incrementalTracker, DataProvider dataProvider, DataWriter dataWriter) {
    this.config = config;
    this.storageProvider = storageProvider;
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
        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("Pipeline '{}' is complete with {} combinations - skipping (signature: {}, {}ms)",
            pipelineName, totalBatches, dimensionSignature, elapsed);
        return EtlResult.skipped(pipelineName, elapsed);
      }

      if (progressListener != null) {
        progressListener.onPhaseStart("dimension_expansion", totalBatches);
        progressListener.onPhaseComplete("dimension_expansion", totalBatches);
      }

      // Phase 2: Bulk filter to find unprocessed combinations
      LOGGER.info("Phase 2: Bulk filtering {} combinations", totalBatches);
      long filterStartMs = System.currentTimeMillis();
      Set<Integer> unprocessedIndices =
          incrementalTracker.filterUnprocessed(pipelineName, pipelineName, combinations);
      long filterElapsedMs = System.currentTimeMillis() - filterStartMs;

      int neededCount = unprocessedIndices.size();
      skippedBatches = totalBatches - neededCount;
      LOGGER.info("Bulk filtering: {} unprocessed of {} total ({}ms, {}% cached)",
          neededCount, totalBatches, filterElapsedMs,
          totalBatches > 0 ? (skippedBatches * 100 / totalBatches) : 0);

      // If all combinations are already processed, mark complete and return
      // But only if there were actual combinations - empty dimensions is a config error
      if (neededCount == 0 && totalBatches > 0) {
        incrementalTracker.markTableComplete(pipelineName, dimensionSignature);
        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("All {} combinations already processed - marking complete ({}ms)",
            totalBatches, elapsed);
        return EtlResult.builder()
            .pipelineName(pipelineName)
            .totalRows(0)
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
      MaterializeConfig materializeConfig = config.getMaterialize();
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

          // Write batch - use custom writer if available, otherwise built-in MaterializationWriter
          long batchRows;
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
          totalRows += batchRows;

          LOGGER.debug("Wrote {} rows for batch {}", batchRows, processedCount);

          // Mark batch as successfully processed for incremental tracking
          incrementalTracker.markProcessed(pipelineName, pipelineName, variables, null);

          successfulBatches++;

          if (progressListener != null) {
            progressListener.onBatchComplete(processedCount, neededCount, (int) batchRows, null);
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
              throw new IOException("Pipeline failed at batch " + processedCount, e);
            case SKIP:
              skippedBatches++;
              LOGGER.warn("Skipping batch {} due to error: {}", processedCount, e.getMessage());
              break;
            case WARN:
              failedBatches++;
              LOGGER.warn("Batch {} failed (continuing): {}", processedCount, e.getMessage());
              break;
            default:
              failedBatches++;
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
        incrementalTracker.markTableComplete(pipelineName, dimensionSignature);
        LOGGER.info("Marked pipeline '{}' as complete with signature: {}", pipelineName, dimensionSignature);
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

    // Default to HTTP source
    LOGGER.info("Creating HttpSource for type: {}", sourceType);
    return new HttpSource(config.getSource(), config.getHooks());
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
