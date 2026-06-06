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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
  private final String operatingDirectory;
  private final DataProvider dataProvider;
  private final DataWriter dataWriter;

  /** Lock serializing all writer and tracker operations when parallel threads are active. */
  private final Object writeLock = new Object();

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
        dataProvider, dataWriter, null);
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
    this(config, storageProvider, sourceStorageProvider, baseDirectory, progressListener,
        incrementalTracker, dataProvider, dataWriter, null);
  }

  /**
   * Creates a new ETL pipeline with separate source storage provider and operating directory.
   *
   * @param config Pipeline configuration
   * @param storageProvider Storage provider for materialized output (parquet)
   * @param sourceStorageProvider Storage provider for raw cache; if null, uses storageProvider
   * @param baseDirectory Base directory for output (parquet/materialized data)
   * @param progressListener Listener for progress updates
   * @param incrementalTracker Tracker for incremental processing
   * @param dataProvider Custom data provider (if null, uses built-in HttpSource)
   * @param dataWriter Custom data writer (if null, uses built-in MaterializationWriter)
   * @param operatingDirectory Operating directory for local caching; may be null
   */
  public EtlPipeline(EtlPipelineConfig config, StorageProvider storageProvider,
      StorageProvider sourceStorageProvider, String baseDirectory,
      ProgressListener progressListener, IncrementalTracker incrementalTracker,
      DataProvider dataProvider, DataWriter dataWriter, String operatingDirectory) {
    this.config = config;
    this.storageProvider = storageProvider;
    // Default to main storageProvider if sourceStorageProvider not specified
    this.sourceStorageProvider = sourceStorageProvider != null ? sourceStorageProvider : storageProvider;
    this.baseDirectory = baseDirectory;
    this.progressListener = progressListener;
    this.incrementalTracker = incrementalTracker != null ? incrementalTracker : IncrementalTracker.NOOP;
    this.operatingDirectory = operatingDirectory;
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

    // Memory tracking
    List<MemorySnapshot> memSnapshots = new ArrayList<MemorySnapshot>();

    // Track execution statistics
    long totalRows = 0;
    int successfulBatches = 0;
    int failedBatches = 0;
    int skippedBatches = 0;
    int consecutiveFailures = 0;
    final int maxConsecutiveFailures =
        Integer.parseInt(
            System.getProperty("calcite.etl.maxConsecutiveFailures",
            System.getenv("ETL_MAX_CONSECUTIVE_FAILURES") != null
                ? System.getenv("ETL_MAX_CONSECUTIVE_FAILURES") : "10"));
    List<String> errors = new ArrayList<String>();

    MemorySnapshot startSnap = MemorySnapshot.capture("pipeline_start");
    memSnapshots.add(startSnap);
    long peakUsedBytes = startSnap.getUsedBytes();
    LOGGER.debug("Memory at pipeline_start: {}", startSnap);

    MaterializationWriter writer = null;
    // Freshness token probed in Phase 3b; stored to tracker after a successful commit.
    String probedFreshnessToken = null;

    try {
      boolean forceReprocessAll = false;

      // Fast-path: Check cached completion from DuckDB to skip dimension expansion entirely
      // This works for both Parquet and Iceberg formats
      MaterializeConfig materializeConfig = config.getMaterialize();
      String configHash = IncrementalTracker.computeConfigHash(config.getDimensions());

      IncrementalTracker.CachedCompletion cached = incrementalTracker.getCachedCompletion(pipelineName);
      if (cached != null) {
        // A table-level completion marker exists. It is NOT a whole-table skip gate any more:
        // the per-period markers (isPeriodComplete) + the per-batch filter are authoritative,
        // because a table-level marker is period-blind — it says a table "completed" but not
        // WHICH periods, so a daily run (current year) could otherwise shortcut-skip a later
        // historical backfill. We only use the marker here for stale-data recovery.
        if (!verifyDataExists(pipelineName, config)) {
          // Data doesn't exist despite a completion marker — invalidate and reprocess all.
          LOGGER.warn("Pipeline '{}' marked complete but no data found - invalidating",
              pipelineName);
          incrementalTracker.invalidateTableCompletion(pipelineName);
          forceReprocessAll = true;
        } else {
          // Data exists — check if the zero-row empty-result TTL has expired.
          // If it has, invalidate so the pipeline re-runs the empty batches.
          long emptyTtlMillisForCacheCheck = materializeConfig != null
              && materializeConfig.getOptions() != null
              ? materializeConfig.getOptions().getEmptyResultTtlMillis()
              : 0;
          if (cached.isEmptyResultTtlExpired(emptyTtlMillisForCacheCheck)) {
            LOGGER.info("Pipeline '{}': empty-result TTL expired — invalidating for re-run",
                pipelineName);
            incrementalTracker.invalidateTableCompletion(pipelineName);
            forceReprocessAll = true;
          } else {
            // Data exists and TTL not expired — fall through to dimension expansion and the
            // period-aware filter, which processes only periods whose marker isn't 'complete'.
            LOGGER.debug("Pipeline '{}' has a table-level marker and data — expanding for "
                + "per-period filter", pipelineName);
          }
        }
      } else if (materializeConfig != null
          && materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG) {
        // (c) Cold-start recovery, NARROWED: no table-level marker but Iceberg data exists.
        // Do NOT return early for the whole table (that was period-blind). Instead, fall
        // through to dimension expansion; Phase 5 skip-if-materialized marks combos whose
        // Iceberg partition already exists as processed, then markCompletedPeriods promotes
        // any period whose full combo set is present to a per-period 'complete' marker.
        if (verifyDataExists(pipelineName, config)) {
          LOGGER.info("Pipeline '{}': no marker but Iceberg data exists — expanding for "
              + "per-period filter (cold-start, not whole-table skip)", pipelineName);
        }
      }

      // Phase 1: Expand dimensions (with optional custom DimensionResolver from hooks)
      LOGGER.info("Phase 1: Expanding dimensions for pipeline '{}'", pipelineName);
      DimensionResolver dimensionResolver = loadDimensionResolver(config.getHooks());
      DimensionIterator dimensionIterator = dimensionResolver != null
          ? new DimensionIterator(dimensionResolver, storageProvider)
          : new DimensionIterator();

      // Check if partitioned expansion is applicable (CUSTOM dimensions with resolver).
      // planPartitions returns null if not applicable, signaling standard expand.
      DimensionPartitionPlan partitionPlan =
          dimensionIterator.planPartitions(config.getDimensions());
      boolean usePartitionedExpansion = partitionPlan != null;

      List<Map<String, String>> combinations = null;
      int totalBatches;
      String dimensionSignature;

      if (usePartitionedExpansion) {
        // Partitioned mode: we know the prefix count and partition count,
        // but don't expand CUSTOM dimension yet (that's done lazily per-partition).
        // Sign by the partition PERIOD values (the few context-key values — years,
        // quarters, months, or days) rather than the full 3M+ expanded combinations.
        // configHash alone is period-blind: a daily run (e.g. year=2026) and a historical
        // run (year=2022..2025) of the same table share a configHash, so the daily
        // completion would shortcut-skip the historical backfill. Including the sorted
        // period values keeps the marker cheap yet period-keyed, so each run's completion
        // reflects exactly the periods it covered.
        List<String> periodValues = new ArrayList<>(partitionPlan.getContextValues());
        Collections.sort(periodValues);
        dimensionSignature = "partitioned:" + configHash + ":"
            + partitionPlan.getContextKey() + "=" + String.join(",", periodValues);
        // totalBatches is unknown until we expand each partition — use prefix count as placeholder
        // for the fast-path check. Actual count is computed during processing.
        totalBatches = partitionPlan.getTotalPrefixCount();
        LOGGER.info("Partitioned plan: {} partitions by '{}', {} prefix combinations",
            partitionPlan.getPartitionCount(), partitionPlan.getContextKey(),
            totalBatches);
      } else {
        combinations = dimensionIterator.expand(config.getDimensions());
        totalBatches = combinations.size();
        dimensionSignature = IncrementalTracker.computeDimensionSignature(combinations);
        LOGGER.info("Expanded to {} dimension combinations", totalBatches);
      }

      MemorySnapshot dimSnap = MemorySnapshot.capture("after_dim_expansion");
      memSnapshots.add(dimSnap);
      if (dimSnap.getUsedBytes() > peakUsedBytes) {
        peakUsedBytes = dimSnap.getUsedBytes();
      }
      LOGGER.debug("Memory after_dim_expansion: {}", dimSnap);

      // Stale-data recovery only (NOT a whole-table skip): if a table-level signature marker
      // exists but the data is gone (bucket cleared), force a full reprocess. The whole-table
      // skip that used to live here was period-blind — it skipped historical backfills after a
      // daily run completed the current year. Completion is now per-period: the Phase 2
      // per-batch filter (seeded by isPeriodComplete) decides what to process, and the
      // neededCount==0 path below marks the table complete and returns when nothing is left.
      if (incrementalTracker.isTableComplete(pipelineName, dimensionSignature)
          && !verifyDataExists(pipelineName, config)) {
        LOGGER.warn("Pipeline '{}' marked complete but no data found - invalidating stale marker",
            pipelineName);
        incrementalTracker.invalidateTableCompletion(pipelineName);
        forceReprocessAll = true;
      }

      // No completion marker found — if there is also no Iceberg data, skip the
      // expensive full-bucket batch scan and treat all combinations as unprocessed.
      // This covers fresh rebuilds where the tracker was cleared but batch parquet
      // files from other schemas remain, making the glob scan very slow.
      if (!forceReprocessAll
          && materializeConfig != null
          && materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG
          && !verifyDataExists(pipelineName, config)) {
        LOGGER.info("Pipeline '{}': no completion marker and no Iceberg data — "
            + "skipping batch scan, force-reprocessing all {} combinations",
            pipelineName, totalBatches);
        forceReprocessAll = true;
      }

      // Table was explicitly cleared (invalidateTableCompletion was called) — skip both the
      // Phase 1.5 Iceberg manifest scan AND the Phase 2 full-bucket batch scan.
      if (!forceReprocessAll && incrementalTracker.wasTableCleared(pipelineName)) {
        LOGGER.info("Pipeline '{}': tracker was explicitly cleared — "
            + "skipping Phase 2 batch scan, force-reprocessing all {} combinations",
            pipelineName, totalBatches);
        forceReprocessAll = true;
      }

      if (progressListener != null) {
        progressListener.onPhaseStart("dimension_expansion", totalBatches);
        progressListener.onPhaseComplete("dimension_expansion", totalBatches);
      }

      long emptyResultTtlMillis = materializeConfig != null && materializeConfig.getOptions() != null
          ? materializeConfig.getOptions().getEmptyResultTtlMillis()
          : MaterializeOptionsConfig.defaults().getEmptyResultTtlMillis();
      int neededCount;
      // Standard mode stores filtered indices here; partitioned mode filters per-partition in Phase 5
      Set<Integer> standardUnprocessedIndices = null;

      if (forceReprocessAll) {
        // Stale completion marker detected — skip expensive tracker scans
        // and reprocess all combinations from scratch
        neededCount = totalBatches;
        if (!usePartitionedExpansion) {
          standardUnprocessedIndices = allIndicesSet(totalBatches);
        }
        LOGGER.info("Phase 2: Force reprocess all {} combinations (stale marker)", totalBatches);
      } else if (usePartitionedExpansion) {
        // Partitioned mode: skip Phase 1.5 and Phase 2 pre-filter.
        // Filtering happens lazily per-partition during Phase 5 to avoid
        // materializing all partition combinations at once.
        // neededCount is unknown until processing; use totalBatches as upper bound.
        neededCount = totalBatches;
        LOGGER.info("Partitioned mode: deferring filter to per-partition processing");
      } else {
        // Standard mode: Phase 1.5 + Phase 2 as before

        // Phase 1.5: Self-healing - rebuild cache from existing Iceberg data if needed.
        // Skip when the table was explicitly cleared — cleared means reprocess from scratch,
        // not self-heal from existing partitions (which scans all Iceberg manifests on S3).
        Set<Integer> prefilteredIndices = null;
        if (materializeConfig != null
            && materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG
            && materializeConfig.isEnabled()
            && verifyDataExists(pipelineName, config)
            && !incrementalTracker.wasTableCleared(pipelineName)) {
          prefilteredIndices = rebuildCacheFromIceberg(pipelineName, config, combinations);
        } else if (incrementalTracker.wasTableCleared(pipelineName)) {
          LOGGER.info("Pipeline '{}': tracker was explicitly cleared — "
              + "skipping Phase 1.5 manifest scan, treating all {} combinations as unprocessed",
              pipelineName, totalBatches);
        }

        // Phase 2: Bulk filter to find unprocessed combinations
        long filterStartMs = System.currentTimeMillis();
        if (prefilteredIndices != null && prefilteredIndices.size() < totalBatches) {
          standardUnprocessedIndices = prefilteredIndices;
          LOGGER.info("Phase 2: Reusing Phase 1.5 filter result: {} unprocessed of {} total",
              standardUnprocessedIndices.size(), totalBatches);
        } else {
          LOGGER.info("Phase 2: Bulk filtering {} combinations", totalBatches);
          standardUnprocessedIndices =
              incrementalTracker.filterUnprocessedWithEmptyTtl(
                  pipelineName, pipelineName, combinations, emptyResultTtlMillis);
        }
        long filterElapsedMs = System.currentTimeMillis() - filterStartMs;

        // Authoritative per-period skip: drop any combo whose latest period marker is
        // 'complete'. This makes a second identical run skip every already-done period
        // even if a per-combo TTL would otherwise re-queue it.
        removePeriodCompleteIndices(pipelineName, combinations, standardUnprocessedIndices);

        neededCount = standardUnprocessedIndices.size();
        skippedBatches = totalBatches - neededCount;
        LOGGER.info("Bulk filtering: {} unprocessed of {} total ({}ms, {}% cached)",
            neededCount, totalBatches, filterElapsedMs,
            totalBatches > 0 ? (skippedBatches * 100 / totalBatches) : 0);

        MemorySnapshot filterSnap = MemorySnapshot.capture("after_bulk_filter");
        memSnapshots.add(filterSnap);
        if (filterSnap.getUsedBytes() > peakUsedBytes) {
          peakUsedBytes = filterSnap.getUsedBytes();
        }
        LOGGER.debug("Memory after_bulk_filter: {}", filterSnap);
      }

      // If all combinations are already processed, mark complete and return
      if (neededCount == 0 && totalBatches > 0) {
        long cachedRowCount = 0;
        if (materializeConfig != null
            && materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG) {
          String tableLocation = baseDirectory + "/" + pipelineName;
          cachedRowCount = readRowCountFromIceberg(tableLocation);
        }
        // If we couldn't read from Iceberg (table not reachable / parquet format),
        // use the tracker's cached row count as the authoritative figure — it was set
        // at the prior successful commit.
        if (cachedRowCount == 0 && cached != null && cached.rowCount > 0) {
          cachedRowCount = cached.rowCount;
        }
        incrementalTracker.markTableCompleteWithConfig(pipelineName, configHash,
            dimensionSignature, cachedRowCount);
        // Per-period markers: every period is fully processed here (neededCount==0).
        // Standard mode only — partitioned mode keeps the full combo set out of memory
        // and relies on the per-combo incremental tracker (unchanged behavior).
        if (!usePartitionedExpansion) {
          markCompletedPeriods(pipelineName, combinations);
        }
        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("All {} combinations already processed - marking complete ({}ms, {} rows)",
            totalBatches, elapsed, cachedRowCount);
        return EtlResult.builder()
            .pipelineName(pipelineName)
            .totalRows(cachedRowCount)
            .successfulBatches(0)
            .skippedBatches(totalBatches)
            .skippedEntirePipeline(true)
            .elapsedMs(elapsed)
            .build();
      } else if (totalBatches == 0) {
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

      // Dataset-type gate (standard mode only, not partitioned):
      // snapshot → reduce to the single most-recent combination and never skip via
      //   period-complete (the open period always re-runs for a snapshot source).
      String datasetType = config.getDatasetType(); // "delta" by default
      if (!usePartitionedExpansion && "snapshot".equals(datasetType)
          && combinations != null && !combinations.isEmpty()) {
        // Keep only the first combo (most-recent — YEAR_RANGE emits descending)
        List<Map<String, String>> snapshotCombo =
            Collections.singletonList(combinations.get(0));
        combinations = snapshotCombo;
        totalBatches = 1;
        standardUnprocessedIndices = new HashSet<Integer>(Collections.singletonList(0));
        neededCount = 1;
        skippedBatches = 0;
        dimensionSignature = IncrementalTracker.computeDimensionSignature(combinations);
        LOGGER.info("dataset_type=snapshot: processing only the most-recent combination: {}",
            combinations.get(0));
      }

      // Phase 3: Create data source based on type
      LOGGER.info("Phase 3: Creating data source (type={})", config.getSourceType());
      DataSource dataSource = createDataSource(config);

      // Phase 3b: Freshness skip-gate (pre-download types only).
      // If freshness: is configured and the source hasn't changed since the last successful
      // commit, skip the fetch and the materialize — no new Iceberg snapshot is created.
      // hash-type is post-download and is handled after the fetch; deferred for now.
      FreshnessConfig freshnessConfig = config.getFreshness();
      if (freshnessConfig != null
          && freshnessConfig.getType() != FreshnessConfig.Type.HASH
          && dataSource instanceof HttpSource) {
        HttpSource httpSource = (HttpSource) dataSource;
        try {
          HttpSource.ProbeResult probeResult =
              httpSource.probe(freshnessConfig, Collections.<String, String>emptyMap());
          String currentToken = FreshnessCheck.token(
              freshnessConfig, probeResult.getHeaders(), probeResult.getBody(), null);
          String previousToken = incrementalTracker.getFreshnessToken(pipelineName);
          if (!FreshnessCheck.changed(previousToken, currentToken)) {
            long elapsed = System.currentTimeMillis() - startTime;
            LOGGER.info("Pipeline '{}': freshness check UNCHANGED (token={}) — skipping fetch "
                + "and commit ({}ms)", pipelineName, currentToken, elapsed);
            return EtlResult.builder()
                .pipelineName(pipelineName)
                .totalRows(0)
                .successfulBatches(0)
                .skippedBatches(totalBatches)
                .elapsedMs(elapsed)
                .build();
          }
          LOGGER.info("Pipeline '{}': freshness check CHANGED (prev={}, cur={}) — proceeding",
              pipelineName,
              previousToken == null ? "<none>" : previousToken,
              currentToken == null ? "<null>" : currentToken);
          // Capture so we can persist it after a successful commit
          probedFreshnessToken = currentToken;
        } catch (Exception e) {
          LOGGER.warn("Pipeline '{}': freshness probe failed ({}), proceeding with full fetch",
              pipelineName, e.getMessage());
          // Probe failure = treat as changed; don't skip
        }
      }

      // Phase 4: Create and initialize materialization writer
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
              .tableComment(materializeConfig.getTableComment())
              .columnComments(materializeConfig.getColumnComments())
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

      // Wire per-row effective year/month fields from dimension config into Iceberg writer.
      if (writer instanceof IcebergMaterializationWriter) {
        IcebergMaterializationWriter icebergWriter = (IcebergMaterializationWriter) writer;
        if (config.getDimensions() != null) {
          for (DimensionConfig dimConfig : config.getDimensions().values()) {
            if (dimConfig.getEffectiveYearField() != null) {
              icebergWriter.setEffectiveYearField(dimConfig.getEffectiveYearField());
            }
            if (dimConfig.getEffectiveMonthField() != null) {
              icebergWriter.setEffectiveMonthField(dimConfig.getEffectiveMonthField());
            }
          }
        }
      }

      // Phase 5: Process unprocessed dimension combinations
      LOGGER.info("Phase 5: Processing {} unprocessed batches (of {} total)", neededCount, totalBatches);
      if (progressListener != null) {
        progressListener.onPhaseStart("data_processing", neededCount);
      }

      int processedCount = 0;

      if (usePartitionedExpansion) {
        // Partitioned processing: lazily expand one partition at a time to bound memory.
        // Each partition's combinations are built, processed, then discarded before the next.
        List<String> contextValues = partitionPlan.getContextValues();
        int partCount = contextValues.size();
        int actualTotalBatches = 0;

        // Pre-query Iceberg for existing partitions to skip redundant regeneration.
        // If tracker says "not processed" but Iceberg already has the partition data,
        // mark combos as processed and skip them (avoids re-fetching + re-writing).
        Set<Map<String, String>> existingIcebergPartitions =
            java.util.Collections.emptySet();
        List<String> icebergPartitionColumns = Collections.emptyList();
        if (!forceReprocessAll && materializeConfig != null
            && materializeConfig.getFormat() == MaterializeConfig.Format.ICEBERG
            && materializeConfig.isEnabled()) {
          MaterializePartitionConfig partConfig = materializeConfig.getPartition();
          icebergPartitionColumns = partConfig != null
              ? partConfig.getColumns() : Collections.<String>emptyList();
          if (!icebergPartitionColumns.isEmpty()) {
            Map<String, Object> catalogConfig = buildIcebergCatalogConfig(materializeConfig);
            String targetTableId = materializeConfig.getTargetTableId();
            if (targetTableId == null || targetTableId.isEmpty()) {
              targetTableId = materializeConfig.getName();
            }
            if (targetTableId == null || targetTableId.isEmpty()) {
              targetTableId = config.getName();
            }
            LOGGER.info("Skip-if-materialized: querying Iceberg table '{}' for existing partitions",
                targetTableId);
            try {
              existingIcebergPartitions =
                  IcebergMaterializationWriter.getExistingPartitions(catalogConfig, targetTableId, icebergPartitionColumns);
            } catch (Exception e) {
              LOGGER.warn("Skip-if-materialized: failed to query Iceberg partitions for '{}': {}",
                  targetTableId, e.getMessage());
            }
            if (!existingIcebergPartitions.isEmpty()) {
              LOGGER.info("Found {} existing Iceberg partitions for skip-if-materialized check",
                  existingIcebergPartitions.size());
            } else {
              LOGGER.info("Skip-if-materialized: no existing partitions found for '{}'",
                  targetTableId);
            }
          }
        }

        for (int pi = 0; pi < partCount; pi++) {
          String contextValue = contextValues.get(pi);

          // Lazily expand this single partition (resolves CUSTOM dimension via S3)
          DimensionPartition partition =
              dimensionIterator.expandPartition(partitionPlan, contextValue);
          if (partition == null) {
            LOGGER.debug("Partition {}/{} ({}={}) resolved to empty, skipping",
                pi + 1, partCount, partitionPlan.getContextKey(), contextValue);
            continue;
          }
          List<Map<String, String>> partCombos = partition.getCombinations();
          actualTotalBatches += partCombos.size();

          // Filter unprocessed within this partition
          Set<Integer> unprocessedIndices;
          if (forceReprocessAll) {
            unprocessedIndices = allIndicesSet(partCombos.size());
          } else {
            unprocessedIndices =
                incrementalTracker.filterUnprocessedWithEmptyTtl(
                    pipelineName, pipelineName, partCombos, emptyResultTtlMillis);
            // NOTE: no per-period skip here. Partitioned mode does not write per-period
            // markers (a period spans partitions, so completeness can't be decided from
            // one partition's combos), and the per-combo incremental filter above is the
            // authority — keeping partitioned behavior identical to before this change.
          }

          if (unprocessedIndices.isEmpty()) {
            skippedBatches += partCombos.size();
            LOGGER.debug("Partition {}/{} ({}={}) fully processed ({} combos), skipping",
                pi + 1, partCount, partitionPlan.getContextKey(), contextValue,
                partCombos.size());
            continue;
          }

          // Skip-if-materialized: if tracker says unprocessed but Iceberg already has
          // the partition data, mark all combos as processed and skip regeneration.
          if (!existingIcebergPartitions.isEmpty() && !icebergPartitionColumns.isEmpty()
              && !unprocessedIndices.isEmpty()) {
            // Extract the Iceberg partition key from the first unprocessed combo
            Map<String, String> sampleCombo = partCombos.get(unprocessedIndices.iterator().next());
            Map<String, String> icebergKey = new LinkedHashMap<String, String>();
            for (String col : icebergPartitionColumns) {
              String val = sampleCombo.get(col);
              if (val != null) {
                icebergKey.put(col, val);
              }
            }
            if (existingIcebergPartitions.contains(icebergKey)) {
              // Iceberg already has this partition — mark all unprocessed combos as done
              int skippedCount = unprocessedIndices.size();
              for (int idx : unprocessedIndices) {
                incrementalTracker.markProcessedWithRowCount(
                    pipelineName, pipelineName, partCombos.get(idx), null, -1);
              }
              skippedBatches += partCombos.size();
              LOGGER.info("Partition {}/{} ({}={}): skipped {} combos — Iceberg partition {} "
                  + "already materialized",
                  pi + 1, partCount, partitionPlan.getContextKey(), contextValue,
                  skippedCount, icebergKey);
              continue;
            }
          }

          skippedBatches += partCombos.size() - unprocessedIndices.size();
          LOGGER.info("Partition {}/{} ({}={}): {} unprocessed of {} combinations",
              pi + 1, partCount, partitionPlan.getContextKey(), contextValue,
              unprocessedIndices.size(), partCombos.size());

          int partThreadCount = getParallelThreadCount();
          if (partThreadCount > 1 && unprocessedIndices.size() > 1) {
            // Parallel within partition: fetch concurrently, writes serialized via writeLock
            final AtomicLong partRows = new AtomicLong();
            final AtomicInteger partSucceeded = new AtomicInteger();
            final AtomicInteger partFailed = new AtomicInteger();
            final AtomicInteger partSkipped = new AtomicInteger();
            final List<String> partErrors = Collections.synchronizedList(new ArrayList<String>());
            final AtomicInteger partProcessed = new AtomicInteger(processedCount);

            final EtlPipelineConfig cfgFinal = config;
            final DataSource dsFinal = dataSource;
            final MaterializationWriter writerFinal = writer;
            final String pipelineNameFinal = pipelineName;
            final int neededCountFinal = neededCount;
            final int piFinal = pi;
            final int partCountFinal = partCount;
            final long startTimeFinal = startTime;

            ExecutorService partExecutor = Executors.newFixedThreadPool(partThreadCount);
            List<Future<Void>> partFutures = new ArrayList<Future<Void>>();

            for (final int idx : unprocessedIndices) {
              final Map<String, String> variables = partCombos.get(idx);

              partFutures.add(
                  partExecutor.submit(new Callable<Void>() {
                @Override public Void call() {
                  int currentBatch = partProcessed.incrementAndGet();
                  try {
                    LOGGER.info("Processing batch {} (partition {}/{}) {}: {}",
                        currentBatch, piFinal + 1, partCountFinal,
                        formatProgress(currentBatch, neededCountFinal, startTimeFinal),
                        variables);
                    long batchRows =
                        processSingleBatch(cfgFinal, variables, dsFinal, writerFinal, currentBatch, pipelineNameFinal);
                    partRows.addAndGet(batchRows);
                    partSucceeded.incrementAndGet();

                    if (currentBatch % 10 == 0) {
                      System.gc();
                    }
                  } catch (Exception e) {
                    String errorMsg =
                        String.format("Batch %d (partition %d/%d) failed: %s", currentBatch, piFinal + 1, partCountFinal, e.getMessage());
                    LOGGER.error(errorMsg, e);
                    partErrors.add(errorMsg);

                    EtlPipelineConfig.ErrorHandlingConfig.ErrorAction action =
                        determineErrorAction(e, cfgFinal.getErrorHandling());

                    synchronized (writeLock) {
                      incrementalTracker.invalidateTableCompletion(pipelineNameFinal);
                      switch (action) {
                        case SKIP:
                          partSkipped.incrementAndGet();
                          incrementalTracker.markProcessedWithError(pipelineNameFinal,
                              pipelineNameFinal, variables, null, e.getMessage());
                          break;
                        case WARN:
                        default:
                          partFailed.incrementAndGet();
                          incrementalTracker.markProcessedWithError(pipelineNameFinal,
                              pipelineNameFinal, variables, null, e.getMessage());
                          break;
                        case FAIL:
                          partFailed.incrementAndGet();
                          incrementalTracker.markProcessedWithError(pipelineNameFinal,
                              pipelineNameFinal, variables, null, e.getMessage());
                          break;
                      }
                    }
                  }
                  return null;
                }
              }));
            }

            for (Future<Void> f : partFutures) {
              try {
                f.get();
              } catch (Exception e) {
                LOGGER.error("Unexpected error in parallel partition batch: {}", e.getMessage());
              }
            }
            partExecutor.shutdown();

            totalRows += partRows.get();
            successfulBatches += partSucceeded.get();
            failedBatches += partFailed.get();
            skippedBatches += partSkipped.get();
            errors.addAll(partErrors);
            processedCount = partProcessed.get();

          } else {
            // Sequential within partition (original behavior)
            for (int idx : unprocessedIndices) {
              Map<String, String> variables = partCombos.get(idx);
              processedCount++;

              // Self-heal: if data files already exist on storage for this partition,
              // re-register them in Iceberg and skip re-fetching from source.
              long healedRows = trySelfHealFromStoredFiles(writer, variables);
              if (healedRows > 0) {
                LOGGER.info("Self-heal: skipping source fetch for partition {}/{} {} — "
                    + "re-registered {} orphaned files (~{} rows)",
                    pi + 1, partCount, variables, healedRows, healedRows);
                incrementalTracker.markProcessedWithRowCount(pipelineName, pipelineName,
                    variables, null, healedRows);
                skippedBatches++;
                totalRows += healedRows;
                continue;
              }

              if (progressListener != null) {
                progressListener.onBatchStart(processedCount, neededCount, variables);
              }

              try {
                LOGGER.info("Processing batch {} (partition {}/{}) {}: {}",
                    processedCount, pi + 1, partCount,
                    formatProgress(processedCount, neededCount, startTime),
                    variables);
                long batchRows =
                    processSingleBatch(config, variables, dataSource, writer, processedCount, pipelineName);
                totalRows += batchRows;
                successfulBatches++;
                consecutiveFailures = 0;

                if (progressListener != null) {
                  progressListener.onBatchComplete(processedCount, neededCount, (int) batchRows, null);
                }

                if (processedCount % 10 == 0) {
                  System.gc();
                  MemorySnapshot batchSnap =
                      MemorySnapshot.capture("batch_" + processedCount);
                  memSnapshots.add(batchSnap);
                  if (batchSnap.getUsedBytes() > peakUsedBytes) {
                    peakUsedBytes = batchSnap.getUsedBytes();
                  }
                  LOGGER.debug("Memory {}: {}", batchSnap.getPhase(), batchSnap);
                }

              } catch (SkippedBatchException e) {
                skippedBatches++;
                consecutiveFailures = 0;
                LOGGER.debug("Batch {} skipped (skipOn match): {}", processedCount, e.getMessage());
              } catch (Exception e) {
                consecutiveFailures++;
                String errorMsg =
                    String.format("Batch %d (partition %d/%d) failed: %s",
                        processedCount, pi + 1, partCount, e.getMessage());
                LOGGER.error(errorMsg, e);
                errors.add(errorMsg);
                incrementalTracker.invalidateTableCompletion(pipelineName);

                if (consecutiveFailures >= maxConsecutiveFailures) {
                  LOGGER.error("Aborting table '{}': {} consecutive failures — "
                      + "data source appears unreachable (last error: {})",
                      pipelineName, consecutiveFailures, e.getMessage());
                  incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                      variables, null, e.getMessage());
                  throw new IOException("Aborting after " + consecutiveFailures
                      + " consecutive failures", e);
                }

                EtlPipelineConfig.ErrorHandlingConfig.ErrorAction action =
                    determineErrorAction(e, config.getErrorHandling());

                switch (action) {
                  case FAIL:
                    incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                        variables, null, e.getMessage());
                    throw new IOException("Pipeline failed at batch " + processedCount, e);
                  case SKIP:
                    skippedBatches++;
                    incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                        variables, null, e.getMessage());
                    LOGGER.warn("Skipping batch {} due to error (will retry after TTL): {}",
                        processedCount, e.getMessage());
                    break;
                  case WARN:
                    failedBatches++;
                    incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                        variables, null, e.getMessage());
                    LOGGER.warn("Batch {} failed (will retry after TTL): {}",
                        processedCount, e.getMessage());
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
          }
          // Partition's combinations are now eligible for GC
          LOGGER.debug("Partition {}/{} complete, {} combos eligible for GC",
              pi + 1, partCount, partCombos.size());
        }
        // Update totalBatches with actual count now that all partitions have been expanded
        totalBatches = actualTotalBatches;
      } else {
        // Standard (non-partitioned) processing
        int threadCount = getParallelThreadCount();

        if (threadCount > 1 && neededCount > 1) {
          // Parallel mode: fetch data concurrently, writes are serialized via writeLock
          LOGGER.info("Using {} parallel threads for {} batches", threadCount, neededCount);

          // Build work list of variables for unprocessed batches
          final List<Map<String, String>> workVariables = new ArrayList<Map<String, String>>();
          for (int idx = 0; idx < combinations.size(); idx++) {
            if (!standardUnprocessedIndices.contains(idx)) {
              continue;
            }
            workVariables.add(combinations.get(idx));
          }

          final AtomicLong parallelTotalRows = new AtomicLong();
          final AtomicInteger parallelSucceeded = new AtomicInteger();
          final AtomicInteger parallelFailed = new AtomicInteger();
          final AtomicInteger parallelSkipped = new AtomicInteger();
          final List<String> parallelErrors = Collections.synchronizedList(new ArrayList<String>());
          final AtomicInteger parallelProcessed = new AtomicInteger();

          // Capture effectively-final copies for use in inner class
          final EtlPipelineConfig cfgFinal = config;
          final DataSource dsFinal = dataSource;
          final MaterializationWriter writerFinal = writer;
          final String pipelineNameFinal = pipelineName;
          final int neededCountFinal = neededCount;

          ExecutorService executor = Executors.newFixedThreadPool(threadCount);
          List<Future<Void>> futures = new ArrayList<Future<Void>>();

          for (int wi = 0; wi < workVariables.size(); wi++) {
            final Map<String, String> vars = workVariables.get(wi);

            futures.add(
                executor.submit(new Callable<Void>() {
              @Override public Void call() {
                int currentBatch = parallelProcessed.incrementAndGet();
                try {
                  LOGGER.info("Processing batch {}/{}: {}", currentBatch, neededCountFinal, vars);
                  long batchRows =
                      processSingleBatch(cfgFinal, vars, dsFinal, writerFinal, currentBatch, pipelineNameFinal);
                  parallelTotalRows.addAndGet(batchRows);
                  parallelSucceeded.incrementAndGet();

                  if (currentBatch % 10 == 0) {
                    System.gc();
                  }
                } catch (Exception e) {
                  String errorMsg =
                      String.format("Batch %d/%d failed: %s", currentBatch, neededCountFinal, e.getMessage());
                  LOGGER.error(errorMsg, e);
                  parallelErrors.add(errorMsg);

                  EtlPipelineConfig.ErrorHandlingConfig.ErrorAction action =
                      determineErrorAction(e, cfgFinal.getErrorHandling());

                  // Serialize tracker writes for error marking
                  synchronized (writeLock) {
                    incrementalTracker.invalidateTableCompletion(pipelineNameFinal);
                    switch (action) {
                      case SKIP:
                        parallelSkipped.incrementAndGet();
                        incrementalTracker.markProcessedWithError(pipelineNameFinal,
                            pipelineNameFinal, vars, null, e.getMessage());
                        break;
                      case WARN:
                      default:
                        parallelFailed.incrementAndGet();
                        incrementalTracker.markProcessedWithError(pipelineNameFinal,
                            pipelineNameFinal, vars, null, e.getMessage());
                        break;
                      case FAIL:
                        incrementalTracker.markProcessedWithError(pipelineNameFinal,
                            pipelineNameFinal, vars, null, e.getMessage());
                        // FAIL action in parallel mode — logged but doesn't abort other threads
                        parallelFailed.incrementAndGet();
                        break;
                    }
                  }
                }
                return null;
              }
            }));
          }

          // Wait for all tasks
          for (Future<Void> f : futures) {
            try {
              f.get();
            } catch (Exception e) {
              LOGGER.error("Unexpected error in parallel batch: {}", e.getMessage(), e);
            }
          }
          executor.shutdown();

          totalRows += parallelTotalRows.get();
          successfulBatches += parallelSucceeded.get();
          failedBatches += parallelFailed.get();
          skippedBatches += parallelSkipped.get();
          errors.addAll(parallelErrors);
          processedCount += parallelProcessed.get();

        } else {
          // Sequential mode (original behavior)
          for (int idx = 0; idx < combinations.size(); idx++) {
            if (!standardUnprocessedIndices.contains(idx)) {
              continue;
            }

            Map<String, String> variables = combinations.get(idx);
            processedCount++;

            // Self-heal: if data files already exist on storage for this partition,
            // re-register them in Iceberg and skip re-fetching from source.
            long healedRows = trySelfHealFromStoredFiles(writer, variables);
            if (healedRows > 0) {
              LOGGER.info("Self-heal: skipping source fetch for batch {}/{} {} — "
                  + "re-registered orphaned files (~{} rows)",
                  processedCount, neededCount, variables, healedRows);
              incrementalTracker.markProcessedWithRowCount(pipelineName, pipelineName,
                  variables, null, healedRows);
              skippedBatches++;
              totalRows += healedRows;
              continue;
            }

            if (progressListener != null) {
              progressListener.onBatchStart(processedCount, neededCount, variables);
            }

            try {
              LOGGER.info("Processing batch {}/{}: {}", processedCount, neededCount, variables);
              long batchRows =
                  processSingleBatch(config, variables, dataSource, writer, processedCount, pipelineName);
              totalRows += batchRows;
              successfulBatches++;
              consecutiveFailures = 0;

              if (progressListener != null) {
                progressListener.onBatchComplete(processedCount, neededCount, (int) batchRows, null);
              }

              if (processedCount % 10 == 0) {
                System.gc();
                MemorySnapshot batchSnap =
                    MemorySnapshot.capture("batch_" + processedCount);
                memSnapshots.add(batchSnap);
                if (batchSnap.getUsedBytes() > peakUsedBytes) {
                  peakUsedBytes = batchSnap.getUsedBytes();
                }
                LOGGER.debug("Memory {}: {}", batchSnap.getPhase(), batchSnap);
              }

            } catch (SkippedBatchException e) {
              skippedBatches++;
              consecutiveFailures = 0;
              LOGGER.debug("Batch {} skipped (skipOn match): {}", processedCount, e.getMessage());
            } catch (Exception e) {
              consecutiveFailures++;
              String errorMsg =
                  String.format("Batch %d/%d failed: %s", processedCount, neededCount, e.getMessage());

              // Enhanced diagnostics for HTTP/S3 errors
              String msg = e.getMessage();
              boolean isHttpError = msg != null && (msg.contains("HTTP") || msg.contains("404") || msg.contains("<!doctype")
                  || msg.contains("<html") || msg.contains("console.log") || msg.contains("adobe-launch"));

              if (isHttpError) {
                LOGGER.error("Batch {}/{} failed with HTTP/API error. Variables: {}. Full cause chain:",
                    processedCount, neededCount, variables);
                // Log full exception with cause chain
                Throwable cause = e;
                int depth = 0;
                while (cause != null && depth < 5) {
                  String causeName = cause.getClass().getSimpleName();
                  String causeMsg = cause.getMessage() != null ? cause.getMessage() : "(no message)";
                  LOGGER.error("  [{}] {}: {}", depth, causeName, causeMsg);
                  cause = cause.getCause();
                  depth++;
                }
              } else {
                LOGGER.error(errorMsg, e);
              }
              errors.add(errorMsg);
              incrementalTracker.invalidateTableCompletion(pipelineName);

              if (consecutiveFailures >= maxConsecutiveFailures) {
                LOGGER.error("Aborting table '{}': {} consecutive failures — "
                    + "data source appears unreachable (last error: {})",
                    pipelineName, consecutiveFailures, e.getMessage());
                incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                    variables, null, e.getMessage());
                throw new IOException("Aborting after " + consecutiveFailures
                    + " consecutive failures", e);
              }

              EtlPipelineConfig.ErrorHandlingConfig.ErrorAction action =
                  determineErrorAction(e, config.getErrorHandling());

              switch (action) {
                case FAIL:
                  incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                      variables, null, e.getMessage());
                  throw new IOException("Pipeline failed at batch " + processedCount, e);
                case SKIP:
                  skippedBatches++;
                  incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                      variables, null, e.getMessage());
                  LOGGER.warn("Skipping batch {} due to error (will retry after TTL): {}",
                      processedCount, e.getMessage());
                  break;
                case WARN:
                  failedBatches++;
                  incrementalTracker.markProcessedWithError(pipelineName, pipelineName,
                      variables, null, e.getMessage());
                  LOGGER.warn("Batch {} failed (will retry after TTL): {}",
                      processedCount, e.getMessage());
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
      if (dataSource != null) {
        dataSource.close();
      }

      // Mark table as complete if all batches succeeded without errors
      if (failedBatches == 0 && errors.isEmpty()) {
        incrementalTracker.markTableCompleteWithConfig(pipelineName, configHash, dimensionSignature, totalRows);
        LOGGER.info("Marked pipeline '{}' as complete with configHash={}, signature={}, rows={}",
            pipelineName, configHash, dimensionSignature, totalRows);
        // Persist the freshness token so the next run can skip if unchanged.
        // Only written on a clean run (no failed batches) to avoid caching a partial state.
        if (probedFreshnessToken != null) {
          incrementalTracker.putFreshnessToken(pipelineName, probedFreshnessToken);
          LOGGER.info("Pipeline '{}': persisted freshness token after clean commit: {}",
              pipelineName, probedFreshnessToken);
        }
      }
      // Per-period markers: mark each period whose full combo set is now processed,
      // even if OTHER periods in this table failed — markCompletedPeriods self-guards
      // via the per-combo tracker, so a period with any unprocessed combo is never marked.
      // Standard mode only (partitioned keeps combos out of memory; per-combo tracker
      // remains its authority). It flushes pending marks first so this run's writes are seen.
      if (!usePartitionedExpansion) {
        markCompletedPeriods(pipelineName, combinations);
      }

      long elapsed = System.currentTimeMillis() - startTime;

      MemorySnapshot endSnap = MemorySnapshot.capture("pipeline_end");
      memSnapshots.add(endSnap);
      if (endSnap.getUsedBytes() > peakUsedBytes) {
        peakUsedBytes = endSnap.getUsedBytes();
      }
      LOGGER.info("ETL pipeline '{}' complete: {} rows, {} successful, {} failed, {} skipped in {}ms, peakHeap={}MB",
          pipelineName, totalRows, successfulBatches, failedBatches, skippedBatches, elapsed,
          peakUsedBytes / (1024 * 1024));

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
          .peakUsedBytes(peakUsedBytes)
          .memorySnapshots(memSnapshots)
          .build();

    } catch (Exception e) {
      long elapsed = System.currentTimeMillis() - startTime;
      String errorMsg =
          String.format("ETL pipeline '%s' failed after %dms: %s", pipelineName, elapsed, e.getMessage());
      LOGGER.error(errorMsg, e);

      MemorySnapshot failSnap = MemorySnapshot.capture("pipeline_failure");
      memSnapshots.add(failSnap);
      if (failSnap.getUsedBytes() > peakUsedBytes) {
        peakUsedBytes = failSnap.getUsedBytes();
      }

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
          .peakUsedBytes(peakUsedBytes)
          .memorySnapshots(memSnapshots)
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
      IncrementalTracker tracker,
      String pipelineName) throws IOException {

    Map<String, String> fieldMappings = partitionConfig.getFields();
    LOGGER.info("Response partitioning enabled with fields: {}", fieldMappings);

    // Check for year filtering
    final boolean hasYearFilter = partitionConfig.hasYearFilter();
    final String yearField = partitionConfig.getYearField();
    if (hasYearFilter) {
      LOGGER.info("Year filter enabled: field={}, range={}-{}",
          yearField, partitionConfig.getYearStart(), partitionConfig.getYearEnd());
    }

    // Lazy year-filter iterator — streams rows directly into writeBatch without pre-buffering.
    // writeBatch chunks at batchSize (default 10k) so the full dataset never lives in heap.
    final int[] counts = {0, 0}; // [totalRows, filteredRows]
    final Iterator<Map<String, Object>> source = data;
    Iterator<Map<String, Object>> filtered = new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending = null;
      private boolean fetched = false;

      private void prefetch() {
        if (fetched) {
          return;
        }
        fetched = true;
        pending = null;
        while (source.hasNext()) {
          Map<String, Object> row = source.next();
          counts[0]++;
          if (hasYearFilter) {
            Object val = row.get(yearField);
            if (!partitionConfig.isYearInRange(val)) {
              counts[1]++;
              continue;
            }
          }
          pending = row;
          return;
        }
      }

      @Override public boolean hasNext() {
        prefetch();
        return pending != null;
      }

      @Override public Map<String, Object> next() {
        prefetch();
        if (pending == null) {
          throw new NoSuchElementException();
        }
        fetched = false;
        Map<String, Object> result = pending;
        pending = null;
        return result;
      }
    };

    if (!filtered.hasNext()) {
      LOGGER.info("No rows to write after year filtering (total scanned: {})", counts[0]);
      tracker.markProcessedWithRowCount(pipelineName, pipelineName, urlVariables, null, 0);
      return 0;
    }

    // Stream rows through writeBatch — DuckDB PARTITION_BY handles physical partitioning.
    // writeBatch chunks internally so heap usage is bounded by batchSize, not dataset size.
    long writtenRows = writer.writeBatch(filtered, urlVariables);

    if (counts[1] > 0) {
      LOGGER.info("Response partitioning: scanned={}, filtered={}, written={}",
          counts[0], counts[1], writtenRows);
    } else {
      LOGGER.info("Response partitioning complete: {} rows written", writtenRows);
    }

    // Track at URL dimension level (e.g., indicator), not every partition combination
    tracker.markProcessedWithRowCount(pipelineName, pipelineName, urlVariables, null, writtenRows);

    return writtenRows;
  }

  /**
   * Processes a single batch: fetches data and writes via the materialization writer.
   * Extracted to avoid duplicating the fetch/write/track logic across partitioned and
   * non-partitioned code paths.
   *
   * @return Number of rows written
   */
  /** Returns a compact rate+ETA string for progress log lines, e.g. {@code "at 73.2/s ~1h24m remaining"}. */
  private static String formatProgress(int done, int total, long startTimeMs) {
    if (done <= 0) {
      return "";
    }
    long elapsedMs = System.currentTimeMillis() - startTimeMs;
    if (elapsedMs < 1000) {
      return "";
    }
    double rate = (double) done / (elapsedMs / 1000.0);
    int remaining = total - done;
    long etaSeconds = remaining > 0 && rate > 0 ? (long) (remaining / rate) : 0;

    String rateStr = rate >= 10 ? String.format("%.0f/s", rate) : String.format("%.1f/s", rate);
    String etaStr;
    if (etaSeconds <= 0) {
      etaStr = "done";
    } else if (etaSeconds < 60) {
      etaStr = etaSeconds + "s remaining";
    } else if (etaSeconds < 3600) {
      etaStr = (etaSeconds / 60) + "m remaining";
    } else {
      long h = etaSeconds / 3600;
      long m = (etaSeconds % 3600) / 60;
      etaStr = m > 0 ? h + "h" + m + "m remaining" : h + "h remaining";
    }
    return "at " + rateStr + " ~" + etaStr;
  }

  /** Tracker key suffix for the computed_delta high-water mark. */
  private static final String COMPUTED_DELTA_HWM_SUFFIX = "::computed_delta_hwm";

  private long processSingleBatch(EtlPipelineConfig config, Map<String, String> variables,
      DataSource dataSource, MaterializationWriter writer,
      int processedCount, String pipelineName) throws IOException {

    // Enrich variables with period_start / period_end when backfill_period is set.
    // This is a delta-only feature; snapshot/computed_delta don't use it, but the
    // enrichment is a no-op if backfillPeriod is null (which it is for non-delta tables).
    variables = enrichWithPeriodBounds(variables, config.getBackfillPeriod());

    // Document sources use dataWriter directly — serialize writes
    if (EtlPipelineConfig.SOURCE_TYPE_DOCUMENT.equals(config.getSourceType())) {
      LOGGER.info("Document source - using custom DataWriter for processing");
      if (dataWriter != null) {
        synchronized (writeLock) {
          long batchRows = dataWriter.write(config, null, variables);
          incrementalTracker.markProcessedWithRowCount(
              pipelineName, pipelineName, variables, null, batchRows);
          return batchRows;
        }
      } else {
        LOGGER.warn("Document source requires custom DataWriter - skipping batch");
        return 0;
      }
    }

    // Fetch data — this is the expensive network I/O that benefits from parallelism.
    // Multiple threads can fetch concurrently; writes are serialized below.
    Iterator<Map<String, Object>> data = null;
    if (dataProvider != null) {
      data = dataProvider.fetch(config, variables);
      if (data != null) {
        LOGGER.debug("Using custom DataProvider for batch {}", processedCount);
      }
    }
    if (data == null) {
      data = dataSource.fetch(variables);
    }

    // computed_delta: filter rows to only those whose modifiedField advanced since the
    // last stored high-water mark.  The HWM is stored via the freshness-token slot
    // so it persists across runs without a new tracker abstraction.
    // NOTE: this materialises the iterator into memory when computed_delta is active
    // so we can capture max(modifiedField) before writing. For very large payloads
    // this is a deliberate trade-off (the whole dump is being pulled anyway).
    final String computedDeltaHwmKey = pipelineName + COMPUTED_DELTA_HWM_SUFFIX;
    String newComputedDeltaHwm = null; // set below if computed_delta active + rows filtered
    if ("computed_delta".equals(config.getDatasetType())) {
      String modifiedField = config.getModifiedField();
      String prevHwm = incrementalTracker.getFreshnessToken(computedDeltaHwmKey);
      LOGGER.info("computed_delta: modifiedField={}, prevHwm={}", modifiedField, prevHwm);

      if (modifiedField != null && !modifiedField.isEmpty()) {
        // Materialise the full pull, filter to changed rows, track new HWM
        List<Map<String, Object>> allRows = new ArrayList<Map<String, Object>>();
        while (data.hasNext()) {
          allRows.add(data.next());
        }
        List<Map<String, Object>> changedRows = new ArrayList<Map<String, Object>>();
        String maxSeen = prevHwm;
        for (Map<String, Object> row : allRows) {
          Object modVal = row.get(modifiedField);
          String modStr = modVal == null ? null : String.valueOf(modVal);
          // Track max modified
          if (modStr != null && (maxSeen == null || modStr.compareTo(maxSeen) > 0)) {
            maxSeen = modStr;
          }
          // Include row if modified is newer than prevHwm (or if this is first run)
          if (prevHwm == null || modStr == null || modStr.compareTo(prevHwm) > 0) {
            changedRows.add(row);
          }
        }
        LOGGER.info("computed_delta: {} of {} rows changed (maxModified={})",
            changedRows.size(), allRows.size(), maxSeen);
        data = changedRows.iterator();
        newComputedDeltaHwm = maxSeen; // will be persisted after the write
      } else {
        // No modifiedField configured: write all rows (full-upsert fallback)
        LOGGER.info("computed_delta: no modifiedField configured — writing all rows (full upsert)");
      }
    }

    // Serialize all writer and tracker operations to prevent concurrent DuckDB access.
    // Data is streamed directly — no pre-buffering; writeWithResponsePartitioning filters lazily.
    final String finalNewComputedDeltaHwm = newComputedDeltaHwm;
    synchronized (writeLock) {
      HttpSourceConfig sourceConfig = config.getSource();
      boolean hasResponsePartitioning = sourceConfig != null
          && sourceConfig.hasResponsePartitioning();

      long batchRows;
      if (hasResponsePartitioning) {
        batchRows =
            writeWithResponsePartitioning(data, variables,
            sourceConfig.getResponsePartitioning(),
            writer, incrementalTracker, pipelineName);
      } else {
        if (dataWriter != null) {
          batchRows = dataWriter.write(config, data, variables);
          if (batchRows >= 0) {
            LOGGER.debug("Used custom DataWriter for batch {}: {} rows", processedCount, batchRows);
          } else {
            batchRows = writer.writeBatch(data, variables);
          }
        } else {
          batchRows = writer.writeBatch(data, variables);
        }
        incrementalTracker.markProcessedWithRowCount(
            pipelineName, pipelineName, variables, null, batchRows);
        // Persist computed_delta HWM after a successful write
        if (finalNewComputedDeltaHwm != null) {
          incrementalTracker.putFreshnessToken(computedDeltaHwmKey, finalNewComputedDeltaHwm);
          LOGGER.info("computed_delta: persisted HWM={} for pipeline '{}'",
              finalNewComputedDeltaHwm, pipelineName);
        }
      }

      LOGGER.debug("Wrote {} rows for batch {}", batchRows, processedCount);
      return batchRows;
    }
  }

  /**
   * Determines the error action based on the exception type.
   */
  private EtlPipelineConfig.ErrorHandlingConfig.ErrorAction determineErrorAction(
      Throwable e, EtlPipelineConfig.ErrorHandlingConfig errorHandling) {

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
  protected DataSource createDataSource(EtlPipelineConfig config) {
    String sourceType = config.getSourceType();

    if (EtlPipelineConfig.SOURCE_TYPE_CONSTANTS.equals(sourceType)) {
      LOGGER.info("Creating ConstantsSource for type: {}", sourceType);
      return ConstantsSource.fromMap(config.getRawSourceConfig());
    }

    if (EtlPipelineConfig.SOURCE_TYPE_FILE.equals(sourceType)) {
      LOGGER.info("Creating FileSource for type: {}", sourceType);
      FileSourceConfig fileConfig = FileSourceConfig.fromMap(config.getRawSourceConfig());
      return new FileSource(fileConfig, sourceStorageProvider);
    }

    if (EtlPipelineConfig.SOURCE_TYPE_DOCUMENT.equals(sourceType)) {
      LOGGER.info("Creating DocumentSource for type: {}", sourceType);
      // Document sources use DocumentETLProcessor which writes files directly
      // Return null to indicate no standard data fetching needed
      return null;
    }

    // Default to HTTP source
    // Pass sourceStorageProvider and rawCachePath for persistent response caching
    HttpSourceConfig sourceConfig = config.getSource();
    String rawCachePath = null;
    HttpSourceConfig.RawCacheConfig rawCacheConfig = sourceConfig.getRawCache();
    if (rawCacheConfig.isEnabled()) {
      // Build raw cache path: just use table name as relative path
      // (or sharedKey when multiple pipelines need to reuse one upstream fetch).
      // The sourceStorageProvider has its base path configured (e.g., s3://bucket/raw/)
      // so files go to {baseS3Path}/{tableName}/{partitionKey}/response.json
      rawCachePath = rawCacheConfig.getSharedKey() != null
          ? rawCacheConfig.getSharedKey()
          : config.getName();
      LOGGER.info("Creating HttpSource with raw cache: {} (via {})",
          rawCachePath, sourceStorageProvider.getStorageType());
    } else {
      LOGGER.info("Creating HttpSource for type: {}", sourceType);
    }
    // Use sourceStorageProvider for raw cache (not the materialized storage provider)
    return new HttpSource(sourceConfig, config.getHooks(), sourceStorageProvider, rawCachePath,
        operatingDirectory);
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
   * @return The filterUnprocessed result from the quick check (for reuse by Phase 2),
   *         or null if the method returned early without filtering
   */
  private Set<Integer> rebuildCacheFromIceberg(String pipelineName, EtlPipelineConfig config,
      List<Map<String, String>> combinations) {

    // Quick check: if cache already has data, skip rebuild
    Set<Integer> unprocessed =
        incrementalTracker.filterUnprocessed(pipelineName, pipelineName, combinations);
    if (unprocessed.size() < combinations.size()) {
      LOGGER.debug("Cache has {} processed entries, skipping Iceberg rebuild",
          combinations.size() - unprocessed.size());
      return unprocessed;
    }

    // Cache is empty - check if Iceberg table has data we can use
    MaterializeConfig materializeConfig = config.getMaterialize();
    if (materializeConfig == null || !materializeConfig.isEnabled()) {
      return unprocessed;
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
      return unprocessed;
    }

    // Build catalog config for querying Iceberg metadata
    Map<String, Object> catalogConfig = buildIcebergCatalogConfig(materializeConfig);

    // Query existing partitions from Iceberg table
    Set<Map<String, String>> existingPartitions =
        IcebergMaterializationWriter.getExistingPartitions(catalogConfig, targetTableId, partitionColumns);

    if (existingPartitions.isEmpty()) {
      LOGGER.debug("No existing partitions in Iceberg table '{}', no cache rebuild needed",
          targetTableId);
      return unprocessed;
    }

    // Verify existing partitions are not stale - they should be a subset of expected combinations
    // Convert combinations to a set for efficient lookup
    Set<Map<String, String>> expectedSet =
        new HashSet<Map<String, String>>(combinations);
    Set<Map<String, String>> stalePartitions =
        new HashSet<Map<String, String>>();

    for (Map<String, String> existing : existingPartitions) {
      // Extract only the partition columns we care about for comparison
      Map<String, String> partitionOnly = new LinkedHashMap<String, String>();
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
    Set<Map<String, String>> rebuildable =
        new HashSet<Map<String, String>>();
    for (Map<String, String> existing : existingPartitions) {
      Map<String, String> partitionOnly = new LinkedHashMap<String, String>();
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
      return unprocessed;
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

    // Re-filter after rebuild so Phase 2 can reuse the result
    return incrementalTracker.filterUnprocessed(pipelineName, pipelineName, combinations);
  }

  /**
   * Builds Iceberg catalog configuration from MaterializeConfig.
   */
  private Map<String, Object> buildIcebergCatalogConfig(MaterializeConfig materializeConfig) {
    Map<String, Object> catalogConfig = new HashMap<String, Object>();

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
      warehousePath = StorageProviderFactory.normalizeForHadoop(warehousePath);
      catalogConfig.put("warehousePath", warehousePath);
    } else {
      catalogConfig.put("catalog", "hadoop");
      String warehousePath = baseDirectory;
      warehousePath = StorageProviderFactory.normalizeForHadoop(warehousePath);
      catalogConfig.put("warehousePath", warehousePath);
    }

    // Add S3 credentials from storage provider
    Map<String, String> s3Config = storageProvider != null ? storageProvider.getS3Config() : null;
    if (s3Config != null && !s3Config.isEmpty()) {
      Map<String, String> hadoopConfig = new HashMap<String, String>();
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
   * @param size number of indices
   * @return set of all indices from 0 to size-1
   */
  private static Set<Integer> allIndicesSet(int size) {
    Set<Integer> all = new HashSet<Integer>();
    for (int i = 0; i < size; i++) {
      all.add(i);
    }
    return all;
  }

  /**
   * Writes a {@code complete} per-period marker for every period whose ENTIRE combo set
   * (all non-period partition dimensions, e.g. every {@code geography}) is processed.
   *
   * <p>The marker is period-LEVEL, not per-batch: a period is marked complete only when
   * the per-combo {@code incremental} tracker (the fine-grained authority, which includes
   * non-period dimensions) shows zero unprocessed combos for that period. This prevents
   * over-marking tables that fan a period across many non-period combos.
   *
   * <p>Only canonical-period combos participate; all-NA combos are left to the per-combo
   * tracker (their NA key would collide across the whole pipeline).
   *
   * @param pipelineName  the pipeline name
   * @param allCombinations every combination for the pipeline (the full period+partition set)
   */
  private void markCompletedPeriods(String pipelineName,
      List<Map<String, String>> allCombinations) {
    if (allCombinations == null || allCombinations.isEmpty()) {
      return;
    }
    // Flush buffered per-combo marks so filterUnprocessed (an S3 read) observes THIS run's
    // just-written marks; otherwise write-behind buffering hides them and no period is
    // promoted to complete.
    incrementalTracker.flushPending();
    // Group combo indices by period key (canonical periods only).
    Map<String, List<Map<String, String>>> byPeriod =
        new LinkedHashMap<String, List<Map<String, String>>>();
    for (Map<String, String> combo : allCombinations) {
      if (!IncrementalTracker.hasCanonicalPeriod(combo)) {
        continue;
      }
      String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, combo);
      List<Map<String, String>> group = byPeriod.get(periodKey);
      if (group == null) {
        group = new ArrayList<Map<String, String>>();
        byPeriod.put(periodKey, group);
      }
      group.add(combo);
    }
    int marked = 0;
    for (Map.Entry<String, List<Map<String, String>>> e : byPeriod.entrySet()) {
      List<Map<String, String>> group = e.getValue();
      // A period is complete iff EVERY one of its combos is individually processed per the
      // fine-grained per-combo tracker (which keys on the full combo incl. non-period dims
      // such as geography). isProcessed reads the authoritative per-combo source_key state.
      boolean allDone = true;
      for (Map<String, String> combo : group) {
        if (!incrementalTracker.isProcessed(pipelineName, pipelineName, combo)) {
          allDone = false;
          break;
        }
      }
      if (allDone) {
        incrementalTracker.markPeriodComplete(pipelineName, group.get(0));
        marked++;
      }
    }
    if (marked > 0) {
      LOGGER.info("Per-period markers: marked {} of {} periods complete for '{}'",
          marked, byPeriod.size(), pipelineName);
    }
  }

  /**
   * Removes from {@code unprocessedIndices} any combination whose period is already
   * marked complete (authoritative per-period skip). Periods are deduplicated so each
   * distinct period key is checked once even when many combos share it.
   *
   * @param pipelineName       the pipeline name
   * @param combinations       all combinations (indexed by the set entries)
   * @param unprocessedIndices mutable set of indices considered unprocessed; pruned in place
   */
  private void removePeriodCompleteIndices(String pipelineName,
      List<Map<String, String>> combinations, Set<Integer> unprocessedIndices) {
    if (unprocessedIndices.isEmpty()) {
      return;
    }
    // Cache the per-period decision so duplicate periods (e.g. many combos in one year)
    // trigger a single tracker check.
    Map<String, Boolean> periodDecision = new HashMap<String, Boolean>();
    Iterator<Integer> it = unprocessedIndices.iterator();
    int removed = 0;
    while (it.hasNext()) {
      int idx = it.next();
      Map<String, String> combo = combinations.get(idx);
      // Non-canonical combos (all-NA period key) are not period-tracked — leave them
      // to the per-combo incremental filter so we never over-skip them.
      if (!IncrementalTracker.hasCanonicalPeriod(combo)) {
        continue;
      }
      String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, combo);
      Boolean complete = periodDecision.get(periodKey);
      if (complete == null) {
        complete = incrementalTracker.isPeriodComplete(pipelineName, combo);
        periodDecision.put(periodKey, complete);
      }
      if (complete) {
        it.remove();
        removed++;
      }
    }
    if (removed > 0) {
      LOGGER.info("Per-period skip: dropped {} of {} combos already marked complete for '{}'",
          removed, removed + unprocessedIndices.size(), pipelineName);
    }
  }

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
        // For Iceberg, check data directory exists — metadata dir alone means an empty table
        // (current-snapshot-id: -1) created but never written to, which counts as no data.
        // When warehousePath is configured, Iceberg stores data there, not baseDirectory.
        String icebergBase = baseDirectory;
        if (materializeConfig.getIceberg() != null
            && materializeConfig.getIceberg().getWarehousePath() != null
            && !materializeConfig.getIceberg().getWarehousePath().isEmpty()) {
          icebergBase = materializeConfig.getIceberg().getWarehousePath();
        }
        String dataPath = icebergBase + "/" + pipelineName + "/data";
        if (storageProvider.isDirectory(dataPath)) {
          LOGGER.debug("Verified Iceberg data exists at {}", dataPath);
          return true;
        }
        if (!icebergBase.equals(baseDirectory)) {
          String fallbackPath = baseDirectory + "/" + pipelineName + "/data";
          if (storageProvider.isDirectory(fallbackPath)) {
            LOGGER.debug("Verified Iceberg data exists at fallback {}", fallbackPath);
            return true;
          }
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
    return readRowCountFromIceberg(tableLocation, null);
  }

  /**
   * Read row count from Iceberg metadata with optional schema validation.
   * If expectedColumns is provided and there's a schema mismatch, the table
   * is dropped and 0 is returned to trigger full ETL recreation.
   *
   * @param tableLocation The Iceberg table location
   * @param expectedColumns Optional list of expected columns for schema validation
   * @return Row count from metadata, or 0 if unable to read or schema mismatch
   */
  private long readRowCountFromIceberg(String tableLocation, List<ColumnConfig> expectedColumns) {
    try {
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

      // Configure S3 filesystem if using S3 storage
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        org.apache.calcite.adapter.file.storage.S3StorageProvider s3Provider =
            (org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider;
        Map<String, String> s3Config = s3Provider.getS3Config();
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

        // Schema validation if expected columns provided
        if (expectedColumns != null && !expectedColumns.isEmpty()) {
          org.apache.iceberg.Schema icebergSchema = table.schema();
          Set<String> existingColumns = new HashSet<String>();
          for (org.apache.iceberg.types.Types.NestedField field : icebergSchema.columns()) {
            existingColumns.add(field.name().toLowerCase());
          }

          Set<String> missingColumns = new HashSet<String>();
          for (ColumnConfig col : expectedColumns) {
            String colName = col.getName().toLowerCase();
            if (!existingColumns.contains(colName)) {
              missingColumns.add(col.getName());
            }
          }

          if (!missingColumns.isEmpty()) {
            LOGGER.warn("Iceberg table '{}' schema mismatch: missing columns {}. "
                + "Dropping table for recreation.",
                tableLocation, missingColumns);
            catalog.dropTable(tableId, true);
            return 0;
          }
        }

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

  @SuppressWarnings("UnusedMethod")
  private CachedEtlProperties readEtlPropertiesFromIceberg(String tableLocation) {
    try {
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

      // Configure S3 filesystem if using S3 storage
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        org.apache.calcite.adapter.file.storage.S3StorageProvider s3Provider =
            (org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider;
        Map<String, String> s3Config = s3Provider.getS3Config();
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

  @SuppressWarnings("UnusedMethod")
  private void storeEtlPropertiesToIceberg(String tableLocation, String configHash,
      String signature, long rowCount) {
    try {
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

      // Configure S3 filesystem if using S3 storage
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        org.apache.calcite.adapter.file.storage.S3StorageProvider s3Provider =
            (org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider;
        Map<String, String> s3Config = s3Provider.getS3Config();
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

  /**
   * Returns the configured parallel thread count. Checks the per-table
   * {@code httpSource.parallel} config first, then falls back to the
   * {@code calcite.etl.threads} system property. Returns 1 (sequential) if neither is set.
   */
  private int getParallelThreadCount() {
    // Per-table parallel config takes precedence
    HttpSourceConfig sourceConfig = config.getSource();
    if (sourceConfig != null && sourceConfig.getParallel() > 1) {
      return sourceConfig.getParallel();
    }
    try {
      return Integer.parseInt(System.getProperty("calcite.etl.threads", "1"));
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  /**
   * Attempts to self-heal a partition by detecting parquet files that exist on storage
   * but are not registered in the Iceberg catalog. If found, re-registers them and marks
   * the tracker complete — no source re-fetch needed.
   *
   * <p>Processing output is immutable: given the same inputs the same files are produced,
   * so existing files on storage are always safe to re-register.
   *
   * @return estimated row count of re-registered files, or 0 if self-heal was not possible
   */
  private long trySelfHealFromStoredFiles(MaterializationWriter writer,
      Map<String, String> variables) {
    if (!(writer instanceof IcebergMaterializationWriter)) {
      return 0;
    }
    try {
      return ((IcebergMaterializationWriter) writer).selfHealPartition(variables);
    } catch (IOException e) {
      LOGGER.debug("Self-heal attempt failed for {}: {}", variables, e.getMessage());
      return 0;
    }
  }

  // ===== Dataset-type / Backfill helpers =====

  /**
   * Returns a copy of {@code variables} enriched with {@code period_start} and
   * {@code period_end} when {@code backfill_period} is configured on the pipeline.
   *
   * <p>The window boundaries are computed from the combination's canonical period
   * values (year, quarter, month, week, day) at the requested granularity:
   * <ul>
   *   <li>{@code annual} — full calendar year: {@code YYYY-01-01} / {@code YYYY-12-31}</li>
   *   <li>{@code quarterly} — ISO quarter boundaries: {@code YYYY-Q1-01-01} → {@code YYYY-03-31}, etc.</li>
   *   <li>{@code monthly} — first/last day of the month</li>
   *   <li>{@code weekly} — ISO week Monday / Sunday</li>
   *   <li>{@code daily} — the single day</li>
   * </ul>
   *
   * <p>If the combo does not carry the required period value for the requested
   * granularity (e.g. no {@code quarter} field for {@code quarterly}), the method
   * returns the original map unchanged — the source template can still use the raw
   * dimension variables, or the call is a no-op.
   *
   * @param variables    the current dimension combination
   * @param backfillPeriod  one of {@code annual|quarterly|monthly|weekly|daily}, or null
   * @return enriched copy (or original if no enrichment possible), never null
   */
  static Map<String, String> enrichWithPeriodBounds(Map<String, String> variables,
      String backfillPeriod) {
    if (backfillPeriod == null || backfillPeriod.isEmpty()) {
      return variables;
    }
    try {
      java.time.LocalDate start = null;
      java.time.LocalDate end = null;

      String yearStr = variables.get("year");
      if (yearStr == null) {
        return variables; // Can't compute without year
      }
      int year;
      try {
        year = Integer.parseInt(yearStr);
      } catch (NumberFormatException e) {
        return variables;
      }

      switch (backfillPeriod.toLowerCase()) {
      case "annual":
        start = java.time.LocalDate.of(year, 1, 1);
        end = java.time.LocalDate.of(year, 12, 31);
        break;

      case "quarterly": {
        String qStr = variables.get("quarter");
        if (qStr == null) {
          return variables;
        }
        // Quarter may be stored as "1","2","3","4" or "01".."04"
        int q;
        try {
          q = Integer.parseInt(qStr.trim());
        } catch (NumberFormatException e) {
          return variables;
        }
        int startMonth = (q - 1) * 3 + 1;
        start = java.time.LocalDate.of(year, startMonth, 1);
        // End: last day of the third month of the quarter
        java.time.YearMonth endYm = java.time.YearMonth.of(year, startMonth + 2);
        end = endYm.atEndOfMonth();
        break;
      }

      case "monthly": {
        String mStr = variables.get("month");
        if (mStr == null) {
          return variables;
        }
        int month;
        try {
          month = Integer.parseInt(mStr.trim());
        } catch (NumberFormatException e) {
          return variables;
        }
        java.time.YearMonth ym = java.time.YearMonth.of(year, month);
        start = ym.atDay(1);
        end = ym.atEndOfMonth();
        break;
      }

      case "weekly": {
        String wStr = variables.get("week");
        if (wStr == null) {
          return variables;
        }
        int week;
        try {
          week = Integer.parseInt(wStr.trim());
        } catch (NumberFormatException e) {
          return variables;
        }
        // Use ISO week: week 1 Monday of the given ISO week-year
        java.time.LocalDate weekStart = java.time.LocalDate.now()
            .with(java.time.temporal.IsoFields.WEEK_BASED_YEAR, year)
            .with(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR, week)
            .with(java.time.DayOfWeek.MONDAY);
        start = weekStart;
        end = weekStart.plusDays(6);
        break;
      }

      case "daily": {
        String dStr = variables.get("day");
        String mStr = variables.get("month");
        if (dStr == null || mStr == null) {
          return variables;
        }
        int month;
        int day;
        try {
          month = Integer.parseInt(mStr.trim());
          day = Integer.parseInt(dStr.trim());
        } catch (NumberFormatException e) {
          return variables;
        }
        start = java.time.LocalDate.of(year, month, day);
        end = start;
        break;
      }

      default:
        return variables;
      }

      if (start == null || end == null) {
        return variables;
      }

      Map<String, String> enriched = new LinkedHashMap<String, String>(variables);
      enriched.put("period_start", start.toString()); // ISO-8601: YYYY-MM-DD
      enriched.put("period_end", end.toString());
      return enriched;

    } catch (Exception e) {
      // Never fail a batch due to period-bounds computation
      LOGGER.debug("enrichWithPeriodBounds failed for {} / {}: {}",
          backfillPeriod, variables, e.getMessage());
      return variables;
    }
  }
}
