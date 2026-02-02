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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.partition.PartitionDetector;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.table.PartitionedParquetTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Refreshable partitioned Parquet table that can discover new partitions.
 */
public class RefreshablePartitionedParquetTable extends AbstractTable
    implements ScannableTable, RefreshableTable, CommentableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshablePartitionedParquetTable.class);

  private final String tableName;
  private final String directoryPath;
  private final String pattern;
  private final PartitionedTableConfig config;
  private final ExecutionEngineConfig engineConfig;
  private final @Nullable Duration refreshInterval;
  private @Nullable Instant lastRefreshTime;
  private final @Nullable Map<String, Object> constraintConfig;
  private final @Nullable String schemaName;
  private final org.apache.calcite.adapter.file.storage.@Nullable StorageProvider storageProvider;

  private volatile PartitionedParquetTable currentTable;
  private volatile List<String> lastDiscoveredFiles;

  // For refresh notifications to DuckDB
  private org.apache.calcite.adapter.file.@Nullable FileSchema fileSchema;
  private @Nullable String tableNameForNotification;

  // Async refresh state
  private volatile boolean refreshInProgress = false;
  private volatile CompletableFuture<Void> refreshFuture = null;

  public RefreshablePartitionedParquetTable(String tableName, String directoryPath,
      String pattern, PartitionedTableConfig config,
      ExecutionEngineConfig engineConfig, @Nullable Duration refreshInterval) {
    this(tableName, directoryPath, pattern, config, engineConfig, refreshInterval, null, null, null);
  }

  public RefreshablePartitionedParquetTable(String tableName, String directoryPath,
      String pattern, PartitionedTableConfig config,
      ExecutionEngineConfig engineConfig, @Nullable Duration refreshInterval,
      @Nullable Map<String, Object> constraintConfig, @Nullable String schemaName) {
    this(tableName, directoryPath, pattern, config, engineConfig, refreshInterval, constraintConfig, schemaName, null);
  }

  public RefreshablePartitionedParquetTable(String tableName, String directoryPath,
      String pattern, PartitionedTableConfig config,
      ExecutionEngineConfig engineConfig, @Nullable Duration refreshInterval,
      @Nullable Map<String, Object> constraintConfig, @Nullable String schemaName,
      org.apache.calcite.adapter.file.storage.@Nullable StorageProvider storageProvider) {
    this.tableName = tableName;
    this.directoryPath = directoryPath;
    this.pattern = pattern;
    this.config = config;
    this.engineConfig = engineConfig;
    this.refreshInterval = refreshInterval;
    this.constraintConfig = constraintConfig;
    this.schemaName = schemaName;
    this.storageProvider = storageProvider;

    LOGGER.info("Creating RefreshablePartitionedParquetTable for table: '{}', comment: '{}'",
        tableName, config != null ? config.getComment() : "null");
    LOGGER.debug("Creating RefreshablePartitionedParquetTable for table: {}, hasPartitionConfig: {}",
        tableName, config.getPartitions() != null);
    if (config.getPartitions() != null) {
      LOGGER.debug("Partition config - style: {}, columnDefinitions: {}",
          config.getPartitions().getStyle(),
          config.getPartitions().getColumnDefinitions() != null ?
              config.getPartitions().getColumnDefinitions().size() : "null");
    }

    // Check if we can skip initial scan (DuckDB+Hive optimization)
    if (shouldSkipInitialScan()) {
      LOGGER.info("Skipping initial file scan for DuckDB+Hive table '{}' - will use lazy detection", tableName);
      // Table will be initialized on first query via refresh check
    } else {
      // Initial discovery
      refreshTableDefinition();
    }
  }

  /**
   * Sets the FileSchema and table name for refresh notifications.
   */
  public void setRefreshContext(org.apache.calcite.adapter.file.FileSchema fileSchema, String tableName) {
    this.fileSchema = fileSchema;
    this.tableNameForNotification = tableName;
  }

  /**
   * Checks if we should skip the initial file scan at startup.
   * Only for DuckDB+Hive tables where the pattern is sufficient for DDL.
   */
  private boolean shouldSkipInitialScan() {
    // Must have engine config
    if (engineConfig == null) {
      return false;
    }

    // Check if DuckDB is available either as the primary engine or nested within PARQUET engine
    boolean hasDuckDB = false;
    if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB) {
      hasDuckDB = true;
    } else if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET) {
      // Check if there's a nested DuckDB config
      if (engineConfig.getDuckDBConfig() != null) {
        hasDuckDB = true;
      }
    }

    if (!hasDuckDB) {
      return false;
    }

    // Must have Hive-style partitioning
    if (config.getPartitions() == null || !"hive".equals(config.getPartitions().getStyle())) {
      return false;
    }

    // Must have a pattern (required for DuckDB parquet_scan)
    if (pattern == null || pattern.trim().isEmpty()) {
      return false;
    }

    return true;
  }

  /**
   * Returns whether this table used lazy initialization (skipped initial file scan).
   * Used by ConversionMetadata to avoid enumerating files during initial metadata extraction.
   * @return true if table used shouldSkipInitialScan optimization
   */
  public boolean usedLazyInitialization() {
    return shouldSkipInitialScan();
  }

  /**
   * Returns the directory path for this table.
   * Used by ConversionMetadata to construct viewScanPattern for lazy-initialized tables.
   * @return the directory path (may be S3 URI)
   */
  public String getDirectoryPath() {
    return directoryPath;
  }

  /**
   * Returns the file pattern for this table.
   * Used by ConversionMetadata to construct viewScanPattern for lazy-initialized tables.
   * @return the file pattern
   */
  public String getPattern() {
    return pattern;
  }

  /**
   * Prime HLL statistics for the current table if it supports StatisticsProvider.
   * This is called during refresh to build/update HLL sketches for COUNT(DISTINCT) optimization.
   */
  private void primeHLLStatistics() {
    if (currentTable == null) {
      return;
    }

    // Check if table supports statistics
    if (currentTable instanceof org.apache.calcite.adapter.file.statistics.StatisticsProvider) {
      try {
        long startTime = System.currentTimeMillis();
        org.apache.calcite.adapter.file.statistics.StatisticsProvider statsProvider =
            (org.apache.calcite.adapter.file.statistics.StatisticsProvider) currentTable;

        // This will load/build statistics including HLL sketches into cache
        org.apache.calcite.adapter.file.statistics.TableStatistics stats =
            statsProvider.getTableStatistics(null);

        if (stats != null) {
          long elapsed = System.currentTimeMillis() - startTime;
          LOGGER.info("Primed HLL statistics for table '{}': {} rows, {} columns with sketches ({} ms)",
              tableName, stats.getRowCount(), stats.getColumnStatistics().size(), elapsed);
        } else {
          LOGGER.debug("No HLL statistics available for table '{}'", tableName);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to prime HLL statistics for table '{}': {}", tableName, e.getMessage());
      }
    } else {
      LOGGER.debug("Table '{}' does not support StatisticsProvider - skipping HLL priming", tableName);
    }
  }

  /**
   * Creates a baseline snapshot of current file metadata.
   * Used for fast change detection on subsequent refreshes.
   */
  private org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline snapshotFileMetadata(
      List<String> filePaths) {
    if (filePaths == null || filePaths.isEmpty() || storageProvider == null) {
      return null;
    }

    try {
      java.util.Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline> fileMap =
          new java.util.HashMap<>();

      for (String filePath : filePaths) {
        try {
          org.apache.calcite.adapter.file.storage.StorageProvider.FileMetadata metadata =
              storageProvider.getMetadata(filePath);
          if (metadata != null) {
            org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline baseline =
                new org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline(
                    metadata.getSize(),
                    metadata.getEtag(),
                    metadata.getLastModified());
            fileMap.put(filePath, baseline);
          }
        } catch (Exception e) {
          LOGGER.debug("Failed to get metadata for file '{}': {}", filePath, e.getMessage());
        }
      }

      org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline baseline =
          new org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline();
      baseline.files = fileMap;
      baseline.snapshotTimestamp = System.currentTimeMillis();

      LOGGER.info("Created baseline snapshot with {} files for table '{}'", fileMap.size(), tableName);
      return baseline;

    } catch (Exception e) {
      LOGGER.error("Failed to create baseline snapshot: {}", e.getMessage(), e);
      return null;
    }
  }

  /**
   * Compares current files against baseline to detect changes.
   * Returns true if any files were added, removed, or modified.
   */
  private boolean hasFilesChanged(org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline baseline) {
    if (baseline == null || baseline.isEmpty() || storageProvider == null) {
      return true; // No baseline, must scan
    }

    try {
      // Discover current files
      List<String> currentFiles = discoverFiles();
      java.util.Set<String> baselineFiles = baseline.files.keySet();

      // Check for added or removed files
      if (currentFiles.size() != baselineFiles.size()) {
        LOGGER.info("File count changed for table '{}': {} -> {}",
            tableName, baselineFiles.size(), currentFiles.size());
        return true;
      }

      // Check each file for modifications
      for (String filePath : currentFiles) {
        org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline baselineMetadata =
            baseline.files.get(filePath);

        if (baselineMetadata == null) {
          LOGGER.info("New file detected in table '{}': {}", tableName, filePath);
          return true; // File added
        }

        // Get current metadata and compare
        try {
          org.apache.calcite.adapter.file.storage.StorageProvider.FileMetadata currentMetadata =
              storageProvider.getMetadata(filePath);
          if (currentMetadata != null) {
            org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline currentBaseline =
                new org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline(
                    currentMetadata.getSize(),
                    currentMetadata.getEtag(),
                    currentMetadata.getLastModified());

            if (baselineMetadata.hasChanged(currentBaseline)) {
              LOGGER.info("File modified in table '{}': {}", tableName, filePath);
              return true;
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Failed to get metadata for file '{}': {}", filePath, e.getMessage());
          return true; // Assume changed if we can't check
        }
      }

      // Check for removed files
      for (String baselineFile : baselineFiles) {
        if (!currentFiles.contains(baselineFile)) {
          LOGGER.info("File removed from table '{}': {}", tableName, baselineFile);
          return true;
        }
      }

      LOGGER.debug("No file changes detected for table '{}'", tableName);
      return false;

    } catch (Exception e) {
      LOGGER.error("Error checking for file changes: {}", e.getMessage(), e);
      return true; // Assume changed on error
    }
  }

  /**
   * Compares a given list of files against baseline to detect changes.
   * This version takes the file list as a parameter to avoid re-enumerating files.
   * Returns true if any files were added, removed, or modified.
   */
  private boolean filesChangedComparedToBaseline(List<String> currentFiles,
      org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline baseline) {
    if (baseline == null || baseline.isEmpty() || storageProvider == null) {
      return true; // No baseline, must refresh
    }

    try {
      java.util.Set<String> baselineFiles = baseline.files.keySet();

      // Check for added or removed files
      if (currentFiles.size() != baselineFiles.size()) {
        LOGGER.info("File count changed for table '{}': {} -> {}",
            tableName, baselineFiles.size(), currentFiles.size());
        return true;
      }

      // Check each file for modifications
      for (String filePath : currentFiles) {
        org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline baselineMetadata =
            baseline.files.get(filePath);

        if (baselineMetadata == null) {
          LOGGER.info("New file detected in table '{}': {}", tableName, filePath);
          return true; // File added
        }

        // Get current metadata and compare
        try {
          org.apache.calcite.adapter.file.storage.StorageProvider.FileMetadata currentMetadata =
              storageProvider.getMetadata(filePath);
          if (currentMetadata != null) {
            org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline currentBaseline =
                new org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline(
                    currentMetadata.getSize(),
                    currentMetadata.getEtag(),
                    currentMetadata.getLastModified());

            if (baselineMetadata.hasChanged(currentBaseline)) {
              LOGGER.info("File modified in table '{}': {}", tableName, filePath);
              return true;
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Failed to get metadata for file '{}': {}", filePath, e.getMessage());
          return true; // Assume changed if we can't check
        }
      }

      // Check for removed files
      for (String baselineFile : baselineFiles) {
        if (!currentFiles.contains(baselineFile)) {
          LOGGER.info("File removed from table '{}': {}", tableName, baselineFile);
          return true;
        }
      }

      LOGGER.debug("No file changes detected for table '{}'", tableName);
      return false;

    } catch (Exception e) {
      LOGGER.error("Error checking for file changes: {}", e.getMessage(), e);
      return true; // Assume changed on error
    }
  }

  /**
   * Get the list of parquet file paths for this partitioned table.
   * Used by conversion metadata to register with DuckDB.
   */
  public List<String> getFilePaths() {
    if (currentTable != null) {
      return currentTable.getFilePaths();
    }
    return lastDiscoveredFiles != null ? lastDiscoveredFiles : java.util.Collections.emptyList();
  }

  @Override public @Nullable Duration getRefreshInterval() {
    return refreshInterval;
  }

  @Override public @Nullable Instant getLastRefreshTime() {
    return lastRefreshTime;
  }

  @Override public boolean needsRefresh() {
    if (refreshInterval == null) {
      return false;
    }

    // First time - always refresh
    if (lastRefreshTime == null) {
      return true;
    }

    // Check if interval has elapsed
    return Instant.now().isAfter(lastRefreshTime.plus(refreshInterval));
  }

  @Override public void refresh() {
    if (!needsRefresh()) {
      return;
    }

    // Start async refresh if not already running
    // This allows expensive file enumeration to happen in background without blocking queries
    if (!refreshInProgress) {
      synchronized (this) {
        if (!refreshInProgress) {
          refreshInProgress = true;
          refreshFuture = CompletableFuture.runAsync(() -> refreshTableDefinitionAsync());
          LOGGER.debug("Started async refresh for table '{}'", tableName);
        }
      }
    }

    lastRefreshTime = Instant.now();
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.PARTITIONED_TABLE;
  }

  /**
   * Recreates the DuckDB view or table with new file list.
   * This method is synchronized but FAST - only drops/creates view in DuckDB.
   * File enumeration happens BEFORE calling this method (async in background).
   */
  private synchronized void recreateViewWithNewFiles(List<String> newFiles) {
    try {
      if (shouldSkipInitialScan() && fileSchema != null && tableNameForNotification != null) {
        // DuckDB+Hive path: Just recreate the view with pattern
        org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata = fileSchema.getConversionMetadata();
        if (metadata != null) {
          org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
              metadata.getAllConversions().get(tableNameForNotification);
          if (record != null && record.viewScanPattern != null) {
            // Recreate DuckDB view (fast - just DROP/CREATE)
            fileSchema.notifyTableRefreshedWithPattern(tableNameForNotification, record.viewScanPattern);
            LOGGER.info("Recreated DuckDB view for table '{}' with pattern: {}",
                tableName, record.viewScanPattern);

            // Update baseline with full file metadata
            org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline newBaseline =
                snapshotFileMetadata(newFiles);
            if (newBaseline != null) {
              fileSchema.updateTableBaseline(tableNameForNotification, newBaseline);
              LOGGER.info("Updated baseline for table '{}' with {} files", tableName, newFiles.size());
            }

            // Create PartitionedParquetTable for schema introspection even though queries go to DuckDB view
            // Extract partition info from config for schema purposes
            PartitionDetector.PartitionInfo partitionInfo = null;
            if (config.getPartitions() != null && config.getPartitions().getColumnDefinitions() != null) {
              List<String> explicitColumns = new ArrayList<>();
              for (PartitionedTableConfig.ColumnDefinition colDef : config.getPartitions().getColumnDefinitions()) {
                explicitColumns.add(colDef.getName());
              }
              partitionInfo =
                  new PartitionDetector.PartitionInfo(new java.util.LinkedHashMap<>(),
                      explicitColumns, true);
            }

            // Get column types from config
            Map<String, String> columnTypes = null;
            if (config.getPartitions() != null && config.getPartitions().getColumnDefinitions() != null) {
              columnTypes = new java.util.HashMap<>();
              for (PartitionedTableConfig.ColumnDefinition colDef : config.getPartitions().getColumnDefinitions()) {
                columnTypes.put(colDef.getName(), colDef.getType());
              }
            }

            // Create table instance for schema introspection
            currentTable =
                new PartitionedParquetTable(newFiles, partitionInfo,
                    engineConfig, columnTypes, null, null, constraintConfig, schemaName, tableName, storageProvider, config);

            // Prime HLL statistics if the table supports it
            primeHLLStatistics();

            lastDiscoveredFiles = newFiles;
            return;
          }
        }
      }

      // Standard table path: Create PartitionedParquetTable
      if (!newFiles.equals(lastDiscoveredFiles)) {
        // Detect partitions based on configuration
        PartitionDetector.PartitionInfo partitionInfo = null;
        if (!newFiles.isEmpty()) {
          if (config.getPartitions() != null) {
            PartitionedTableConfig.PartitionConfig partConfig = config.getPartitions();
            String style = partConfig.getStyle();

            if (style == null || "auto".equals(style)) {
              partitionInfo = PartitionDetector.detectPartitionScheme(newFiles);
            } else if ("hive".equals(style)) {
              if (partConfig.getColumnDefinitions() != null && !partConfig.getColumnDefinitions().isEmpty()) {
                List<String> explicitColumns = new ArrayList<>();
                for (PartitionedTableConfig.ColumnDefinition colDef : partConfig.getColumnDefinitions()) {
                  explicitColumns.add(colDef.getName());
                }
                partitionInfo =
                    new PartitionDetector.PartitionInfo(new java.util.LinkedHashMap<>(),
                        explicitColumns, true);
              } else {
                partitionInfo = PartitionDetector.extractHivePartitions(newFiles.get(0));
              }
            } else if ("directory".equals(style) && partConfig.getColumns() != null) {
              partitionInfo =
                  PartitionDetector.extractDirectoryPartitions(newFiles.get(0), partConfig.getColumns());
            } else if ("custom".equals(style) && partConfig.getRegex() != null) {
              partitionInfo =
                  PartitionDetector.extractCustomPartitions(
                      newFiles.get(0), partConfig.getRegex(), partConfig.getColumnMappings());
            }
          } else {
            partitionInfo = PartitionDetector.detectPartitionScheme(newFiles);
          }
        }

        // Get column types if configured
        Map<String, String> columnTypes = null;
        if (config.getPartitions() != null) {
          if (config.getPartitions().getColumnDefinitions() != null) {
            columnTypes = new java.util.HashMap<>();
            for (PartitionedTableConfig.ColumnDefinition colDef
                : config.getPartitions().getColumnDefinitions()) {
              columnTypes.put(colDef.getName(), colDef.getType());
            }
          } else if (config.getPartitions().getColumns() != null) {
            columnTypes = new java.util.HashMap<>();
            for (String col : config.getPartitions().getColumns()) {
              columnTypes.put(col, "VARCHAR");
            }
          }
        }

        if (columnTypes == null && partitionInfo != null && partitionInfo.getPartitionColumns() != null) {
          columnTypes = new java.util.HashMap<>();
          for (String partCol : partitionInfo.getPartitionColumns()) {
            columnTypes.put(partCol, "VARCHAR");
          }
        }

        // Extract custom regex info if available
        String regex = null;
        List<PartitionedTableConfig.ColumnMapping> colMappings = null;
        if (config.getPartitions() != null && "custom".equals(config.getPartitions().getStyle())) {
          regex = config.getPartitions().getRegex();
          colMappings = config.getPartitions().getColumnMappings();
        }

        // Create new table instance
        currentTable =
            new PartitionedParquetTable(newFiles, partitionInfo,
                engineConfig, columnTypes, regex, colMappings, constraintConfig, schemaName, tableName, storageProvider, config);
        lastDiscoveredFiles = newFiles;

        LOGGER.info("Recreated table '{}' with {} files", tableName, newFiles.size());

        // Notify listeners
        // Note: notifyTableRefreshed currently expects File parameter but we're passing String path
        // For DuckDB+Hive tables, this notification is not critical since we use pattern-based views
        if (fileSchema != null && tableNameForNotification != null && !newFiles.isEmpty()) {
          // Create temporary File object for notification - this is legacy API
          // In the future, this should be refactored to accept String paths
          try {
            java.io.File dummyFile = new java.io.File(newFiles.get(0));
            fileSchema.notifyTableRefreshed(tableNameForNotification, dummyFile);
          } catch (Exception e) {
            LOGGER.debug("Could not create File object for notification (S3 path?): {}", newFiles.get(0));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to recreate view/table for '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Asynchronous refresh that performs expensive file enumeration in background.
   * This method is NOT synchronized - it doesn't block queries.
   * Only the final view recreation step (recreateViewWithNewFiles) is synchronized and fast.
   */
  private void refreshTableDefinitionAsync() {
    try {
      LOGGER.debug("Starting async refresh for table '{}'", tableName);

      // PHASE 1: File enumeration and change detection (NOT synchronized - doesn't block queries)
      boolean needsViewRecreation = false;
      List<String> newFiles = null;

      if (shouldSkipInitialScan() && fileSchema != null && tableNameForNotification != null) {
        // For DuckDB+Hive: enumerate files and compare baseline
        LOGGER.debug("DuckDB+Hive table '{}' - enumerating files for change detection", tableName);
        newFiles = discoverFiles();  // EXPENSIVE - runs in background

        org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline baseline =
            fileSchema.getTableBaseline(tableNameForNotification);

        if (baseline == null) {
          LOGGER.info("No baseline found for table '{}' - will create view", tableName);
          needsViewRecreation = true;
        } else {
          needsViewRecreation = filesChangedComparedToBaseline(newFiles, baseline);
          if (needsViewRecreation) {
            LOGGER.info("Changes detected for table '{}' - will recreate view", tableName);
          } else {
            LOGGER.debug("No changes detected for table '{}'", tableName);
          }
        }
      } else {
        // Standard path: enumerate and compare
        newFiles = discoverFiles();
        needsViewRecreation = !newFiles.equals(lastDiscoveredFiles);
      }

      if (!needsViewRecreation) {
        LOGGER.debug("Async refresh complete for table '{}' - no changes", tableName);
        return;
      }

      // PHASE 2: View recreation (synchronized - brief block)
      LOGGER.debug("Recreating view for table '{}' with {} files", tableName, newFiles.size());
      recreateViewWithNewFiles(newFiles);
      LOGGER.info("Async refresh complete for table '{}' - view recreated", tableName);

    } catch (Exception e) {
      LOGGER.error("Async refresh failed for table '{}': {}", tableName, e.getMessage(), e);
    } finally {
      refreshInProgress = false;
      refreshFuture = null;
    }
  }

  /**
   * Synchronous refresh used during table initialization.
   * This is called from the constructor for non-DuckDB+Hive tables.
   * For async refreshes (during periodic refresh checks), use refreshTableDefinitionAsync().
   */
  private synchronized void refreshTableDefinition() {
    try {
      // Discover files and create the table
      List<String> matchingFiles = discoverFiles();
      if (!matchingFiles.isEmpty()) {
        recreateViewWithNewFiles(matchingFiles);
        LOGGER.debug("Initial table definition created for '{}' with {} files", tableName, matchingFiles.size());
      } else {
        LOGGER.warn("No matching files found for table '{}' with pattern '{}'", tableName, pattern);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to create initial table definition for '{}': {}", tableName, e.getMessage(), e);
    }
  }

  /**
   * Discovers files using StorageProvider that match the glob pattern.
   * This supports both local filesystem and S3 storage.
   */
  private List<String> discoverFiles() {
    if (storageProvider == null) {
      LOGGER.error("Cannot discover files for table '{}' - StorageProvider is null", tableName);
      return java.util.Collections.emptyList();
    }

    try {
      // Use StorageProvider to list all files recursively
      List<org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry> allFiles =
          storageProvider.listFiles(directoryPath, true);

      // Create PathMatcher for glob pattern matching
      PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

      // Filter files that match the pattern
      List<String> matchingFiles = new ArrayList<>();
      for (org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry entry : allFiles) {
        if (!entry.isDirectory()) {
          // Get relative path from directoryPath for pattern matching
          String relativePath = getRelativePath(directoryPath, entry.getPath());
          if (relativePath != null) {
            // Convert to Path for matching
            java.nio.file.Path relPath = java.nio.file.Paths.get(relativePath);
            if (matcher.matches(relPath)) {
              matchingFiles.add(entry.getPath());
            }
          }
        }
      }

      LOGGER.debug("Discovered {} files matching pattern '{}' in directory '{}'",
          matchingFiles.size(), pattern, directoryPath);
      return matchingFiles;

    } catch (Exception e) {
      LOGGER.error("Failed to discover files for table '{}' in directory '{}': {}",
          tableName, directoryPath, e.getMessage(), e);
      return java.util.Collections.emptyList();
    }
  }

  /**
   * Computes relative path from base to full path.
   * Handles both local paths and S3 URIs.
   */
  private String getRelativePath(String basePath, String fullPath) {
    // Normalize paths to use forward slashes
    String normalizedBase = basePath.replace('\\', '/');
    String normalizedFull = fullPath.replace('\\', '/');

    // Ensure base path ends with separator
    if (!normalizedBase.endsWith("/")) {
      normalizedBase = normalizedBase + "/";
    }

    // Check if fullPath starts with basePath
    if (normalizedFull.startsWith(normalizedBase)) {
      return normalizedFull.substring(normalizedBase.length());
    }

    // If not a subpath, return just the filename
    int lastSeparator = normalizedFull.lastIndexOf('/');
    if (lastSeparator >= 0) {
      return normalizedFull.substring(lastSeparator + 1);
    }

    return normalizedFull;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    refresh(); // Kicks off async refresh if needed

    // If refresh is in progress, wait for it to complete
    // This ensures queries see the latest view after refresh
    if (refreshInProgress && refreshFuture != null) {
      try {
        refreshFuture.get(120, TimeUnit.SECONDS); // Wait for completion with timeout (increased for S3)
        LOGGER.debug("Waited for refresh to complete for table '{}'", tableName);
      } catch (TimeoutException e) {
        LOGGER.warn("Refresh timeout for table '{}', proceeding with current view", tableName);
      } catch (Exception e) {
        LOGGER.error("Refresh error for table '{}': {}", tableName, e.getMessage());
      }
    }

    return currentTable.getRowType(typeFactory);
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    refresh(); // Kicks off async refresh if needed

    // If refresh is in progress, wait for it to complete
    // This ensures queries see the latest view after refresh
    if (refreshInProgress && refreshFuture != null) {
      try {
        refreshFuture.get(120, TimeUnit.SECONDS); // Wait for completion with timeout (increased for S3)
        LOGGER.debug("Waited for refresh to complete for table '{}'", tableName);
      } catch (TimeoutException e) {
        LOGGER.warn("Refresh timeout for table '{}', proceeding with current view", tableName);
      } catch (Exception e) {
        LOGGER.error("Refresh error for table '{}': {}", tableName, e.getMessage());
      }
    }

    return currentTable.scan(root);
  }

  // CommentableTable implementation
  // Return comments directly from config, not from currentTable, to support lazy initialization.
  // During schema introspection (e.g., INFORMATION_SCHEMA queries), currentTable may be null.

  @Override public @Nullable String getTableComment() {
    return config != null ? config.getComment() : null;
  }

  @Override public @Nullable String getColumnComment(String columnName) {
    if (config != null && config.getColumnComments() != null) {
      return config.getColumnComments().get(columnName);
    }
    return null;
  }

  @Override public String toString() {
    return "RefreshablePartitionedParquetTable(" + tableName + ")";
  }
}
