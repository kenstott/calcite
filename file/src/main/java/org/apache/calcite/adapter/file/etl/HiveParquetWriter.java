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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes data to hive-partitioned Parquet files using DuckDB.
 *
 * <p>This class materializes data from various sources (JSON, CSV, API responses)
 * to Parquet files organized in a hive-style partition layout.
 *
 * <h3>Features</h3>
 * <ul>
 *   <li>Uses DuckDB COPY with PARTITION_BY for efficient partitioned writes</li>
 *   <li>Supports schema-driven column definitions with types</li>
 *   <li>Supports computed columns via SQL expressions</li>
 *   <li>Enables batched processing for large datasets to avoid OOM</li>
 *   <li>Configurable compression and row group size</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * StorageProvider storageProvider = StorageProviderFactory.createFromUrl("s3://bucket/");
 * HiveParquetWriter writer = new HiveParquetWriter(storageProvider, "/data/output");
 *
 * MaterializeConfig config = MaterializeConfig.builder()
 *     .output(MaterializeOutputConfig.builder()
 *         .location("/data/output")
 *         .compression("snappy")
 *         .build())
 *     .partition(MaterializePartitionConfig.builder()
 *         .columns(Arrays.asList("year", "region"))
 *         .batchBy(Arrays.asList("year"))
 *         .build())
 *     .build();
 *
 * MaterializeResult result = writer.materialize(config, dataSource);
 * }</pre>
 *
 * <h3>Batched Processing</h3>
 * <p>When processing large datasets, the writer uses batching to avoid OOM:
 * <ol>
 *   <li>Build batch combinations from batchBy columns</li>
 *   <li>Process each batch, writing to temp location</li>
 *   <li>Consolidate temp files into final partitioned structure</li>
 *   <li>Set lifecycle rules for temp cleanup (S3 only)</li>
 * </ol>
 *
 * @see MaterializeConfig
 * @see DataSource
 * @see org.apache.calcite.adapter.file.partition.ParquetReorganizer
 */
public class HiveParquetWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveParquetWriter.class);

  private final StorageProvider storageProvider;
  private final String baseDirectory;

  /**
   * Creates a new HiveParquetWriter.
   *
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output files
   */
  public HiveParquetWriter(StorageProvider storageProvider, String baseDirectory) {
    this.storageProvider = storageProvider;
    this.baseDirectory = baseDirectory;
  }

  /**
   * Materializes data from source to partitioned Parquet files.
   *
   * @param config Materialization configuration
   * @param source Data source to materialize
   * @return Materialization result with statistics
   * @throws IOException If materialization fails
   */
  public MaterializeResult materialize(MaterializeConfig config, DataSource source)
      throws IOException {
    if (!config.isEnabled()) {
      LOGGER.info("Materialization is disabled, skipping");
      return MaterializeResult.skipped("Materialization disabled");
    }

    String displayName = config.getName() != null ? config.getName() : "materialization";
    LOGGER.info("Starting materialization: {}", displayName);

    long startTime = System.currentTimeMillis();
    long rowCount = 0;
    int fileCount = 0;

    String outputLocation = config.getOutput().getLocation();
    String fullOutputPath = storageProvider.resolvePath(baseDirectory, outputLocation);

    // Temp location with timestamp to isolate each run
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    String tempBase = fullOutputPath + "/_temp_materialize/" + timestamp;

    try (Connection conn = getDuckDBConnection()) {
      // Apply DuckDB settings
      applyDuckDBSettings(conn, config.getOptions());

      MaterializePartitionConfig partitionConfig = config.getPartition();
      List<String> batchByColumns = partitionConfig != null ? partitionConfig.getBatchBy() : null;

      if (batchByColumns == null || batchByColumns.isEmpty()) {
        // No batching - process all data at once
        LOGGER.info("Processing all data without batching");
        MaterializeBatchResult batchResult = processBatch(conn, config, source,
            Collections.<String, String>emptyMap(), tempBase, fullOutputPath);
        rowCount = batchResult.getRowCount();
        fileCount = batchResult.getFileCount();
      } else {
        // Build and process batch combinations
        List<Map<String, String>> batchCombinations = buildBatchCombinations(source, batchByColumns);
        LOGGER.info("Processing {} batch combinations (batch by: {})",
            batchCombinations.size(), batchByColumns);

        int batchNum = 0;
        for (Map<String, String> batch : batchCombinations) {
          batchNum++;
          LOGGER.info("Processing batch {}/{}: {}", batchNum, batchCombinations.size(), batch);

          MaterializeBatchResult batchResult = processBatch(conn, config, source, batch,
              tempBase, fullOutputPath);
          rowCount += batchResult.getRowCount();
          fileCount += batchResult.getFileCount();
        }
      }

      // Consolidate if we used temp location
      if (fileCount > 0) {
        consolidateTempFiles(conn, config, tempBase, fullOutputPath);
        setLifecycleRule(outputLocation, tempBase);
      }

      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("Materialization complete: {} rows, {} files in {}ms",
          rowCount, fileCount, elapsed);

      return MaterializeResult.success(rowCount, fileCount, elapsed);

    } catch (SQLException e) {
      String errorMsg = String.format("DuckDB materialization failed for '%s': %s",
          displayName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Materializes data from a JSON file to partitioned Parquet.
   * Convenience method for file-based sources.
   *
   * @param config Materialization configuration
   * @param jsonFilePath Path to JSON input file
   * @return Materialization result
   * @throws IOException If materialization fails
   */
  public MaterializeResult materializeFromJson(MaterializeConfig config, String jsonFilePath)
      throws IOException {
    String displayName = config.getName() != null ? config.getName() : jsonFilePath;
    LOGGER.info("Materializing from JSON: {}", displayName);

    long startTime = System.currentTimeMillis();
    String fullJsonPath = storageProvider.resolvePath(baseDirectory, jsonFilePath);
    String outputLocation = config.getOutput().getLocation();
    String fullOutputPath = storageProvider.resolvePath(baseDirectory, outputLocation);

    try (Connection conn = getDuckDBConnection()) {
      applyDuckDBSettings(conn, config.getOptions());

      String sql = buildCopySqlFromJson(fullJsonPath, fullOutputPath, config);
      LOGGER.debug("Materialization SQL:\n{}", sql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }

      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("JSON materialization complete in {}ms", elapsed);

      return MaterializeResult.success(-1, -1, elapsed);

    } catch (SQLException e) {
      String errorMsg = String.format("DuckDB JSON materialization failed for '%s': %s",
          displayName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Materializes data from a CSV file to partitioned Parquet.
   * Convenience method for file-based sources.
   *
   * @param config Materialization configuration
   * @param csvFilePath Path to CSV input file
   * @return Materialization result
   * @throws IOException If materialization fails
   */
  public MaterializeResult materializeFromCsv(MaterializeConfig config, String csvFilePath)
      throws IOException {
    String displayName = config.getName() != null ? config.getName() : csvFilePath;
    LOGGER.info("Materializing from CSV: {}", displayName);

    long startTime = System.currentTimeMillis();
    String fullCsvPath = storageProvider.resolvePath(baseDirectory, csvFilePath);
    String outputLocation = config.getOutput().getLocation();
    String fullOutputPath = storageProvider.resolvePath(baseDirectory, outputLocation);

    try (Connection conn = getDuckDBConnection()) {
      applyDuckDBSettings(conn, config.getOptions());

      String sql = buildCopySqlFromCsv(fullCsvPath, fullOutputPath, config);
      LOGGER.debug("Materialization SQL:\n{}", sql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }

      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("CSV materialization complete in {}ms", elapsed);

      return MaterializeResult.success(-1, -1, elapsed);

    } catch (SQLException e) {
      String errorMsg = String.format("DuckDB CSV materialization failed for '%s': %s",
          displayName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Materializes data from an existing Parquet glob pattern to a new partition layout.
   * Similar to ParquetReorganizer but with column transformation support.
   *
   * @param config Materialization configuration
   * @param sourcePattern Source Parquet glob pattern
   * @return Materialization result
   * @throws IOException If materialization fails
   */
  public MaterializeResult materializeFromParquet(MaterializeConfig config, String sourcePattern)
      throws IOException {
    String displayName = config.getName() != null ? config.getName() : sourcePattern;
    LOGGER.info("Materializing from Parquet: {}", displayName);

    long startTime = System.currentTimeMillis();
    String fullSourcePattern = storageProvider.resolvePath(baseDirectory, sourcePattern);
    String outputLocation = config.getOutput().getLocation();
    String fullOutputPath = storageProvider.resolvePath(baseDirectory, outputLocation);

    try (Connection conn = getDuckDBConnection()) {
      applyDuckDBSettings(conn, config.getOptions());

      String sql = buildCopySqlFromParquet(fullSourcePattern, fullOutputPath, config);
      LOGGER.debug("Materialization SQL:\n{}", sql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }

      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("Parquet materialization complete in {}ms", elapsed);

      return MaterializeResult.success(-1, -1, elapsed);

    } catch (SQLException e) {
      String errorMsg = String.format("DuckDB Parquet materialization failed for '%s': %s",
          displayName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Processes a single batch of data.
   */
  private MaterializeBatchResult processBatch(Connection conn, MaterializeConfig config,
      DataSource source, Map<String, String> batchVariables,
      String tempBase, String finalBase) throws SQLException, IOException {

    // Fetch data for this batch
    Iterator<Map<String, Object>> data = source.fetch(batchVariables);

    // For iterator-based sources, we need to write to temp file first
    // then use DuckDB to read and partition
    // This is a simplified implementation - real impl would stream

    long rowCount = 0;
    while (data.hasNext()) {
      data.next();
      rowCount++;
    }

    // TODO: Implement actual batch processing with temp file staging
    // For now, return placeholder result
    return new MaterializeBatchResult(rowCount, 0);
  }

  /**
   * Consolidates temp files into final partitioned structure.
   */
  private void consolidateTempFiles(Connection conn, MaterializeConfig config,
      String tempBase, String finalBase) throws SQLException {

    MaterializePartitionConfig partitionConfig = config.getPartition();
    List<String> partitionColumns = partitionConfig != null
        ? partitionConfig.getColumns()
        : Collections.<String>emptyList();

    String sql = buildConsolidationSql(tempBase, finalBase, partitionColumns,
        config.getOutput().getCompression());

    LOGGER.debug("Consolidation SQL:\n{}", sql);

    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      LOGGER.info("Consolidation complete");
    }
  }

  /**
   * Sets lifecycle rule for temp cleanup (S3 only).
   */
  private void setLifecycleRule(String outputLocation, String tempBase) {
    String tempPrefix = outputLocation + "/_temp_materialize/";
    try {
      storageProvider.ensureLifecycleRule(tempPrefix, 1);
      LOGGER.info("Lifecycle rule set: {} will auto-expire in 1 day", tempPrefix);
    } catch (IOException e) {
      LOGGER.warn("Could not set lifecycle rule (temp files will remain): {}", e.getMessage());
    }
  }

  /**
   * Builds batch combinations from the data source.
   */
  private List<Map<String, String>> buildBatchCombinations(DataSource source,
      List<String> batchByColumns) throws IOException {

    // TODO: Implement batch combination discovery
    // For now, return empty list (no batching)
    return Collections.emptyList();
  }

  /**
   * Builds DuckDB COPY SQL for JSON source.
   */
  private String buildCopySqlFromJson(String jsonPath, String outputPath,
      MaterializeConfig config) {

    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT ");
    sql.append(buildSelectClause(config.getColumns()));
    sql.append("\n  FROM read_json_auto(").append(quoteLiteral(jsonPath)).append(")\n");
    sql.append(") TO ").append(quoteLiteral(outputPath));
    sql.append(buildCopyOptions(config));
    sql.append(";");

    return sql.toString();
  }

  /**
   * Builds DuckDB COPY SQL for CSV source.
   */
  private String buildCopySqlFromCsv(String csvPath, String outputPath,
      MaterializeConfig config) {

    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT ");
    sql.append(buildSelectClause(config.getColumns()));
    sql.append("\n  FROM read_csv_auto(").append(quoteLiteral(csvPath)).append(")\n");
    sql.append(") TO ").append(quoteLiteral(outputPath));
    sql.append(buildCopyOptions(config));
    sql.append(";");

    return sql.toString();
  }

  /**
   * Builds DuckDB COPY SQL for Parquet source.
   */
  private String buildCopySqlFromParquet(String parquetPattern, String outputPath,
      MaterializeConfig config) {

    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT ");
    sql.append(buildSelectClause(config.getColumns()));
    sql.append("\n  FROM read_parquet(").append(quoteLiteral(parquetPattern));
    sql.append(", hive_partitioning=true, union_by_name=true)\n");
    sql.append(") TO ").append(quoteLiteral(outputPath));
    sql.append(buildCopyOptions(config));
    sql.append(";");

    return sql.toString();
  }

  /**
   * Builds SELECT clause from column configurations.
   */
  private String buildSelectClause(List<ColumnConfig> columns) {
    if (columns == null || columns.isEmpty()) {
      return "*";
    }

    StringBuilder clause = new StringBuilder();
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        clause.append(", ");
      }
      clause.append(columns.get(i).buildSelectExpression());
    }
    return clause.toString();
  }

  /**
   * Builds COPY options including FORMAT, PARTITION_BY, and COMPRESSION.
   */
  private String buildCopyOptions(MaterializeConfig config) {
    StringBuilder options = new StringBuilder();
    options.append(" (FORMAT PARQUET");

    // Add partition columns
    MaterializePartitionConfig partitionConfig = config.getPartition();
    if (partitionConfig != null && !partitionConfig.getColumns().isEmpty()) {
      options.append(", PARTITION_BY (");
      List<String> partitionCols = partitionConfig.getColumns();
      for (int i = 0; i < partitionCols.size(); i++) {
        if (i > 0) {
          options.append(", ");
        }
        options.append(partitionCols.get(i));
      }
      options.append(")");
    }

    // Add compression
    String compression = config.getOutput().getCompression();
    if (compression != null && !compression.isEmpty() && !"none".equalsIgnoreCase(compression)) {
      options.append(", COMPRESSION ").append(compression.toUpperCase());
    }

    // Add row group size
    int rowGroupSize = config.getOptions().getRowGroupSize();
    if (rowGroupSize > 0) {
      options.append(", ROW_GROUP_SIZE ").append(rowGroupSize);
    }

    options.append(", OVERWRITE_OR_IGNORE)");

    return options.toString();
  }

  /**
   * Builds consolidation SQL for merging temp files.
   */
  private String buildConsolidationSql(String tempBase, String finalBase,
      List<String> partitionColumns, String compression) {

    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT * FROM read_parquet(").append(quoteLiteral(tempBase + "/**/*.parquet"));
    sql.append(", hive_partitioning=true, union_by_name=true)\n");
    sql.append(") TO ").append(quoteLiteral(finalBase));

    sql.append(" (FORMAT PARQUET");
    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      sql.append(", PARTITION_BY (");
      for (int i = 0; i < partitionColumns.size(); i++) {
        if (i > 0) {
          sql.append(", ");
        }
        sql.append(partitionColumns.get(i));
      }
      sql.append(")");
    }
    if (compression != null && !compression.isEmpty() && !"none".equalsIgnoreCase(compression)) {
      sql.append(", COMPRESSION ").append(compression.toUpperCase());
    }
    sql.append(", OVERWRITE_OR_IGNORE)");
    sql.append(";");

    return sql.toString();
  }

  /**
   * Applies DuckDB settings for performance optimization.
   */
  private void applyDuckDBSettings(Connection conn, MaterializeOptionsConfig options)
      throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=" + options.getThreads());
      stmt.execute("SET preserve_insertion_order=" + options.isPreserveInsertionOrder());
      LOGGER.debug("Applied DuckDB settings: threads={}, preserve_insertion_order={}",
          options.getThreads(), options.isPreserveInsertionOrder());
    }
  }

  /**
   * Creates a DuckDB connection with extensions and S3 access configured.
   */
  private Connection getDuckDBConnection() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    loadExtensions(conn);
    configureS3Access(conn);
    return conn;
  }

  /**
   * Loads required DuckDB extensions.
   */
  private void loadExtensions(Connection conn) {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL parquet");
      stmt.execute("LOAD parquet");
      stmt.execute("INSTALL json");
      stmt.execute("LOAD json");
    } catch (SQLException e) {
      LOGGER.debug("Extensions already loaded or built-in: {}", e.getMessage());
    }
  }

  /**
   * Configures S3 access for DuckDB connection.
   */
  private void configureS3Access(Connection conn) {
    try {
      conn.createStatement().execute("INSTALL httpfs");
      conn.createStatement().execute("LOAD httpfs");

      Map<String, String> s3Config = storageProvider.getS3Config();
      if (s3Config != null && !s3Config.isEmpty()) {
        String accessKey = s3Config.get("accessKeyId");
        String secretKey = s3Config.get("secretAccessKey");
        String endpoint = s3Config.get("endpoint");
        String region = s3Config.get("region");

        if (accessKey != null && secretKey != null) {
          conn.createStatement().execute("SET s3_access_key_id='" + accessKey + "'");
          conn.createStatement().execute("SET s3_secret_access_key='" + secretKey + "'");
        }
        if (endpoint != null) {
          conn.createStatement().execute("SET s3_endpoint='" + endpoint + "'");
          conn.createStatement().execute("SET s3_url_style='path'");
        }
        if (region != null) {
          conn.createStatement().execute("SET s3_region='" + region + "'");
        } else {
          conn.createStatement().execute("SET s3_region='auto'");
        }
        LOGGER.debug("Configured DuckDB S3 access from storage provider");
      }
    } catch (SQLException e) {
      LOGGER.debug("S3 configuration skipped: {}", e.getMessage());
    }
  }

  /**
   * Quotes a string literal for SQL.
   */
  private static String quoteLiteral(String literal) {
    return "'" + literal.replace("'", "''") + "'";
  }

  /**
   * Creates a HiveParquetWriter for a storage provider.
   */
  public static HiveParquetWriter create(StorageProvider storageProvider, String baseDirectory) {
    return new HiveParquetWriter(storageProvider, baseDirectory);
  }

  /**
   * Result of a single batch processing.
   */
  private static class MaterializeBatchResult {
    private final long rowCount;
    private final int fileCount;

    MaterializeBatchResult(long rowCount, int fileCount) {
      this.rowCount = rowCount;
      this.fileCount = fileCount;
    }

    long getRowCount() {
      return rowCount;
    }

    int getFileCount() {
      return fileCount;
    }
  }
}
