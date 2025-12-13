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
package org.apache.calcite.adapter.file.partition;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reorganizes parquet data by consolidating files into different partition layouts.
 *
 * <p>Handles large datasets using batched processing to avoid OOM errors:
 * <ul>
 *   <li>Phase 1: Process data in batches (by year, region, etc.) writing to temp location</li>
 *   <li>Phase 2: Consolidate temp files into final single-file partitions</li>
 *   <li>Phase 3: Set lifecycle rules for automatic temp cleanup</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * ParquetReorganizer reorganizer = new ParquetReorganizer(storageProvider, baseDirectory);
 * reorganizer.reorganize(
 *     "type=income/year=*\/geo=*\/*.parquet",     // source pattern
 *     "type=income_by_geo",                        // target base
 *     Arrays.asList("geo"),                        // partition columns
 *     Collections.singletonMap("geo", "GeoFips"), // column mappings
 *     Arrays.asList("year", "geo_fips_set"),      // batch columns
 *     2020, 2024                                   // year range
 * );
 * </pre>
 */
public class ParquetReorganizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReorganizer.class);

  private final StorageProvider storageProvider;
  private final String baseDirectory;

  public ParquetReorganizer(StorageProvider storageProvider, String baseDirectory) {
    this.storageProvider = storageProvider;
    this.baseDirectory = baseDirectory;
  }

  /**
   * Configuration for a reorganization job.
   */
  public static class ReorgConfig {
    private static final int DEFAULT_THREADS = 2;

    private final String sourcePattern;
    private final String targetBase;
    private final List<String> partitionColumns;
    private final Map<String, String> columnMappings;
    private final List<String> batchPartitionColumns;
    private final int startYear;
    private final int endYear;
    private final String name;
    private final int threads;

    private ReorgConfig(Builder builder) {
      this.sourcePattern = builder.sourcePattern;
      this.targetBase = builder.targetBase;
      this.partitionColumns = builder.partitionColumns != null
          ? builder.partitionColumns : Collections.<String>emptyList();
      this.columnMappings = builder.columnMappings != null
          ? builder.columnMappings : Collections.<String, String>emptyMap();
      this.batchPartitionColumns = builder.batchPartitionColumns != null
          ? builder.batchPartitionColumns : Collections.<String>emptyList();
      this.startYear = builder.startYear;
      this.endYear = builder.endYear;
      this.name = builder.name;
      this.threads = builder.threads > 0 ? builder.threads : DEFAULT_THREADS;
    }

    public String getSourcePattern() {
      return sourcePattern;
    }

    public String getTargetBase() {
      return targetBase;
    }

    public List<String> getPartitionColumns() {
      return partitionColumns;
    }

    public Map<String, String> getColumnMappings() {
      return columnMappings;
    }

    public List<String> getBatchPartitionColumns() {
      return batchPartitionColumns;
    }

    public int getStartYear() {
      return startYear;
    }

    public int getEndYear() {
      return endYear;
    }

    public String getName() {
      return name;
    }

    public int getThreads() {
      return threads;
    }

    public static Builder builder() {
      return new Builder();
    }

    /**
     * Builder for ReorgConfig.
     */
    public static class Builder {
      private String sourcePattern;
      private String targetBase;
      private List<String> partitionColumns;
      private Map<String, String> columnMappings;
      private List<String> batchPartitionColumns;
      private int startYear;
      private int endYear;
      private String name;
      private int threads;

      public Builder sourcePattern(String sourcePattern) {
        this.sourcePattern = sourcePattern;
        return this;
      }

      public Builder targetBase(String targetBase) {
        this.targetBase = targetBase;
        return this;
      }

      public Builder partitionColumns(List<String> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
      }

      public Builder columnMappings(Map<String, String> columnMappings) {
        this.columnMappings = columnMappings;
        return this;
      }

      public Builder batchPartitionColumns(List<String> batchPartitionColumns) {
        this.batchPartitionColumns = batchPartitionColumns;
        return this;
      }

      public Builder yearRange(int startYear, int endYear) {
        this.startYear = startYear;
        this.endYear = endYear;
        return this;
      }

      public Builder name(String name) {
        this.name = name;
        return this;
      }

      public Builder threads(int threads) {
        this.threads = threads;
        return this;
      }

      public ReorgConfig build() {
        if (sourcePattern == null || sourcePattern.isEmpty()) {
          throw new IllegalArgumentException("sourcePattern is required");
        }
        if (targetBase == null || targetBase.isEmpty()) {
          throw new IllegalArgumentException("targetBase is required");
        }
        return new ReorgConfig(this);
      }
    }
  }

  /**
   * Reorganizes data based on the provided configuration.
   *
   * @param config Reorganization configuration
   * @throws IOException if reorganization fails
   */
  public void reorganize(ReorgConfig config) throws IOException {
    String fullTargetBase = storageProvider.resolvePath(baseDirectory, config.getTargetBase());

    // Temp location with timestamp to isolate each run
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    String tempBase = fullTargetBase + "/_temp_reorg/" + timestamp;
    String tempPrefix = config.getTargetBase() + "/_temp_reorg/" + timestamp + "/";

    String displayName = config.getName() != null ? config.getName() : config.getTargetBase();
    LOGGER.info("Reorganizing '{}' with batching:\n  FROM: {}\n  TO: {} (via temp: {})",
        displayName, config.getSourcePattern(), fullTargetBase, tempBase);

    try (Connection conn = getDuckDBConnection()) {
      // Apply memory-optimizing settings to avoid OOM on large datasets
      try (Statement setupStmt = conn.createStatement()) {
        setupStmt.execute("SET threads=" + config.getThreads());
        setupStmt.execute("SET preserve_insertion_order=false");
        LOGGER.debug("Applied DuckDB settings: threads={}, preserve_insertion_order=false",
            config.getThreads());
      }

      // Phase 1: Process data in batches
      List<Map<String, String>> batchCombinations = buildBatchCombinations(
          conn, config.getSourcePattern(), config.getBatchPartitionColumns(),
          config.getStartYear(), config.getEndYear());

      if (batchCombinations.isEmpty()) {
        // No batching configured - fall back to year-only batching
        processYearBatches(conn, config, tempBase);
      } else {
        processBatchCombinations(conn, config, batchCombinations, tempBase);
      }

      // Phase 2: Consolidate temp files into final location
      LOGGER.info("Phase 2: Consolidating temp files to final location...");
      String consolidateSql = buildConsolidationSql(tempBase, fullTargetBase,
          config.getPartitionColumns());
      LOGGER.debug("Phase 2 SQL:\n{}", consolidateSql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(consolidateSql);
        LOGGER.info("  Consolidation complete for {}", displayName);
      }

      // Phase 3: Set lifecycle rule for temp cleanup AFTER consolidation
      LOGGER.info("Phase 3: Setting lifecycle rule for temp cleanup...");
      try {
        storageProvider.ensureLifecycleRule(tempPrefix, 1);
        LOGGER.info("  Lifecycle rule set: {} will auto-expire in 1 day", tempPrefix);
      } catch (IOException e) {
        LOGGER.warn("  Could not set lifecycle rule (temp files will remain): {}", e.getMessage());
      }

      LOGGER.info("Successfully reorganized: {}", displayName);

    } catch (java.sql.SQLException e) {
      String errorMsg = String.format("DuckDB batched reorganization failed for '%s': %s",
          displayName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Process data year by year when no batch columns are configured.
   */
  private void processYearBatches(Connection conn, ReorgConfig config, String tempBase)
      throws java.sql.SQLException {
    int startYear = config.getStartYear();
    int endYear = config.getEndYear();

    LOGGER.info("Phase 1: Processing {} years individually (no batch_partition_columns)...",
        (endYear - startYear + 1));

    for (int year = startYear; year <= endYear; year++) {
      String yearSourceGlob = config.getSourcePattern().replace("year=*", "year=" + year);
      String fullYearSourceGlob = storageProvider.resolvePath(baseDirectory, yearSourceGlob);

      String sql = buildReorganizationSql(fullYearSourceGlob, tempBase,
          config.getPartitionColumns(), config.getColumnMappings(), "year_" + year + "_{i}");

      LOGGER.debug("Phase 1 SQL (year={}):\n{}", year, sql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
        LOGGER.info("  Processed year {} for {}", year, config.getName());
      } catch (java.sql.SQLException e) {
        LOGGER.warn("  Skipped year {} (no data or error): {}", year, e.getMessage());
      }
    }
  }

  /**
   * Process data using configured batch combinations.
   */
  private void processBatchCombinations(Connection conn, ReorgConfig config,
      List<Map<String, String>> batchCombinations, String tempBase) throws java.sql.SQLException {

    LOGGER.info("Phase 1: Processing {} batch combinations (batch by: {})...",
        batchCombinations.size(), config.getBatchPartitionColumns());

    int processed = 0;
    for (Map<String, String> batch : batchCombinations) {
      // Build source glob with batch filters applied
      String batchSourceGlob = config.getSourcePattern();
      StringBuilder filenamePattern = new StringBuilder();

      for (Map.Entry<String, String> entry : batch.entrySet()) {
        String col = entry.getKey();
        String val = entry.getValue();
        batchSourceGlob = batchSourceGlob.replace(col + "=*", col + "=" + val);
        if (filenamePattern.length() > 0) {
          filenamePattern.append("_");
        }
        filenamePattern.append(col).append("_").append(val);
      }
      filenamePattern.append("_{i}");

      String fullBatchSourceGlob = storageProvider.resolvePath(baseDirectory, batchSourceGlob);

      String sql = buildReorganizationSql(fullBatchSourceGlob, tempBase,
          config.getPartitionColumns(), config.getColumnMappings(), filenamePattern.toString());

      LOGGER.debug("Phase 1 SQL (batch={}):\n{}", batch, sql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
        processed++;
        if (processed % 10 == 0) {
          LOGGER.info("  Processed {}/{} batches...", processed, batchCombinations.size());
        }
      } catch (java.sql.SQLException e) {
        // Log but continue - some combinations may not have data
        LOGGER.debug("  Skipped batch {} (no data): {}", batch, e.getMessage());
      }
    }
    LOGGER.info("  Completed {} batches for {}", processed, config.getName());
  }

  /**
   * Builds all batch combinations from the configured batch_partition_columns.
   *
   * <p>For example, if batch_partition_columns = [year, geo_fips_set], returns
   * all combinations like [{year=2020, geo_fips_set=STATE}, {year=2020, geo_fips_set=COUNTY}, ...].
   *
   * <p>Special handling for 'year': uses startYear/endYear range instead of querying data.
   */
  private List<Map<String, String>> buildBatchCombinations(Connection conn,
      String sourceGlobTemplate, List<String> batchPartitionColumns,
      int startYear, int endYear) {

    if (batchPartitionColumns == null || batchPartitionColumns.isEmpty()) {
      return Collections.emptyList();
    }

    // Build values for each batch column
    List<List<String>> columnValues = new ArrayList<List<String>>();
    List<String> columnNames = new ArrayList<String>();

    for (String col : batchPartitionColumns) {
      List<String> values;
      if ("year".equalsIgnoreCase(col)) {
        // Use configured year range
        values = new ArrayList<String>();
        for (int y = startYear; y <= endYear; y++) {
          values.add(String.valueOf(y));
        }
      } else {
        // Query distinct values from data
        values = getDistinctPartitionValues(conn, sourceGlobTemplate, col);
      }

      if (values.isEmpty()) {
        LOGGER.warn("No values found for batch column '{}', skipping batching", col);
        return Collections.emptyList();
      }

      columnNames.add(col);
      columnValues.add(values);
    }

    // Build cartesian product of all column values
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    buildCombinationsRecursive(columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), combinations);

    LOGGER.debug("Built {} batch combinations from {} columns",
        combinations.size(), batchPartitionColumns);
    return combinations;
  }

  /**
   * Recursively builds cartesian product of column values.
   */
  private void buildCombinationsRecursive(List<String> columnNames, List<List<String>> columnValues,
      int depth, Map<String, String> current, List<Map<String, String>> result) {
    if (depth == columnNames.size()) {
      result.add(new LinkedHashMap<String, String>(current));
      return;
    }

    String colName = columnNames.get(depth);
    for (String value : columnValues.get(depth)) {
      current.put(colName, value);
      buildCombinationsRecursive(columnNames, columnValues, depth + 1, current, result);
    }
    current.remove(colName);
  }

  /**
   * Gets distinct values for a partition column by listing directories.
   * This is much faster than querying parquet files since values are in directory names.
   *
   * <p>For pattern "type=income/year=*\/geo_fips_set=*\/*.parquet" and column "geo_fips_set",
   * lists directories and extracts values like STATE, COUNTY from "geo_fips_set=STATE".
   */
  private List<String> getDistinctPartitionValues(Connection conn, String sourceGlobTemplate,
      String partitionColumn) {
    java.util.Set<String> values = new java.util.TreeSet<String>();

    // Build directory pattern to list (up to and including the partition column)
    // E.g., "type=income/year=*/geo_fips_set=*/*.parquet" -> "type=income/"
    String columnPattern = partitionColumn + "=";
    String dirPattern = sourceGlobTemplate;

    // Find where this partition column appears in the pattern
    int colIdx = dirPattern.indexOf(columnPattern);
    if (colIdx < 0) {
      LOGGER.warn("Partition column '{}' not found in pattern '{}'", partitionColumn, sourceGlobTemplate);
      return new ArrayList<String>(values);
    }

    // Get the prefix up to but not including this partition level
    String prefix = dirPattern.substring(0, colIdx);
    if (prefix.endsWith("/")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }

    String fullPrefix = storageProvider.resolvePath(baseDirectory, prefix);
    LOGGER.debug("Listing directories under '{}' to find {} values", fullPrefix, partitionColumn);

    try {
      // List files/directories under the prefix
      List<StorageProvider.FileEntry> entries =
          storageProvider.listFiles(fullPrefix, true);

      // Extract partition values from paths
      // Look for paths like ".../{partitionColumn}={value}/..."
      java.util.regex.Pattern valuePattern =
          java.util.regex.Pattern.compile(partitionColumn + "=([^/]+)");

      for (StorageProvider.FileEntry entry : entries) {
        java.util.regex.Matcher matcher = valuePattern.matcher(entry.getPath());
        if (matcher.find()) {
          values.add(matcher.group(1));
        }
      }

      LOGGER.debug("Found {} distinct {} values via directory listing: {}",
          values.size(), partitionColumn, values);

    } catch (java.io.IOException e) {
      LOGGER.warn("Failed to list directories for {}: {}", partitionColumn, e.getMessage());
      // Fall back to SQL query if listing fails
      return getDistinctPartitionValuesViaSql(conn, sourceGlobTemplate, partitionColumn);
    }

    return new ArrayList<String>(values);
  }

  /**
   * Fallback: gets distinct values via SQL query (slower but works if listing fails).
   */
  private List<String> getDistinctPartitionValuesViaSql(Connection conn, String sourceGlobTemplate,
      String partitionColumn) {
    List<String> values = new ArrayList<String>();

    String globPattern = storageProvider.resolvePath(baseDirectory, sourceGlobTemplate);
    String sql = "SELECT DISTINCT " + partitionColumn + " FROM read_parquet("
        + quoteLiteral(globPattern) + ", hive_partitioning=true) ORDER BY " + partitionColumn;

    LOGGER.debug("Getting distinct {} values with SQL (fallback): {}", partitionColumn, sql);

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String value = rs.getString(1);
        if (value != null) {
          values.add(value);
        }
      }
      LOGGER.debug("Found {} distinct {} values via SQL", values.size(), partitionColumn);
    } catch (java.sql.SQLException e) {
      LOGGER.warn("Failed to get distinct {} values via SQL: {}", partitionColumn, e.getMessage());
    }

    return values;
  }

  /**
   * Builds DuckDB SQL for reorganizing data into partitioned files.
   */
  private String buildReorganizationSql(String sourceGlob, String targetBase,
      List<String> partitionColumns, Map<String, String> columnMappings,
      String filenamePattern) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");

    // Build SELECT clause with column aliases if mappings exist
    if (columnMappings != null && !columnMappings.isEmpty()) {
      StringBuilder selectClause = new StringBuilder("  SELECT *");
      for (Map.Entry<String, String> mapping : columnMappings.entrySet()) {
        String targetCol = mapping.getKey();
        String sourceCol = mapping.getValue();
        selectClause.append(", \"").append(sourceCol).append("\" AS ").append(targetCol);
      }
      sql.append(selectClause).append(" FROM read_parquet(");
    } else {
      sql.append("  SELECT * FROM read_parquet(");
    }

    sql.append(quoteLiteral(sourceGlob))
       .append(", hive_partitioning=true, union_by_name=true)\n");
    sql.append(") TO ").append(quoteLiteral(targetBase));

    // Add format and partition options
    sql.append(" (FORMAT PARQUET");
    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      sql.append(", PARTITION_BY (");
      StringBuilder colList = new StringBuilder();
      for (String col : partitionColumns) {
        if (colList.length() > 0) {
          colList.append(", ");
        }
        colList.append(col);
      }
      sql.append(colList);
      sql.append(")");
    }
    if (filenamePattern != null && !filenamePattern.isEmpty()) {
      sql.append(", FILENAME_PATTERN ").append(quoteLiteral(filenamePattern));
    }
    sql.append(", OVERWRITE_OR_IGNORE");
    sql.append(");");

    return sql.toString();
  }

  /**
   * Builds SQL for consolidation phase - reads all temp files and writes to final location.
   */
  private String buildConsolidationSql(String tempBase, String finalBase,
      List<String> partitionColumns) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT * FROM read_parquet(").append(quoteLiteral(tempBase + "/**/*.parquet"))
       .append(", hive_partitioning=true, union_by_name=true)\n");
    sql.append(") TO ").append(quoteLiteral(finalBase));

    sql.append(" (FORMAT PARQUET");
    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      sql.append(", PARTITION_BY (");
      StringBuilder colList = new StringBuilder();
      for (String col : partitionColumns) {
        if (colList.length() > 0) {
          colList.append(", ");
        }
        colList.append(col);
      }
      sql.append(colList);
      sql.append("), OVERWRITE_OR_IGNORE");
    }
    sql.append(");");

    return sql.toString();
  }

  /**
   * Creates a DuckDB connection with S3 access configured.
   */
  private Connection getDuckDBConnection() throws java.sql.SQLException {
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
    } catch (java.sql.SQLException e) {
      LOGGER.debug("Parquet extension already loaded or built-in");
    }
  }

  /**
   * Configures S3 access for DuckDB connection if storage provider is S3-based.
   */
  private void configureS3Access(Connection conn) {
    try {
      conn.createStatement().execute("INSTALL httpfs");
      conn.createStatement().execute("LOAD httpfs");

      // Get S3 config from storage provider
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
    } catch (java.sql.SQLException e) {
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
   * Creates a ParquetReorganizer for a storage provider.
   */
  public static ParquetReorganizer create(StorageProvider storageProvider, String baseDirectory) {
    return new ParquetReorganizer(storageProvider, baseDirectory);
  }
}
