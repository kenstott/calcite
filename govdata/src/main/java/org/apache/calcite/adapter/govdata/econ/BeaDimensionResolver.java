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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves BEA Regional dimension values with context awareness.
 *
 * <p>This resolver enables dependent dimension patterns for BEA Regional tables
 * where valid line_code values depend on both tablename AND geo_fips_set.
 * For example, SAGDP5 only has data for STATE level, not MSA or COUNTY.
 *
 * <h3>Context-Aware Resolution</h3>
 * <p>When resolving the "line_code" dimension, this resolver:
 * <ol>
 *   <li>Gets "tablename" and "geo_fips_set" from the context map</li>
 *   <li>Maps geo_fips_set to geography_level (STATE→state, COUNTY→county, MSA→msa)</li>
 *   <li>Queries regional_linecodes for valid LineCodes matching both constraints</li>
 *   <li>Returns only LineCodes valid for that (tablename, geography_level) combination</li>
 * </ol>
 *
 * <h3>Geography Level Mapping</h3>
 * <ul>
 *   <li>STATE → state (SA* tables like SAGDP, SAINC)</li>
 *   <li>COUNTY → county (CA* tables like CAGDP, CAINC)</li>
 *   <li>MSA, MIC, PORT, DIV, CSA → msa (MA* tables like MARPP)</li>
 * </ul>
 *
 * <h3>Storage Agnostic</h3>
 * <p>The resolver uses the StorageProvider passed from the EtlPipeline,
 * supporting both local filesystem and S3 storage transparently.
 *
 * <h3>Schema Usage</h3>
 * <pre>{@code
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.econ.BeaDimensionResolver"
 *
 * dimensions:
 *   geo_fips_set:
 *     - STATE
 *     - COUNTY
 *     - MSA
 *   tablename:
 *     - SAINC1
 *     - SAGDP5
 *   line_code:
 *     type: custom
 *     properties:
 *       referenceDirectory: "${ECON_REFERENCE_CACHE_DIR:}"
 * }</pre>
 *
 * @see DimensionResolver
 * @see DimensionConfig
 */
public class BeaDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeaDimensionResolver.class);

  /**
   * Cache of line codes per (tablename, geography_level) combination.
   * Key: "tablename|geography_level" (e.g., "SAINC1|state"), Value: List of valid LineCodes
   */
  private final Map<String, List<String>> lineCodeCache = new HashMap<String, List<String>>();

  /**
   * Flag indicating whether the cache has been fully populated.
   */
  private boolean cachePopulated = false;

  /**
   * Maps geo_fips_set dimension values to geography_level in reference data.
   */
  private static final Map<String, String> GEO_FIPS_TO_LEVEL = new HashMap<String, String>();
  static {
    GEO_FIPS_TO_LEVEL.put("STATE", "state");
    GEO_FIPS_TO_LEVEL.put("COUNTY", "county");
    GEO_FIPS_TO_LEVEL.put("MSA", "msa");
    GEO_FIPS_TO_LEVEL.put("MIC", "msa");   // Micropolitan uses same as MSA
    GEO_FIPS_TO_LEVEL.put("PORT", "msa");  // Metropolitan portions
    GEO_FIPS_TO_LEVEL.put("DIV", "msa");   // Metropolitan divisions
    GEO_FIPS_TO_LEVEL.put("CSA", "msa");   // Combined statistical areas
  }

  /**
   * Default constructor required for reflection-based instantiation.
   */
  public BeaDimensionResolver() {
    // No-op - cache populated lazily on first resolve call
  }

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    // Only handle line_code dimension with context-aware resolution
    if (!"line_code".equals(dimensionName)) {
      LOGGER.debug("BeaDimensionResolver: Dimension '{}' not handled, returning empty",
          dimensionName);
      return Collections.emptyList();
    }

    // Get tablename from context
    String tablename = context.get("tablename");
    if (tablename == null || tablename.isEmpty()) {
      throw new IllegalStateException(
          "BeaDimensionResolver: No tablename in context for line_code resolution. "
          + "Ensure tablename dimension is defined before line_code.");
    }

    // Get geo_fips_set from context and map to geography_level
    String geoFipsSet = context.get("geo_fips_set");
    if (geoFipsSet == null || geoFipsSet.isEmpty()) {
      throw new IllegalStateException(
          "BeaDimensionResolver: No geo_fips_set in context for line_code resolution. "
          + "Ensure geo_fips_set dimension is defined before line_code.");
    }

    String geographyLevel = GEO_FIPS_TO_LEVEL.get(geoFipsSet);
    if (geographyLevel == null) {
      LOGGER.warn("BeaDimensionResolver: Unknown geo_fips_set '{}', skipping", geoFipsSet);
      return Collections.emptyList();
    }

    // Ensure cache is populated using config and storage provider
    if (!cachePopulated) {
      populateCache(config, storageProvider);
    }

    // Look up line codes for this (tablename, geography_level) combination
    String cacheKey = tablename + "|" + geographyLevel;
    List<String> lineCodes = lineCodeCache.get(cacheKey);
    if (lineCodes == null || lineCodes.isEmpty()) {
      // Return empty list - this (tablename, geo_fips_set) combination has no data
      // The EtlPipeline will skip this dimension combination
      LOGGER.debug("BeaDimensionResolver: No line codes for table '{}' at geo level '{}' ({}). "
          + "This combination will be skipped.",
          tablename, geographyLevel, geoFipsSet);
      return Collections.emptyList();
    }

    LOGGER.debug("BeaDimensionResolver: Resolved {} line codes for table '{}' at geo level '{}'",
        lineCodes.size(), tablename, geographyLevel);
    return lineCodes;
  }

  /**
   * Populates the line code cache by reading regional_linecodes data.
   * First tries Iceberg table, then falls back to parquet files if Iceberg fails.
   *
   * @param config DimensionConfig containing properties (referenceDirectory must be set)
   * @param storageProvider Storage provider for file access (passed from EtlPipeline)
   */
  private synchronized void populateCache(DimensionConfig config, StorageProvider storageProvider) {
    if (cachePopulated) {
      return;
    }

    // Get reference directory from config - DimensionConfig.getProperty() resolves ${VAR} patterns
    String referenceDir = config.getProperty("referenceDirectory");

    if (referenceDir == null || referenceDir.isEmpty()) {
      throw new IllegalStateException(
          "BeaDimensionResolver: 'referenceDirectory' property must be configured. "
          + "Set referenceDirectory in dimension properties or configure "
          + "ECON_REFERENCE_CACHE_DIR/GOVDATA_CACHE_DIR environment variables.");
    }

    // Try Iceberg table first, then fall back to parquet files
    try {
      populateCacheFromIceberg(config, storageProvider, referenceDir);
    } catch (Exception icebergEx) {
      LOGGER.warn("BeaDimensionResolver: Iceberg scan failed: {}. Falling back to parquet files.",
          icebergEx.getMessage());
      try {
        populateCacheFromParquet(storageProvider, referenceDir);
      } catch (Exception parquetEx) {
        throw new IllegalStateException(
            "BeaDimensionResolver: Failed to load regional_linecodes. "
            + "Iceberg error: " + icebergEx.getMessage() + ". "
            + "Parquet error: " + parquetEx.getMessage(), parquetEx);
      }
    }
  }

  /**
   * Populates cache from Iceberg table.
   */
  private void populateCacheFromIceberg(DimensionConfig config, StorageProvider storageProvider,
      String referenceDir) throws Exception {
    // Build path to regional_linecodes Iceberg table
    String linecodeTablePath = config.getProperty("linecodeTablePath",
        referenceDir + "/regional_linecodes");

    LOGGER.info("BeaDimensionResolver: Loading regional_linecodes from Iceberg table: {}",
        linecodeTablePath);

    try (Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);
         Statement stmt = conn.createStatement()) {

      // Load Iceberg extension for DuckDB and enable version guessing
      stmt.execute("INSTALL iceberg; LOAD iceberg;");
      stmt.execute("SET unsafe_enable_version_guessing = true;");

      // Query Iceberg table using iceberg_scan
      String descSql = "DESCRIBE SELECT * FROM iceberg_scan('" + linecodeTablePath + "')";
      LOGGER.debug("BeaDimensionResolver: Discovering columns: {}", descSql);
      ResultSet descRs = stmt.executeQuery(descSql);
      String tableNameCol = null;
      String lineCodeCol = null;
      String geoLevelCol = null;
      java.util.List<String> allColumns = new java.util.ArrayList<String>();
      while (descRs.next()) {
        String colName = descRs.getString("column_name");
        allColumns.add(colName);
        if (colName.equalsIgnoreCase("tablename")) {
          tableNameCol = colName;
        } else if (colName.equalsIgnoreCase("linecode")) {
          lineCodeCol = colName;
        } else if (colName.equalsIgnoreCase("geography_level")) {
          geoLevelCol = colName;
        }
      }
      descRs.close();

      LOGGER.info("BeaDimensionResolver: All columns in Iceberg table: {}", allColumns);

      if (tableNameCol == null || lineCodeCol == null) {
        throw new IllegalStateException(
            "Required columns not found in regional_linecodes Iceberg table. "
            + "Available columns: " + allColumns + ". Expected: TableName, LineCode.");
      }

      // Include geography_level in query if available
      String sql;
      if (geoLevelCol != null) {
        sql = "SELECT DISTINCT \"" + tableNameCol + "\", \"" + lineCodeCol + "\", \"" + geoLevelCol + "\" "
            + "FROM iceberg_scan('" + linecodeTablePath + "') "
            + "ORDER BY 1, 3, 2";
      } else {
        sql = "SELECT DISTINCT \"" + tableNameCol + "\", \"" + lineCodeCol + "\", NULL as geography_level "
            + "FROM iceberg_scan('" + linecodeTablePath + "') "
            + "ORDER BY 1, 2";
      }

      LOGGER.debug("BeaDimensionResolver: Executing SQL: {}", sql);
      ResultSet rs = stmt.executeQuery(sql);
      int totalCount = loadFromResultSet(rs);

      if (totalCount == 0) {
        throw new IllegalStateException(
            "No line codes found in Iceberg table " + linecodeTablePath);
      }

      LOGGER.info("BeaDimensionResolver: Loaded {} line codes for {} (table,geo) combinations from Iceberg",
          totalCount, lineCodeCache.size());
      cachePopulated = true;
    }
  }

  /**
   * Populates cache from parquet files (fallback when Iceberg fails).
   */
  private void populateCacheFromParquet(StorageProvider storageProvider, String referenceDir)
      throws Exception {
    // Try multiple patterns for parquet file locations
    String[] patterns = {
        referenceDir + "/type=reference/tablename=*/*.parquet",
        referenceDir + "/type=regional_linecodes/tablename=*/*.parquet",
        referenceDir + "/regional_linecodes/data/**/*.parquet"
    };

    LOGGER.info("BeaDimensionResolver: Attempting parquet file fallback from: {}", referenceDir);

    try (Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);
         Statement stmt = conn.createStatement()) {

      for (String pattern : patterns) {
        try {
          // Include geography_level in the query
          String sql = "SELECT DISTINCT TableName, LineCode, geography_level "
              + "FROM read_parquet('" + pattern + "', hive_partitioning=false, union_by_name=true) "
              + "WHERE TableName IS NOT NULL AND LineCode IS NOT NULL "
              + "ORDER BY 1, 3, 2";

          LOGGER.debug("BeaDimensionResolver: Trying parquet pattern: {}", pattern);
          ResultSet rs = stmt.executeQuery(sql);
          int totalCount = loadFromResultSet(rs);

          if (totalCount > 0) {
            LOGGER.info("BeaDimensionResolver: Loaded {} line codes for {} (table,geo) combinations from parquet: {}",
                totalCount, lineCodeCache.size(), pattern);
            cachePopulated = true;
            return;
          }
        } catch (Exception e) {
          LOGGER.debug("BeaDimensionResolver: Pattern '{}' failed: {}", pattern, e.getMessage());
        }
      }

      throw new IllegalStateException(
          "No regional_linecodes data found at any pattern in " + referenceDir);
    }
  }

  /**
   * Loads line codes from a result set into the cache.
   * ResultSet expected columns: TableName, LineCode, geography_level
   */
  private int loadFromResultSet(ResultSet rs) throws Exception {
    int totalCount = 0;
    while (rs.next()) {
      String tableName = rs.getString(1);
      String lineCode = rs.getString(2);
      String geoLevel = rs.getString(3);

      if (tableName != null && lineCode != null) {
        // Default to all geo levels if geography_level is null/empty
        if (geoLevel == null || geoLevel.isEmpty()) {
          // Add to all known geo levels
          for (String level : new String[]{"state", "county", "msa"}) {
            addToCache(tableName, level, lineCode);
            totalCount++;
          }
        } else {
          addToCache(tableName, geoLevel.toLowerCase(), lineCode);
          totalCount++;
        }
      }
    }
    return totalCount;
  }

  /**
   * Adds a line code to the cache for a (tablename, geography_level) combination.
   */
  private void addToCache(String tableName, String geoLevel, String lineCode) {
    String cacheKey = tableName + "|" + geoLevel;
    List<String> codes = lineCodeCache.get(cacheKey);
    if (codes == null) {
      codes = new ArrayList<String>();
      lineCodeCache.put(cacheKey, codes);
    }
    if (!codes.contains(lineCode)) {
      codes.add(lineCode);
    }
  }

  /**
   * Clears the cache (for testing purposes).
   */
  void clearCache() {
    lineCodeCache.clear();
    cachePopulated = false;
  }

  /**
   * Returns the number of cached tables (for testing purposes).
   */
  int getCachedTableCount() {
    return lineCodeCache.size();
  }
}
