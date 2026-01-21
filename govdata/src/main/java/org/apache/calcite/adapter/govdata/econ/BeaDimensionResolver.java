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

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
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
   * Populates the line code cache by reading regional_linecodes Iceberg table.
   *
   * @param config DimensionConfig containing properties (referenceDirectory must be set)
   * @param storageProvider Storage provider for file access (passed from EtlPipeline)
   */
  private synchronized void populateCache(DimensionConfig config, StorageProvider storageProvider) {
    if (cachePopulated) {
      return;
    }

    // Get reference directory from config - DimensionConfig.getProperty() resolves ${VAR} patterns
    // Normalize path to fix malformed S3A URIs (s3a:/ -> s3a://)
    String referenceDir = StorageProvider.normalizePath(config.getProperty("referenceDirectory"));

    if (referenceDir == null || referenceDir.isEmpty()) {
      throw new IllegalStateException(
          "BeaDimensionResolver: 'referenceDirectory' property must be configured. "
          + "Set referenceDirectory in dimension properties or configure "
          + "ECON_REFERENCE_CACHE_DIR/GOVDATA_CACHE_DIR environment variables.");
    }

    // Use Iceberg table with dynamic column discovery
    try {
      populateCacheFromIceberg(config, storageProvider, referenceDir);
    } catch (Exception e) {
      throw new IllegalStateException(
          "BeaDimensionResolver: Failed to load regional_linecodes: " + e.getMessage(), e);
    }
  }

  /**
   * Populates cache from Iceberg table.
   *
   * <p>Reads parquet files directly with hive partitioning to get tablename
   * from partition paths and LineCode/geography_level from data columns.
   */
  private void populateCacheFromIceberg(DimensionConfig config, StorageProvider storageProvider,
      String referenceDir) throws Exception {
    // Build path to regional_linecodes Iceberg table
    // Normalize path to fix malformed S3A URIs (s3a:/ -> s3a://)
    String linecodeTablePath =
        StorageProvider.normalizePath(config.getProperty("linecodeTablePath", referenceDir + "/regional_linecodes"));

    LOGGER.info("BeaDimensionResolver: Loading regional_linecodes from: {}", linecodeTablePath);

    try (Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);
         Statement stmt = conn.createStatement()) {

      // Read parquet files with hive partitioning. The 'tablename' column comes from
      // the partition path (e.g., tablename=SAINC1/), and LineCode/geography_level
      // are data columns in the parquet files.
      // Use COALESCE to handle both 'Key' (raw BEA column) and 'LineCode' (derived column)
      // since the expression mapping may not have been applied during ETL.
      String sql = "SELECT DISTINCT tablename, COALESCE(LineCode, Key) AS LineCode, geography_level "
          + "FROM read_parquet('" + linecodeTablePath + "/data/**/*.parquet', "
          + "hive_partitioning=true, union_by_name=true) "
          + "WHERE tablename IS NOT NULL AND COALESCE(LineCode, Key) IS NOT NULL "
          + "ORDER BY 1, 3, 2";

      LOGGER.debug("BeaDimensionResolver: Executing SQL: {}", sql);
      ResultSet rs;
      try {
        rs = stmt.executeQuery(sql);
      } catch (Exception queryEx) {
        // Likely "No files found" - reference data doesn't exist yet
        throw new IllegalStateException(
            "Cannot read regional_linecodes from: " + linecodeTablePath + "\n"
            + "This table is required to resolve 'line_code' dimension values.\n"
            + "Please run the econ_reference schema ETL first to create this reference data.\n"
            + "Underlying error: " + queryEx.getMessage());
      }
      int totalCount = loadFromResultSet(rs);

      if (totalCount == 0) {
        throw new IllegalStateException(
            "No line codes found in " + linecodeTablePath + "\n"
            + "The regional_linecodes table exists but is empty.\n"
            + "Please run the econ_reference schema ETL to populate this reference data.");
      }

      LOGGER.info("BeaDimensionResolver: Loaded {} line codes for {} (table,geo) combinations",
          totalCount, lineCodeCache.size());
      cachePopulated = true;
    }
  }

  /**
   * Loads line codes from a result set into the cache.
   * ResultSet expected columns: tablename, LineCode, geography_level
   */
  private int loadFromResultSet(ResultSet rs) throws Exception {
    int totalCount = 0;
    while (rs.next()) {
      String tableName = rs.getString(1);
      String lineCode = rs.getString(2);
      String geoLevel = rs.getString(3);

      if (tableName != null && lineCode != null) {
        // Infer geography_level from table prefix if not provided in data
        if (geoLevel == null || geoLevel.isEmpty()) {
          geoLevel = inferGeographyLevelFromTableName(tableName);
        }
        if (geoLevel != null) {
          addToCache(tableName, geoLevel.toLowerCase(), lineCode);
          totalCount++;
        }
      }
    }
    return totalCount;
  }

  /**
   * Infers geography_level from BEA Regional table name prefix.
   *
   * <p>BEA Regional tables follow naming conventions:
   * <ul>
   *   <li>CA* (e.g., CAINC1, CAGDP5) - County level data</li>
   *   <li>SA* (e.g., SAINC1, SAGDP5) - State annual data</li>
   *   <li>SQ* (e.g., SQINC1) - State quarterly data</li>
   *   <li>MA* (e.g., MARPP) - Metropolitan area data</li>
   * </ul>
   *
   * @param tableName BEA table name
   * @return geography level (county, state, msa) or null if unknown
   */
  private String inferGeographyLevelFromTableName(String tableName) {
    if (tableName == null || tableName.length() < 2) {
      return null;
    }
    String prefix = tableName.substring(0, 2).toUpperCase();
    switch (prefix) {
    case "CA":
      return "county";
    case "SA":
    case "SQ":
      return "state";
    case "MA":
      return "msa";
    default:
      LOGGER.debug("Unknown table prefix '{}' for table '{}', cannot infer geography level",
          prefix, tableName);
      return null;
    }
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
