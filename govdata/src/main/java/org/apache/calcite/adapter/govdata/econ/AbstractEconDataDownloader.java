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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Abstract base class for ECON data downloaders providing common infrastructure
 * for cache management, rate limiting, and download/conversion flow patterns.
 *
 * <p>Implements the Template Method pattern to enforce consistent cache manifest
 * usage across all economic data sources while allowing API-specific customization.
 *
 * <h3>Standard Flow Pattern</h3>
 * <pre>
 * Download Flow:
 * 1. Check manifest.isCached() → if true, skip download
 * 2. Check local file exists (defensive) → update manifest if found
 * 3. Download data via API-specific implementation
 * 4. Save to local cache directory
 * 5. Mark as cached in manifest
 *
 * Conversion Flow:
 * 1. Check storageProvider.exists(targetPath) → if true, skip conversion
 * 2. Convert JSON to Parquet via API-specific implementation
 * 3. FileSchema's conversion registry automatically tracks the conversion
 * 4. No need to mark as converted - FileSchema handles this
 * </pre>
 *
 * <h3>Subclass Responsibilities</h3>
 * Subclasses must implement:
 * <ul>
 *   <li>{@link #getTableName()} - Specify the primary table this downloader is associated with</li>
 *   <li>API-specific download methods using provided helper methods</li>
 *   <li>API-specific conversion methods using provided helper methods</li>
 * </ul>
 *
 * <p>Rate limiting is configured via schema metadata (download.rateLimit).
 * Subclasses must override {@link #getTableName()} to specify which table this
 * downloader is associated with. The table's download configuration contains
 * rate limit settings. If not specified, default values are used
 * (1000ms interval, 3 retries, 1000ms retry delay).</p>
 *
 * @see CacheManifest
 * @see StorageProvider
 */
public abstract class AbstractEconDataDownloader extends AbstractGovDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEconDataDownloader.class);

  /** Shared ObjectMapper for JSON serialization */
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Data frequency for partitioning strategy (Phase 4).
   * Enables partition pruning for time-range queries.
   */
  public enum DataFrequency {
    DAILY("daily", "D"),
    MONTHLY("monthly", "M"),
    QUARTERLY("quarterly", "Q"),
    ANNUAL("annual", "A"),
    VARIOUS("various", "V");

    private final String partitionName;
    private final String shortCode;

    DataFrequency(String partitionName, String shortCode) {
      this.partitionName = partitionName;
      this.shortCode = shortCode;
    }

    public String getPartitionName() {
      return partitionName;
    }

    public String getShortCode() {
      return shortCode;
    }

    /**
     * Parse frequency from short code (D, M, Q, A).
     *
     * @param code Short frequency code
     * @return Corresponding DataFrequency
     * @throws IllegalArgumentException if code is unknown
     */
    public static DataFrequency fromShortCode(String code) {
      for (DataFrequency freq : values()) {
        if (freq.shortCode.equals(code)) {
          return freq;
        }
      }
      throw new IllegalArgumentException("Unknown frequency code: " + code);
    }
  }

  /**
   * Constructs base downloader with required infrastructure.
   *
   * @param cacheDirectory Local directory for caching raw JSON data
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   */
  protected AbstractEconDataDownloader(String cacheDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDirectory, cacheDirectory, cacheDirectory, cacheStorageProvider, storageProvider, null);
  }

  /**
   * Constructs base downloader with separate raw cache and operating directories and shared cache manifest.
   * This constructor should be used when multiple downloaders share the same manifest to avoid stale cache issues.
   *
   * @param cacheDirectory Local directory for caching raw JSON data
   * @param operatingDirectory Directory for storing operational metadata (.aperio/<schema>/)
   * @param parquetDirectory Directory for storing parquet files (e.g., s3://govdata-parquet)
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   * @param sharedManifest Shared cache manifest (if null, will load from operatingDirectory)
   */
  protected AbstractEconDataDownloader(String cacheDirectory, String operatingDirectory, String parquetDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDirectory, operatingDirectory, parquetDirectory, cacheStorageProvider, storageProvider, "econ", sharedManifest);
  }

  /**
   * Saves downloaded JSON content to cache and updates manifest.
   * This is the final step in the download flow pattern.
   *
   * @param dataType Type of data being cached
   * @param year Year of data
   * @param params Additional parameters for cache key
   * @param relativePath Relative path within cache directory
   * @param jsonContent JSON content to save
   * @throws IOException If file write fails
   */
  protected final void saveToCache(String dataType, int year, Map<String, String> params,
      String relativePath, String jsonContent) throws IOException {

    // Save raw JSON data via cache storage provider
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    // Mark as cached in manifest (operating metadata stays in .aperio via File API)
    cacheManifest.markCached(dataType, year, params, relativePath, jsonContent.length());
    cacheManifest.save(operatingDirectory);

    LOGGER.info("{} data saved to: {} ({} bytes)", dataType, relativePath, jsonContent.length());
  }

  /**
   * Build partition path with frequency dimension (Phase 4).
   * Format: source=econ/type=X/frequency=Y/year=YYYY/
   *
   * @param dataType Data type (e.g., "state_wages", "treasury_yields")
   * @param frequency Data frequency
   * @param year Year
   * @return Partition path
   */
  protected String buildPartitionPath(String dataType, DataFrequency frequency, int year) {
    return buildPartitionPath(dataType, frequency, year, null);
  }

  protected String buildPartitionPath(String dataType, int year) {
    return buildPartitionPath(dataType, DataFrequency.VARIOUS, year, null);
  }

  /**
   * Build partition path with frequency and optional month (for daily data).
   * Format: source=econ/type=X/frequency=Y/year=YYYY/month=MM/
   *
   * @param dataType Data type (e.g., "treasury_yields")
   * @param frequency Data frequency
   * @param year Year
   * @param month Optional month (1-12) for daily data
   * @return Partition path
   */
  protected String buildPartitionPath(String dataType, DataFrequency frequency, int year, Integer month) {
    StringBuilder path = new StringBuilder();
    // Don't prepend source=econ - baseDirectory in EconSchemaFactory already includes it
    path.append("type=").append(dataType);
    if (frequency != DataFrequency.VARIOUS) {
      path.append("/frequency=").append(frequency.getPartitionName());
    }
    path.append("/year=").append(year);

    if (month != null && frequency == DataFrequency.DAILY) {
      path.append("/month=").append(String.format("%02d", month));
    }

    return path.toString();
  }


  /**
   * Extracts all Hive-style partition parameters from path.
   * For example, from "type=custom/year=2020/maturity=10Y/file.parquet"
   * extracts {"maturity": "10Y"} (year is handled separately).
   *
   * @param path File path with Hive partitions
   * @return Map of partition key-value pairs (excluding year, type, and source)
   */
  protected final Map<String, String> extractPartitionParams(String path) {
    Map<String, String> params = new java.util.HashMap<>();

    // Match all key=value patterns in the path
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([^/=]+)=([^/]+)");
    java.util.regex.Matcher matcher = pattern.matcher(path);

    while (matcher.find()) {
      String key = matcher.group(1);
      String value = matcher.group(2);

      // Skip organizational partitions: year (handled separately), type (added by caller), source (organizational only)
      if (!"year".equals(key) && !"type".equals(key) && !"source".equals(key)) {
        params.put(key, value);
      }
    }

    return params;
  }

  /**
   * Extracts year from Hive-style partitioned path (e.g., "year=2020").
   *
   * @param path File path containing year partition
   * @return Extracted year
   */
  protected final int extractYearFromPath(String path) {
    // Look for year=YYYY pattern in path
    int yearIndex = path.indexOf("year=");
    if (yearIndex != -1) {
      String yearStr = path.substring(yearIndex + 5);
      // Extract 4-digit year
      int endIndex = yearStr.indexOf('/');
      if (endIndex != -1) {
        yearStr = yearStr.substring(0, endIndex);
      } else {
        // Check for file extension
        endIndex = yearStr.indexOf('.');
        if (endIndex != -1) {
          yearStr = yearStr.substring(0, endIndex);
        }
      }
      return Integer.parseInt(yearStr);
    }
    throw new IllegalArgumentException("Path does not contain year partition: " + path);
  }

  /**
   * Enriches an Avro Schema with documentation from a PartitionedTableConfig.
   * This method takes a base schema (with types defined) and adds .doc() annotations
   * for each field that has a corresponding entry in the config's columnComments.
   *
   * <p>Implementation note: Avro Schema is immutable, so this method creates a new
   * schema by manipulating the schema's JSON representation, then parsing it back.
   *
   * @param baseSchema The base Avro schema with field names and types defined
   * @param config The partitioned table config containing columnComments
   * @return A new Schema identical to baseSchema but with doc annotations from config
   */
  protected static org.apache.avro.Schema enrichSchemaWithComments(
      org.apache.avro.Schema baseSchema,
      org.apache.calcite.adapter.file.partition.PartitionedTableConfig config) {
    // If no column comments, return original schema unchanged
    if (config == null || config.getColumnComments() == null
        || config.getColumnComments().isEmpty()) {
      return baseSchema;
    }

    try {
      // Convert schema to JSON for manipulation
      com.fasterxml.jackson.databind.JsonNode schemaJson =
          MAPPER.readTree(baseSchema.toString());

      // Get column comments map
      java.util.Map<String, String> columnComments = config.getColumnComments();

      // Schema JSON is an object with a "fields" array
      if (schemaJson.has("fields") && schemaJson.get("fields").isArray()) {
        com.fasterxml.jackson.databind.node.ArrayNode fields =
            (com.fasterxml.jackson.databind.node.ArrayNode) schemaJson.get("fields");

        // Iterate through fields and add doc if available
        for (com.fasterxml.jackson.databind.JsonNode field : fields) {
          if (field.has("name")) {
            String fieldName = field.get("name").asText();
            String comment = columnComments.get(fieldName);

            if (comment != null && !comment.isEmpty()) {
              // Add doc field to this field object
              ((com.fasterxml.jackson.databind.node.ObjectNode) field).put("doc", comment);
            }
          }
        }
      }

      // Convert back to Schema
      return new org.apache.avro.Schema.Parser().parse(schemaJson.toString());
    } catch (Exception e) {
      // If enrichment fails, log warning and return original schema
      LOGGER.warn("Failed to enrich schema with comments from config for table {}: {}",
          config.getName(), e.getMessage());
      return baseSchema;
    }
  }

  /**
   * Loads table column metadata from the econ-schema.json resource file.
   * This enables metadata-driven schema generation, eliminating hardcoded type definitions.
   *
   * <p>NOTE: Consider using {@link AbstractGovDataDownloader#loadTableColumnsFromMetadata(String)} instead
   * for new code. That method is schema-agnostic and works for ECON, GEO, and SEC schemas
   * using the schemaResourceName from the instance.
   *
   * @param tableName The name of the table (must match "name" in econ-schema.json)
   * @return List of TableColumn definitions with type, nullability, and comments
   * @throws IllegalArgumentException if table not found or schema file cannot be loaded
   */
  protected static java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadTableColumns(String tableName) {
    try {
      // Load econ-schema.json from resources
      java.io.InputStream schemaStream =
          AbstractEconDataDownloader.class.getResourceAsStream("/econ-schema.json");
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            "econ-schema.json not found in resources");
      }

      // Parse JSON
      com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the "tables" array
      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid econ-schema.json: missing 'tables' array");
      }

      for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("partitionedTables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Found the table - extract columns
          if (!tableNode.has("columns") || !tableNode.get("columns").isArray()) {
            throw new IllegalArgumentException(
                "Table '" + tableName + "' has no 'columns' array in econ-schema.json");
          }

          java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
              columns = new java.util.ArrayList<>();

          for (com.fasterxml.jackson.databind.JsonNode colNode : tableNode.get("columns")) {
            String colName = colNode.has("name") ? colNode.get("name").asText() : null;
            String colType = colNode.has("type") ? colNode.get("type").asText() : "string";
            boolean nullable = colNode.has("nullable") && colNode.get("nullable").asBoolean();
            String comment = colNode.has("comment") ? colNode.get("comment").asText() : "";

            if (colName != null) {
              columns.add(
                  new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
                  colName, colType, nullable, comment));
            }
          }

          return columns;
        }
      }

      // Table not found
      throw new IllegalArgumentException(
          "Table '" + tableName + "' not found in econ-schema.json");

    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Failed to load column metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

  /**
   * Check if parquet file has been converted, with defensive fallback to file existence and timestamp check.
   * This prevents unnecessary reconversion when the manifest is deleted but parquet files still exist.
   *
   * <p>This method supports optional partition parameters for tables with additional partitioning
   * beyond year (e.g., FRED indicators partitioned by series ID).
   *
   * @param dataType Type of data (e.g., "fred_indicators", "gdp_components")
   * @param year Year of data
   * @param params Additional partition parameters (e.g., {"series": "GDP"}), or null for year-only partitioning
   * @param rawFilePath Full path to raw source file (JSON)
   * @param parquetPath Full path to parquet file
   * @return true if parquet exists and is newer than raw file, false if conversion needed
   */
  protected final boolean isParquetConvertedOrExists(String dataType, int year,
      java.util.Map<String, String> params, String rawFilePath, String parquetPath) {

    // 1. Check manifest first - trust it as source of truth
    if (cacheManifest.isParquetConverted(dataType, year, params)) {
      return true;
    }

    // 2. Defensive check: if a parquet file exists but not in manifest, verify it's up-to-date
    try {
      if (storageProvider.exists(parquetPath)) {
        // Get timestamps for both files
        long parquetModTime = storageProvider.getMetadata(parquetPath).getLastModified();

        // Check if a raw file exists and compare timestamps
        if (cacheStorageProvider.exists(rawFilePath)) {
          long rawModTime = cacheStorageProvider.getMetadata(rawFilePath).getLastModified();

          if (parquetModTime > rawModTime) {
            // Parquet is newer than raw file - update manifest and skip conversion
            LOGGER.info("⚡ Parquet exists and is up-to-date, updating cache manifest: {} (year={}, params={})",
                dataType, year, params);
            cacheManifest.markParquetConverted(dataType, year, params, parquetPath);
            cacheManifest.save(operatingDirectory);
            return true;
          } else {
            // Raw file is newer - needs reconversion
            LOGGER.info("Raw file is newer than parquet, will reconvert: {} (year={})", dataType, year);
            return false;
          }
        } else {
          // No raw file exists - parquet is valid
          LOGGER.info("⚡ Parquet exists and raw file not found, updating cache manifest: {} (year={})",
              dataType, year);
          cacheManifest.markParquetConverted(dataType, year, params, parquetPath);
          cacheManifest.save(operatingDirectory);
          return true;
        }
      }
    } catch (java.io.IOException e) {
      LOGGER.debug("Error checking parquet file existence: {}", e.getMessage());
      // If we can't check, assume it doesn't exist
    }

    return false;
  }

}
