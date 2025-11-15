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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.model.JsonTable;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Interface for government data sub-schema factories that build operand configurations
 * without directly creating schemas.
 *
 * <p>Sub-schema factories (ECON, GEO, SEC, etc.) implement this interface to provide
 * specialized configuration building while delegating actual schema creation to the
 * unified {@link GovDataSchemaFactory}.
 *
 * <p>This pattern allows:
 * <ul>
 *   <li>Consistent constraint metadata handling across all government data sources</li>
 *   <li>Unified schema creation through a single FileSchema instance</li>
 *   <li>Specialized data source configuration and download logic</li>
 *   <li>Clear separation between configuration building and schema creation</li>
 * </ul>
 *
 * <p>Note: This interface does NOT include a create() method because sub-schema
 * factories are operand builders, not schema creators. Only GovDataSchemaFactory
 * creates actual Schema instances.
 */
public interface GovDataSubSchemaFactory {

  Logger LOGGER = LoggerFactory.getLogger(GovDataSubSchemaFactory.class);
  ObjectMapper JSON_MAPPER = new ObjectMapper();

  /**
   * Build operand configuration for this sub-schema without creating a Schema instance.
   *
   * <p>This method is called by {@link GovDataSchemaFactory} to collect specialized
   * configuration that will be merged into a unified FileSchema operand.
   *
   * <p>Implementations should:
   * <ul>
   *   <li>Handle data source-specific environment variables and configuration</li>
   *   <li>Perform data download and conversion if auto-download is enabled</li>
   *   <li>Load constraint metadata from schema JSON files</li>
   *   <li>Set up partitioned table definitions</li>
   *   <li>Configure storage directories and execution engine settings</li>
   * </ul>
   *
   * <p>Sub-factories access shared services via parent reference instead of parameters:
   * <ul>
   *   <li>{@code parent.getStorageProvider()} - Parquet storage provider</li>
   *   <li>{@code parent.getCacheStorageProvider()} - Cache storage provider</li>
   *   <li>{@code parent.getStorageConfig()} - Enriched storage configuration</li>
   *   <li>{@code parent.getOperatingDirectory(dataSource)} - Operating directory path</li>
   * </ul>
   *
   * @param operand Base operand configuration from model file
   * @param parent Parent factory providing shared services
   * @return Modified operand with sub-schema-specific configuration
   */
  Map<String, Object> buildOperand(Map<String, Object> operand, GovDataSchemaFactory parent);

  /**
   * Returns the schema resource file name for this factory.
   *
   * @return Schema resource name (e.g., "/econ/econ-schema.json", "/geo/geo-schema.json")
   */
  String getSchemaResourceName();

  /**
   * Load table definitions from schema JSON resource file.
   * Loads both "partitionedTables" (Parquet files) and "tables" (views, etc.).
   *
   * @return List of all table definitions (partitioned tables + views)
   */
  default List<Map<String, Object>> loadTableDefinitions() {
    try (InputStream is = getClass().getResourceAsStream(getSchemaResourceName())) {
      if (is == null) {
        throw new IllegalStateException(
            "Could not find " + getSchemaResourceName() + " resource file");
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);

      // Load partitioned tables (Parquet files)
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> partitionedTables = (List<Map<String, Object>>) schema.get("partitionedTables");
      if (partitionedTables == null) {
        throw new IllegalStateException("No 'partitionedTables' field found in " + getSchemaResourceName());
      }

      // Load regular tables (views, etc.) - optional
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> regularTables = (List<Map<String, Object>>) schema.get("tables");

      // Combine both lists
      List<Map<String, Object>> allTables = new ArrayList<>(partitionedTables);
      if (regularTables != null && !regularTables.isEmpty()) {
        allTables.addAll(regularTables);
        LOGGER.info("Loaded {} partitioned tables and {} view/regular tables from {}",
            partitionedTables.size(), regularTables.size(), getSchemaResourceName());
      } else {
        LOGGER.info("Loaded {} partitioned tables from {} (no views)",
            partitionedTables.size(), getSchemaResourceName());
      }

      for (Map<String, Object> table : allTables) {
        String tableType = (String) table.get("type");
        if ("view".equals(tableType)) {
          LOGGER.debug("  - View: {}", table.get("name"));
        } else {
          LOGGER.debug("  - Table: {} with pattern: {}", table.get("name"), table.get("pattern"));
        }
      }
      return allTables;
    } catch (IOException e) {
      throw new RuntimeException("Error loading " + getSchemaResourceName(), e);
    }
  }

  /**
   * Load schema-level comment from schema JSON resource file.
   *
   * @return Schema comment, or null if not defined
   */
  default String loadSchemaComment() {
    try (InputStream is = getClass().getResourceAsStream(getSchemaResourceName())) {
      if (is == null) {
        LOGGER.warn("Could not find {} resource file for loading comment", getSchemaResourceName());
        return null;
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      String comment = (String) schema.get("comment");
      if (comment != null) {
        LOGGER.debug("Loaded schema comment from {}: {}", getSchemaResourceName(),
            comment.length() > 80 ? comment.substring(0, 80) + "..." : comment);
      }
      return comment;
    } catch (IOException e) {
      LOGGER.warn("Error loading schema comment from {}: {}", getSchemaResourceName(), e.getMessage());
      return null;
    }
  }

  /**
   * Load declared schema name from schema JSON resource file.
   * This is the canonical schema name that FK definitions reference.
   * If not present, defaults to lowercase filename stem (e.g., "econ" from "econ-schema.json").
   *
   * @return Declared schema name
   */
  default String loadDeclaredSchemaName() {
    try (InputStream is = getClass().getResourceAsStream(getSchemaResourceName())) {
      if (is == null) {
        LOGGER.warn("Could not find {} resource file for loading schema name", getSchemaResourceName());
        return getDefaultSchemaName();
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      String schemaName = (String) schema.get("schemaName");
      if (schemaName != null) {
        LOGGER.debug("Loaded declared schema name from {}: {}", getSchemaResourceName(), schemaName);
        return schemaName;
      } else {
        // Default to lowercase filename stem
        String defaultName = getDefaultSchemaName();
        LOGGER.debug("No schemaName in {}, using default: {}", getSchemaResourceName(), defaultName);
        return defaultName;
      }
    } catch (IOException e) {
      LOGGER.warn("Error loading schema name from {}: {}", getSchemaResourceName(), e.getMessage());
      return getDefaultSchemaName();
    }
  }

  /**
   * Get default schema name from resource filename (lowercase stem).
   * For example, "/econ/econ-schema.json" -> "econ"
   */
  default String getDefaultSchemaName() {
    String resourceName = getSchemaResourceName();
    // Remove leading slash and extension
    String name = resourceName.startsWith("/") ? resourceName.substring(1) : resourceName;
    int dotIndex = name.indexOf('.');
    if (dotIndex > 0) {
      name = name.substring(0, dotIndex);
    }
    // Remove "-schema" suffix if present
    if (name.endsWith("-schema")) {
      name = name.substring(0, name.length() - "-schema".length());
    }
    return name.toLowerCase(java.util.Locale.ROOT);
  }

  /**
   * Rewrite foreign key schema names in table definitions.
   * For each FK, if targetSchema matches the declared schema name, rewrite it to use the actual schema name.
   * This allows FK definitions to use a canonical name like "econ" while the schema is instantiated as "ECON".
   *
   * @param tables List of table definitions with potential FK definitions
   * @param actualSchemaName Actual schema name from model.json (e.g., "ECON")
   */
  default void rewriteForeignKeySchemaNames(List<Map<String, Object>> tables, String actualSchemaName) {
    // Load the declared schema name from JSON
    String declaredSchemaName = loadDeclaredSchemaName();

    LOGGER.info("FK Rewriting: declared='{}', actual='{}'", declaredSchemaName, actualSchemaName);

    if (declaredSchemaName.equals(actualSchemaName)) {
      LOGGER.debug("FK Rewriting: Schema names match, no rewriting needed");
      return;
    }

    int rewriteCount = 0;
    int preserveCount = 0;

    // Iterate through all tables
    for (Map<String, Object> table : tables) {
      String tableName = (String) table.get("name");

      // Check if table has foreignKeys
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> foreignKeys = (List<Map<String, Object>>) table.get("foreignKeys");

      if (foreignKeys == null || foreignKeys.isEmpty()) {
        continue;
      }

      // Rewrite FK targetSchema values
      for (Map<String, Object> fk : foreignKeys) {
        String targetSchema = (String) fk.get("targetSchema");

        if (targetSchema == null) {
          continue;
        }

        if (targetSchema.equals(declaredSchemaName)) {
          // Rewrite to actual schema name
          fk.put("targetSchema", actualSchemaName);
          rewriteCount++;
          LOGGER.debug("FK Rewriting: table={}, rewritten targetSchema '{}' -> '{}'",
              tableName, declaredSchemaName, actualSchemaName);
        } else {
          // Cross-schema FK - preserve as-is
          preserveCount++;
          LOGGER.debug("FK Rewriting: table={}, preserved cross-schema targetSchema='{}'",
              tableName, targetSchema);
        }
      }
    }

    LOGGER.info("FK Rewriting complete: rewrote {} FKs, preserved {} cross-schema FKs",
        rewriteCount, preserveCount);
  }

  /**
   * Load constraint definitions from schema JSON resource file.
   *
   * @return Map of table name to constraint definitions
   */
  default Map<String, Map<String, Object>> loadTableConstraints() {
    try (InputStream is = getClass().getResourceAsStream(getSchemaResourceName())) {
      if (is == null) {
        throw new IllegalStateException("Could not find " + getSchemaResourceName() + " resource file");
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      @SuppressWarnings("unchecked")
      Map<String, Map<String, Object>> constraints = (Map<String, Map<String, Object>>) schema.get("constraints");
      if (constraints == null) {
        throw new IllegalStateException("No 'constraints' field found in " + getSchemaResourceName());
      }
      LOGGER.info("Loaded constraints for {} tables from {}", constraints.size(), getSchemaResourceName());
      return constraints;
    } catch (IOException e) {
      throw new RuntimeException("Error loading " + getSchemaResourceName(), e);
    }
  }

  /**
   * Rewrite foreign key schema names in constraint definitions.
   * For each FK, if targetSchema matches the declared schema name, rewrite it to use the actual schema name.
   * This allows FK definitions to use a canonical name like "econ" while the schema is instantiated as "ECON".
   *
   * @param constraints Map of table name to constraint definitions
   * @param actualSchemaName Actual schema name from model.json (e.g., "ECON")
   */
  default void rewriteConstraintForeignKeySchemaNames(
      Map<String, Map<String, Object>> constraints, String actualSchemaName) {
    // Load the declared schema name from JSON
    String declaredSchemaName = loadDeclaredSchemaName();

    LOGGER.info("FK Constraint Rewriting: declared='{}', actual='{}'", declaredSchemaName, actualSchemaName);

    if (declaredSchemaName.equals(actualSchemaName)) {
      LOGGER.debug("FK Constraint Rewriting: Schema names match, no rewriting needed");
      return;
    }

    int rewriteCount = 0;
    int preserveCount = 0;

    // Iterate through all table constraints
    for (Map.Entry<String, Map<String, Object>> entry : constraints.entrySet()) {
      String tableName = entry.getKey();
      Map<String, Object> tableConstraints = entry.getValue();

      // Check if table has foreignKeys
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> foreignKeys = (List<Map<String, Object>>) tableConstraints.get("foreignKeys");

      if (foreignKeys == null || foreignKeys.isEmpty()) {
        continue;
      }

      // Rewrite FK targetSchema values
      for (Map<String, Object> fk : foreignKeys) {
        String targetSchema = (String) fk.get("targetSchema");

        if (targetSchema == null) {
          continue;
        }

        if (targetSchema.equals(declaredSchemaName)) {
          // Rewrite to actual schema name
          fk.put("targetSchema", actualSchemaName);
          rewriteCount++;
          LOGGER.debug("FK Constraint Rewriting: table={}, rewritten targetSchema '{}' -> '{}'",
              tableName, declaredSchemaName, actualSchemaName);
        } else {
          // Cross-schema FK - preserve as-is
          preserveCount++;
          LOGGER.debug("FK Constraint Rewriting: table={}, preserved cross-schema targetSchema='{}'",
              tableName, targetSchema);
        }
      }
    }

    LOGGER.info("FK Constraint Rewriting complete: rewrote {} FKs, preserved {} cross-schema FKs",
        rewriteCount, preserveCount);
  }

  /**
   * Get the cache directory from operand, environment variable, or system property.
   * Checks in order: operand.cacheDirectory, GOVDATA_CACHE_DIR env, GOVDATA_CACHE_DIR property.
   *
   * @param operand Configuration map (optional)
   * @return Cache directory path or null if not set
   */
  default String getGovDataCacheDir(Map<String, Object> operand) {
    // First check operand
    if (operand != null) {
      String dir = (String) operand.get("cacheDirectory");
      if (dir != null && !dir.isEmpty()) {
        // Resolve environment variable placeholders like ${VAR:default}
        return resolveEnvVar(dir);
      }
    }
    return null;
  }

  /**
   * Get the cache directory from environment variable or system property only.
   * For backward compatibility.
   *
   * @return Cache directory path or null if not set
   */
  default String getGovDataCacheDir() {
    return getGovDataCacheDir(null);
  }

  /**
   * Get the parquet directory from operand ONLY.
   * Does NOT fall back to environment variables - directory must be explicit in model.json.
   *
   * @param operand Configuration map (optional)
   * @return Parquet directory path or null if not set
   */
  default String getGovDataParquetDir(Map<String, Object> operand) {
    // ONLY check operand - NO environment variable fallback
    if (operand != null) {
      String dir = (String) operand.get("directory");
      if (dir != null && !dir.isEmpty()) {
        // Resolve environment variable placeholders like ${VAR:default}
        return resolveEnvVar(dir);
      }
    }
    return null;
  }

  /**
   * Normalize parquet directory path - NO automatic S3 conversion.
   * If you want S3, specify it explicitly as "s3://bucket/path" in model.json.
   */
  default String normalizeParquetDir(String dir) {
    // Just return the directory as-is - no magic conversions
    return dir;
  }

  /**
   * Get the parquet directory from environment variable or system property only.
   * For backward compatibility.
   *
   * @return Parquet directory path or null if not set
   */
  default String getGovDataParquetDir() {
    return getGovDataParquetDir(null);
  }

  /**
   * Get configured start year from operand or environment.
   *
   * @param operand Configuration map
   * @return Start year or default (5 years ago)
   */
  default Integer getConfiguredStartYear(Map<String, Object> operand) {
    Integer year = (Integer) operand.get("startYear");
    if (year != null) return year;

    String envYear = System.getenv("GOVDATA_START_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", envYear);
      }
    }

    // Default to 5 years ago
    return java.time.Year.now().getValue() - 5;
  }

  /**
   * Get configured end year from operand or environment.
   *
   * @param operand Configuration map
   * @return End year or default (current year)
   */
  default Integer getConfiguredEndYear(Map<String, Object> operand) {
    Integer year = (Integer) operand.get("endYear");
    if (year != null) return year;

    String envYear = System.getenv("GOVDATA_END_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", envYear);
      }
    }

    // Default to current year
    return java.time.Year.now().getValue();
  }

  /**
   * Check if auto-download should be enabled based on operand configuration.
   *
   * @param operand Configuration map
   * @return true if auto-download should be enabled (default: true)
   */
  default boolean shouldAutoDownload(Map<String, Object> operand) {
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    return autoDownload == null ? true : autoDownload;
  }

  /**
   * Returns whether this schema factory supports constraint metadata processing.
   * Default implementation returns true for all govdata sub-schema factories.
   *
   * @return true if this factory supports constraint metadata
   */
  default boolean supportsConstraints() {
    return true;
  }

  /**
   * Resolve environment variable placeholders in configuration values.
   * Supports ${VAR_NAME} and ${VAR_NAME:default} syntax.
   *
   * @param value Configuration value that may contain ${...} placeholders
   * @return Resolved value or null if value is null
   */
  default String resolveEnvVar(String value) {
    if (value == null || !value.contains("${")) {
      return value;
    }

    // Pattern: ${VAR_NAME} or ${VAR_NAME:default}
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?\\}");
    java.util.regex.Matcher matcher = pattern.matcher(value);

    if (matcher.find()) {
      String varName = matcher.group(1);
      String defaultValue = matcher.group(2);

      // Try environment variable first
      String resolvedValue = System.getenv(varName);
      if (resolvedValue == null) {
        // Try system property
        resolvedValue = System.getProperty(varName);
      }
      if (resolvedValue == null) {
        // Use default value if provided
        resolvedValue = defaultValue;
      }

      return resolvedValue;
    }

    return value;
  }

  /**
   * Called to provide constraint metadata for tables.
   * Default implementation does nothing, allowing factories to opt-in by overriding.
   *
   * @param tableConstraints Map from table name to constraint definitions
   * @param tableDefinitions List of table definitions from model file
   */
  default void setTableConstraints(
      Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    // Default implementation - do nothing
  }

  /**
   * Validates that a trend pattern is properly derived from a detail pattern.
   *
   * <p>A valid trend pattern must:
   * <ul>
   *   <li>Have the same base structure as the detail pattern</li>
   *   <li>Only differ by having fewer partition key segments (e.g., key=value/ removed)</li>
   *   <li>Keep the same file name at the end</li>
   * </ul>
   *
   * <p>Example valid trend pattern:
   * <pre>
   * Detail:  "type=employment/frequency={frequency}/year={year}/data.parquet"
   * Trend:   "type=employment/frequency={frequency}/data.parquet"
   * </pre>
   *
   * @param detailPattern The detail table pattern (with all partition keys)
   * @param trendPattern The trend table pattern (with subset of partition keys)
   * @param tableName The table name (for logging)
   * @return true if trend pattern is valid, false otherwise
   */
  default boolean validateTrendPattern(String detailPattern, String trendPattern,
      String tableName) {
    // Split patterns by '/' to analyze path segments
    String[] detailSegments = detailPattern.split("/");
    String[] trendSegments = trendPattern.split("/");

    // Trend pattern must have fewer or equal segments
    if (trendSegments.length > detailSegments.length) {
      LOGGER.warn("Trend pattern for '{}' has more path segments than detail pattern",
          tableName);
      return false;
    }

    // Last segment (file name) must match exactly
    String detailFileName = detailSegments[detailSegments.length - 1];
    String trendFileName = trendSegments[trendSegments.length - 1];
    if (!detailFileName.equals(trendFileName)) {
      LOGGER.warn("Trend pattern for '{}' has different file name: '{}' vs '{}'",
          tableName, trendFileName, detailFileName);
      return false;
    }

    // Check that all trend segments exist in detail pattern in the same order
    int detailIdx = 0;
    for (int trendIdx = 0; trendIdx < trendSegments.length; trendIdx++) {
      String trendSeg = trendSegments[trendIdx];
      boolean found = false;

      // Search for matching segment in remaining detail segments
      while (detailIdx < detailSegments.length) {
        String detailSeg = detailSegments[detailIdx];

        // Extract partition key name (before '=') for comparison
        String trendKey = extractPartitionKey(trendSeg);
        String detailKey = extractPartitionKey(detailSeg);

        if (trendKey != null && detailKey != null) {
          // Both are partition segments - compare keys
          if (trendKey.equals(detailKey)) {
            found = true;
            detailIdx++;
            break;
          }
        } else if (trendKey == null && detailKey == null) {
          // Both are non-partition segments - must match exactly
          if (trendSeg.equals(detailSeg)) {
            found = true;
            detailIdx++;
            break;
          }
        }
        detailIdx++;
      }

      if (!found) {
        LOGGER.warn("Trend pattern for '{}' contains segment '{}' not found in detail pattern",
            tableName, trendSeg);
        return false;
      }
    }

    LOGGER.debug("Validated trend pattern for '{}': removed {} partition key(s)",
        tableName, detailSegments.length - trendSegments.length);
    return true;
  }

  /**
   * Extracts partition key name from a path segment (e.g., "year={year}" → "year").
   * Returns null if segment is not a partition pattern.
   */
  default String extractPartitionKey(String segment) {
    if (segment.contains("=")) {
      int equalsIdx = segment.indexOf('=');
      return segment.substring(0, equalsIdx);
    }
    return null;
  }

  /**
   * Expands trend_patterns from table definitions into standalone table definitions.
   *
   * <p>For each table with a "trend_patterns" array, creates additional table definitions
   * for the consolidated trend tables. These trend tables consolidate year-partitioned data
   * into single files for faster time-series queries and trend analysis across periods.
   *
   * <p>Example: employment_statistics with pattern
   * "type=employment_statistics/frequency={frequency}/year={year}/employment_statistics.parquet"
   * and trend_pattern "type=employment_statistics/frequency={frequency}/employment_statistics.parquet"
   * will create a new table definition for the consolidated table.
   *
   * @param tables List of table definitions (will be modified in-place to add trend tables)
   */
  default void expandTrendPatterns(List<Map<String, Object>> tables) {
    java.util.List<Map<String, Object>> trendTables = new ArrayList<>();

    for (Map<String, Object> table : tables) {
      // Check if table has trend_patterns
      @SuppressWarnings("unchecked")
      java.util.List<Map<String, Object>> trendPatterns =
          (java.util.List<Map<String, Object>>) table.get("trend_patterns");

      if (trendPatterns == null || trendPatterns.isEmpty()) {
        continue;
      }

      // Get detail table pattern for validation
      String detailPattern = (String) table.get("pattern");
      String tableName = (String) table.get("name");

      // For each trend pattern, create a new table definition
      for (Map<String, Object> trendPattern : trendPatterns) {
        String trendName = (String) trendPattern.get("name");
        String trendPatternStr = (String) trendPattern.get("pattern");

        if (trendName == null || trendPatternStr == null) {
          LOGGER.warn("Skipping trend pattern with missing name or pattern in table '{}'",
              tableName);
          continue;
        }

        // Validate that trend pattern is derived from detail pattern
        if (!validateTrendPattern(detailPattern, trendPatternStr, tableName)) {
          LOGGER.error("Invalid trend pattern '{}' for table '{}': "
              + "trend pattern must be detail pattern with partition keys removed. "
              + "Detail: '{}', Trend: '{}'",
              trendName, tableName, detailPattern, trendPatternStr);
          continue;
        }

        // Create new table definition for the trend
        Map<String, Object> trendTable = new java.util.HashMap<>();
        trendTable.put("name", trendName);
        trendTable.put("pattern", trendPatternStr);

        // Mark this as a trend table and link to its detail table for materialization registration
        trendTable.put("_isTrendTable", true);
        trendTable.put("_detailTableName", tableName);
        trendTable.put("_detailTablePattern", detailPattern);

        // Copy column definitions from source table (same schema)
        if (table.containsKey("columns")) {
          trendTable.put("columns", table.get("columns"));
        }

        // Copy comment if present, or create descriptive comment
        String comment = (String) trendPattern.get("comment");
        if (comment == null) {
          comment = "Consolidated trend data from " + table.get("name");
        }
        trendTable.put("comment", comment);

        // Copy partitions configuration (minus year dimension)
        // Note: Trend tables don't have year partitions, but may have other dimensions like frequency
        if (table.containsKey("partitions")) {
          trendTable.put("partitions", table.get("partitions"));
        }

        // Add to trend tables list
        trendTables.add(trendTable);
        LOGGER.debug("Expanded trend pattern '{}' from detail table '{}' for materialized view registration",
            trendName, tableName);
      }
    }

    // Add all trend tables to the main tables list
    if (!trendTables.isEmpty()) {
      tables.addAll(trendTables);
      LOGGER.info("Expanded {} trend patterns into table definitions", trendTables.size());
    }
  }
}
