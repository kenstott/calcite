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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

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
  ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory()
      .enable(com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.MINIMIZE_QUOTES)
      .disable(com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

  /**
   * Get the appropriate ObjectMapper based on schema file extension.
   *
   * @param schemaResourceName Schema resource name (e.g., "/econ/econ-schema.yaml")
   * @return YAML mapper for .yaml/.yml files, JSON mapper otherwise
   */
  default ObjectMapper getMapperForSchema(String schemaResourceName) {
    return (schemaResourceName.endsWith(".yaml") || schemaResourceName.endsWith(".yml"))
        ? YAML_MAPPER : JSON_MAPPER;
  }

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
   * Load table definitions from schema JSON/YAML resource file.
   * Loads both "partitionedTables" (Parquet files) and "tables" (views, etc.).
   *
   * <p>For YAML files, uses SnakeYAML via {@link YamlUtils} to properly resolve
   * YAML anchors and aliases. Jackson's YAML parser does not correctly resolve
   * anchors for complex structures.
   *
   * @return List of all table definitions (partitioned tables + views)
   */
  default List<Map<String, Object>> loadTableDefinitions() {
    String schemaResourceName = getSchemaResourceName();

    try (InputStream is = getClass().getResourceAsStream(schemaResourceName)) {
      if (is == null) {
        throw new IllegalStateException(
            "Could not find " + schemaResourceName + " resource file");
      }

      // Use YamlUtils for proper YAML anchor/alias resolution
      // Jackson YAML doesn't resolve anchors correctly for complex structures
      com.fasterxml.jackson.databind.JsonNode schemaNode =
          YamlUtils.parseYamlOrJson(is, schemaResourceName);
      @SuppressWarnings("unchecked")
      Map<String, Object> schema = JSON_MAPPER.convertValue(schemaNode, Map.class);

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
   * Load constraint definitions from schema JSON/YAML resource file.
   *
   * @return Map of table name to constraint definitions
   */
  default Map<String, Map<String, Object>> loadTableConstraints() {
    String schemaResourceName = getSchemaResourceName();
    ObjectMapper mapper = getMapperForSchema(schemaResourceName);

    try (InputStream is = getClass().getResourceAsStream(schemaResourceName)) {
      if (is == null) {
        throw new IllegalStateException("Could not find " + schemaResourceName + " resource file");
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> schema = mapper.readValue(is, Map.class);
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
   * Validates that an alternate partition pattern is properly related to the source pattern.
   *
   * <p>A valid alternate partition pattern must:
   * <ul>
   *   <li>Have a valid pattern structure</li>
   *   <li>Reference the same logical data (same file name at the end)</li>
   * </ul>
   *
   * <p>Example valid alternate partition:
   * <pre>{@code
   * Source:    type=population/year=* /population.parquet
   * Alternate: type=population_by_state/state=* /population.parquet
   * }</pre>
   *
   * @param sourcePattern The source table pattern
   * @param alternatePattern The alternate partition pattern
   * @param tableName The table name (for logging)
   * @return true if alternate pattern is valid, false otherwise
   */
  default boolean validateAlternatePattern(String sourcePattern, String alternatePattern,
      String tableName) {
    // Split patterns by '/' to analyze path segments
    String[] sourceSegments = sourcePattern.split("/");
    String[] alternateSegments = alternatePattern.split("/");

    // Last segment (file name) must match exactly
    String sourceFileName = sourceSegments[sourceSegments.length - 1];
    String alternateFileName = alternateSegments[alternateSegments.length - 1];
    if (!sourceFileName.equals(alternateFileName)) {
      LOGGER.warn("Alternate pattern for '{}' has different file name: '{}' vs '{}'",
          tableName, alternateFileName, sourceFileName);
      return false;
    }

    LOGGER.debug("Validated alternate pattern for '{}': source has {} segments, alternate has {}",
        tableName, sourceSegments.length, alternateSegments.length);
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
   * Counts the number of partition keys in a pattern.
   *
   * @param pattern The file pattern (e.g., type=X/year=star/state=star/data.parquet)
   * @return Number of partition key segments (key equals value pairs)
   */
  default int countPartitionKeys(String pattern) {
    if (pattern == null) {
      return 0;
    }
    int count = 0;
    for (String segment : pattern.split("/")) {
      if (segment.contains("=")) {
        count++;
      }
    }
    return count;
  }

  /**
   * Expands alternate_partitions from table definitions into standalone table definitions.
   *
   * <p>For each table with an alternate_partitions array, creates additional table definitions
   * for each alternate partition scheme. These alternate tables provide different physical
   * layouts of the same logical data, allowing the query optimizer to select the best layout
   * based on query predicates.
   *
   * <p>Example: population with pattern type=population/year=star/population.parquet
   * and alternate_partition type=population_by_state/state=star/population.parquet
   * will create a new table definition for the state-partitioned table.
   *
   * @param tables List of table definitions (will be modified in-place to add alternate tables)
   */
  default void expandAlternatePartitions(List<Map<String, Object>> tables) {
    java.util.List<Map<String, Object>> alternateTables = new ArrayList<>();

    for (Map<String, Object> table : tables) {
      // Check if table has alternate_partitions
      @SuppressWarnings("unchecked")
      java.util.List<Map<String, Object>> alternatePartitions =
          (java.util.List<Map<String, Object>>) table.get("alternate_partitions");

      if (alternatePartitions == null || alternatePartitions.isEmpty()) {
        continue;
      }

      // Get source table info
      String sourcePattern = (String) table.get("pattern");
      String tableName = (String) table.get("name");
      int sourcePartitionKeyCount = countPartitionKeys(sourcePattern);

      // Store partition key count on source table for optimizer
      table.put("_partitionKeyCount", sourcePartitionKeyCount);

      // For each alternate partition, create a new table definition
      for (Map<String, Object> alternatePartition : alternatePartitions) {
        String alternateName = (String) alternatePartition.get("name");
        String alternatePatternStr = (String) alternatePartition.get("pattern");

        if (alternateName == null || alternatePatternStr == null) {
          LOGGER.warn("Skipping alternate partition with missing name or pattern in table '{}'",
              tableName);
          continue;
        }

        // Validate alternate pattern
        if (!validateAlternatePattern(sourcePattern, alternatePatternStr, tableName)) {
          LOGGER.error("Invalid alternate partition '{}' for table '{}': "
              + "Source: '{}', Alternate: '{}'",
              alternateName, tableName, sourcePattern, alternatePatternStr);
          continue;
        }

        // Create new table definition for the alternate
        Map<String, Object> alternateTable = new java.util.HashMap<>();
        alternateTable.put("name", alternateName);
        alternateTable.put("pattern", alternatePatternStr);

        // Mark as alternate partition and link to source table
        alternateTable.put("_isAlternatePartition", true);
        alternateTable.put("_sourceTableName", tableName);
        alternateTable.put("_sourceTablePattern", sourcePattern);

        // Count and store partition keys for optimizer selection
        int alternatePartitionKeyCount = countPartitionKeys(alternatePatternStr);
        alternateTable.put("_partitionKeyCount", alternatePartitionKeyCount);

        // Copy partition config from alternate definition
        if (alternatePartition.containsKey("partition")) {
          alternateTable.put("partitions", alternatePartition.get("partition"));
        }

        // Copy column definitions from source table (same schema)
        if (table.containsKey("columns")) {
          alternateTable.put("columns", table.get("columns"));
        }

        // Copy comment if present, or create descriptive comment
        String comment = (String) alternatePartition.get("comment");
        if (comment == null) {
          comment = "Alternate partition of " + tableName + " with "
              + alternatePartitionKeyCount + " partition keys";
        }
        alternateTable.put("comment", comment);

        // Add to alternate tables list
        alternateTables.add(alternateTable);
        LOGGER.debug("Expanded alternate partition '{}' from source table '{}' "
            + "(source keys: {}, alternate keys: {})",
            alternateName, tableName, sourcePartitionKeyCount, alternatePartitionKeyCount);
      }
    }

    // Add all alternate tables to the main tables list
    if (!alternateTables.isEmpty()) {
      tables.addAll(alternateTables);
      LOGGER.info("Expanded {} alternate partitions into table definitions", alternateTables.size());
    }
  }
}
