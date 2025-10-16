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
   * @return Schema resource name (e.g., "/econ-schema.json", "/geo-schema.json")
   */
  String getSchemaResourceName();

  /**
   * Load table definitions from schema JSON resource file.
   *
   * @return List of partitioned table definitions
   */
  default List<Map<String, Object>> loadTableDefinitions() {
    try (InputStream is = getClass().getResourceAsStream(getSchemaResourceName())) {
      if (is == null) {
        throw new IllegalStateException("Could not find " + getSchemaResourceName() + " resource file");
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> tables = (List<Map<String, Object>>) schema.get("partitionedTables");
      if (tables == null) {
        throw new IllegalStateException("No 'partitionedTables' field found in " + getSchemaResourceName());
      }
      LOGGER.info("Loaded {} table definitions from {}", tables.size(), getSchemaResourceName());
      for (Map<String, Object> table : tables) {
        LOGGER.debug("  - Table: {} with pattern: {}", table.get("name"), table.get("pattern"));
      }
      return tables;
    } catch (IOException e) {
      throw new RuntimeException("Error loading " + getSchemaResourceName(), e);
    }
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
}
