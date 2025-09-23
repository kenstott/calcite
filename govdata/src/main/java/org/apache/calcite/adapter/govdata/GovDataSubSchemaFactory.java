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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

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
   * @param operand Base operand configuration from model file
   * @param storageProvider Storage provider for data operations
   * @return Modified operand with sub-schema-specific configuration
   */
  Map<String, Object> buildOperand(Map<String, Object> operand, StorageProvider storageProvider);

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
   * Get the GOVDATA_CACHE_DIR environment variable with fallback to system property.
   *
   * @return Cache directory path or null if not set
   */
  default String getGovDataCacheDir() {
    String dir = System.getenv("GOVDATA_CACHE_DIR");
    return dir != null ? dir : System.getProperty("GOVDATA_CACHE_DIR");
  }

  /**
   * Get the GOVDATA_PARQUET_DIR environment variable with fallback to system property.
   *
   * @return Parquet directory path or null if not set
   */
  default String getGovDataParquetDir() {
    String dir = System.getenv("GOVDATA_PARQUET_DIR");
    return dir != null ? dir : System.getProperty("GOVDATA_PARQUET_DIR");
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