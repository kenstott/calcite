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

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for reading schema configuration files (census-schema.json, econ-schema.yaml, etc.).
 *
 * <p>Provides common methods to extract download configurations, table metadata, and other
 * schema-level settings from JSON/YAML schema files. Automatically detects format based on
 * file extension. This reduces code duplication across Census, Econ, and other govdata adapters.
 */
public class SchemaConfigReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConfigReader.class);

  /**
   * Get list of table names that have download configurations for a specific type/category.
   *
   * @param schemaFile Name of schema file (e.g., "census-schema.json", "econ-schema.yaml")
   * @param filterKey Key to filter on (e.g., "censusType", "dataSource")
   * @param filterValue Value to match (e.g., "acs", "fred")
   * @return List of table names matching the filter
   */
  public static List<String> getTablesWithDownloadConfig(String schemaFile,
      String filterKey, String filterValue) {
    List<String> tableNames = new ArrayList<>();

    try (InputStream schemaStream = SchemaConfigReader.class.getResourceAsStream("/" + schemaFile)) {
      if (schemaStream == null) {
        LOGGER.warn("{} not found in resources", schemaFile);
        return tableNames;
      }

      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaFile);
      JsonNode tables = root.path("partitionedTables");

      if (tables.isArray()) {
        for (JsonNode tableNode : tables) {
          JsonNode downloadConfig = tableNode.path("download");
          if (!downloadConfig.isMissingNode()
              && downloadConfig.path("enabled").asBoolean(false)
              && filterValue.equals(downloadConfig.path(filterKey).asText())) {
            String tableName = tableNode.path("name").asText();
            if (!tableName.isEmpty()) {
              tableNames.add(tableName);
            }
          }
        }
      }

      LOGGER.info("Found {} tables with {}={} in {}", tableNames.size(), filterKey, filterValue, schemaFile);

    } catch (IOException e) {
      LOGGER.error("Error loading table configurations from {}: {}", schemaFile, e.getMessage());
    }

    return tableNames;
  }

  /**
   * Get a specific download configuration field for a table.
   *
   * @param schemaFile Name of schema file
   * @param tableName Name of the table
   * @param fieldName Field to retrieve from download config (e.g., "censusType", "geographies")
   * @return Field value as string, or null if not found
   */
  public static String getDownloadConfigField(String schemaFile, String tableName, String fieldName) {
    try (InputStream schemaStream = SchemaConfigReader.class.getResourceAsStream("/" + schemaFile)) {
      if (schemaStream == null) {
        return null;
      }

      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaFile);
      JsonNode tables = root.path("partitionedTables");

      if (tables.isArray()) {
        for (JsonNode tableNode : tables) {
          if (tableName.equals(tableNode.path("name").asText())) {
            JsonNode fieldValue = tableNode.path("download").path(fieldName);
            return fieldValue.isTextual() ? fieldValue.asText() : null;
          }
        }
      }

    } catch (IOException e) {
      LOGGER.error("Error getting {} for table {} from {}: {}", fieldName, tableName, schemaFile, e.getMessage());
    }

    return null;
  }

  /**
   * Get geographies array from download configuration.
   *
   * @param schemaFile Name of schema file
   * @param tableName Name of the table
   * @return List of geography strings (e.g., ["state:*", "county:*"])
   */
  public static List<String> getDownloadGeographies(String schemaFile, String tableName) {
    List<String> geographies = new ArrayList<>();

    try (InputStream schemaStream = SchemaConfigReader.class.getResourceAsStream("/" + schemaFile)) {
      if (schemaStream == null) {
        return geographies;
      }

      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaFile);
      JsonNode tables = root.path("partitionedTables");

      if (tables.isArray()) {
        for (JsonNode tableNode : tables) {
          if (tableName.equals(tableNode.path("name").asText())) {
            JsonNode geoArray = tableNode.path("download").path("geographies");
            if (geoArray.isArray()) {
              for (JsonNode geoNode : geoArray) {
                geographies.add(geoNode.asText());
              }
            }
            break;
          }
        }
      }

    } catch (IOException e) {
      LOGGER.error("Error getting geographies for table {} from {}: {}", tableName, schemaFile, e.getMessage());
    }

    return geographies;
  }

  /**
   * Get conceptual mapping file name for a table.
   *
   * @param schemaFile Name of schema file
   * @param tableName Name of the table
   * @return Mapping file name (e.g., "census/census-variable-mappings.json") or null
   */
  public static String getConceptualMappingFile(String schemaFile, String tableName) {
    try (InputStream schemaStream = SchemaConfigReader.class.getResourceAsStream("/" + schemaFile)) {
      if (schemaStream == null) {
        return null;
      }

      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaFile);
      JsonNode tables = root.path("partitionedTables");

      if (tables.isArray()) {
        for (JsonNode tableNode : tables) {
          if (tableName.equals(tableNode.path("name").asText())) {
            JsonNode mappingFile = tableNode.path("conceptualMappingFile");
            return mappingFile.isTextual() ? mappingFile.asText() : null;
          }
        }
      }

    } catch (IOException e) {
      LOGGER.error("Error getting mapping file for table {} from {}: {}", tableName, schemaFile, e.getMessage());
    }

    return null;
  }

  /**
   * Get data availability rules for a specific data type from schema file.
   *
   * @param schemaFile Name of schema file (e.g., "census-schema.json")
   * @param dataType Data type key (e.g., "acs", "decennial", "economic", "population")
   * @return DataAvailabilityRule or null if not defined
   */
  public static DataAvailabilityRule getDataAvailabilityRule(String schemaFile, String dataType) {
    try (InputStream schemaStream = SchemaConfigReader.class.getResourceAsStream("/" + schemaFile)) {
      if (schemaStream == null) {
        LOGGER.warn("{} not found in resources", schemaFile);
        return null;
      }

      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaFile);
      JsonNode availabilityNode = root.path("dataAvailability").path(dataType);

      if (availabilityNode.isMissingNode()) {
        LOGGER.debug("No dataAvailability rule for {} in {}", dataType, schemaFile);
        return null;
      }

      DataAvailabilityRule rule = new DataAvailabilityRule();
      rule.description = availabilityNode.path("description").asText(null);
      rule.frequency = availabilityNode.path("frequency").asText("annual");

      if (availabilityNode.has("startYear")) {
        rule.startYear = availabilityNode.path("startYear").asInt();
      }
      if (availabilityNode.has("releaseLagYears")) {
        rule.releaseLagYears = availabilityNode.path("releaseLagYears").asInt();
      }
      if (availabilityNode.has("frequencyYears")) {
        rule.frequencyYears = availabilityNode.path("frequencyYears").asInt();
      }

      JsonNode validYearsNode = availabilityNode.path("validYears");
      if (validYearsNode.isArray()) {
        rule.validYears = new int[validYearsNode.size()];
        for (int i = 0; i < validYearsNode.size(); i++) {
          rule.validYears[i] = validYearsNode.get(i).asInt();
        }
      }

      LOGGER.debug("Loaded data availability rule for {}: startYear={}, releaseLag={}, frequency={}",
          dataType, rule.startYear, rule.releaseLagYears, rule.frequency);

      return rule;

    } catch (IOException e) {
      LOGGER.error("Error loading data availability for {} from {}: {}", dataType, schemaFile, e.getMessage());
    }

    return null;
  }

  /**
   * Data availability rule parsed from schema file.
   * Defines when data is expected to be available for a census type.
   */
  public static class DataAvailabilityRule {
    /** Description of the data availability pattern. */
    public String description;

    /** Earliest year with available data. */
    public Integer startYear;

    /** Number of years after the reference year before data is released. */
    public Integer releaseLagYears;

    /** Frequency in years for periodic data (e.g., 5 for quinquennial). */
    public Integer frequencyYears;

    /** Explicit list of valid years (for decennial-style data). */
    public int[] validYears;

    /** Frequency type: "annual", "decennial", "quinquennial", etc. */
    public String frequency;

    /**
     * Check if data is expected to be available for the given year.
     *
     * @param year The data year to check
     * @param currentYear The current calendar year
     * @return true if data is expected to be available, false if we know it won't exist
     */
    public boolean isYearAvailable(int year, int currentYear) {
      // Check against explicit valid years if defined
      if (validYears != null && validYears.length > 0) {
        for (int validYear : validYears) {
          if (year == validYear && validYear <= currentYear) {
            return true;
          }
        }
        return false;
      }

      // Check start year constraint
      if (startYear != null && year < startYear) {
        return false;
      }

      // Check release lag (data must be old enough to have been released)
      if (releaseLagYears != null) {
        int latestAvailableYear = currentYear - releaseLagYears;
        if (year > latestAvailableYear) {
          return false;
        }
      }

      // Check frequency constraint (for quinquennial data like economic census)
      if (frequencyYears != null && startYear != null) {
        // Year must be aligned with the frequency starting from startYear
        int yearsSinceStart = year - startYear;
        if (yearsSinceStart < 0 || yearsSinceStart % frequencyYears != 0) {
          return false;
        }
      }

      return true;
    }
  }
}
