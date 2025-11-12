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
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for reading schema configuration files (census-schema.json, econ-schema.json, etc.).
 *
 * <p>Provides common methods to extract download configurations, table metadata, and other
 * schema-level settings from JSON schema files. This reduces code duplication across
 * Census, Econ, and other govdata adapters.
 */
public class SchemaConfigReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConfigReader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Get list of table names that have download configurations for a specific type/category.
   *
   * @param schemaFile Name of schema file (e.g., "census-schema.json", "econ-schema.json")
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

      JsonNode root = OBJECT_MAPPER.readTree(schemaStream);
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

      JsonNode root = OBJECT_MAPPER.readTree(schemaStream);
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

      JsonNode root = OBJECT_MAPPER.readTree(schemaStream);
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
   * @return Mapping file name (e.g., "census-variable-mappings.json") or null
   */
  public static String getConceptualMappingFile(String schemaFile, String tableName) {
    try (InputStream schemaStream = SchemaConfigReader.class.getResourceAsStream("/" + schemaFile)) {
      if (schemaStream == null) {
        return null;
      }

      JsonNode root = OBJECT_MAPPER.readTree(schemaStream);
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
}
