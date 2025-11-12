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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.govdata.AbstractConceptualMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Year-aware mapper for Census Bureau variables that handles variable evolution
 * across different census studies and time periods.
 *
 * <p>This class implements {@link ConceptualMapper} and provides a dynamic approach
 * to map conceptual variables (like "total_population") to the appropriate
 * Census API variables based on the specific year and census type.
 *
 * <p>For example, total population is:
 * - 2000/2010 Decennial: P001001 in sf1 dataset
 * - 2020 Decennial: P1_001N in pl dataset
 * - ACS (all years): B01001_001E in acs5 dataset
 */
public class ConceptualVariableMapper extends AbstractConceptualMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConceptualVariableMapper.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static JsonNode mappingsConfig;
  private static String currentMappingFile = "census-variable-mappings.json";

  // Singleton instance for interface-based usage
  private static final ConceptualVariableMapper INSTANCE = new ConceptualVariableMapper();

  static {
    loadMappingsConfig();
  }

  /**
   * Get singleton instance.
   */
  public static ConceptualVariableMapper getInstance() {
    return INSTANCE;
  }

  /**
   * Result of variable mapping for a specific year and census type.
   */
  public static class VariableMapping {
    private final String dataset;
    private final String variable;
    private final String conceptualName;
    private final String dataType;

    public VariableMapping(String dataset, String variable, String conceptualName, String dataType) {
      this.dataset = dataset;
      this.variable = variable;
      this.conceptualName = conceptualName;
      this.dataType = dataType;
    }

    public String getDataset() { return dataset; }
    public String getVariable() { return variable; }
    public String getConceptualName() { return conceptualName; }
    public String getDataType() { return dataType; }

    @Override public String toString() {
      return String.format("VariableMapping{dataset='%s', variable='%s', conceptualName='%s'}",
          dataset, variable, conceptualName);
    }
  }

  /**
   * Load the conceptual mappings configuration from default file.
   */
  private static void loadMappingsConfig() {
    loadMappingsConfig(currentMappingFile);
  }

  /**
   * Load the conceptual mappings configuration from specified file.
   *
   * @param mappingFile Path to mapping file (relative to classpath)
   */
  private static void loadMappingsConfig(String mappingFile) {
    try (InputStream is = ConceptualVariableMapper.class.getClassLoader()
        .getResourceAsStream(mappingFile)) {
      if (is == null) {
        throw new RuntimeException(mappingFile + " not found in classpath");
      }
      mappingsConfig = OBJECT_MAPPER.readTree(is);
      currentMappingFile = mappingFile;
      LOGGER.info("Loaded census variable mappings configuration from {}", mappingFile);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load census variable mappings from " + mappingFile, e);
    }
  }

  @Override public void loadMappingConfig(String mappingFile) {
    loadMappingsConfig(mappingFile);
  }

  /**
   * Get variable mappings for a specific table, year, and census type.
   *
   * @param tableName Name of the table (e.g., "decennial_population", "acs_income")
   * @param year Year of the census data
   * @param censusType Type of census ("decennial", "acs", "economic")
   * @return Map from Census API variable codes to conceptual column names
   */
  public static Map<String, String> getVariablesForTable(String tableName, int year, String censusType) {
    Map<String, String> result = new LinkedHashMap<>();

    try {
      // Get table definition
      JsonNode tableDefinition = mappingsConfig.path("tableDefinitions").path(tableName);
      if (tableDefinition.isMissingNode()) {
        LOGGER.warn("No table definition found for: {}", tableName);
        return result;
      }

      // Get conceptual columns for this table
      JsonNode conceptualColumns = tableDefinition.path("conceptualColumns");
      if (conceptualColumns.isArray()) {
        for (JsonNode columnNode : conceptualColumns) {
          String conceptualName = columnNode.asText();
          VariableMapping mapping = getVariableMapping(conceptualName, year, censusType);
          if (mapping != null) {
            result.put(mapping.getVariable(), conceptualName);
          } else {
            LOGGER.debug("No mapping found for conceptual variable '{}' in {} year {} - will be null in result",
                conceptualName, censusType, year);
          }
        }
      }

      LOGGER.debug("Mapped {} variables for table {} ({} year {})",
          result.size(), tableName, censusType, year);

    } catch (Exception e) {
      LOGGER.error("Error mapping variables for table {} ({} year {}): {}",
          tableName, censusType, year, e.getMessage());
    }

    return result;
  }

  /**
   * Get the specific variable mapping for a conceptual variable in a given year and census type.
   *
   * @param conceptualName Name of the conceptual variable (e.g., "total_population")
   * @param year Year of the census data
   * @param censusType Type of census ("decennial", "acs", "economic")
   * @return VariableMapping object or null if not available
   */
  public static VariableMapping getVariableMapping(String conceptualName, int year, String censusType) {
    try {
      JsonNode conceptualVar = mappingsConfig.path("conceptualVariables").path(conceptualName);
      if (conceptualVar.isMissingNode()) {
        return null;
      }

      String dataType = conceptualVar.path("dataType").asText("STRING");
      JsonNode censusTypeNode = conceptualVar.path(censusType);

      if (censusTypeNode.isMissingNode()) {
        return null;
      }

      // Check for year-specific mapping (exact year match)
      JsonNode yearMapping = censusTypeNode.path(String.valueOf(year));
      if (!yearMapping.isMissingNode()) {
        String dataset = yearMapping.path("dataset").asText();
        String variable = yearMapping.path("variable").asText();
        return new VariableMapping(dataset, variable, conceptualName, dataType);
      }

      // Check for year range mappings (e.g., "2007-2011")
      Iterator<String> fieldNames = censusTypeNode.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        if (fieldName.contains("-")) {
          String[] range = fieldName.split("-");
          if (range.length == 2) {
            try {
              int startYear = Integer.parseInt(range[0]);
              int endYear = Integer.parseInt(range[1]);
              if (year >= startYear && year <= endYear) {
                JsonNode rangeMapping = censusTypeNode.path(fieldName);
                String variable = rangeMapping.path("variable").asText();
                String dataset = rangeMapping.path("dataset").asText();
                if (dataset.isEmpty()) {
                  dataset = getDefaultDataset(censusType, year);
                }
                return new VariableMapping(dataset, variable, conceptualName, dataType);
              }
            } catch (NumberFormatException e) {
              // Skip invalid range
            }
          }
        }
      }

      // Check for allYears mapping
      JsonNode allYearsMapping = censusTypeNode.path("allYears");
      if (!allYearsMapping.isMissingNode()) {
        String variable = allYearsMapping.path("variable").asText();
        // For ACS, determine dataset based on year and availability
        String dataset = getDefaultDataset(censusType, year);
        return new VariableMapping(dataset, variable, conceptualName, dataType);
      }

      // Check for note (indicates unavailable)
      JsonNode note = censusTypeNode.path("note");
      if (!note.isMissingNode()) {
        LOGGER.debug("Variable '{}' not available for {} year {}: {}",
            conceptualName, censusType, year, note.asText());
        return null;
      }

    } catch (Exception e) {
      LOGGER.error("Error getting variable mapping for '{}' ({} year {}): {}",
          conceptualName, censusType, year, e.getMessage());
    }

    return null;
  }

  /**
   * Get the primary dataset to use for a given census type and year.
   *
   * @param censusType Type of census
   * @param year Year of the data
   * @return Dataset name (e.g., "sf1", "pl", "acs5")
   */
  public static String getDataset(String censusType, int year) {
    try {
      JsonNode datasetMappings = mappingsConfig.path("datasetMappings").path(censusType);

      // Check for year-specific mapping
      JsonNode yearMapping = datasetMappings.path(String.valueOf(year));
      if (!yearMapping.isMissingNode()) {
        return yearMapping.path("primary").asText();
      }

      // Check for year range mappings (e.g., "2020-2029")
      Iterator<String> fieldNames = datasetMappings.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        if (fieldName.contains("-")) {
          String[] range = fieldName.split("-");
          if (range.length == 2) {
            try {
              int startYear = Integer.parseInt(range[0]);
              int endYear = Integer.parseInt(range[1]);
              if (year >= startYear && year <= endYear) {
                return datasetMappings.path(fieldName).path("primary").asText();
              }
            } catch (NumberFormatException e) {
              // Skip invalid range
            }
          }
        }
      }

      // Check for allYears mapping
      JsonNode allYearsMapping = datasetMappings.path("allYears");
      if (!allYearsMapping.isMissingNode()) {
        return allYearsMapping.path("primary").asText();
      }

    } catch (Exception e) {
      LOGGER.error("Error getting dataset for {} year {}: {}", censusType, year, e.getMessage());
    }

    return getDefaultDataset(censusType, year);
  }

  /**
   * Get fallback datasets if the primary dataset fails.
   *
   * @param censusType Type of census
   * @param year Year of the data
   * @return Array of fallback dataset names
   */
  public static String[] getFallbackDatasets(String censusType, int year) {
    try {
      JsonNode datasetMappings = mappingsConfig.path("datasetMappings").path(censusType);

      // Check for year-specific mapping
      JsonNode yearMapping = datasetMappings.path(String.valueOf(year));
      if (!yearMapping.isMissingNode()) {
        JsonNode fallbacks = yearMapping.path("fallbacks");
        if (fallbacks.isArray()) {
          String[] result = new String[fallbacks.size()];
          for (int i = 0; i < fallbacks.size(); i++) {
            result[i] = fallbacks.get(i).asText();
          }
          return result;
        }
      }

      // Check for year range mappings (e.g., "2020-2029")
      Iterator<String> fieldNames = datasetMappings.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        if (fieldName.contains("-")) {
          String[] range = fieldName.split("-");
          if (range.length == 2) {
            try {
              int startYear = Integer.parseInt(range[0]);
              int endYear = Integer.parseInt(range[1]);
              if (year >= startYear && year <= endYear) {
                JsonNode fallbacks = datasetMappings.path(fieldName).path("fallbacks");
                if (fallbacks.isArray()) {
                  String[] result = new String[fallbacks.size()];
                  for (int i = 0; i < fallbacks.size(); i++) {
                    result[i] = fallbacks.get(i).asText();
                  }
                  return result;
                }
              }
            } catch (NumberFormatException e) {
              // Skip invalid range
            }
          }
        }
      }

      // Check for allYears mapping
      JsonNode allYearsMapping = datasetMappings.path("allYears");
      if (!allYearsMapping.isMissingNode()) {
        JsonNode fallbacks = allYearsMapping.path("fallbacks");
        if (fallbacks.isArray()) {
          String[] result = new String[fallbacks.size()];
          for (int i = 0; i < fallbacks.size(); i++) {
            result[i] = fallbacks.get(i).asText();
          }
          return result;
        }
      }

    } catch (Exception e) {
      LOGGER.error("Error getting fallback datasets for {} year {}: {}", censusType, year, e.getMessage());
    }

    return new String[0];
  }

  /**
   * Get default dataset for a census type when no mapping is found.
   */
  private static String getDefaultDataset(String censusType, int year) {
    switch (censusType) {
      case "decennial":
        if (year >= 2020) return "pl";
        return "sf1";
      case "acs":
        return "acs5";
      case "economic":
        return "ecn";
      default:
        return "unknown";
    }
  }

  /**
   * Get all conceptual columns defined for a table (static helper).
   *
   * @param tableName Name of the table
   * @return Array of conceptual column names
   */
  private static String[] getConceptualColumnsStatic(String tableName) {
    try {
      JsonNode tableDefinition = mappingsConfig.path("tableDefinitions").path(tableName);
      JsonNode conceptualColumns = tableDefinition.path("conceptualColumns");

      if (conceptualColumns.isArray()) {
        String[] result = new String[conceptualColumns.size()];
        for (int i = 0; i < conceptualColumns.size(); i++) {
          result[i] = conceptualColumns.get(i).asText();
        }
        return result;
      }
    } catch (Exception e) {
      LOGGER.error("Error getting conceptual columns for table {}: {}", tableName, e.getMessage());
    }

    return new String[0];
  }

  /**
   * Get variables to download for a table - includes only variables that exist for the given year/type (static helper).
   *
   * @param tableName Name of the table
   * @param year Year of the data
   * @param censusType Type of census
   * @return Array of variable codes to request from Census API
   */
  private static String[] getVariablesToDownloadStatic(String tableName, int year, String censusType) {
    Map<String, String> mappings = getVariablesForTable(tableName, year, censusType);
    return mappings.keySet().toArray(new String[0]);
  }

  // ========== Public Static API (for backward compatibility) ==========
  // These delegate to the singleton instance

  /**
   * Get all conceptual columns defined for a table.
   *
   * @param tableName Name of the table
   * @return Array of conceptual column names
   */
  public static String[] getConceptualColumnsForTable(String tableName) {
    return getConceptualColumnsStatic(tableName);
  }

  /**
   * Get variables to download for a table - includes only variables that exist for the given year/type.
   *
   * @param tableName Name of the table
   * @param year Year of the data
   * @param censusType Type of census
   * @return Array of variable codes to request from Census API
   */
  public static String[] getVariablesToDownload(String tableName, int year, String censusType) {
    return getVariablesToDownloadStatic(tableName, year, censusType);
  }

  // ========== ConceptualMapper Interface Implementation ==========

  @Override public Map<String, AbstractConceptualMapper.VariableMapping> getVariablesForTable(
      String tableName, Map<String, Object> dimensions) {
    int year = extractYear(dimensions);
    String censusType = extractCensusType(dimensions);

    Map<String, String> variableToConceptualMap = getVariablesForTable(tableName, year, censusType);
    Map<String, AbstractConceptualMapper.VariableMapping> result = new java.util.LinkedHashMap<>();

    for (Map.Entry<String, String> entry : variableToConceptualMap.entrySet()) {
      String variable = entry.getKey();
      String conceptualName = entry.getValue();
      VariableMapping mapping = getVariableMapping(conceptualName, year, censusType);
      if (mapping != null) {
        result.put(
            variable, new AbstractConceptualMapper.VariableMapping(
            mapping.getDataset(), mapping.getVariable(), mapping.getConceptualName(), mapping.getDataType()));
      }
    }

    return result;
  }

  @Override public String[] getVariablesToDownload(String tableName, Map<String, Object> dimensions) {
    int year = extractYear(dimensions);
    String censusType = extractCensusType(dimensions);
    return getVariablesToDownloadStatic(tableName, year, censusType);
  }

  /**
   * Extract census type from dimensions map with default fallback.
   * Overrides parent extractType() to provide Census-specific default behavior.
   */
  private String extractCensusType(Map<String, Object> dimensions) {
    String censusType = extractType(dimensions);
    if (censusType == null) {
      // Default to "decennial" for Census if not specified
      return "decennial";
    }
    return censusType;
  }
}
