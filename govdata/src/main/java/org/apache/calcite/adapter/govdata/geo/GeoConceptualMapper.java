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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.govdata.AbstractConceptualMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Year and data-source aware mapper for geographic data that handles:
 * <ul>
 *   <li>TIGER shapefile field evolution across years (e.g., NAME → NAMELSAD in 2020)</li>
 *   <li>Census API variables for demographic/housing tables (delegates to ConceptualVariableMapper)</li>
 *   <li>HUD crosswalk field mappings</li>
 * </ul>
 *
 * <p>This mapper provides a unified interface for all geo schema tables regardless of data source
 * (TIGER shapefiles, Census API, HUD API, etc.), allowing table definitions to reference
 * conceptual field names instead of source-specific codes.
 *
 * <p>Example usage for TIGER data:
 * <pre>{@code
 * GeoConceptualMapper mapper = GeoConceptualMapper.getInstance();
 * Map<String, Object> dimensions = Map.of("year", 2020, "dataSource", "tiger");
 * String[] fields = mapper.getVariablesToDownload("states", dimensions);
 * // Returns: ["STATEFP", "NAME", "STUSPS", "ALAND", "AWATER"]
 * }</pre>
 *
 * <p>Example usage for Census API data:
 * <pre>{@code
 * Map<String, Object> dimensions = Map.of("year", 2020, "dataSource", "census_api", "censusType", "acs");
 * String[] variables = mapper.getVariablesToDownload("population_demographics", dimensions);
 * // Returns: ["B01001_001E", "B01001_002E", ...] (appropriate for year 2020 ACS)
 * }</pre>
 */
public class GeoConceptualMapper extends AbstractConceptualMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoConceptualMapper.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static JsonNode mappingsConfig;
  private static String currentMappingFile = "geo/geo-variable-mappings.json";

  // Singleton instance
  private static final GeoConceptualMapper INSTANCE = new GeoConceptualMapper();

  static {
    loadMappingsConfig();
  }

  /**
   * Get singleton instance.
   */
  public static GeoConceptualMapper getInstance() {
    return INSTANCE;
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
    try (InputStream is = GeoConceptualMapper.class.getClassLoader()
        .getResourceAsStream(mappingFile)) {
      if (is == null) {
        throw new RuntimeException(mappingFile + " not found in classpath");
      }
      mappingsConfig = OBJECT_MAPPER.readTree(is);
      currentMappingFile = mappingFile;
      LOGGER.info("Loaded geo variable mappings configuration from {}", mappingFile);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load geo variable mappings from " + mappingFile, e);
    }
  }

  @Override public void loadMappingConfig(String mappingFile) {
    loadMappingsConfig(mappingFile);
  }

  /**
   * Get variable mappings for a specific table with given dimensions.
   *
   * <p>Dimensions map should contain:
   * <ul>
   *   <li>"year" - Year of data (e.g., 2020)</li>
   *   <li>"dataSource" - Data source type: "tiger", "census_api", "hud"</li>
   *   <li>"censusType" - Census type if dataSource is "census_api": "acs", "decennial"</li>
   * </ul>
   *
   * @param tableName Name of the table (e.g., "states", "counties", "population_demographics")
   * @param dimensions Map of dimension values for resolving variables
   * @return Map from source field/variable names to VariableMapping objects
   */
  @Override public Map<String, VariableMapping> getVariablesForTable(
      String tableName, Map<String, Object> dimensions) {

    Map<String, VariableMapping> result = new LinkedHashMap<>();

    // Extract dimensions
    int year = extractYear(dimensions);
    String dataSource = extractType(dimensions);

    if (dataSource == null) {
      LOGGER.warn("No dataSource specified in dimensions for table: {}", tableName);
      return result;
    }

    try {
      // Get table definition
      JsonNode tableDefinition = mappingsConfig.path("tableDefinitions").path(tableName);
      if (tableDefinition.isMissingNode()) {
        LOGGER.warn("No table definition found for: {}", tableName);
        return result;
      }

      // Get conceptual fields/variables for this table
      JsonNode conceptualItems = tableDefinition.path("conceptualItems");
      if (conceptualItems.isArray()) {
        for (JsonNode itemNode : conceptualItems) {
          String conceptualName = itemNode.asText();

          // Route to appropriate mapping based on data source
          VariableMapping mapping = null;
          if ("tiger".equals(dataSource)) {
            mapping = getTigerFieldMapping(conceptualName, year);
          } else if ("census_api".equals(dataSource)) {
            String censusType = (String) dimensions.get("censusType");
            mapping = getCensusVariableMapping(conceptualName, year, censusType);
          } else if ("hud".equals(dataSource)) {
            mapping = getHudFieldMapping(conceptualName, year);
          }

          if (mapping != null) {
            result.put(mapping.getVariable(), mapping);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error resolving variables for table {} with dimensions {}: {}",
          tableName, dimensions, e.getMessage());
    }

    return result;
  }

  /**
   * Get array of field/variable codes to download for a table.
   *
   * @param tableName Name of the table
   * @param dimensions Map of dimension values
   * @return Array of field/variable codes to request from the data source
   */
  @Override public String[] getVariablesToDownload(String tableName, Map<String, Object> dimensions) {
    Map<String, VariableMapping> mappings = getVariablesForTable(tableName, dimensions);
    List<String> variables = new ArrayList<>(mappings.keySet());
    return variables.toArray(new String[0]);
  }

  /**
   * Get TIGER shapefile field mapping for a conceptual field name.
   * Handles field name evolution across TIGER years.
   *
   * @param conceptualName Conceptual field name (e.g., "state_fips", "county_name")
   * @param year Year of TIGER data
   * @return VariableMapping or null if not found
   */
  private VariableMapping getTigerFieldMapping(String conceptualName, int year) {
    JsonNode conceptualFields = mappingsConfig.path("conceptualFields").path(conceptualName);
    if (conceptualFields.isMissingNode()) {
      LOGGER.warn("No conceptual field definition for: {}", conceptualName);
      return null;
    }

    JsonNode tigerNode = conceptualFields.path("tiger");
    if (tigerNode.isMissingNode()) {
      return null;
    }

    // Find the appropriate year range
    Iterator<Map.Entry<String, JsonNode>> fields = tigerNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String yearRange = entry.getKey();
      JsonNode fieldNode = entry.getValue();

      if (yearInRange(year, yearRange)) {
        String fieldName = fieldNode.path("field").asText();
        String dataType = fieldNode.path("type").asText("VARCHAR");
        return new VariableMapping("tiger", fieldName, conceptualName, dataType);
      }
    }

    LOGGER.warn("No TIGER field mapping found for {} in year {}", conceptualName, year);
    return null;
  }

  /**
   * Get Census API variable mapping for a conceptual variable name.
   * Delegates to /census/census-variable-mappings.json for Census API variables.
   *
   * @param conceptualName Conceptual variable name (e.g., "total_population")
   * @param year Year of census data
   * @param censusType Type of census ("acs", "decennial")
   * @return VariableMapping or null if not found
   */
  private VariableMapping getCensusVariableMapping(
      String conceptualName, int year, String censusType) {

    JsonNode conceptualVariables = mappingsConfig.path("conceptualVariables").path(conceptualName);
    if (conceptualVariables.isMissingNode()) {
      LOGGER.warn("No conceptual variable definition for: {}", conceptualName);
      return null;
    }

    JsonNode censusNode = conceptualVariables.path("census").path(censusType);
    if (censusNode.isMissingNode()) {
      return null;
    }

    // Check for allYears first
    JsonNode allYears = censusNode.path("allYears");
    if (!allYears.isMissingNode()) {
      String variable = allYears.path("variable").asText();
      String dataset = allYears.path("dataset").asText(censusType);
      String dataType = conceptualVariables.path("dataType").asText("VARCHAR");
      return new VariableMapping(dataset, variable, conceptualName, dataType);
    }

    // Check for specific year
    String yearStr = String.valueOf(year);
    JsonNode yearNode = censusNode.path(yearStr);
    if (!yearNode.isMissingNode()) {
      String variable = yearNode.path("variable").asText();
      String dataset = yearNode.path("dataset").asText(censusType);
      String dataType = conceptualVariables.path("dataType").asText("VARCHAR");
      return new VariableMapping(dataset, variable, conceptualName, dataType);
    }

    LOGGER.warn("No Census variable mapping found for {} in {} year {}",
        conceptualName, censusType, year);
    return null;
  }

  /**
   * Get HUD field mapping for a conceptual field name.
   *
   * @param conceptualName Conceptual field name (e.g., "zip_code", "county_fips")
   * @param year Year of HUD data
   * @return VariableMapping or null if not found
   */
  private VariableMapping getHudFieldMapping(String conceptualName, int year) {
    JsonNode conceptualFields = mappingsConfig.path("conceptualFields").path(conceptualName);
    if (conceptualFields.isMissingNode()) {
      LOGGER.warn("No conceptual field definition for: {}", conceptualName);
      return null;
    }

    JsonNode hudNode = conceptualFields.path("hud");
    if (hudNode.isMissingNode()) {
      return null;
    }

    // HUD crosswalk fields are generally stable across years
    String fieldName = hudNode.path("field").asText();
    String dataType = hudNode.path("type").asText("VARCHAR");
    return new VariableMapping("hud", fieldName, conceptualName, dataType);
  }

  /**
   * Check if a year falls within a year range specification.
   * Supports formats: "2020", "2020-2029", "allYears"
   *
   * @param year Year to check
   * @param rangeSpec Year range specification string
   * @return true if year is in range
   */
  private boolean yearInRange(int year, String rangeSpec) {
    if ("allYears".equals(rangeSpec)) {
      return true;
    }

    if (rangeSpec.contains("-")) {
      String[] parts = rangeSpec.split("-");
      int startYear = Integer.parseInt(parts[0]);
      int endYear = Integer.parseInt(parts[1]);
      return year >= startYear && year <= endYear;
    } else {
      int specYear = Integer.parseInt(rangeSpec);
      return year == specYear;
    }
  }
}
