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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Abstract base class for mapping conceptual variable names to data source-specific variable codes.
 *
 * <p>Handles variable evolution across time (years, versions, datasets) by providing
 * a layer of indirection between semantic variable names (like "total_population")
 * and technical API variable codes (like "P1_001N" in 2020 vs "P001001" in 2010).
 *
 * <p>This pattern allows:
 * <ul>
 *   <li>Same table definition to work across multiple years/versions</li>
 *   <li>Graceful handling of missing variables in certain time periods</li>
 *   <li>Automatic dataset/endpoint selection based on dimensions</li>
 *   <li>Centralized variable mapping configuration</li>
 * </ul>
 *
 * <p>Provides shared utility methods for:
 * <ul>
 *   <li>Loading JSON mapping files from classpath</li>
 *   <li>Extracting common dimensions (year, type, etc.) from dimension maps</li>
 *   <li>Converting between mapping formats</li>
 *   <li>Default error handling patterns</li>
 * </ul>
 *
 * <p>Subclasses implement the domain-specific logic for each data source (Census, FRED, BLS, etc.).
 *
 * <p>Example usage:
 * <pre>{@code
 * ConceptualMapper mapper = ConceptualVariableMapper.getInstance();
 * Map<String, Object> dimensions = ImmutableMap.of("year", 2020, "censusType", "decennial");
 * Map<String, VariableMapping> mappings = mapper.getVariablesForTable("decennial_population", dimensions);
 * // Returns: {"P1_001N" -> VariableMapping{conceptualName="total_population", ...}}
 * }</pre>
 */
public abstract class AbstractConceptualMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConceptualMapper.class);

  /**
   * Variable mapping result containing all metadata needed to process a conceptual variable.
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

    public String getDataset() {
      return dataset;
    }

    public String getVariable() {
      return variable;
    }

    public String getConceptualName() {
      return conceptualName;
    }

    public String getDataType() {
      return dataType;
    }

    @Override public String toString() {
      return String.format("VariableMapping{dataset='%s', variable='%s', conceptualName='%s', dataType='%s'}",
          dataset, variable, conceptualName, dataType);
    }
  }

  // ========== Abstract methods to be implemented by subclasses ==========

  /**
   * Get variable mappings for a specific table with given dimensions.
   * Subclasses implement domain-specific logic for resolving variables.
   *
   * @param tableName Name of the table (e.g., "decennial_population", "fred_indicators")
   * @param dimensions Map of dimension values (e.g., {year: 2020, censusType: "decennial", region: "US"})
   * @return Map from API variable codes to VariableMapping objects
   */
  public abstract Map<String, VariableMapping> getVariablesForTable(String tableName, Map<String, Object> dimensions);

  /**
   * Get array of variable codes to download for a table.
   * Subclasses implement domain-specific logic for determining download variables.
   *
   * @param tableName Name of the table
   * @param dimensions Map of dimension values
   * @return Array of API variable codes to request
   */
  public abstract String[] getVariablesToDownload(String tableName, Map<String, Object> dimensions);

  /**
   * Load or reload mapping configuration from specified file.
   * Subclasses implement domain-specific logic for loading and parsing configuration.
   *
   * @param mappingFile Path to mapping configuration JSON file (relative to classpath or absolute)
   */
  public abstract void loadMappingConfig(String mappingFile);

  // ========== Shared utility methods ==========

  /**
   * Extract year dimension from dimensions map with default fallback.
   *
   * @param dimensions Map of dimension values
   * @return Year value, or current year if not specified
   */
  protected int extractYear(Map<String, Object> dimensions) {
    if (dimensions == null) {
      return java.time.Year.now().getValue();
    }
    Object yearObj = dimensions.get("year");
    if (yearObj instanceof Integer) {
      return (Integer) yearObj;
    } else if (yearObj instanceof String) {
      try {
        return Integer.parseInt((String) yearObj);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid year format in dimensions: {}", yearObj);
      }
    }
    return java.time.Year.now().getValue();
  }

  /**
   * Extract type dimension from dimensions map with multiple key aliases.
   * Tries common keys: "type", "censusType", "dataSource", "category".
   *
   * @param dimensions Map of dimension values
   * @return Type value, or null if not specified
   */
  protected String extractType(Map<String, Object> dimensions) {
    if (dimensions == null) {
      return null;
    }
    // Try common type keys
    String[] typeKeys = {"type", "censusType", "dataSource", "category"};
    for (String key : typeKeys) {
      Object typeObj = dimensions.get(key);
      if (typeObj != null) {
        return typeObj.toString();
      }
    }
    return null;
  }

}
