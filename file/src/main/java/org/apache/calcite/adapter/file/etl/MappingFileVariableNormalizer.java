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
package org.apache.calcite.adapter.file.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Generic variable normalizer that maps API variable names to canonical names
 * using a JSON mapping file.
 *
 * <p>This enables schema evolution by allowing different API variable codes
 * to be normalized to consistent canonical column names. For example:
 * <ul>
 *   <li>{@code B01001_001E} (Census ACS) → {@code total_population}</li>
 *   <li>{@code P1_001N} (Census 2020) → {@code total_population}</li>
 *   <li>{@code P001001} (Census 2010) → {@code total_population}</li>
 * </ul>
 *
 * <h3>Mapping File Format</h3>
 * <p>The mapping file should be JSON with a {@code conceptualVariables} object:
 * <pre>{@code
 * {
 *   "conceptualVariables": {
 *     "total_population": {
 *       "description": "Total population count",
 *       "acs": { "allYears": { "variable": "B01001_001E" } },
 *       "decennial": {
 *         "2010": { "variable": "P001001" },
 *         "2020": { "variable": "P1_001N" }
 *       }
 *     }
 *   }
 * }
 * }</pre>
 *
 * <h3>Usage in schema YAML</h3>
 * <pre>{@code
 * hooks:
 *   variableNormalizer: "org.apache.calcite.adapter.file.etl.MappingFileVariableNormalizer"
 *   variableNormalizerConfig:
 *     mappingFile: "census/census-variable-mappings.json"
 *     defaultType: "acs"
 * }</pre>
 *
 * @see VariableNormalizer
 */
public class MappingFileVariableNormalizer implements VariableNormalizer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MappingFileVariableNormalizer.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final JsonNode mappingsConfig;
  private final String defaultType;

  /**
   * Creates a normalizer with the specified mapping file.
   *
   * @param mappingFilePath Path to the JSON mapping file (classpath resource)
   */
  public MappingFileVariableNormalizer(String mappingFilePath) {
    this(mappingFilePath, null);
  }

  /**
   * Creates a normalizer with the specified mapping file and default type.
   *
   * @param mappingFilePath Path to the JSON mapping file (classpath resource)
   * @param defaultType Default data type (e.g., "acs", "decennial") for lookups
   */
  public MappingFileVariableNormalizer(String mappingFilePath, String defaultType) {
    this.defaultType = defaultType != null ? defaultType : "acs";
    this.mappingsConfig = loadMappingFile(mappingFilePath);
  }

  /**
   * Creates a normalizer from a configuration map.
   * Expected keys: "mappingFile", "defaultType" (optional)
   *
   * @param config Configuration map from schema YAML
   */
  public MappingFileVariableNormalizer(Map<String, Object> config) {
    String mappingFile = (String) config.get("mappingFile");
    if (mappingFile == null) {
      throw new IllegalArgumentException(
          "MappingFileVariableNormalizer requires 'mappingFile' in config");
    }
    this.defaultType = config.containsKey("defaultType")
        ? (String) config.get("defaultType") : "acs";
    this.mappingsConfig = loadMappingFile(mappingFile);
  }

  private JsonNode loadMappingFile(String path) {
    try {
      // Try classpath first
      InputStream is = getClass().getClassLoader().getResourceAsStream(path);
      if (is == null) {
        // Try with leading slash
        is = getClass().getResourceAsStream("/" + path);
      }
      if (is == null) {
        throw new IOException("Mapping file not found: " + path);
      }
      JsonNode config = MAPPER.readTree(is);
      LOGGER.info("Loaded variable mapping file: {} with {} conceptual variables",
          path, config.path("conceptualVariables").size());
      return config;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load mapping file: " + path, e);
    }
  }

  @Override
  public String normalize(String apiVariable, Map<String, String> context) {
    if (apiVariable == null || apiVariable.isEmpty()) {
      return apiVariable;
    }

    // Extract context values
    String year = context.getOrDefault("year", "");
    String type = context.getOrDefault("type", defaultType);

    // Try to find the conceptual name for this API variable
    String conceptualName = findConceptualName(apiVariable, year, type);

    if (conceptualName != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Normalized {} → {} (year={}, type={})",
            apiVariable, conceptualName, year, type);
      }
      return conceptualName;
    }

    // No mapping found - return original
    return apiVariable;
  }

  /**
   * Finds the conceptual name for an API variable by searching through mappings.
   */
  private String findConceptualName(String apiVariable, String year, String type) {
    if (mappingsConfig == null) {
      return null;
    }

    JsonNode conceptualVariables = mappingsConfig.path("conceptualVariables");
    if (conceptualVariables.isMissingNode()) {
      return null;
    }

    // Iterate through all conceptual variables to find a match
    Iterator<String> fieldNames = conceptualVariables.fieldNames();
    while (fieldNames.hasNext()) {
      String conceptualName = fieldNames.next();
      JsonNode varConfig = conceptualVariables.get(conceptualName);

      // Check in the specified type section
      if (matchesVariable(varConfig, type, year, apiVariable)) {
        return conceptualName;
      }
    }

    // If not found in specified type, check other common types
    String[] typesToCheck = {"acs", "decennial", "population", "economic"};
    for (String checkType : typesToCheck) {
      if (checkType.equals(type)) {
        continue; // Already checked
      }

      Iterator<String> names = conceptualVariables.fieldNames();
      while (names.hasNext()) {
        String conceptualName = names.next();
        JsonNode varConfig = conceptualVariables.get(conceptualName);

        if (matchesVariable(varConfig, checkType, year, apiVariable)) {
          LOGGER.debug("Found {} in {} instead of {} for variable {}",
              conceptualName, checkType, type, apiVariable);
          return conceptualName;
        }
      }
    }

    return null;
  }

  /**
   * Checks if a variable config matches the given API variable.
   */
  private boolean matchesVariable(JsonNode varConfig, String type, String year,
      String apiVariable) {
    JsonNode typeNode = varConfig.path(type);
    if (typeNode.isMissingNode()) {
      return false;
    }

    // Check year-specific mapping
    if (!year.isEmpty()) {
      JsonNode yearNode = typeNode.path(year);
      if (!yearNode.isMissingNode()) {
        String variable = yearNode.path("variable").asText();
        if (apiVariable.equals(variable)) {
          return true;
        }
      }
    }

    // Check allYears mapping
    JsonNode allYearsNode = typeNode.path("allYears");
    if (!allYearsNode.isMissingNode()) {
      String variable = allYearsNode.path("variable").asText();
      if (apiVariable.equals(variable)) {
        return true;
      }
    }

    return false;
  }
}
