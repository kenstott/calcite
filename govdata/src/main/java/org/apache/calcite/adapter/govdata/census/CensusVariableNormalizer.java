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

import org.apache.calcite.adapter.file.etl.VariableNormalizer;
import org.apache.calcite.adapter.govdata.AbstractConceptualMapper;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Normalizes Census Bureau API variable names to conceptual names.
 *
 * <p>This normalizer implements schema evolution for Census Bureau data by
 * mapping source-specific variable codes to consistent conceptual names.
 * For example:
 * <ul>
 *   <li>{@code B01001_001E} (ACS) → {@code total_population}</li>
 *   <li>{@code P1_001N} (Decennial 2020) → {@code total_population}</li>
 *   <li>{@code P001001} (Decennial 2010) → {@code total_population}</li>
 * </ul>
 *
 * <p>This enables column expressions like
 * {@code TRY_CAST(src."total_population" AS BIGINT)} to work
 * across all Census products and years.
 *
 * <h3>Usage</h3>
 * <p>Configure in schema YAML:
 * <pre>{@code
 * hooks:
 *   variableNormalizer: "org.apache.calcite.adapter.govdata.census.CensusVariableNormalizer"
 * }</pre>
 *
 * <p>Then use conceptual names in column expressions:
 * <pre>{@code
 * columns:
 *   - name: total_population
 *     expression: 'TRY_CAST(src."total_population" AS BIGINT)'
 * }</pre>
 *
 * @see ConceptualVariableMapper
 * @see VariableNormalizer
 */
public class CensusVariableNormalizer implements VariableNormalizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CensusVariableNormalizer.class);

  private final ConceptualVariableMapper mapper;

  /**
   * Creates a new CensusVariableNormalizer.
   */
  public CensusVariableNormalizer() {
    this.mapper = ConceptualVariableMapper.getInstance();
  }

  @Override
  public String normalize(String apiVariable, Map<String, String> context) {
    if (apiVariable == null || apiVariable.isEmpty()) {
      return apiVariable;
    }

    // Extract year and census type from context
    int year = parseYear(context.get("year"));
    String censusType = context.getOrDefault("type", "acs");

    // Try to find the conceptual name for this API variable
    String conceptualName = findConceptualName(apiVariable, year, censusType);

    if (conceptualName != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Census: Normalized {} → {} (year={}, type={})",
            apiVariable, conceptualName, year, censusType);
      }
      return conceptualName;
    }

    // No mapping found - return original
    return apiVariable;
  }

  /**
   * Finds the conceptual name for an API variable by searching through mappings.
   *
   * @param apiVariable The API variable code to look up
   * @param year Year of the data
   * @param censusType Type of census (acs, decennial, etc.)
   * @return Conceptual name, or null if not found
   */
  private String findConceptualName(String apiVariable, int year, String censusType) {
    try {
      // Get the mappings configuration
      JsonNode mappingsConfig = mapper.getMappingsConfig();
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
        ConceptualVariableMapper.VariableMapping mapping =
            mapper.getVariableMapping(conceptualName, year, censusType);

        if (mapping != null && apiVariable.equals(mapping.getVariable())) {
          return conceptualName;
        }
      }

      // Also check other census types for backwards compatibility
      String[] typesToCheck = {"acs", "decennial", "population", "economic"};
      for (String type : typesToCheck) {
        if (type.equals(censusType)) {
          continue; // Already checked
        }

        Iterator<String> names = conceptualVariables.fieldNames();
        while (names.hasNext()) {
          String conceptualName = names.next();
          ConceptualVariableMapper.VariableMapping mapping =
              mapper.getVariableMapping(conceptualName, year, type);

          if (mapping != null && apiVariable.equals(mapping.getVariable())) {
            LOGGER.debug("Census: Found {} in {} instead of {} for variable {}",
                conceptualName, type, censusType, apiVariable);
            return conceptualName;
          }
        }
      }

    } catch (Exception e) {
      LOGGER.debug("Census: Error finding conceptual name for {}: {}",
          apiVariable, e.getMessage());
    }

    return null;
  }

  /**
   * Parses year from string with fallback to current year.
   */
  private int parseYear(String yearStr) {
    if (yearStr == null || yearStr.isEmpty()) {
      return java.time.Year.now().getValue();
    }
    try {
      return Integer.parseInt(yearStr);
    } catch (NumberFormatException e) {
      return java.time.Year.now().getValue();
    }
  }
}
