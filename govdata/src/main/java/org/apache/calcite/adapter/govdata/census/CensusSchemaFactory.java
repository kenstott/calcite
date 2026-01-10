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

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for U.S. Census Bureau demographic and socioeconomic data schemas.
 *
 * <p>This factory provides access to comprehensive Census data including:
 * <ul>
 *   <li>American Community Survey (ACS) 5-year estimates</li>
 *   <li>Decennial Census (2010, 2020)</li>
 *   <li>Population Estimates Program</li>
 * </ul>
 *
 * <p>Data is organized by topic:
 * <ul>
 *   <li>Population: Total population, demographics, age, sex</li>
 *   <li>Income: Household income, per capita income</li>
 *   <li>Housing: Units, occupancy, values, rent</li>
 *   <li>Education: Attainment levels</li>
 *   <li>Employment: Labor force, unemployment</li>
 *   <li>Poverty: Population below poverty level</li>
 * </ul>
 *
 * <p>Geographic coverage includes:
 * <ul>
 *   <li>States (50 + DC + territories)</li>
 *   <li>Counties (3,000+)</li>
 * </ul>
 *
 * <p>Implements {@link GovDataSubSchemaFactory} to configure ETL hooks via
 * {@link #configureHooks(FileSchemaBuilder, Map)}.
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "CENSUS",
 *   "schemas": [{
 *     "name": "CENSUS",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "schemaType": "CENSUS",
 *       "autoDownload": true
 *     }
 *   }]
 * }
 * </pre>
 */
public class CensusSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CensusSchemaFactory.class);

  // ACS tables
  private static final Set<String> ACS_TABLES = new HashSet<>(Arrays.asList(
      "acs_population", "acs_income", "acs_housing",
      "acs_education", "acs_employment", "acs_poverty"
  ));

  @Override
  public String getSchemaResourceName() {
    return "/census/census-schema.yaml";
  }

  @Override
  public List<String> getDependencies() {
    // Census schema has no dependencies on other schemas
    return Collections.emptyList();
  }

  @Override
  public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for CENSUS schema");

    // Parse filtering configuration
    Set<String> enabledSources = parseEnabledSources(operand);

    // Add isEnabled hooks for all tables based on enabledSources
    addIsEnabledHooks(builder, enabledSources);

    LOGGER.debug("Configured hooks for CENSUS schema: enabledSources={}",
        enabledSources != null ? enabledSources : "all");
  }

  /**
   * Add isEnabled hooks for all tables based on enabledSources filtering.
   */
  private void addIsEnabledHooks(FileSchemaBuilder builder, Set<String> enabledSources) {
    // Add hooks for ACS tables
    for (String tableName : ACS_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "acs", enabledSources));
    }

    LOGGER.debug("Added isEnabled hooks for {} ACS tables", ACS_TABLES.size());
  }

  /**
   * Check if a table is enabled based on enabledSources.
   */
  private boolean isTableEnabled(String tableName, String dataSource,
      Set<String> enabledSources) {
    // Check if data source is enabled
    if (enabledSources != null && !enabledSources.contains(dataSource.toLowerCase())) {
      LOGGER.debug("Table '{}' disabled: source '{}' not in enabledSources", tableName, dataSource);
      return false;
    }
    return true;
  }

  /**
   * Parse enabledSources configuration from operand.
   */
  private Set<String> parseEnabledSources(Map<String, Object> operand) {
    Object sourcesObj = operand.get("enabledSources");
    if (sourcesObj == null) {
      return null; // No filtering - all sources enabled
    }

    Set<String> sources = new HashSet<>();
    if (sourcesObj instanceof List) {
      for (Object source : (List<?>) sourcesObj) {
        if (source instanceof String) {
          sources.add(((String) source).toLowerCase());
        }
      }
    } else if (sourcesObj instanceof String[]) {
      for (String source : (String[]) sourcesObj) {
        sources.add(source.toLowerCase());
      }
    }

    LOGGER.info("Enabled data sources: {}", sources);
    return sources;
  }
}
