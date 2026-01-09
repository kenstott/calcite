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
package org.apache.calcite.adapter.govdata.econ;

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
 * Schema factory for U.S. economic data sources.
 *
 * <p>Provides access to economic data from:
 * <ul>
 *   <li>Bureau of Labor Statistics (BLS) - Employment, inflation, wages</li>
 *   <li>Federal Reserve (FRED) - Interest rates, GDP, economic indicators</li>
 *   <li>U.S. Treasury - Treasury yields, auction results, debt statistics</li>
 *   <li>Bureau of Economic Analysis (BEA) - GDP components, trade data</li>
 *   <li>World Bank - International economic indicators</li>
 * </ul>
 *
 * <p>Implements {@link GovDataSubSchemaFactory} to configure ETL hooks via
 * {@link #configureHooks(FileSchemaBuilder, Map)}.
 */
public class EconSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconSchemaFactory.class);

  // Data source mappings for tables
  private static final Set<String> BLS_TABLES = new HashSet<>(Arrays.asList(
      "employment_statistics", "inflation_metrics", "regional_cpi", "metro_cpi",
      "state_industry", "state_wages", "metro_industry", "metro_wages",
      "county_qcew", "county_wages", "jolts_regional", "jolts_state",
      "wage_growth", "regional_employment"
  ));

  private static final Set<String> TREASURY_TABLES = new HashSet<>(Arrays.asList(
      "treasury_yields", "federal_debt"
  ));

  private static final Set<String> WORLDBANK_TABLES = new HashSet<>(Arrays.asList(
      "world_indicators"
  ));

  private static final Set<String> FRED_TABLES = new HashSet<>(Arrays.asList(
      "fred_indicators"
  ));

  private static final Set<String> BEA_TABLES = new HashSet<>(Arrays.asList(
      "national_accounts", "regional_income", "ita_data", "gdp_statistics", "industry_gdp",
      // Bulk download tables (using ZIP files instead of per-API-call approach)
      "state_personal_income", "state_gdp", "state_quarterly_income",
      "state_quarterly_gdp", "state_consumption"
  ));

  @Override
  public String getSchemaResourceName() {
    return "/econ/econ-schema.yaml";
  }

  @Override
  public List<String> getDependencies() {
    // The econ schema depends on econ_reference for dimension lookup tables
    // (e.g., regional_linecodes needed by BeaDimensionResolver)
    return Collections.singletonList("econ_reference");
  }

  @Override
  public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for ECON schema");

    // Parse filtering configuration
    Set<String> enabledSources = parseEnabledSources(operand);
    Set<String> enabledBlsTables = parseBlsTableFilter(operand);

    // WorldBank dimensions are now declarative in YAML using json_catalog type
    // No need for Java hooks - dimensions are resolved automatically by DimensionIterator

    // Add isEnabled hooks for all tables based on enabledSources and blsConfig
    addIsEnabledHooks(builder, enabledSources, enabledBlsTables);

    LOGGER.debug("Configured hooks for ECON schema: enabledSources={}, blsTables={}",
        enabledSources, enabledBlsTables != null ? enabledBlsTables.size() : "all");
  }

  /**
   * Add isEnabled hooks for all tables based on enabledSources and blsConfig filtering.
   */
  private void addIsEnabledHooks(FileSchemaBuilder builder,
      Set<String> enabledSources, Set<String> enabledBlsTables) {
    // Add hooks for BLS tables
    for (String tableName : BLS_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "bls", enabledSources, enabledBlsTables));
    }

    // Add hooks for Treasury tables
    for (String tableName : TREASURY_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "treasury", enabledSources, null));
    }

    // Add hooks for WorldBank tables
    for (String tableName : WORLDBANK_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "worldbank", enabledSources, null));
    }

    // Add hooks for FRED tables
    for (String tableName : FRED_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "fred", enabledSources, null));
    }

    // Add hooks for BEA tables
    for (String tableName : BEA_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "bea", enabledSources, null));
    }

    LOGGER.debug("Added isEnabled hooks for {} BLS, {} Treasury, {} WorldBank, {} FRED, {} BEA tables",
        BLS_TABLES.size(), TREASURY_TABLES.size(), WORLDBANK_TABLES.size(),
        FRED_TABLES.size(), BEA_TABLES.size());
  }

  /**
   * Check if a table is enabled based on enabledSources and blsConfig.
   */
  private boolean isTableEnabled(String tableName, String dataSource,
      Set<String> enabledSources, Set<String> enabledBlsTables) {
    // Check if data source is enabled
    if (enabledSources != null && !enabledSources.contains(dataSource.toLowerCase())) {
      LOGGER.debug("Table '{}' disabled: source '{}' not in enabledSources", tableName, dataSource);
      return false;
    }

    // For BLS tables, also check blsConfig filtering
    if ("bls".equals(dataSource) && enabledBlsTables != null) {
      if (!enabledBlsTables.contains(tableName)) {
        LOGGER.debug("Table '{}' disabled: not in blsConfig enabled tables", tableName);
        return false;
      }
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

  /**
   * Parse BLS table filtering configuration from operand.
   */
  private Set<String> parseBlsTableFilter(Map<String, Object> operand) {
    @SuppressWarnings("unchecked")
    Map<String, Object> config = (Map<String, Object>) operand.get("blsConfig");
    if (config == null) {
      return null; // No filtering - include all tables
    }

    @SuppressWarnings("unchecked")
    List<String> includeTables = (List<String>) config.get("includeTables");
    @SuppressWarnings("unchecked")
    List<String> excludeTables = (List<String>) config.get("excludeTables");

    // Validate mutual exclusivity
    if (includeTables != null && excludeTables != null) {
      throw new IllegalArgumentException(
          "Cannot specify both 'includeTables' and 'excludeTables' in blsConfig.");
    }

    if (includeTables != null) {
      Set<String> filtered = new HashSet<>(includeTables);
      LOGGER.info("BLS table filter: including {} tables", filtered.size());
      return filtered;
    }

    if (excludeTables != null) {
      Set<String> filtered = new HashSet<>(BLS_TABLES);
      excludeTables.forEach(filtered::remove);
      LOGGER.info("BLS table filter: excluding {} tables, keeping {} tables",
          excludeTables.size(), filtered.size());
      return filtered;
    }

    return null; // No filtering
  }
}
