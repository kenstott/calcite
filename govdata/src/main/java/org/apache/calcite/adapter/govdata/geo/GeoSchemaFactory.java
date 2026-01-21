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
 * Factory for geographic data schemas that provides access to U.S. government
 * geographic datasets.
 *
 * <p>This factory uses a declarative YAML schema configuration and the file adapter's
 * infrastructure for HTTP operations and Parquet storage.
 *
 * <p>Supported data sources:
 * <ul>
 *   <li>Census TIGER/Line boundary files (states, counties, tracts, etc.)</li>
 *   <li>HUD-USPS ZIP code crosswalk (ZIP to county, CBSA, tract)</li>
 *   <li>Census demographic data via API (population, housing, economic)</li>
 * </ul>
 *
 * <p>Implements {@link GovDataSubSchemaFactory} to configure ETL hooks via
 * {@link #configureHooks(FileSchemaBuilder, Map)}.
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "GEO",
 *   "schemas": [{
 *     "name": "GEO",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "dataSource": "geo",
 *       "autoDownload": true,
 *       "enabledSources": ["tiger", "hud", "census"]
 *     }
 *   }]
 * }
 * </pre>
 */
public class GeoSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoSchemaFactory.class);

  // TIGER boundary tables
  private static final Set<String> TIGER_TABLES =
      new HashSet<>(
          Arrays.asList(
              "states",
              "counties",
              "places",
              "zctas",
              "census_tracts",
              "block_groups",
              "cbsa",
              "congressional_districts",
              "school_districts"));

  // HUD crosswalk tables
  private static final Set<String> HUD_TABLES =
      new HashSet<>(
          Arrays.asList(
              "zip_county_crosswalk",
              "zip_cbsa_crosswalk",
              "tract_zip_crosswalk"));

  // Note: Census demographic tables removed - use CENSUS schema for demographics
  // Join GEO.states/counties with CENSUS.acs_population for demographic data

  @Override public String getSchemaResourceName() {
    return "/geo/geo-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    // GEO schema has no dependencies on other schemas
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for GEO schema");

    // Parse filtering configuration
    Set<String> enabledSources = parseEnabledSources(operand);

    // Add isEnabled hooks for all tables based on enabledSources
    addIsEnabledHooks(builder, enabledSources);

    LOGGER.debug("Configured hooks for GEO schema: enabledSources={}",
        enabledSources != null ? enabledSources : "all");
  }

  /**
   * Add isEnabled hooks for all tables based on enabledSources filtering.
   */
  private void addIsEnabledHooks(FileSchemaBuilder builder, Set<String> enabledSources) {
    // Add hooks for TIGER tables
    for (String tableName : TIGER_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "tiger", enabledSources));
    }

    // Add hooks for HUD tables
    for (String tableName : HUD_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "hud", enabledSources));
    }

    LOGGER.debug("Added isEnabled hooks for {} TIGER, {} HUD tables",
        TIGER_TABLES.size(), HUD_TABLES.size());
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
