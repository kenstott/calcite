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
package org.apache.calcite.adapter.govdata.energy;

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

public class EnergySchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnergySchemaFactory.class);

  private static final Set<String> EIA_API_TABLES = new HashSet<>(Arrays.asList(
      "eia_electricity_generation",
      "eia_electricity_prices",
      "eia_fossil_fuel_production",
      "eia_state_energy_consumption",
      "eia_natural_gas_storage",
      "eia_petroleum_stocks",
      "eia_refinery_operations"
  ));

  private static final Set<String> EIA_BULK_TABLES = new HashSet<>(Arrays.asList(
      "eia_utility_annual",
      "eia_power_plants",
      "eia_capacity_changes",
      "eia_crude_oil_imports"
  ));

  private static final Set<String> MSHA_TABLES = new HashSet<>(Arrays.asList(
      "eia_coal_mines"
  ));

  @Override
  public String getSchemaResourceName() {
    return "/energy/energy-schema.yaml";
  }

  @Override
  public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for ENERGY schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    for (final String tableName : EIA_API_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "eia_api", enabledSources));
    }

    for (final String tableName : EIA_BULK_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "eia_bulk", enabledSources));
    }

    for (final String tableName : MSHA_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "msha", enabledSources));
    }

    LOGGER.debug("Configured ENERGY schema hooks: enabledSources={}",
        enabledSources != null ? enabledSources : "all");
  }

  private boolean isTableEnabled(String tableName, String source, Set<String> enabledSources) {
    if (enabledSources != null && !enabledSources.contains(source.toLowerCase())) {
      LOGGER.debug("Table '{}' disabled: source '{}' not in enabledSources", tableName, source);
      return false;
    }
    return true;
  }

  private Set<String> parseEnabledSources(Map<String, Object> operand) {
    Object sourcesObj = operand.get("enabledSources");
    if (sourcesObj == null) {
      return null;
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

    LOGGER.info("ENERGY enabled data sources: {}", sources);
    return sources;
  }
}
