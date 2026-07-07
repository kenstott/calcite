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
package org.apache.calcite.adapter.govdata.ag;

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
 * Sub-schema factory for the USDA agriculture (AG) schema.
 *
 * <p>Tables are grouped by upstream source so an {@code enabledSources} operand can
 * select a subset (mirrors the ENERGY schema pattern):
 * <ul>
 *   <li>{@code nass} — NASS QuickStats REST API (crop production, livestock inventory)</li>
 * </ul>
 * ERS (farm income) and RMA (crop insurance) source groups are added as those
 * tables land.
 */
public class AgSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(AgSchemaFactory.class);

  private static final Set<String> NASS_TABLES = new HashSet<>(Arrays.asList(
      "nass_crop_production",
      "nass_livestock_inventory"
  ));

  @Override
  public String getSchemaResourceName() {
    return "/ag/ag-schema.yaml";
  }

  @Override
  public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for AG schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    for (final String tableName : NASS_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "nass", enabledSources));
    }

    LOGGER.debug("Configured AG schema hooks: enabledSources={}",
        enabledSources != null ? enabledSources : "all");
  }

  private boolean isTableEnabled(String tableName, String source, Set<String> enabledSources) {
    if (enabledSources != null && !enabledSources.contains(source.toLowerCase())) {
      LOGGER.debug("Table '{}' disabled: source '{}' not in enabledSources", tableName, source);
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private Set<String> parseEnabledSources(Map<String, Object> operand) {
    Object sourcesObj = operand.get("enabledSources");
    if (sourcesObj == null) {
      return null;
    }
    Set<String> sources = new HashSet<>();
    if (sourcesObj instanceof List) {
      for (Object source : (List<Object>) sourcesObj) {
        if (source instanceof String) {
          sources.add(((String) source).toLowerCase());
        }
      }
    } else if (sourcesObj instanceof String[]) {
      for (String source : (String[]) sourcesObj) {
        sources.add(source.toLowerCase());
      }
    }
    return sources;
  }
}
