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
package org.apache.calcite.adapter.govdata.lands;

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
 * Factory for the U.S. public lands schema.
 *
 * <p>Covers federal land management units and their associated economic activity
 * across three agencies: USDA Forest Service (USFS), National Park Service (NPS),
 * and Bureau of Land Management (BLM). Together these agencies manage ~640 million
 * acres — roughly 28% of U.S. land area.
 *
 * <p>Tables:
 * <ul>
 *   <li>{@code national_forests} — USDA FS unit boundaries (~175 units); static reference</li>
 *   <li>{@code timber_sales} — USDA FS FACTS timber sale contracts; partitioned by sale_year</li>
 *   <li>{@code forest_inventory} — USDA FIA state-level forest area and carbon stock; partitioned by inventory_year</li>
 *   <li>{@code nps_units} — NPS unit boundaries (~430 units); static reference</li>
 *   <li>{@code nps_visitation} — NPS IRMA monthly visitation stats; partitioned by visit_year</li>
 *   <li>{@code blm_field_offices} — BLM administrative unit boundaries (~150 offices); static reference</li>
 *   <li>{@code onrr_revenues} — ONRR federal/tribal mineral royalties; partitioned by revenue_year</li>
 * </ul>
 *
 * <p>Cross-schema value:
 * <ul>
 *   <li>{@code national_forests} + {@code disasters.wildfire_perimeters} — burned NF acreage driving USFS suppression cost</li>
 *   <li>{@code timber_sales} + {@code sec.filing_metadata} — FS contract volume as leading indicator for timber/paper companies</li>
 *   <li>{@code nps_visitation} + {@code econ.bls_employment} — tourism employment in gateway counties</li>
 *   <li>{@code onrr_revenues} + {@code sec.filing_metadata} — federal royalty exposure for E&P and mining companies</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "lands",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "lands",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class LandsSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(LandsSchemaFactory.class);

  private static final Set<String> USFS_TABLES =
      new HashSet<>(Arrays.asList(
          "national_forests",
          "timber_sales",
          "forest_inventory"));

  private static final Set<String> NPS_TABLES =
      new HashSet<>(Arrays.asList(
          "nps_units",
          "nps_visitation"));

  private static final Set<String> BLM_TABLES =
      new HashSet<>(Collections.singletonList("blm_field_offices"));

  private static final Set<String> ONRR_TABLES =
      new HashSet<>(Collections.singletonList("onrr_revenues"));

  @Override public String getSchemaResourceName() {
    return "/lands/lands-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for LANDS schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    for (String tableName : USFS_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "usfs", enabledSources));
    }

    for (String tableName : NPS_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "nps", enabledSources));
    }

    for (String tableName : BLM_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "blm", enabledSources));
    }

    for (String tableName : ONRR_TABLES) {
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, "onrr", enabledSources));
    }

    LOGGER.debug("Configured hooks for LANDS schema: enabledSources={}",
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

    Set<String> sources = new HashSet<String>();
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

    LOGGER.info("LANDS enabled sources: {}", sources);
    return sources;
  }
}
