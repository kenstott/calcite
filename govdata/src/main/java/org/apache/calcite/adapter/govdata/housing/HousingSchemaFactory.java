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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for the U.S. housing schema.
 *
 * <p>Consolidates housing-market and affordability data from three sources:
 * <ul>
 *   <li><b>FHFA</b> House Price Index — {@code house_price_index}
 *       (bulk {@code hpi_master.csv}, streamed; no key)</li>
 *   <li><b>Census</b> Building Permits Survey — {@code building_permits}
 *       (per-year county text file; no key)</li>
 *   <li><b>HUD</b> USER API — {@code fair_market_rents}, {@code income_limits}
 *       (per-state JSON, Bearer {@code HUD_TOKEN})</li>
 * </ul>
 *
 * <p>The two HUD tables are token-gated: when {@code ${HUD_TOKEN}} resolves to
 * empty they are disabled via {@link FileSchemaBuilder#isEnabled} so the schema
 * still builds (the FHFA and Census tables need no secret). The optional
 * {@code enabledSources} operand ({@code fhfa} / {@code census} / {@code hud})
 * narrows the schema to one source for targeted DQ/backfill runs.
 */
public class HousingSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(HousingSchemaFactory.class);

  private static final Map<String, String> TABLE_SOURCE;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("house_price_index", "fhfa");
    m.put("building_permits", "census");
    m.put("fair_market_rents", "hud");
    m.put("income_limits", "hud");
    m.put("income_limits_county", "hud");
    TABLE_SOURCE = Collections.unmodifiableMap(m);
  }

  @Override public String getSchemaResourceName() {
    return "/housing/housing-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    Set<String> enabledSources = parseEnabledSources(operand);
    boolean hudTokenPresent = isHudTokenPresent();
    if (!hudTokenPresent) {
      LOGGER.info("HOUSING: HUD_TOKEN absent — fair_market_rents and income_limits disabled");
    }

    for (Map.Entry<String, String> entry : TABLE_SOURCE.entrySet()) {
      String tableName = entry.getKey();
      String source = entry.getValue();
      builder.isEnabled(tableName,
          ctx -> isTableEnabled(tableName, source, enabledSources, hudTokenPresent));
    }
  }

  private boolean isTableEnabled(String tableName, String source, Set<String> enabledSources,
      boolean hudTokenPresent) {
    if ("hud".equals(source) && !hudTokenPresent) {
      return false;
    }
    if (enabledSources != null && !enabledSources.contains(source)) {
      LOGGER.debug("Table '{}' disabled: source '{}' not in enabledSources", tableName, source);
      return false;
    }
    return true;
  }

  /** Resolves {@code ${HUD_TOKEN}} through the sanctioned VariableResolver (env/property). */
  private boolean isHudTokenPresent() {
    String token = VariableResolver.resolveEnvVars("${HUD_TOKEN:}");
    return token != null && !token.trim().isEmpty();
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
    LOGGER.info("HOUSING enabled sources: {}", sources);
    return sources;
  }
}
