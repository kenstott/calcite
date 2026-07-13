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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
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
 * Factory for the U.S. environment schema.
 *
 * <p>Consolidates environmental data across keyless federal sources:
 * <ul>
 *   <li><b>EPA AQS</b> (aqs.epa.gov AirData) — {@code air_quality_annual},
 *       {@code air_quality_daily}, {@code aqs_monitors} (moved here from the
 *       weather schema)</li>
 *   <li><b>EPA TRI</b> — {@code tri_releases}</li>
 *   <li><b>EPA GHGRP</b> (Envirofacts) — {@code ghg_facilities}, {@code ghg_emissions}</li>
 *   <li><b>USGS NWIS</b> — {@code water_sites}, {@code streamflow}</li>
 *   <li><b>Water Quality Portal</b> — {@code water_quality_samples}</li>
 *   <li><b>EPA SDWIS</b> (Envirofacts) — {@code drinking_water},
 *       {@code drinking_water_violations}</li>
 *   <li><b>EPA ECHO/FRS</b> — {@code epa_facilities}</li>
 *   <li><b>EPA SEMS</b> — {@code superfund_sites}</li>
 *   <li><b>EPA RCRAInfo</b> — {@code rcra_facilities}</li>
 * </ul>
 *
 * <p>No source needs a secret, so nothing is token-gated. The optional
 * {@code enabledSources} operand ({@code aqs} / {@code tri} / {@code ghg} /
 * {@code usgs} / {@code wqp} / {@code sdwis} / {@code echo} / {@code superfund} /
 * {@code rcra}) narrows the schema to one source for targeted DQ/backfill runs.
 */
public class EnvironmentSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentSchemaFactory.class);

  private static final Map<String, String> TABLE_SOURCE;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("air_quality_annual", "aqs");
    m.put("air_quality_daily", "aqs");
    m.put("aqs_monitors", "aqs");
    m.put("tri_releases", "tri");
    m.put("ghg_facilities", "ghg");
    m.put("ghg_emissions", "ghg");
    m.put("water_sites", "usgs");
    m.put("streamflow", "usgs");
    m.put("water_quality_samples", "wqp");
    m.put("drinking_water", "sdwis");
    m.put("drinking_water_violations", "sdwis");
    m.put("epa_facilities", "echo");
    m.put("superfund_sites", "superfund");
    m.put("rcra_facilities", "rcra");
    TABLE_SOURCE = Collections.unmodifiableMap(m);
  }

  @Override public String getSchemaResourceName() {
    return "/environment/environment-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    Set<String> enabledSources = parseEnabledSources(operand);
    if (enabledSources == null) {
      return;
    }
    for (Map.Entry<String, String> entry : TABLE_SOURCE.entrySet()) {
      String tableName = entry.getKey();
      String source = entry.getValue();
      builder.isEnabled(tableName, ctx -> enabledSources.contains(source));
    }
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
    LOGGER.info("ENVIRONMENT enabled sources: {}", sources);
    return sources;
  }
}
