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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for the U.S. disasters schema.
 *
 * <p>Consolidates discrete emergency-event data from three authoritative federal sources:
 * <ul>
 *   <li><b>FEMA</b> OpenFEMA API — {@code disaster_declarations},
 *       {@code public_assistance_projects}, {@code hazard_mitigation_projects},
 *       {@code nfip_claims}, {@code nfip_policies} (paginated REST JSON, no key)</li>
 *   <li><b>NOAA</b> NCEI Storm Events — {@code storm_events} (per-year bulk gzip CSV, streamed)</li>
 *   <li><b>NIFC</b> WFIGS — {@code wildfire_perimeters} (ArcGIS polygons, streamed with
 *       Esri-ring-to-WKT geometry conversion)</li>
 * </ul>
 *
 * <p>Every source is free and unauthenticated, so no table is key-gated. The optional
 * {@code enabledSources} operand ({@code fema} / {@code noaa} / {@code wfigs}) narrows the schema
 * to one agency's tables for targeted DQ/backfill runs.
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "disasters",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "disasters",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class DisastersSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DisastersSchemaFactory.class);

  private static final Map<String, String> TABLE_SOURCE;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("disaster_declarations", "fema");
    m.put("public_assistance_projects", "fema");
    m.put("hazard_mitigation_projects", "fema");
    m.put("nfip_claims", "fema");
    m.put("nfip_policies", "fema");
    m.put("storm_events", "noaa");
    m.put("wildfire_perimeters", "wfigs");
    TABLE_SOURCE = Collections.unmodifiableMap(m);
  }

  @Override public String getSchemaResourceName() {
    return "/disasters/disasters-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for DISASTERS schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    for (Map.Entry<String, String> entry : TABLE_SOURCE.entrySet()) {
      String tableName = entry.getKey();
      String source = entry.getValue();
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, source, enabledSources));
    }

    LOGGER.debug("Configured hooks for DISASTERS schema: enabledSources={}",
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

    LOGGER.info("DISASTERS enabled sources: {}", sources);
    return sources;
  }
}
