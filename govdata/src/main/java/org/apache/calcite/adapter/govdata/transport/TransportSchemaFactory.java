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
package org.apache.calcite.adapter.govdata.transport;

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
 * Factory for the U.S. transportation schema.
 *
 * <p>Consolidates transportation data across five keyless federal sources:
 * <ul>
 *   <li><b>NHTSA</b> — {@code vehicle_recalls} (Socrata), {@code safety_complaints}
 *       (FLAT_CMPL.zip), {@code fatal_crashes} (FARS per-year zips)</li>
 *   <li><b>BTS</b> — {@code airline_ontime} (PREZIP monthly zip),
 *       {@code t100_segments} (TranStats postback DataProvider)</li>
 *   <li><b>FAA</b> — {@code airports} (NTAD ArcGIS, paginated)</li>
 *   <li><b>FTA</b> — {@code transit_ridership} (Socrata monthly long-format)</li>
 *   <li><b>FHWA</b> — {@code vehicle_registrations} (Highway Statistics MV-1 XLSX)</li>
 * </ul>
 *
 * <p>No source needs a secret, so nothing is token-gated. The optional
 * {@code enabledSources} operand ({@code nhtsa} / {@code bts} / {@code faa} /
 * {@code fta} / {@code fhwa}) narrows the schema to one source for targeted
 * DQ/backfill runs.
 */
public class TransportSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportSchemaFactory.class);

  private static final Map<String, String> TABLE_SOURCE;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("vehicle_recalls", "nhtsa");
    m.put("safety_complaints", "nhtsa");
    m.put("fatal_crashes", "nhtsa");
    m.put("airline_ontime", "bts");
    m.put("t100_segments", "bts");
    m.put("airports", "faa");
    m.put("transit_ridership", "fta");
    m.put("vehicle_registrations", "fhwa");
    TABLE_SOURCE = Collections.unmodifiableMap(m);
  }

  @Override public String getSchemaResourceName() {
    return "/transport/transport-schema.yaml";
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
    LOGGER.info("TRANSPORT enabled sources: {}", sources);
    return sources;
  }
}
