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
package org.apache.calcite.adapter.govdata.research;

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
 * Factory for the U.S. research &amp; development schema.
 *
 * <p>Serves NSF NCSES (National Center for Science and Engineering Statistics) R&amp;D
 * statistics — one of the 13 Principal Federal Statistical Agencies. Three annual surveys,
 * each mapped to one tall Iceberg table (one ETL table = one upstream NCSES series):
 * <ul>
 *   <li><b>National Patterns of R&amp;D Resources</b> — {@code nsf_national_rd}: national
 *       aggregate R&amp;D expenditure by performing sector &times; funding source &times; year</li>
 *   <li><b>Higher Education R&amp;D (HERD) Survey</b> — {@code nsf_herd_by_institution}: R&amp;D
 *       expenditure by academic institution &times; field &times; funding source &times; year,
 *       carrying the IPEDS UnitID crosswalk into {@code edu}</li>
 *   <li><b>Federal Funds for R&amp;D Survey</b> — {@code nsf_federal_rd_obligations}: federal
 *       R&amp;D obligations by funding agency &times; performing sector &times; field &times; year</li>
 * </ul>
 *
 * <p>Every NCSES series is free and unauthenticated, so no table is key-gated. The optional
 * {@code enabledSources} operand ({@code national_patterns} / {@code herd} / {@code federal_funds})
 * narrows the schema to one survey for targeted DQ/backfill runs.
 *
 * <p>Placement note: the R&amp;D data plan proposed these tables inside {@code econ}; they live in
 * a dedicated {@code research} schema here at the project owner's direction. The marquee
 * patents-per-R&amp;D-dollar join therefore crosses {@code research} &perp; {@code patents} on
 * {@code (state_fips, year)} rather than {@code econ} &perp; {@code patents}.
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "research",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "research",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class ResearchSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResearchSchemaFactory.class);

  private static final Map<String, String> TABLE_SOURCE;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("nsf_national_rd", "national_patterns");
    m.put("nsf_herd_by_institution", "herd");
    m.put("nsf_federal_rd_obligations", "federal_funds");
    TABLE_SOURCE = Collections.unmodifiableMap(m);
  }

  @Override public String getSchemaResourceName() {
    return "/research/research-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for RESEARCH schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    for (Map.Entry<String, String> entry : TABLE_SOURCE.entrySet()) {
      String tableName = entry.getKey();
      String source = entry.getValue();
      builder.isEnabled(tableName, ctx -> isTableEnabled(tableName, source, enabledSources));
    }

    LOGGER.debug("Configured hooks for RESEARCH schema: enabledSources={}",
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

    LOGGER.info("RESEARCH enabled sources: {}", sources);
    return sources;
  }
}
