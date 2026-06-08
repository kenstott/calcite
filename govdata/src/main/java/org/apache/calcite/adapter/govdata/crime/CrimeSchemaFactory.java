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
package org.apache.calcite.adapter.govdata.crime;

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
 * Factory for crime and public safety data schemas.
 *
 * <p>Provides access to FBI Crime Data Explorer (CDE) and Bureau of Justice
 * Statistics (BJS) datasets. Supports conditional table enablement via
 * {@code enabledSources} configuration.
 *
 * <p>Supported data sources:
 * <ul>
 *   <li>cde - FBI Crime Data Explorer (agencies, offenses, police employment,
 *       hate crimes, use of force)</li>
 *   <li>bjs - Bureau of Justice Statistics (NIBRS estimates, NCVS victimization)</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "CRIME",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "crime",
 *     "enabledSources": ["cde", "bjs"]
 *   }
 * }
 * </pre>
 */
public class CrimeSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrimeSchemaFactory.class);

  // FBI CDE tables - working endpoints on cde.ucr.cjis.gov/LATEST/
  private static final Set<String> CDE_TABLES =
      new HashSet<>(
          Arrays.asList(
              "cde_agencies",
              "cde_offenses",
              "cde_police_employment",
              "cde_hate_crimes",
              "cde_use_of_force",
              "cde_crime_agency",
              "cde_leoka",
              "cde_arrests",
              "cde_shr",
              "cde_trends",
              "cde_supplemental"));

  // FBI CDE tables - broken endpoints (missing routes as of 2026-02)
  // These will be re-enabled once FBI fixes the /cde/ API gateway migration
  private static final Set<String> CDE_BROKEN_TABLES =
      new HashSet<>(
          Arrays.asList(
              "cde_cargo_theft",       // 403 route not migrated
              "cde_human_trafficking", // 403 route not migrated
              "cde_participation",     // 404 route not migrated
              "cde_estimates"));       // 403 route not migrated

  // Bureau of Justice Statistics tables
  private static final Set<String> BJS_TABLES =
      new HashSet<>(
          Arrays.asList(
              "bjs_nibrs_estimates",
              "bjs_ncvs_personal",
              "bjs_ncvs_personal_pop",
              "bjs_ncvs_household",
              "bjs_ncvs_household_pop"));

  @Override public String getSchemaResourceName() {
    return "/crime/crime-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for CRIME schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    addIsEnabledHooks(builder, enabledSources);

    LOGGER.debug("Configured hooks for CRIME schema: enabledSources={}",
        enabledSources != null ? enabledSources : "all");
  }

  private void addIsEnabledHooks(FileSchemaBuilder builder, Set<String> enabledSources) {
    for (String tableName : CDE_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "cde", enabledSources));
    }

    // Broken CDE tables are disabled by default unless explicitly force-enabled
    for (String tableName : CDE_BROKEN_TABLES) {
      builder.isEnabled(tableName, ctx -> {
        if (enabledSources != null && enabledSources.contains("cde_broken")) {
          return isTableEnabled(tableName, "cde", enabledSources);
        }
        LOGGER.debug("Table '{}' disabled: CDE endpoint broken (server bug or missing route)",
            tableName);
        return false;
      });
    }

    for (String tableName : BJS_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "bjs", enabledSources));
    }

    LOGGER.debug("Added isEnabled hooks for {} CDE working, {} CDE broken, {} BJS tables",
        CDE_TABLES.size(), CDE_BROKEN_TABLES.size(), BJS_TABLES.size());
  }

  private boolean isTableEnabled(String tableName, String dataSource,
      Set<String> enabledSources) {
    if (enabledSources != null && !enabledSources.contains(dataSource.toLowerCase())) {
      LOGGER.debug("Table '{}' disabled: source '{}' not in enabledSources",
          tableName, dataSource);
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

    LOGGER.info("Enabled data sources: {}", sources);
    return sources;
  }
}
