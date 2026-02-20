/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  // FBI CDE tables
  private static final Set<String> CDE_TABLES =
      new HashSet<>(
          Arrays.asList(
              "cde_agencies",
              "cde_offenses",
              "cde_police_employment",
              "cde_hate_crimes",
              "cde_use_of_force",
              "cde_crime_agency",
              "cde_arrests",
              "cde_shr",
              "cde_leoka",
              "cde_cargo_theft",
              "cde_human_trafficking",
              "cde_participation",
              "cde_estimates"));

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

    for (String tableName : BJS_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "bjs", enabledSources));
    }

    LOGGER.debug("Added isEnabled hooks for {} CDE, {} BJS tables",
        CDE_TABLES.size(), BJS_TABLES.size());
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
