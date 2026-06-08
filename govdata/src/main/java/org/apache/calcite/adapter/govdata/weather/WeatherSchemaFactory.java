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
package org.apache.calcite.adapter.govdata.weather;

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
 * Sub-schema factory for U.S. weather, climate, and air quality data.
 *
 * <p>Data sources and their authentication requirements:
 * <ul>
 *   <li>NWS (weather.gov) — no auth: nws_stations, nws_alerts</li>
 *   <li>NOAA CDO v2 — requires NOAA_CDO_TOKEN: cdo_stations,
 *       cdo_monthly_summaries, cdo_annual_summaries</li>
 *   <li>EPA AQS — requires EPA_AQS_EMAIL + EPA_AQS_KEY: epa_annual_aqi</li>
 * </ul>
 *
 * <p>Tables are conditionally enabled based on environment variables.
 * NWS tables are always available. CDO and EPA tables are disabled when
 * their respective tokens are missing.
 */
public class WeatherSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(WeatherSchemaFactory.class);

  // NWS tables (no auth required)
  private static final Set<String> NWS_TABLES =
      new HashSet<>(Arrays.asList(
          "nws_stations",
          "nws_alerts"));

  // NOAA CDO tables (require NOAA_CDO_TOKEN)
  private static final Set<String> CDO_TABLES =
      new HashSet<>(Arrays.asList(
          "cdo_stations",
          "cdo_monthly_summaries",
          "cdo_annual_summaries",
          "climate_normals_monthly"));

  // EPA AQS tables (require EPA_AQS_EMAIL + EPA_AQS_KEY)
  private static final Set<String> EPA_TABLES =
      new HashSet<>(Arrays.asList(
          "epa_annual_aqi",
          "epa_daily_aqi"));

  // GHCND tables (no auth required — station inventory + bulk daily files)
  private static final Set<String> GHCND_STATION_TABLES =
      new HashSet<>(Arrays.asList(
          "ghcnd_stations_with_county",
          "ghcnd_daily"));

  // Drought Monitor tables (no auth required)
  private static final Set<String> DROUGHT_TABLES =
      new HashSet<>(Collections.singletonList(
          "drought_monitor_weekly"));

  // HMS Smoke tables (no auth; requires offline geopandas ETL for pre-processed CSV)
  private static final Set<String> HMS_TABLES =
      new HashSet<>(Collections.singletonList(
          "hms_smoke_daily"));

  @Override public String getSchemaResourceName() {
    return "/weather/weather-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder,
      Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for WEATHER schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    boolean hasCdoToken = System.getenv("NOAA_CDO_TOKEN") != null
        && !System.getenv("NOAA_CDO_TOKEN").isEmpty();
    boolean hasEpaCredentials = System.getenv("EPA_AQS_EMAIL") != null
        && !System.getenv("EPA_AQS_EMAIL").isEmpty()
        && System.getenv("EPA_AQS_KEY") != null
        && !System.getenv("EPA_AQS_KEY").isEmpty();

    if (!hasCdoToken) {
      LOGGER.info("NOAA_CDO_TOKEN not set — CDO tables (cdo_stations, "
          + "cdo_monthly_summaries, cdo_annual_summaries, "
          + "climate_normals_monthly) will be disabled");
    }
    if (!hasEpaCredentials) {
      LOGGER.info("EPA_AQS_EMAIL/EPA_AQS_KEY not set — EPA tables "
          + "(epa_annual_aqi) will be disabled");
    }

    // NWS tables: always enabled (unless filtered by enabledSources)
    for (String tableName : NWS_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "nws", enabledSources));
    }

    // CDO tables: enabled only when NOAA_CDO_TOKEN is set
    for (String tableName : CDO_TABLES) {
      builder.isEnabled(tableName, ctx ->
          hasCdoToken && isTableEnabled(tableName, "cdo", enabledSources));
    }

    // EPA tables: enabled only when both EPA_AQS_EMAIL and EPA_AQS_KEY are set
    for (String tableName : EPA_TABLES) {
      builder.isEnabled(tableName, ctx ->
          hasEpaCredentials && isTableEnabled(tableName, "epa", enabledSources));
    }

    // GHCND station inventory: always enabled (no auth required)
    for (String tableName : GHCND_STATION_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "ghcnd", enabledSources));
    }

    // Drought Monitor: always enabled (no auth required)
    for (String tableName : DROUGHT_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "drought", enabledSources));
    }

    // HMS Smoke: always enabled (no auth; data must be pre-processed via offline ETL)
    for (String tableName : HMS_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "hms", enabledSources));
    }

    LOGGER.debug("Configured hooks for WEATHER schema: {} NWS, {} CDO (token={}), "
            + "{} EPA (credentials={}), {} GHCND, {} drought, {} HMS",
        NWS_TABLES.size(), CDO_TABLES.size(), hasCdoToken,
        EPA_TABLES.size(), hasEpaCredentials,
        GHCND_STATION_TABLES.size(), DROUGHT_TABLES.size(), HMS_TABLES.size());
  }

  private boolean isTableEnabled(String tableName, String dataSource,
      Set<String> enabledSources) {
    if (enabledSources != null
        && !enabledSources.contains(dataSource.toLowerCase())) {
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

    LOGGER.info("Enabled weather data sources: {}", sources);
    return sources;
  }
}
