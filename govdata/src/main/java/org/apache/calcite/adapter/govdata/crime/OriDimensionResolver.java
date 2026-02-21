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

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves ORI (Originating Agency Identifier) dimension values per state.
 *
 * <p>This resolver reads from the already-materialized {@code cde_agencies} Iceberg
 * table and returns the list of ORIs for the current {@code state_abbr} context
 * dimension. This enables the {@code cde_crime_agency} table to iterate over
 * agencies per state without hardcoding 19,581 ORI values.
 *
 * <h3>Context-Aware Resolution</h3>
 * <p>The resolver uses {@code state_abbr} from the context map to filter agencies.
 * The {@code state_abbr} dimension must be defined before {@code ori} in the
 * table's dimension list.
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.crime.OriDimensionResolver"
 *
 * dimensions:
 *   state_abbr:
 *     - 'CA'
 *     - 'NY'
 *   ori:
 *     type: custom
 *     properties:
 *       agenciesTablePath: "${GOVDATA_PARQUET_DIR}/source=crime/CRIME/cde_agencies"
 * }</pre>
 */
public class OriDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(OriDimensionResolver.class);

  /**
   * Cache of ORI lists per state abbreviation.
   * Populated lazily when first queried for a given state.
   */
  private final Map<String, List<String>> oriCache = new HashMap<String, List<String>>();

  /**
   * Default constructor required for reflection-based instantiation.
   */
  public OriDimensionResolver() {
  }

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"ori".equals(dimensionName)) {
      LOGGER.debug("OriDimensionResolver: Dimension '{}' not handled, returning empty",
          dimensionName);
      return Collections.emptyList();
    }

    String stateAbbr = context.get("state_abbr");
    if (stateAbbr == null || stateAbbr.isEmpty()) {
      throw new IllegalStateException(
          "OriDimensionResolver: No state_abbr in context for ORI resolution. "
          + "Ensure state_abbr dimension is defined before ori.");
    }

    // Check cache first
    List<String> cached = oriCache.get(stateAbbr);
    if (cached != null) {
      LOGGER.debug("OriDimensionResolver: Returning {} cached ORIs for state={}",
          cached.size(), stateAbbr);
      return cached;
    }

    // Load from Iceberg table
    List<String> oris = loadOrisForState(stateAbbr, config, storageProvider);
    oriCache.put(stateAbbr, oris);

    LOGGER.debug("OriDimensionResolver: Resolved {} ORIs for state={}",
        oris.size(), stateAbbr);
    return oris;
  }

  private List<String> loadOrisForState(String stateAbbr, DimensionConfig config,
      StorageProvider storageProvider) {
    String agenciesPath =
        StorageProvider.normalizePath(config.getProperty("agenciesTablePath"));

    if (agenciesPath == null || agenciesPath.isEmpty()) {
      LOGGER.warn("OriDimensionResolver: No agenciesTablePath configured, returning empty");
      return Collections.emptyList();
    }

    try (Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider);
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT DISTINCT ori "
          + "FROM read_parquet('" + agenciesPath + "/data/**/*.parquet', "
          + "hive_partitioning=true, union_by_name=true) "
          + "WHERE state_abbr = '" + stateAbbr.replace("'", "''") + "' "
          + "AND ori IS NOT NULL "
          + "ORDER BY ori";

      LOGGER.debug("OriDimensionResolver: Querying ORIs for state={}: {}", stateAbbr, sql);
      ResultSet rs = stmt.executeQuery(sql);

      List<String> oris = new ArrayList<String>();
      while (rs.next()) {
        String ori = rs.getString(1);
        if (ori != null && !ori.isEmpty()) {
          oris.add(ori);
        }
      }

      LOGGER.info("OriDimensionResolver: Loaded {} ORIs for state={}", oris.size(), stateAbbr);
      return oris;

    } catch (Exception e) {
      LOGGER.warn("OriDimensionResolver: Failed to load ORIs for state={}: {}. "
              + "Ensure cde_agencies has been materialized first.",
          stateAbbr, e.getMessage());
      return Collections.emptyList();
    }
  }
}
