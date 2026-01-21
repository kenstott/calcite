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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dimension resolver for TIGER Voting District (VTD) data.
 *
 * <p>Filters state_fips values based on which states have state-level VTD files
 * available for each census year. Some states only provide county-level VTD files.
 *
 * <h3>Data Availability:</h3>
 * <ul>
 *   <li>2012 (2010 census): All 50 states + DC have state-level files</li>
 *   <li>2020 (2020 census): ~30 states have state-level files; others have county-level only</li>
 * </ul>
 *
 * <h3>Usage in schema YAML:</h3>
 * <pre>{@code
 * dimensions:
 *   year:
 *     - "2012"
 *     - "2020"
 *   state_fips:
 *     type: resolver
 *     class: "org.apache.calcite.adapter.govdata.geo.TigerVtdDimensionResolver"
 * }</pre>
 */
public class TigerVtdDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TigerVtdDimensionResolver.class);

  // States with state-level VTD files for 2020 census
  // Source: https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/VTD/2020/
  private static final Set<String> STATES_WITH_2020_STATE_LEVEL_VTD = new HashSet<>(
      Arrays.asList(
          "01", // Alabama
          "02", // Alaska
          "04", // Arizona
          "05", // Arkansas
          "08", // Colorado
          "12", // Florida
          "13", // Georgia
          "16", // Idaho
          "17", // Illinois
          "18", // Indiana
          "19", // Iowa
          "20", // Kansas
          "21", // Kentucky
          "22", // Louisiana
          "26", // Michigan
          "27", // Minnesota
          "28", // Mississippi
          "29", // Missouri
          "30", // Montana
          "40", // Oklahoma
          "42", // Pennsylvania
          "45", // South Carolina
          "47", // Tennessee
          "48", // Texas
          "51", // Virginia
          "53", // Washington
          "54", // West Virginia
          "55", // Wisconsin
          "56", // Wyoming
          "72"  // Puerto Rico
      ));

  // States WITHOUT 2012 state-level VTD files (only have county-level)
  // These states did not provide state-level aggregated VTD files for the 2010 census vintage
  private static final Set<String> STATES_WITHOUT_2012_STATE_LEVEL_VTD = new HashSet<>(
      Arrays.asList(
          "21", // Kentucky - only county-level files available
          "44"  // Rhode Island - only county-level files available
      ));

  // All 50 states + DC (FIPS 01-56, excluding invalid codes)
  private static final List<String> ALL_STATE_FIPS = Arrays.asList(
      "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
      "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
      "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
      "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
      "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"
  );

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {

    String year = context.get("year");

    if (!"state_fips".equals(dimensionName)) {
      LOGGER.warn("TigerVtdDimensionResolver only handles 'state_fips' dimension, got: {}",
          dimensionName);
      return Collections.emptyList();
    }

    if (year == null) {
      LOGGER.warn("TigerVtdDimensionResolver requires 'year' in context");
      return ALL_STATE_FIPS;
    }

    int yearInt;
    try {
      yearInt = Integer.parseInt(year);
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid year format: {}", year);
      return ALL_STATE_FIPS;
    }

    if (yearInt >= 2020) {
      // 2020 census: only return states with state-level VTD files
      List<String> filtered = new ArrayList<>();
      for (String state : ALL_STATE_FIPS) {
        if (STATES_WITH_2020_STATE_LEVEL_VTD.contains(state)) {
          filtered.add(state);
        }
      }
      LOGGER.info("TigerVtdDimensionResolver: year={} -> {} states with state-level VTD files",
          year, filtered.size());
      return filtered;
    } else {
      // 2012 (2010 census vintage): exclude states without state-level VTD files
      List<String> filtered = new ArrayList<>();
      for (String state : ALL_STATE_FIPS) {
        if (!STATES_WITHOUT_2012_STATE_LEVEL_VTD.contains(state)) {
          filtered.add(state);
        }
      }
      LOGGER.info("TigerVtdDimensionResolver: year={} -> {} states (excluding {} without state-level files)",
          year, filtered.size(), STATES_WITHOUT_2012_STATE_LEVEL_VTD.size());
      return filtered;
    }
  }
}
