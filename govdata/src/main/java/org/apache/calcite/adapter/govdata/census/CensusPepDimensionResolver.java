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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Dimension resolver for Population Estimates Program (PEP) API.
 *
 * <p>Handles the API structure change between 2019 and 2020+ vintages:
 * <ul>
 *   <li>2019 and earlier: /pep/population with POP, DENSITY variables</li>
 *   <li>2020+: /pep/charv with POP variable and YEAR parameter</li>
 * </ul>
 *
 * <p>The resolver provides different values based on the year in context:
 * <ul>
 *   <li>endpoint: "pep/population" or "pep/charv"</li>
 *   <li>variables: "POP,DENSITY" or "POP"</li>
 *   <li>vintage: The API vintage year (2019 for historical, 2023 for current)</li>
 *   <li>year_filter: Empty for 2019, "&amp;YEAR={year}" for 2020+</li>
 * </ul>
 *
 * <h3>Usage in schema YAML:</h3>
 * <pre>{@code
 * dimensions:
 *   year: ['2019', '2020', '2021', '2022', '2023']
 *   endpoint:
 *     type: resolver
 *     class: "org.apache.calcite.adapter.govdata.census.CensusPepDimensionResolver"
 *     dimension: endpoint
 *   variables:
 *     type: resolver
 *     class: "org.apache.calcite.adapter.govdata.census.CensusPepDimensionResolver"
 *     dimension: variables
 *
 * source:
 *   url: "https://api.census.gov/data/{vintage}/{endpoint}?get=NAME,{variables}&for={geography}:*{year_filter}"
 * }</pre>
 */
public class CensusPepDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CensusPepDimensionResolver.class);

  // Cutoff year - 2019 uses old API, 2020+ uses new API
  private static final int NEW_API_CUTOFF = 2020;

  // Current vintage for new API (should be updated as new vintages are released)
  private static final String CURRENT_VINTAGE = "2023";

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {

    String yearStr = context.getOrDefault("year", "2023");
    int year;
    try {
      year = Integer.parseInt(yearStr);
    } catch (NumberFormatException e) {
      year = 2023;
    }

    boolean isNewApi = year >= NEW_API_CUTOFF;

    switch (dimensionName) {
    case "endpoint":
      String endpoint = isNewApi ? "pep/charv" : "pep/population";
      LOGGER.debug("PEP: year={} -> endpoint={}", year, endpoint);
      return Collections.singletonList(endpoint);

    case "variables":
      String vars = isNewApi ? "POP" : "POP,DENSITY";
      LOGGER.debug("PEP: year={} -> variables={}", year, vars);
      return Collections.singletonList(vars);

    case "vintage":
      // For old API, vintage = year; for new API, use current vintage
      String vintage = isNewApi ? CURRENT_VINTAGE : yearStr;
      LOGGER.debug("PEP: year={} -> vintage={}", year, vintage);
      return Collections.singletonList(vintage);

    case "year_filter":
      // For new API, add YEAR filter parameter; for old API, empty
      String filter = isNewApi ? "&YEAR=" + yearStr : "";
      LOGGER.debug("PEP: year={} -> year_filter={}", year, filter);
      return Collections.singletonList(filter);

    default:
      return Collections.emptyList();
    }
  }
}
