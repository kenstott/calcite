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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dimension resolver for Decennial Census API.
 *
 * <p>Provides year-specific dataset and variable names for the Census API.
 * This enables a single table definition to query across census years that
 * have different API structures:
 *
 * <ul>
 *   <li>2000/2010: /dec/sf1 with variables P001001, P003002, P003003, P003005</li>
 *   <li>2020: /dec/pl with variables P1_001N, P1_003N, P1_004N, P1_006N</li>
 * </ul>
 *
 * <p>The VariableNormalizer then maps these year-specific variable names to
 * canonical column names (total_population, white_alone, etc.).
 *
 * <h3>Usage in schema YAML:</h3>
 * <pre>{@code
 * dimensions:
 *   year: ['2000', '2010', '2020']
 *   dataset:
 *     type: resolver
 *     class: "org.apache.calcite.adapter.govdata.census.CensusDecennialDimensionResolver"
 *     dimension: dataset
 *   variables:
 *     type: resolver
 *     class: "org.apache.calcite.adapter.govdata.census.CensusDecennialDimensionResolver"
 *     dimension: population_variables
 *
 * source:
 *   url: "https://api.census.gov/data/{year}/dec/{dataset}?get=NAME,{variables}&for={geography}:*"
 * }</pre>
 */
public class CensusDecennialDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CensusDecennialDimensionResolver.class);

  // Dataset mappings by year
  private static final Map<String, String> DATASET_BY_YEAR = new HashMap<>();

  // Variable mappings for population table by year
  private static final Map<String, String> POPULATION_VARS_BY_YEAR = new HashMap<>();

  // Variable mappings for housing table by year
  private static final Map<String, String> HOUSING_VARS_BY_YEAR = new HashMap<>();

  static {
    // Dataset varies by census year
    DATASET_BY_YEAR.put("2000", "sf1");
    DATASET_BY_YEAR.put("2010", "sf1");
    DATASET_BY_YEAR.put("2020", "pl");

    // Population variables for 2000/2010 (Summary File 1)
    // P001001=total, P003002=white, P003003=black, P003005=asian
    POPULATION_VARS_BY_YEAR.put("2000", "P001001,P003002,P003003,P003005");
    POPULATION_VARS_BY_YEAR.put("2010", "P001001,P003002,P003003,P003005");

    // Population variables for 2020 (Redistricting PL 94-171)
    // P1_001N=total, P1_003N=white, P1_004N=black, P1_006N=asian
    POPULATION_VARS_BY_YEAR.put("2020", "P1_001N,P1_003N,P1_004N,P1_006N");

    // Housing variables for 2000/2010
    // H001001=total, H003001=occupied, H003003=vacant
    HOUSING_VARS_BY_YEAR.put("2000", "H001001,H003001,H003003");
    HOUSING_VARS_BY_YEAR.put("2010", "H001001,H003001,H003003");

    // Housing variables for 2020
    // H1_001N=total, H1_002N=occupied, H1_003N=vacant
    HOUSING_VARS_BY_YEAR.put("2020", "H1_001N,H1_002N,H1_003N");
  }

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {

    String year = context.getOrDefault("year", "2020");

    if ("dataset".equals(dimensionName)) {
      // Return dataset based on year from context
      String dataset = DATASET_BY_YEAR.getOrDefault(year, "pl");
      LOGGER.debug("Census Decennial: year={} -> dataset={}", year, dataset);
      return Collections.singletonList(dataset);

    } else if ("population_variables".equals(dimensionName)
        || "variables".equals(dimensionName)) {
      // Return population variables based on year
      String vars =
          POPULATION_VARS_BY_YEAR.getOrDefault(year, POPULATION_VARS_BY_YEAR.get("2020"));
      LOGGER.debug("Census Decennial: year={} -> variables={}", year, vars);
      return Collections.singletonList(vars);

    } else if ("housing_variables".equals(dimensionName)) {
      // Return housing variables based on year
      String vars =
          HOUSING_VARS_BY_YEAR.getOrDefault(year, HOUSING_VARS_BY_YEAR.get("2020"));
      LOGGER.debug("Census Decennial: year={} -> housing_variables={}", year, vars);
      return Collections.singletonList(vars);
    }

    return Collections.emptyList();
  }
}
