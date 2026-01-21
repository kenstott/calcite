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
 * Dimension resolver for Economic Census API.
 *
 * <p>Handles the NAICS version change between census years:
 * <ul>
 *   <li>2017: Uses NAICS2017 variable</li>
 *   <li>2022: Uses NAICS2022 variable</li>
 * </ul>
 *
 * <p>The resolver provides the correct NAICS variable name based on year:
 *
 * <h3>Usage in schema YAML:</h3>
 * <pre>{@code
 * dimensions:
 *   year: ['2017', '2022']
 *   naics_var:
 *     type: resolver
 *     class: "org.apache.calcite.adapter.govdata.census.CensusEconomicDimensionResolver"
 *     dimension: naics_var
 *
 * source:
 *   url: "https://api.census.gov/data/{year}/ecnbasic?get=NAME,{naics_var},ESTAB,EMP,PAYANN,RCPTOT&for={geography}:*"
 * }</pre>
 */
public class CensusEconomicDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CensusEconomicDimensionResolver.class);

  // NAICS variable name by census year
  private static final Map<String, String> NAICS_VAR_BY_YEAR = new HashMap<>();

  static {
    NAICS_VAR_BY_YEAR.put("2017", "NAICS2017");
    NAICS_VAR_BY_YEAR.put("2022", "NAICS2022");
    // Future years would use their respective NAICS versions
    // NAICS_VAR_BY_YEAR.put("2027", "NAICS2027");
  }

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {

    String year = context.getOrDefault("year", "2022");

    if ("naics_var".equals(dimensionName)) {
      // Return NAICS variable based on year
      String naicsVar = NAICS_VAR_BY_YEAR.getOrDefault(year, "NAICS2022");
      LOGGER.debug("Economic Census: year={} -> naics_var={}", year, naicsVar);
      return Collections.singletonList(naicsVar);
    }

    return Collections.emptyList();
  }
}
