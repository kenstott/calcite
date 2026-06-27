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
 * <p>Handles the API structure change at the 2020 data-year boundary. All decisions key on the
 * data reference year ({@code publish year - dataLag}), not the publish year:
 * <ul>
 *   <li>data year &le; 2019: /pep/population, vintage = the data year, POP+DENSITY, no YEAR</li>
 *   <li>data year &ge; 2020: /pep/charv in the latest vintage, POP, selected via &amp;YEAR=&lt;data year&gt;</li>
 * </ul>
 *
 * <p>The resolver injects these companion dimensions, used in the URL template:
 * <ul>
 *   <li>endpoint: "pep/population" or "pep/charv"</li>
 *   <li>variables: "POP,DENSITY" or "POP"</li>
 *   <li>vintage: the data year (old API) or the latest published vintage (new API)</li>
 *   <li>year_filter: Empty (old API) or "&amp;YEAR=&lt;data year&gt;" (new API)</li>
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

  // PEP restructured at Vintage 2020: data years 2020+ live in the pep/charv dataset, selected
  // by a YEAR predicate; 2019 and earlier are standalone pep/population vintages.
  private static final int NEW_API_CUTOFF = 2020;

  // Latest published PEP vintage. pep/charv serves every year it covers (2020..this) via the
  // YEAR predicate, so this is a fixed value — bump it when the Census Bureau releases a new
  // vintage (and revisit the year-range floor / dataLag in census-schema.yaml's pep block).
  private static final String CURRENT_VINTAGE = "2023";

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {

    // PEP is keyed on the data reference year, not the publish year. effective_year is not yet
    // in this context (it is injected after custom-dimension resolution), so derive it from the
    // publish year and the dataLag carried on this custom dimension (must match the year
    // dimension's dataLag in the schema).
    int dataLag = config.getDataLag() != null ? config.getDataLag() : 0;
    int dataYear = Integer.parseInt(context.get("year")) - dataLag;
    boolean isNewApi = dataYear >= NEW_API_CUTOFF;

    switch (dimensionName) {
    case "endpoint":
      return Collections.singletonList(isNewApi ? "pep/charv" : "pep/population");

    case "variables":
      return Collections.singletonList(isNewApi ? "POP" : "POP,DENSITY");

    case "vintage":
      // New API: every covered year lives in the single latest vintage, selected via YEAR.
      // Old API: each data year is its own vintage.
      LOGGER.debug("PEP: dataYear={} -> vintage={}", dataYear,
          isNewApi ? CURRENT_VINTAGE : dataYear);
      return Collections.singletonList(isNewApi ? CURRENT_VINTAGE : String.valueOf(dataYear));

    case "year_filter":
      // New API: select the data year within the vintage. Old API: no YEAR predicate.
      return Collections.singletonList(isNewApi ? "&YEAR=" + dataYear : "");

    default:
      return Collections.emptyList();
    }
  }
}
