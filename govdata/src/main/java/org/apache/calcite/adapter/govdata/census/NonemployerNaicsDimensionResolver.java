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
 * Dimension resolver for the Census Nonemployer Statistics (NES) API.
 *
 * <p>The NES industry variable is renamed each time the Census Bureau adopts a new NAICS
 * vintage, so a single hard-coded variable name only retrieves a five-year slice. The
 * variable that is valid for a given data reference year is:
 * <ul>
 *   <li>reference year &le; 2011: {@code NAICS2007}</li>
 *   <li>2012&ndash;2016: {@code NAICS2012}</li>
 *   <li>2017&ndash;2021: {@code NAICS2017}</li>
 *   <li>2022 and later: {@code NAICS2022}</li>
 * </ul>
 *
 * <p>The resolver injects the {@code naics_var} companion dimension (used in the URL
 * {@code get=} list as {@code {naics_var}}), keyed on the data reference year
 * ({@code year - dataLag}). {@link org.apache.calcite.adapter.govdata.geo.CensusResponseTransformer}
 * renames the matching response column to the canonical {@code NAICS} header so the
 * materialization column expression ({@code src."NAICS"}) is vintage-independent.
 *
 * <h3>Usage in schema YAML:</h3>
 * <pre>{@code
 * dimensions:
 *   year:
 *     type: yearRange
 *     dataLag: 3
 *   naics_var:
 *     type: custom
 *     dataLag: 3        # must match the year dimension's dataLag
 *
 * source:
 *   url: "https://api.census.gov/data/{effective_year}/nonemp?get=NAME,{naics_var},NESTAB,NRCPTOT&for={geography}:*"
 *
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.census.NonemployerNaicsDimensionResolver"
 * }</pre>
 */
public class NonemployerNaicsDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NonemployerNaicsDimensionResolver.class);

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"naics_var".equals(dimensionName)) {
      return Collections.emptyList();
    }

    // The NAICS vintage keys off the data reference year, not the publish year. The
    // effective_year companion is not yet present in this context (it is injected after
    // custom-dimension resolution), so derive it here from the publish year and the same
    // dataLag the year dimension uses.
    int dataLag = config.getDataLag() != null ? config.getDataLag() : 0;
    int dataYear = Integer.parseInt(context.get("year")) - dataLag;

    String naicsVar;
    if (dataYear <= 2011) {
      naicsVar = "NAICS2007";
    } else if (dataYear <= 2016) {
      naicsVar = "NAICS2012";
    } else if (dataYear <= 2021) {
      naicsVar = "NAICS2017";
    } else {
      naicsVar = "NAICS2022";
    }
    LOGGER.debug("Nonemployer NAICS: dataYear={} -> {}", dataYear, naicsVar);
    return Collections.singletonList(naicsVar);
  }
}
