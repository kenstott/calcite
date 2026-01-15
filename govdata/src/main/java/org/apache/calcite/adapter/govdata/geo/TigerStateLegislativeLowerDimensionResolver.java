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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dimension resolver for TIGER State Legislative District Lower Chamber (SLDL) data.
 *
 * <p>Filters state_fips values to exclude states/territories without lower legislative chambers:
 * <ul>
 *   <li>DC (11): Federal district, no state legislature</li>
 *   <li>Nebraska (31): Unicameral legislature (only has upper chamber/Senate)</li>
 * </ul>
 *
 * <h3>Usage in schema YAML:</h3>
 * <pre>{@code
 * dimensions:
 *   year: *tiger_year_range
 *   state_fips:
 *     type: custom
 *
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.geo.TigerStateLegislativeLowerDimensionResolver"
 * }</pre>
 */
public class TigerStateLegislativeLowerDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TigerStateLegislativeLowerDimensionResolver.class);

  // States/territories WITHOUT lower legislative chambers
  private static final Set<String> EXCLUDED_STATES = new HashSet<>(
      Arrays.asList(
          "11", // DC - federal district, no state legislature
          "31"  // Nebraska - unicameral legislature (only Senate/upper chamber)
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

    if (!"state_fips".equals(dimensionName)) {
      LOGGER.warn("TigerStateLegislativeLowerDimensionResolver only handles 'state_fips' "
          + "dimension, got: {}", dimensionName);
      return ALL_STATE_FIPS;
    }

    List<String> filtered = new ArrayList<>();
    for (String state : ALL_STATE_FIPS) {
      if (!EXCLUDED_STATES.contains(state)) {
        filtered.add(state);
      }
    }

    LOGGER.info("TigerStateLegislativeLowerDimensionResolver: {} states "
        + "(excluding {} without lower chambers)", filtered.size(), EXCLUDED_STATES.size());
    return filtered;
  }
}
