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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pass-through transformer for pre-processed HMS Smoke county-level data.
 *
 * <p>NOAA HMS Smoke data (https://www.ospo.noaa.gov/Products/land/hms.html) is
 * distributed as shapefiles containing polygon smoke plume boundaries. Converting
 * those polygons to county-level coverage percentages requires an offline spatial
 * join using geopandas or similar GIS tooling. This transformer therefore expects
 * input that has already been preprocessed into a flat JSON array.
 *
 * <p>Expected pre-processed format (one object per county per day):
 * <pre>{@code
 * [
 *   {
 *     "county_fips": "17031",
 *     "state_fips": "17",
 *     "date": "2023-08-15",
 *     "year": 2023,
 *     "smoke_coverage_pct": 45.2,
 *     "heavy_smoke_pct": 10.1,
 *     "medium_smoke_pct": 15.3,
 *     "light_smoke_pct": 19.8
 *   }
 * ]
 * }</pre>
 *
 * <p>If the response is raw binary (e.g. a shapefile ZIP), this transformer logs
 * a diagnostic message and returns an empty array rather than silently discarding data.
 */
public class HmsSmokeDailyTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(HmsSmokeDailyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("HMS Smoke: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isArray()) {
        LOGGER.warn("HMS Smoke: Response is not pre-processed JSON. HMS smoke data requires "
            + "offline geopandas spatial join. See weather-enhanced.md for ETL instructions. "
            + "Returning empty result.");
        return "[]";
      }

      ArrayNode array = (ArrayNode) root;

      for (JsonNode item : array) {
        if (!item.has("county_fips") || !item.has("date")) {
          LOGGER.warn("HMS Smoke: Pre-processed record missing required fields "
              + "(county_fips, date) for {}", context.getUrl());
          return "[]";
        }
        break;
      }

      LOGGER.debug("HMS Smoke: Pass-through {} pre-processed records for {}",
          array.size(), context.getUrl());
      return response;

    } catch (Exception e) {
      LOGGER.warn("HMS Smoke: Response is not pre-processed JSON. HMS smoke data requires "
          + "offline geopandas spatial join. See weather-enhanced.md for ETL instructions. "
          + "Returning empty result.");
      return "[]";
    }
  }
}
