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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.geo.CensusResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response transformer for {@code acs_congressional_district}: the standard Census 2-D array to
 * object conversion, then removal of the placeholder rows the API returns for states whose
 * districts are not defined for the vintage.
 *
 * <p>For a state whose lines were being redrawn when the vintage was tabulated (Connecticut,
 * Illinois, Michigan in the 2018 vintage; Connecticut, Illinois and New Hampshire for 2022 and
 * 2023) the API returns one row named "Congressional Districts not defined" with district code
 * {@code ZZ} and a population of 0 or NULL. It is not a district and has no estimates, so it is
 * dropped here rather than stored as a zero-population district.
 */
public class AcsCongressionalDistrictTransformer extends CensusResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AcsCongressionalDistrictTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  static final String DISTRICT_FIELD = "congressional district";
  static final String NOT_DEFINED = "ZZ";

  @Override public String transform(String response, RequestContext context) {
    String objects = super.transform(response, context);
    try {
      JsonNode root = MAPPER.readTree(objects);
      if (!root.isArray()) {
        throw new IllegalStateException("acs_congressional_district: expected an array from "
            + context.getUrl() + " but got " + objects.substring(0, Math.min(200, objects.length())));
      }
      ArrayNode kept = MAPPER.createArrayNode();
      int dropped = 0;
      for (JsonNode row : root) {
        JsonNode district = row.get(DISTRICT_FIELD);
        if (district == null || district.isNull()) {
          throw new IllegalStateException("acs_congressional_district: row without '"
              + DISTRICT_FIELD + "' from " + context.getUrl() + ": " + row);
        }
        if (NOT_DEFINED.equals(district.asText())) {
          dropped++;
        } else {
          kept.add(row);
        }
      }
      LOGGER.info("acs_congressional_district: kept {} districts, dropped {} 'not defined' rows from {}",
          Integer.valueOf(kept.size()), Integer.valueOf(dropped), context.getUrl());
      return MAPPER.writeValueAsString(kept);
    } catch (java.io.IOException e) {
      throw new IllegalStateException("acs_congressional_district: unparsable response from "
          + context.getUrl(), e);
    }
  }
}
