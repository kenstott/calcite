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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms Urban Institute CCD school directory responses.
 *
 * <p>The API field names match the schema column names directly.
 */
public class CcdSchoolsResponseTransformer extends AbstractUrbanInstituteResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CcdSchoolsResponseTransformer.class);

  @Override protected void augmentRecord(ObjectNode row, RequestContext context) {
    // DQ-005: county_code arrives as a 4-digit integer (e.g. 6037 for CA/LA).
    // Pad to 5-digit string so views can join directly to geo.counties.county_fips.
    JsonNode countyNode = row.path("county_code");
    if (!countyNode.isMissingNode() && !countyNode.isNull()) {
      try {
        int code = countyNode.asInt();
        if (code > 0) {
          row.put("county_code", String.format("%05d", code));
        }
      } catch (Exception e) {
        LOGGER.warn("CCD Schools: could not pad county_code '{}' for ncessch={}",
            countyNode.asText(), row.path("ncessch").asText("?"));
      }
    }
  }
}
