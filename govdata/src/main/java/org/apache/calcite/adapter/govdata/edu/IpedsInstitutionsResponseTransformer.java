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

/**
 * Transforms Urban Institute IPEDS institution directory responses.
 *
 * <p>Handles two field-name variants for county FIPS (Urban Institute API returns
 * {@code countycd}; the field is sometimes also present as {@code county_fips}).
 * Computes {@code county_fips_str} — the zero-padded 5-character string required to
 * join against {@code geo.counties.county_fips}.
 */
public class IpedsInstitutionsResponseTransformer extends AbstractUrbanInstituteResponseTransformer {

  @Override protected void augmentRecord(ObjectNode row, RequestContext context) {
    // Urban Institute IPEDS API uses "countycd" (NCES column name); normalise to county_fips.
    if (!row.has("county_fips") && row.has("countycd")) {
      row.set("county_fips", row.get("countycd"));
    }

    // Compute county_fips_str: zero-pad county_fips integer to 5 digits for geo.counties join.
    JsonNode fipsNode = row.path("county_fips");
    if (!fipsNode.isMissingNode() && !fipsNode.isNull()) {
      try {
        int fipsInt = fipsNode.asInt();
        if (fipsInt > 0) {
          row.put("county_fips_str", String.format("%05d", fipsInt));
        } else {
          row.putNull("county_fips_str");
        }
      } catch (Exception e) {
        row.putNull("county_fips_str");
      }
    } else {
      row.putNull("county_fips_str");
    }
  }
}
