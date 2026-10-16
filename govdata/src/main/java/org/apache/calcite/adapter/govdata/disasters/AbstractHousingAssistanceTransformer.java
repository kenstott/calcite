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
package org.apache.calcite.adapter.govdata.disasters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Shared mapping for the OpenFEMA housing-assistance datasets (owners and renters). Both carry
 * the disaster number, the state, the county as text and the ZIP code, and the same approved
 * assistance fields; they differ in how damage is reported.
 *
 * <p>The county FIPS comes from {@link FemaDeclaredCountyFips}: it is null for tribal areas and
 * for area names the declarations do not list, never a guess.
 */
abstract class AbstractHousingAssistanceTransformer extends AbstractOpenFemaTransformer {

  /** disaster_number, state_abbr, state_fips, county_fips, county_name, city, zip_code. */
  protected final void mapLocation(JsonNode rec, ObjectNode row) {
    putInt(row, "disaster_number", rec, "disasterNumber");
    putText(row, "state_abbr", rec, "state");
    putText(row, "county_name", rec, "county");
    putText(row, "city", rec, "city");
    putText(row, "zip_code", rec, "zipCode");
    String code;
    try {
      code = FemaDeclaredCountyFips.lookup(rec.path("disasterNumber").asInt(),
          text(rec, "state"), text(rec, "county"));
    } catch (IOException e) {
      throw new UncheckedIOException(entityName() + ": county FIPS lookup failed", e);
    }
    row.put("state_fips", code == null ? null : code.substring(0, 2));
    row.put("county_fips", FemaDeclaredCountyFips.countyFips(code));
  }

  /** The approved-assistance fields common to owners and renters. */
  protected final void mapAssistance(JsonNode rec, ObjectNode row) {
    putInt(row, "approved_for_assistance", rec, "approvedForFemaAssistance");
    putDouble(row, "total_approved_ihp_amount", rec, "totalApprovedIhpAmount");
    putDouble(row, "repair_replace_amount", rec, "repairReplaceAmount");
    putDouble(row, "rental_amount", rec, "rentalAmount");
    putDouble(row, "other_needs_amount", rec, "otherNeedsAmount");
    putInt(row, "approved_1_to_10000", rec, "approvedBetween1And10000");
    putInt(row, "approved_10001_to_25000", rec, "approvedBetween10001And25000");
    putInt(row, "approved_25001_to_max", rec, "approvedBetween25001AndMax");
    putInt(row, "total_max_grants", rec, "totalMaxGrants");
  }
}
