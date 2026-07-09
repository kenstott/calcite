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

/**
 * Maps FEMA {@code FimaNfipClaims} (OpenFEMA v2) records into {@code nfip_claims} rows.
 * {@code countyCode} is the 5-digit county FIPS; the state FIPS is its first two digits.
 */
public class FemaNfipClaimsTransformer extends AbstractOpenFemaTransformer {

  @Override protected String entityName() {
    return "FimaNfipClaims";
  }

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    String countyFips = text(rec, "countyCode");
    if (countyFips != null && countyFips.length() != 5) {
      countyFips = null;
    }
    row.put("state_fips", countyFips != null ? countyFips.substring(0, 2) : null);
    row.put("county_fips", countyFips);
    putText(row, "state_abbr", rec, "state");

    putDate(row, "date_of_loss", rec, "dateOfLoss");
    putText(row, "flood_zone", rec, "ratedFloodZone");
    putText(row, "cause_of_damage", rec, "causeOfDamage");
    putInt(row, "occupancy_type", rec, "occupancyType");
    putInt(row, "policy_count", rec, "policyCount");
    putDouble(row, "amount_paid_building", rec, "amountPaidOnBuildingClaim");
    putDouble(row, "amount_paid_contents", rec, "amountPaidOnContentsClaim");
    putDouble(row, "amount_paid_icc", rec, "amountPaidOnIncreasedCostOfComplianceClaim");
    putDouble(row, "total_building_coverage", rec, "totalBuildingInsuranceCoverage");
    putDouble(row, "total_contents_coverage", rec, "totalContentsInsuranceCoverage");
    putText(row, "census_tract", rec, "censusTract");
    putText(row, "reported_zip_code", rec, "reportedZipCode");
    putDouble(row, "latitude", rec, "latitude");
    putDouble(row, "longitude", rec, "longitude");
    putText(row, "id", rec, "id");
  }
}
