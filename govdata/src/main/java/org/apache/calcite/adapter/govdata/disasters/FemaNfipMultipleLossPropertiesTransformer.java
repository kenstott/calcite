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
 * Maps FEMA {@code NfipMultipleLossProperties} (OpenFEMA v1) records into
 * {@code nfip_multiple_loss_properties} rows. {@code fipsCountyCode} is already the 5-digit
 * county FIPS; the state FIPS is its first two digits.
 */
public class FemaNfipMultipleLossPropertiesTransformer extends AbstractOpenFemaTransformer {

  @Override protected String entityName() {
    return "NfipMultipleLossProperties";
  }

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    String countyFips = text(rec, "fipsCountyCode");
    if (countyFips != null && countyFips.length() != 5) {
      countyFips = null;
    }
    row.put("state_fips", countyFips != null ? countyFips.substring(0, 2) : null);
    row.put("county_fips", countyFips);
    putText(row, "state_abbr", rec, "stateAbbreviation");
    putText(row, "county_name", rec, "county");
    putText(row, "zip_code", rec, "zipCode");
    putText(row, "reported_city", rec, "reportedCity");
    putText(row, "community_id_number", rec, "communityIdNumber");
    putText(row, "community_name", rec, "communityName");
    putText(row, "flood_zone", rec, "floodZone");
    putDouble(row, "latitude", rec, "latitude");
    putDouble(row, "longitude", rec, "longitude");
    putInt(row, "occupancy_type", rec, "occupancyType");
    putDate(row, "original_construction_date", rec, "originalConstructionDate");
    putDate(row, "original_nb_date", rec, "originalNBDate");
    putBool(row, "post_firm_construction_indicator", rec, "postFIRMConstructionIndicator");
    putBool(row, "primary_residence_indicator", rec, "primaryResidenceIndicator");
    putBool(row, "mitigated_indicator", rec, "mitigatedIndicator");
    putBool(row, "insured_indicator", rec, "insuredIndicator");
    putBool(row, "nfip_rl", rec, "nfipRl");
    putBool(row, "nfip_srl", rec, "nfipSrl");
    putBool(row, "fma_rl", rec, "fmaRl");
    putBool(row, "fma_srl", rec, "fmaSrl");
    putInt(row, "total_losses", rec, "totalLosses");
    putDate(row, "most_recent_date_of_loss", rec, "mostRecentDateofLoss");
    putDate(row, "as_of_date", rec, "asOfDate");
    putText(row, "id", rec, "id");
  }
}
