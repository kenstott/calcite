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
 * Maps FEMA {@code HazardMitigationAssistanceProjects} (OpenFEMA v4) records into
 * {@code hazard_mitigation_projects} rows. {@code stateNumberCode} is the 2-digit state FIPS and
 * {@code countyCode} the 3-digit county part.
 */
public class FemaHazardMitigationTransformer extends AbstractOpenFemaTransformer {

  @Override protected String entityName() {
    return "HazardMitigationAssistanceProjects";
  }

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putText(row, "project_identifier", rec, "projectIdentifier");
    putText(row, "program_area", rec, "programArea");
    putInt(row, "disaster_number", rec, "disasterNumber");
    putInt(row, "region", rec, "region");

    String stateFips = padStateFips(text(rec, "stateNumberCode"));
    row.put("state_fips", stateFips);
    row.put("county_fips", buildCountyFips(stateFips, text(rec, "countyCode")));
    putText(row, "state_name", rec, "state");
    putText(row, "county_name", rec, "county");

    putText(row, "project_type", rec, "projectType");
    putText(row, "status", rec, "status");
    putText(row, "recipient", rec, "recipient");
    putText(row, "subrecipient", rec, "subrecipient");
    putDate(row, "date_approved", rec, "dateApproved");
    putDate(row, "date_closed", rec, "dateClosed");
    putDouble(row, "project_amount", rec, "projectAmount");
    putDouble(row, "federal_share_obligated", rec, "federalShareObligated");
    putDouble(row, "cost_share_percentage", rec, "costSharePercentage");
  }
}
