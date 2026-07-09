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
 * Maps FEMA {@code PublicAssistanceFundedProjectsDetails} (OpenFEMA v2) records into
 * {@code public_assistance_projects} rows. {@code stateNumberCode} is the 2-digit state FIPS and
 * {@code countyCode} the 3-digit county part.
 */
public class FemaPublicAssistanceTransformer extends AbstractOpenFemaTransformer {

  @Override protected String entityName() {
    return "PublicAssistanceFundedProjectsDetails";
  }

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putInt(row, "disaster_number", rec, "disasterNumber");
    putInt(row, "pw_number", rec, "pwNumber");
    putText(row, "application_title", rec, "applicationTitle");
    putText(row, "applicant_id", rec, "applicantId");
    putDate(row, "declaration_date", rec, "declarationDate");
    putText(row, "incident_type", rec, "incidentType");
    putText(row, "damage_category_code", rec, "damageCategoryCode");
    putText(row, "damage_category", rec, "damageCategoryDescrip");
    putText(row, "project_status", rec, "projectStatus");
    putText(row, "project_size", rec, "projectSize");

    String stateFips = padStateFips(text(rec, "stateNumberCode"));
    row.put("state_fips", stateFips);
    row.put("county_fips", buildCountyFips(stateFips, text(rec, "countyCode")));
    putText(row, "county_name", rec, "county");
    putText(row, "state_abbr", rec, "stateAbbreviation");

    putDouble(row, "project_amount", rec, "projectAmount");
    putDouble(row, "federal_share_obligated", rec, "federalShareObligated");
    putDouble(row, "total_obligated", rec, "totalObligated");
    putText(row, "hash", rec, "hash");
  }
}
