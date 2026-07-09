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
 * Maps FEMA {@code DisasterDeclarationsSummaries} (OpenFEMA v2) records into
 * {@code disaster_declarations} rows. {@code fipsStateCode} + {@code fipsCountyCode} form the
 * 5-digit county FIPS; {@code fipsStateCode} is the state FIPS.
 */
public class FemaDisasterDeclarationsTransformer extends AbstractOpenFemaTransformer {

  @Override protected String entityName() {
    return "DisasterDeclarationsSummaries";
  }

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putInt(row, "disaster_number", rec, "disasterNumber");
    putText(row, "fema_declaration_string", rec, "femaDeclarationString");
    putText(row, "declaration_type", rec, "declarationType");
    putText(row, "declaration_title", rec, "declarationTitle");
    putDate(row, "declaration_date", rec, "declarationDate");
    putText(row, "incident_type", rec, "incidentType");
    putDate(row, "incident_begin_date", rec, "incidentBeginDate");
    putDate(row, "incident_end_date", rec, "incidentEndDate");

    String stateFips = padStateFips(text(rec, "fipsStateCode"));
    row.put("state_fips", stateFips);
    row.put("county_fips", buildCountyFips(stateFips, text(rec, "fipsCountyCode")));

    putText(row, "designated_area", rec, "designatedArea");
    putInt(row, "region", rec, "region");
    putBool(row, "ih_program_declared", rec, "ihProgramDeclared");
    putBool(row, "ia_program_declared", rec, "iaProgramDeclared");
    putBool(row, "pa_program_declared", rec, "paProgramDeclared");
    putBool(row, "hm_program_declared", rec, "hmProgramDeclared");
    putBool(row, "tribal_request", rec, "tribalRequest");
    putText(row, "id", rec, "id");
  }
}
