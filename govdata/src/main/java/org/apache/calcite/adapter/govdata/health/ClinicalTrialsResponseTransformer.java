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
package org.apache.calcite.adapter.govdata.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms clinicaltrials.gov studies into one row per study.
 */
public class ClinicalTrialsResponseTransformer extends AbstractClinicalTrialsResponseTransformer {

  @Override
  protected void flattenStudy(JsonNode ps, ArrayNode out) {
    ObjectNode row = MAPPER.createObjectNode();

    // identificationModule
    JsonNode idModule = ps.path("identificationModule");
    put(row, "nct_id", text(idModule, "nctId"));
    put(row, "brief_title", text(idModule, "briefTitle"));

    // statusModule
    JsonNode statusModule = ps.path("statusModule");
    put(row, "overall_status", text(statusModule, "overallStatus"));
    put(row, "start_date", nestedText(statusModule, "startDateStruct.date"));
    put(row, "primary_completion_date", nestedText(statusModule, "primaryCompletionDateStruct.date"));
    put(row, "completion_date", nestedText(statusModule, "completionDateStruct.date"));
    put(row, "first_submit_date", text(statusModule, "studyFirstSubmitDate"));
    put(row, "last_update_date", text(statusModule, "lastUpdateSubmitDate"));

    // designModule
    JsonNode designModule = ps.path("designModule");
    put(row, "study_type", text(designModule, "studyType"));
    put(row, "phase", firstText(designModule, "phases"));
    put(row, "enrollment_count", asInt(designModule, "enrollmentInfo.count"));

    // sponsorCollaboratorsModule
    JsonNode sponsorModule = ps.path("sponsorCollaboratorsModule");
    JsonNode leadSponsor = sponsorModule.path("leadSponsor");
    put(row, "lead_sponsor", text(leadSponsor, "name"));
    put(row, "funder_type", text(leadSponsor, "class"));

    // descriptionModule
    JsonNode descModule = ps.path("descriptionModule");
    String briefSummary = text(descModule, "briefSummary");
    put(row, "brief_summary", truncate(briefSummary, 2000));

    put(row, "type", "clinical_trials");
    out.add(row);
  }
}
