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
 * Transforms clinicaltrials.gov studies into one row per condition per study.
 */
public class ClinicalTrialConditionsResponseTransformer extends AbstractClinicalTrialsResponseTransformer {

  @Override
  protected void flattenStudy(JsonNode ps, ArrayNode out) {
    String nctId = nestedText(ps, "identificationModule.nctId");
    if (nctId == null) {
      return;
    }

    JsonNode conditionsModule = ps.path("conditionsModule");
    JsonNode conditions = conditionsModule.path("conditions");

    for (JsonNode conditionNode : conditions) {
      String conditionName = conditionNode.asText(null);
      if (conditionName != null && !conditionName.isEmpty()) {
        ObjectNode row = MAPPER.createObjectNode();
        put(row, "nct_id", nctId);
        put(row, "condition_name", conditionName);
        put(row, "type", "clinical_trial_conditions");
        out.add(row);
      }
    }
  }
}
