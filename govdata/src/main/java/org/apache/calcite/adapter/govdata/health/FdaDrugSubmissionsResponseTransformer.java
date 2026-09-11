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
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flattens one openFDA {@code drugsfda} application record into one row per
 * {@code submissions[]} entry, unlike {@link FdaDrugApprovalsResponseTransformer} (same source,
 * one row per application, keeping only the latest submission). The ORIG submission's own
 * {@code submission_status_date} is an application's approval date when
 * {@code submission_status} is {@code AP} — openFDA does not expose a separate
 * filing/received date anywhere in this record, confirmed live (NDA020702/Lipitor's ORIG entry
 * carries only {@code submission_status_date}), so review-time (submission-to-approval duration)
 * is not computable from this source at all, only the approval date and cohort year.
 */
public class FdaDrugSubmissionsResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected List<ObjectNode> flattenRecords(JsonNode record) {
    String applicationNumber = text(record, "application_number");
    JsonNode submissions = record.path("submissions");
    List<ObjectNode> rows = new ArrayList<ObjectNode>();
    if (!submissions.isArray()) {
      return rows;
    }
    for (JsonNode sub : submissions) {
      ObjectNode row = MAPPER.createObjectNode();
      put(row, "application_number", applicationNumber);
      put(row, "submission_type", text(sub, "submission_type"));
      put(row, "submission_number", text(sub, "submission_number"));
      put(row, "submission_status", text(sub, "submission_status"));
      put(row, "submission_status_date", text(sub, "submission_status_date"));
      put(row, "submission_class_code", text(sub, "submission_class_code"));
      put(row, "submission_class_code_description", text(sub, "submission_class_code_description"));
      put(row, "review_priority", text(sub, "review_priority"));
      row.put("type", "fda_drug_submissions");
      rows.add(row);
    }
    return rows;
  }
}
