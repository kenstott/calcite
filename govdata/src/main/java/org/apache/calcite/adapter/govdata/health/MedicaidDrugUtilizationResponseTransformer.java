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

/**
 * Transforms Medicaid Drug Utilization data (Socrata API).
 *
 * <p>Response structure: { "d": [ { ... }, ... ] }
 * Each element represents one drug utilization record.
 */
public class MedicaidDrugUtilizationResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "state", text(record, "state"));
    put(row, "ndc", text(record, "ndc"));
    put(row, "year", text(record, "year"));
    put(row, "quarter", text(record, "quarter"));
    put(row, "utilization_type", text(record, "utilization_type"));
    put(row, "product_name", text(record, "product_name"));
    put(row, "labeler_code", text(record, "labeler_code"));
    put(row, "units_reimbursed", text(record, "units_reimbursed"));
    put(row, "number_of_prescriptions", text(record, "number_of_prescriptions"));
    put(row, "total_amount_reimbursed", text(record, "total_amount_reimbursed"));
    put(row, "medicaid_amount_reimbursed", text(record, "medicaid_amount_reimbursed"));
    put(row, "non_medicaid_amount_reimbursed", text(record, "non_medicaid_amount_reimbursed"));
    put(row, "suppression_used", text(record, "suppression_used"));
    put(row, "type", "medicaid_drug_utilization");
  }
}
