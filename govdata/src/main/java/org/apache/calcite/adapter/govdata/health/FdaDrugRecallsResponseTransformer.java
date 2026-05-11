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
 * Flattens one page of openFDA drug enforcement (recall) results.
 *
 * <p>All fields are top-level in the {@code results[]} array.
 */
public class FdaDrugRecallsResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "recall_number", text(record, "recall_number"));
    put(row, "event_id", text(record, "event_id"));
    put(row, "status", text(record, "status"));
    put(row, "classification", text(record, "classification"));
    put(row, "voluntary_mandated", text(record, "voluntary_mandated"));
    put(row, "recalling_firm", text(record, "recalling_firm"));
    put(row, "city", text(record, "city"));
    put(row, "state", text(record, "state"));
    put(row, "country", text(record, "country"));
    put(row, "product_description", text(record, "product_description"));
    put(row, "reason_for_recall", text(record, "reason_for_recall"));
    put(row, "product_quantity", text(record, "product_quantity"));
    put(row, "distribution_pattern", text(record, "distribution_pattern"));
    put(row, "code_info", text(record, "code_info"));
    put(row, "recall_initiation_date", text(record, "recall_initiation_date"));
    put(row, "report_date", text(record, "report_date"));
    put(row, "termination_date", text(record, "termination_date"));
    row.put("type", "fda_drug_recalls");
  }
}
