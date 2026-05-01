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
 * Flattens one page of openFDA device enforcement (recall) results.
 *
 * <p>All fields are top-level in the {@code results[]} array.
 */
public class FdaDeviceRecallsResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "cfres_id", text(record, "cfres_id"));
    put(row, "product_res_number", text(record, "product_res_number"));
    put(row, "recall_status", text(record, "recall_status"));
    put(row, "product_code", text(record, "product_code"));
    put(row, "k_numbers", joinArray(record, "k_numbers"));
    put(row, "recalling_firm", text(record, "recalling_firm"));
    put(row, "city", text(record, "city"));
    put(row, "state", text(record, "state"));
    put(row, "product_description", text(record, "product_description"));
    put(row, "reason_for_recall", text(record, "reason_for_recall"));
    put(row, "root_cause_description", text(record, "root_cause_description"));
    put(row, "action", text(record, "action"));
    put(row, "product_quantity", text(record, "product_quantity"));
    put(row, "distribution_pattern", text(record, "distribution_pattern"));
    put(row, "event_date_initiated", text(record, "event_date_initiated"));
    put(row, "event_date_terminated", text(record, "event_date_terminated"));
    row.put("type", "fda_device_recalls");
  }
}
