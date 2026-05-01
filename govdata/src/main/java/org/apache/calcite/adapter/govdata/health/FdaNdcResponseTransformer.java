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
 * Flattens one page of openFDA NDC product results.
 *
 * <p>Extracts nested {@code route[0]}, {@code openfda.rxcui[0]}, and
 * {@code finished} from the API response.
 */
public class FdaNdcResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "product_ndc", text(record, "product_ndc"));
    put(row, "generic_name", text(record, "generic_name"));
    put(row, "brand_name", text(record, "brand_name"));
    put(row, "brand_name_base", text(record, "brand_name_base"));
    put(row, "labeler_name", text(record, "labeler_name"));
    put(row, "dosage_form", text(record, "dosage_form"));
    put(row, "route", firstText(record, "route"));
    put(row, "product_type", text(record, "product_type"));
    put(row, "marketing_category", text(record, "marketing_category"));
    put(row, "marketing_start_date", text(record, "marketing_start_date"));

    put(row, "application_number", text(record, "application_number"));
    put(row, "rxcui", nestedFirstText(record, "openfda", "rxcui"));

    JsonNode finishedNode = record.path("finished");
    if (!finishedNode.isMissingNode()) {
      row.put("finished", finishedNode.asBoolean(false));
    } else {
      row.putNull("finished");
    }

    row.put("type", "fda_ndc_products");
  }
}
