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
 * Flattens one page of openFDA drug approvals (drugsfda) results.
 *
 * <p>The drugsfda endpoint nests product info under {@code products[]} and
 * submission history under {@code submissions[]}. This transformer picks the
 * first product entry and the most-recent submission.
 */
public class FdaDrugApprovalsResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "application_number", text(record, "application_number"));
    put(row, "sponsor_name", text(record, "sponsor_name"));

    // First product entry for drug details
    JsonNode products = record.path("products");
    JsonNode product = (products.isArray() && products.size() > 0) ? products.get(0) : MAPPER.createObjectNode();
    put(row, "brand_name", text(product, "brand_name"));
    put(row, "generic_name", text(product, "active_ingredients")); // will be overridden below
    put(row, "product_type", text(record, "application_type"));
    put(row, "dosage_form", text(product, "dosage_form"));
    put(row, "route", text(product, "route"));
    put(row, "marketing_status", text(product, "marketing_status"));
    put(row, "te_code", text(product, "te_code"));

    // Generic name from first active ingredient
    JsonNode ingredients = product.path("active_ingredients");
    if (ingredients.isArray() && ingredients.size() > 0) {
      put(row, "generic_name", text(ingredients.get(0), "name"));
    }

    // Most-recent submission
    JsonNode submissions = record.path("submissions");
    JsonNode latestSubmission = MAPPER.createObjectNode();
    String latestDate = null;
    if (submissions.isArray()) {
      for (JsonNode sub : submissions) {
        String subDate = text(sub, "submission_status_date");
        if (subDate != null && (latestDate == null || subDate.compareTo(latestDate) > 0)) {
          latestDate = subDate;
          latestSubmission = sub;
        }
      }
    }
    put(row, "latest_submission_type", text(latestSubmission, "submission_type"));
    put(row, "latest_submission_status", text(latestSubmission, "submission_status"));
    put(row, "latest_submission_date", latestDate);
    put(row, "review_priority", text(latestSubmission, "review_priority"));

    row.put("type", "fda_drug_approvals");
  }
}
