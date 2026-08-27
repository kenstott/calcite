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
 * Flattens one page of openFDA drug shortage results.
 *
 * <p>Shortage-episode fields ({@code package_ndc}, {@code initial_posting_date}, {@code status},
 * ...) are top-level; product-identity fields ({@code brand_name}, {@code manufacturer_name},
 * {@code product_ndc}, ...) sit one level down under {@code openfda}, each as a single-element
 * array (openFDA's harmonization join to other datasets) — pulled via
 * {@link AbstractOpenFdaResponseTransformer#nestedFirstText}.
 */
public class FdaDrugShortagesResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "package_ndc", text(record, "package_ndc"));
    put(row, "initial_posting_date", text(record, "initial_posting_date"));
    put(row, "generic_name", text(record, "generic_name"));
    put(row, "update_type", text(record, "update_type"));
    put(row, "update_date", text(record, "update_date"));
    put(row, "status", text(record, "status"));
    put(row, "availability", text(record, "availability"));
    put(row, "discontinued_date", text(record, "discontinued_date"));
    put(row, "dosage_form", text(record, "dosage_form"));
    put(row, "presentation", text(record, "presentation"));
    // Source field is an array (a shortage can carry more than one category, e.g.
    // ["Anesthesia", "Pediatric"]) — join rather than text(), which would silently null it out.
    put(row, "therapeutic_category", joinArray(record, "therapeutic_category"));
    put(row, "company_name", text(record, "company_name"));
    put(row, "contact_info", text(record, "contact_info"));
    put(row, "related_info", text(record, "related_info"));
    put(row, "brand_name", nestedFirstText(record, "openfda", "brand_name"));
    put(row, "manufacturer_name", nestedFirstText(record, "openfda", "manufacturer_name"));
    put(row, "product_ndc", nestedFirstText(record, "openfda", "product_ndc"));
    put(row, "application_number", nestedFirstText(record, "openfda", "application_number"));
    put(row, "route", nestedFirstText(record, "openfda", "route"));
    put(row, "substance_name", nestedFirstText(record, "openfda", "substance_name"));
    row.put("type", "fda_drug_shortages");
  }
}
