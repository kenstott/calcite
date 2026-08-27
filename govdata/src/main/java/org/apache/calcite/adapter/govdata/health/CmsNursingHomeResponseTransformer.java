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
 * Transforms CMS Provider Data Catalog "Nursing Homes including Rehab Services — Provider
 * Information" data (dataset 4pq5-n9py, same datastore-query API family as
 * {@link CmsHospitalQualityResponseTransformer}).
 *
 * <p>Response structure: {@code { "results": [ { ... }, ... ] } } — one row per facility (CCN),
 * carrying staffing hours-per-resident-day and total health-deficiency counts together in a
 * single file, so a staffing-vs-deficiency question needs no second CMS dataset joined in.
 */
public class CmsNursingHomeResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "ccn", text(record, "cms_certification_number_ccn"));
    put(row, "provider_name", text(record, "provider_name"));
    put(row, "address", text(record, "provider_address"));
    put(row, "city", text(record, "citytown"));
    put(row, "state", text(record, "state"));
    put(row, "zip_code", text(record, "zip_code"));
    put(row, "county", text(record, "countyparish"));
    put(row, "ownership_type", text(record, "ownership_type"));
    put(row, "certified_beds", text(record, "number_of_certified_beds"));
    put(row, "avg_residents_per_day", text(record, "average_number_of_residents_per_day"));
    put(row, "overall_rating", text(record, "overall_rating"));
    put(row, "health_inspection_rating", text(record, "health_inspection_rating"));
    put(row, "qm_rating", text(record, "qm_rating"));
    put(row, "staffing_rating", text(record, "staffing_rating"));
    put(row, "nurse_aide_hours_per_resident_day",
        text(record, "reported_nurse_aide_staffing_hours_per_resident_per_day"));
    put(row, "lpn_hours_per_resident_day",
        text(record, "reported_lpn_staffing_hours_per_resident_per_day"));
    put(row, "rn_hours_per_resident_day",
        text(record, "reported_rn_staffing_hours_per_resident_per_day"));
    put(row, "total_nurse_hours_per_resident_day",
        text(record, "reported_total_nurse_staffing_hours_per_resident_per_day"));
    put(row, "nursing_staff_turnover_pct", text(record, "total_nursing_staff_turnover"));
    put(row, "rn_turnover_pct", text(record, "registered_nurse_turnover"));
    put(row, "total_health_deficiencies",
        text(record, "rating_cycle_1_total_number_of_health_deficiencies"));
    put(row, "health_deficiency_score",
        text(record, "rating_cycle_1_health_deficiency_score"));
    put(row, "number_of_fines", text(record, "number_of_fines"));
    put(row, "total_fines_dollars", text(record, "total_amount_of_fines_in_dollars"));
    put(row, "number_of_payment_denials", text(record, "number_of_payment_denials"));
    put(row, "total_number_of_penalties", text(record, "total_number_of_penalties"));
    put(row, "latitude", text(record, "latitude"));
    put(row, "longitude", text(record, "longitude"));
    put(row, "type", "cms_nursing_home");
  }
}
