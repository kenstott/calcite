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
 * Transforms CMS Hospital Quality data (Socrata API).
 *
 * <p>Response structure: { "d": [ { ... }, ... ] }
 * Each element is mapped to hospital quality metrics.
 */
public class CmsHospitalQualityResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "facility_id", text(record, "facility_id"));
    put(row, "facility_name", text(record, "facility_name"));
    put(row, "address", text(record, "address"));
    put(row, "city", text(record, "citytown"));
    put(row, "state", text(record, "state"));
    put(row, "zip_code", text(record, "zip_code"));
    put(row, "county", text(record, "countyparish"));
    put(row, "hospital_type", text(record, "hospital_type"));
    put(row, "hospital_ownership", text(record, "hospital_ownership"));
    put(row, "emergency_services", text(record, "emergency_services"));
    put(row, "overall_rating", text(record, "hospital_overall_rating"));
    put(row, "mort_measures_better", text(record, "mort_measures_better"));
    put(row, "mort_measures_worse", text(record, "mort_measures_worse"));
    put(row, "safety_measures_better", text(record, "safety_measures_better"));
    put(row, "safety_measures_worse", text(record, "safety_measures_worse"));
    put(row, "readm_measures_better", text(record, "readm_measures_better"));
    put(row, "readm_measures_worse", text(record, "readm_measures_worse"));
    put(row, "patient_exp_rating", text(record, "pt_exp_group_measure_count"));
    put(row, "birthing_friendly", text(record, "meets_criteria_for_birthing_friendly_designation"));
    put(row, "type", "cms_hospital_quality");
  }
}
