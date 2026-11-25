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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms Urban Institute IPEDS academic-year-tuition responses.
 *
 * <p>One row per (unitid, year, level_of_study, tuition_type).
 * level_of_study: 1=undergraduate, 2=graduate.
 * tuition_type: 1=in-district, 2=in-state, 3=out-of-state, 4=out-of-district.
 *
 * <p>The source codes an absent charge as -1 (missing/not reported), -2 (not applicable) or
 * -3 (suppressed). Those codes load as null in the charge columns.
 */
public class IpedsTuitionResponseTransformer extends AbstractUrbanInstituteResponseTransformer {

  private static final String[] CHARGE_FIELDS = {
      "tuition_fees_ft", "tuition_ft", "fees_ft", "credit_hour_charge_pt",
      "tuition_published", "fees_published", "tuition_fees_published"};

  @Override protected void augmentRecord(ObjectNode row, RequestContext context) {
    for (String field : CHARGE_FIELDS) {
      JsonNode value = row.path(field);
      if (value.isNumber() && isMissingCode(value.asDouble())) {
        row.putNull(field);
      }
    }
  }

  private static boolean isMissingCode(double value) {
    return value == -1 || value == -2 || value == -3;
  }
}
