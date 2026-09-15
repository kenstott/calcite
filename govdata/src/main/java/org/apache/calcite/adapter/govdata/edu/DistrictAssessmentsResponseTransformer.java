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

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms Urban Institute EDFacts district-assessments responses.
 *
 * <p>The source field is {@code grade_edfacts}; renamed to {@code grade} to match
 * this table's schema column (and the sibling naep_scores/naep_achievement_levels
 * grade column naming). All other fields already match the schema column names.
 */
public class DistrictAssessmentsResponseTransformer extends AbstractUrbanInstituteResponseTransformer {

  @Override protected void augmentRecord(ObjectNode row, RequestContext context) {
    if (row.has("grade_edfacts") && !row.has("grade")) {
      row.set("grade", row.remove("grade_edfacts"));
    }
  }
}
