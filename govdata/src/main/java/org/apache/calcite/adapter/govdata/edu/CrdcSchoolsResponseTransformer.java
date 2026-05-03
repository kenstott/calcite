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
 * Transforms Urban Institute CRDC school equity responses.
 *
 * <p>CRDC data is spread across four sub-endpoints (directory, chronic-absenteeism,
 * offenses, teachers-staff) selected by the {@code crdc_topic} dimension. Each API
 * response is in standard Urban Institute format. This transformer injects the
 * {@code crdc_topic} dimension value into every row so that all four topics can be
 * stored in the same partitioned table.
 */
public class CrdcSchoolsResponseTransformer extends AbstractUrbanInstituteResponseTransformer {

  @Override protected void augmentRecord(ObjectNode row, RequestContext context) {
    String topic = context.getDimensionValues().get("crdc_topic");
    if (topic != null) {
      row.put("crdc_topic", topic);
    }
  }
}
