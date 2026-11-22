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
package org.apache.calcite.adapter.govdata.disasters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Maps NFHL layer 1 (LOMRs: Letters of Map Revision) features into {@code fema_nfhl_lomrs}. */
public class FemaNfhlLomrTransformer extends AbstractFemaNfhlTransformer {

  @Override protected String tableName() {
    return "fema_nfhl_lomrs";
  }

  @Override protected void mapAttributes(JsonNode attrs, ObjectNode row) {
    putText(row, "lomr_id", attrs, "LOMR_ID");
    putText(row, "case_no", attrs, "CASE_NO");
    putDate(row, "effective_date", attrs, "EFF_DATE");
    putText(row, "status", attrs, "STATUS");
    putText(row, "map_scale", attrs, "SCALE");
    putText(row, "source_citation", attrs, "SOURCE_CIT");
  }
}
