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

/** Maps NFHL layer 3 (FIRM Panels) features into {@code fema_firm_panels}. */
public class FemaNfhlFirmPanelTransformer extends AbstractFemaNfhlTransformer {

  @Override protected String tableName() {
    return "fema_firm_panels";
  }

  @Override protected void mapAttributes(JsonNode attrs, ObjectNode row) {
    putText(row, "firm_id", attrs, "FIRM_ID");
    putText(row, "firm_panel", attrs, "FIRM_PAN");
    putText(row, "panel_type", attrs, "PANEL_TYP");
    putDate(row, "effective_date", attrs, "EFF_DATE");
    putDate(row, "prior_effective_date", attrs, "PRE_DATE");
    putText(row, "map_scale", attrs, "SCALE");
    putText(row, "not_printed_reason", attrs, "PNP_REASON");
    putText(row, "source_citation", attrs, "SOURCE_CIT");
  }
}
