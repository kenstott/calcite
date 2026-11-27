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
 * Transforms Urban Institute CCD district directory responses.
 *
 * <p>The API field names match the schema column names directly; this transformer
 * only extracts the {@code results} array via the base class.
 */
public class CcdDistrictsResponseTransformer extends AbstractUrbanInstituteResponseTransformer {

  @Override protected void augmentRecord(ObjectNode row, RequestContext context) {
    // Normalise state_location -> state_abbr if the API uses the old field name.
    if (!row.has("state_abbr") && row.has("state_location")) {
      row.set("state_abbr", row.get("state_location"));
    }

    normalizeCcdGeography(row);

    // agency_charter_indicator is never populated by the Urban Institute API.
    // Derive from agency_type: type 7 = independent charter LEA per NCES CCD.
    JsonNode chartNode = row.path("agency_charter_indicator");
    if (chartNode.isMissingNode() || chartNode.isNull()) {
      JsonNode agencyType = row.path("agency_type");
      if (!agencyType.isMissingNode() && !agencyType.isNull()) {
        row.put("agency_charter_indicator", agencyType.asInt() == 7 ? 1 : 0);
      }
    }
  }
}
