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
 * Transforms Urban Institute CCD district directory responses.
 *
 * <p>The API field names match the schema column names directly; this transformer
 * only extracts the {@code results} array via the base class.
 */
public class CcdDistrictsResponseTransformer extends AbstractUrbanInstituteResponseTransformer {

  @Override protected void augmentRecord(ObjectNode row, RequestContext context) {
    // Urban Institute field names match schema columns — no remapping needed.
    // Normalise state_location -> state_abbr if the API uses the old field name.
    if (!row.has("state_abbr") && row.has("state_location")) {
      row.set("state_abbr", row.get("state_location"));
    }
  }
}
