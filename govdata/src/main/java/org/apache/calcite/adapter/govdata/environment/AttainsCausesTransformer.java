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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/**
 * Flattens an EPA ATTAINS {@code usesStateSummary} response (its nested parameter groups) into one row per
 * (water type, designated use, parameter group, assessment category) for {@code attains_cause_summary}.
 */
public class AttainsCausesTransformer implements ResponseTransformer {

  @Override public String transform(String response, RequestContext context) {
    String orgId = context.getDimensionValues().get("organization_id");
    String cycle = context.getDimensionValues().get("reporting_cycle");
    try {
      JsonNode cycles = AttainsSummarySupport.cycles(response, orgId);
      ArrayNode out = AttainsSummarySupport.newArray();
      if (cycles == null) {
        return AttainsSummarySupport.write(out);
      }
      String state = AttainsSummarySupport.stateOf(orgId);
      for (JsonNode rc : cycles) {
        for (JsonNode wt : rc.path("waterTypes")) {
          for (JsonNode use : wt.path("useAttainments")) {
            ObjectNode base = AttainsSummarySupport.newObject();
            base.put("organization_id", orgId);
            base.put("state_abbr", state);
            base.put("reporting_cycle", Integer.parseInt(cycle));
            base.put("cycle_status", rc.path("cycleStatus").asText());
            base.put("water_type", wt.path("waterTypeCode").asText());
            base.put("units", wt.path("unitsCode").asText());
            base.put("use_name", use.path("useName").asText());
            for (JsonNode param : use.path("parameters")) {
              ObjectNode pbase = base.deepCopy();
              pbase.put("parameter_group", param.path("parameterGroup").asText());
              AttainsSummarySupport.categories(param, pbase, (b, cat, size, count) -> {
                ObjectNode row = b.deepCopy();
                row.put("attainment_category", cat);
                row.put("size", size);
                row.put("unit_count", count);
                out.add(row);
              });
            }
          }
        }
      }
      return AttainsSummarySupport.write(out);
    } catch (IOException e) {
      throw new RuntimeException("attains_cause_summary transform failed: " + e.getMessage(), e);
    }
  }
}
