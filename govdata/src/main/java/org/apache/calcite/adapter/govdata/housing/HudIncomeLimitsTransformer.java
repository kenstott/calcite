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
package org.apache.calcite.adapter.govdata.housing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps HUD {@code il/statedata/{state}} responses into {@code income_limits}
 * rows. The endpoint returns one state summary object: {@code median_income}
 * plus the {@code extremely_low} (30% AMI), {@code very_low} (50% AMI), and
 * {@code low} (80% AMI) bracket objects. The 4-person-household value
 * ({@code il30_p4}/{@code il50_p4}/{@code il80_p4}) is HUD's reference figure and
 * the one carried here. {@code stateID} is the numeric state FIPS.
 */
public class HudIncomeLimitsTransformer extends AbstractHudJsonTransformer {

  @Override protected void emitRows(JsonNode data, ArrayNode out) {
    String stateFips = padStateFips(data, "stateID");
    if (stateFips == null) {
      return;
    }
    ObjectNode row = MAPPER.createObjectNode();
    row.put("state_fips", stateFips);
    putText(row, "state_abbr", data, "statecode");
    putInt(row, "median_income", data, "median_income");
    putInt(row, "il30_4person", data.path("extremely_low"), "il30_p4");
    putInt(row, "il50_4person", data.path("very_low"), "il50_p4");
    putInt(row, "il80_4person", data.path("low"), "il80_p4");
    out.add(row);
  }
}
