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
package org.apache.calcite.adapter.govdata.officials;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class CongressMemberCurrentEnricherTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test void partySwitcherGetsPartyHeldInEachCongress() throws Exception {
    JsonNode amash = MAPPER.readTree("[{\"startYear\":2011,\"endYear\":2019,"
        + "\"partyName\":\"Republican\"},{\"startYear\":2020,\"partyName\":\"Libertarian\"},"
        + "{\"startYear\":2019,\"endYear\":2020,\"partyName\":\"Independent\"}]");
    for (int congress = 112; congress <= 115; congress++) {
      assertEquals("Republican", CongressMemberCurrentEnricher.partyForCongress(amash, congress));
    }
    assertEquals("Libertarian", CongressMemberCurrentEnricher.partyForCongress(amash, 116));
  }

  @Test void midCongressSwitchResolvesToPartyAtCongressEnd() throws Exception {
    JsonNode vanDrew = MAPPER.readTree("[{\"startYear\":2019,\"endYear\":2020,"
        + "\"partyName\":\"Democratic\"},{\"startYear\":2020,\"partyName\":\"Republican\"}]");
    assertEquals("Republican", CongressMemberCurrentEnricher.partyForCongress(vanDrew, 116));
  }

  @Test void noSpanStartedByCongressYieldsNull() throws Exception {
    JsonNode later = MAPPER.readTree("[{\"startYear\":2011,\"partyName\":\"Republican\"}]");
    assertNull(CongressMemberCurrentEnricher.partyForCongress(later, 110));
  }
}
