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
package org.apache.calcite.adapter.govdata.sec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parsing of the SEC annual enforcement-results addenda (text copied from the live PDFs, 2026-11-22). */
@Tag("unit")
class SecEnforcementResultsTransformerTest {

  private static final String FY24 = "ADDENDUM TO  \nDIVISION OF ENFORCEMENT PRESS RELEASE \nFISCAL YEAR 2024 \n"
      + "Enforcement Actions Filed in Fiscal Years 2019 to 2024\n"
      + "FY 2024 FY 2023 FY 20221 FY 2021 FY 2020 FY 2019\n"
      + "Standalone Enforcement Actions 431 501 463 434 405 526\n"
      + "Follow -On Admin. Proceedings 93 162 169 143 180 210\n"
      + "Delinquent Filings 59 121 129 120 130 126\n"
      + "Total Actions 583 784 761 697 715 862\n"
      + "Standalone Enforcement Actions is a combined count of Civil Actions and Standalone APs.  \n"
      + "Total Money Ordered (in millions)\n"
      + "FY 2024 FY 2023 FY 2022 FY 2021 FY 2020 FY 2019\n"
      + "Penalties $2,102 $1,580 $4,194 $1,456 $1,091 $1,101\n"
      + "Disgorgement $6,092 $3,369 $2,245 $2,395 $3,588 $3,248\n"
      + "Total $8,194 $4,949 $6,439 $3,852 $4,680 $4,349  \n"
      + "Money Distributed to Harmed Investors (in millions)\n"
      + "FY 2024 FY 2023 FY 2022 FY 2021 FY 2020 FY 2019\n"
      + "$345 $930 $937 $521 $602 $1,197  \n";

  private static final String FY26 = "ADDENDUM TO\nFISCAL YEAR 2026\n"
      + "Enforcement Actions Filed Total Money Ordered (in millions)\n"
      + "Standalone Actions1 235 Penalties $343\n"
      + "Follow-On APs 47\nDisgorgement $752\nDelinquent Filings 66\nTotal2 $1,095\n"
      + "Total Actions 348\nCertain Offsets3 $235\nDistributions (in millions)\n"
      + "Total Less Certain Offsets $860\nTotal Money Distributed to\nHarmed Investors $731\n";

  @Test void multiYearTable() {
    ArrayNode out = JsonNodeFactory.instance.arrayNode();
    SecEnforcementResultsTransformer.parseMultiYear(FY24, "r", "a", out);
    assertEquals(6, out.size());
    JsonNode fy22 = out.get(2);
    assertEquals(2022, fy22.get("fiscal_year").asInt());
    assertEquals(463, fy22.get("standalone_actions").asInt());
    assertEquals(761, fy22.get("total_actions").asInt());
    assertEquals(4194, fy22.get("penalties_millions").asLong());
    assertEquals(937, fy22.get("distributed_to_investors_millions").asLong());
    assertEquals(1197, out.get(5).get("distributed_to_investors_millions").asLong());
    assertTrue(out.get(0).get("offsets_millions").isNull());
  }

  @Test void singleYearTable() {
    ArrayNode out = JsonNodeFactory.instance.arrayNode();
    SecEnforcementResultsTransformer.parseSingleYear(FY26, 2026, 13, "r", "a", out);
    JsonNode r = out.get(0);
    assertEquals(235, r.get("standalone_actions").asInt());
    assertEquals(47, r.get("follow_on_actions").asInt());
    assertEquals(66, r.get("delinquent_filings").asInt());
    assertEquals(348, r.get("total_actions").asInt());
    assertEquals(343, r.get("penalties_millions").asLong());
    assertEquals(752, r.get("disgorgement_millions").asLong());
    assertEquals(1095, r.get("total_money_ordered_millions").asLong());
    assertEquals(235, r.get("offsets_millions").asLong());
    assertEquals(731, r.get("distributed_to_investors_millions").asLong());
    assertEquals(13, r.get("trading_suspensions").asInt());
  }

  @Test void proseLayout() {
    String text = "the Commission filed 456 enforcement actions, including 303 standalone actions and 69"
        + " “follow-on” administrative proceedings seeking to bar, and obtaining orders for"
        + " monetary relief totaling $17.9 billion. These";
    ArrayNode out = JsonNodeFactory.instance.arrayNode();
    SecEnforcementResultsTransformer.parseProse(text, 2025, null, "r", "a", out);
    JsonNode r = out.get(0);
    assertEquals(456, r.get("total_actions").asInt());
    assertEquals(303, r.get("standalone_actions").asInt());
    assertEquals(69, r.get("follow_on_actions").asInt());
    assertEquals(17900, r.get("total_money_ordered_millions").asLong());
    assertTrue(r.get("delinquent_filings").isNull());
    assertTrue(r.get("penalties_millions").isNull());
  }
}
