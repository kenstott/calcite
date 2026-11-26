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
package org.apache.calcite.adapter.govdata.crime;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parsing tests for the EOIR asylum transformers, on text as PDFBox extracts it. */
@Tag("unit")
class EoirAsylumTransformersTest {

  private static final String DECISIONS = "Asylum Decisions1\n"
      + "FY Grants Denials Abandonment Not Adjudicated Withdrawn Admin Closure2 Total\n"
      + "2025 26,599 82,431 51,654 78,243 15,120 13,875 267,922\n"
      + "2026 (Third \nQuarter) 7,231 89,740 92,081 44,062 18,251 3,315 254,680\n"
      + "Data Generated: July 24, 2026\n";

  @Test void decisionsRowsIncludingPartYear() {
    JsonNode rows = new EoirAsylumDecisionsTransformer().parse(DECISIONS);
    assertEquals(2, rows.size());
    assertEquals(4, rows.get(0).get("quarters_reported").asInt());
    assertEquals(82431L, rows.get(0).get("denials").asLong());
    assertEquals(3, rows.get(1).get("quarters_reported").asInt());
    assertEquals(254680L, rows.get(1).get("total_decisions").asLong());
    assertEquals("2026-07-24", rows.get(1).get("data_generated").asText());
  }

  @Test void decisionsRowThatDoesNotAddUpFails() {
    assertThrows(IllegalStateException.class,
        () -> new EoirAsylumDecisionsTransformer().parse(DECISIONS.replace("82,431", "82,432")));
  }

  @Test void unreadableRowFails() {
    assertThrows(IllegalStateException.class,
        () -> new EoirAsylumDecisionsTransformer().parse(DECISIONS.replace("2026 (Third", "FY26 (Third")));
  }

  private static String page(String fiscalYear, String generated, String header, String body) {
    return "ADJUDICATION STATISTICS\nAsylum Decision Rates by Nationality1\n" + header
        + body + "1 Asylum rates for Fiscal Year " + fiscalYear + ". Data generated on "
        + generated + ".\n";
  }

  private static final String RATES_HEADER =
      "Nationality Grants2 Grant Rate Denials Denial Rate Other3 Other Rate Total\n";

  @Test void centredWrappedNamesAndSuppressedCells() {
    String body = "ANTIGUA AND \n* * * * * * *\nBARBUDA\n"
        + "BOSNIA-\n8 21% 15 39% 15 39% 38\nHERZEGOVINA\n"
        + "CHINA 3,481 55% 765 12% 2,127 33% 6,373\n";
    JsonNode rows = EoirAsylumNationalityTransformer.parse(
        Collections.singletonList(page("2023", "October 12, 2023", RATES_HEADER, body)));
    assertEquals(3, rows.size());
    assertEquals("ANTIGUA AND BARBUDA", rows.get(0).get("nationality").asText());
    assertTrue(rows.get(0).get("grants").isNull());
    assertEquals("BOSNIA-HERZEGOVINA", rows.get(1).get("nationality").asText());
    assertEquals(6373L, rows.get(2).get("total_decisions").asLong());
    assertTrue(rows.get(2).get("abandonment").isNull());
  }

  @Test void bottomAlignedNamesWithAdministrativeClosures() {
    String header = "Admin \nDenial Admin \nNationality Grants2 Grant Rate Denials Other3 "
        + "Total\nRate Closure4\nRate\n";
    String body = "BOSNIA-\nHERZEGOVINA 181 62.41% 55 18.97% 49 16.90% 5 1.72% 290\n";
    JsonNode rows = EoirAsylumNationalityTransformer.parse(
        Collections.singletonList(page("2018", "October 24, 2018", header, body)));
    assertEquals("BOSNIA-HERZEGOVINA", rows.get(0).get("nationality").asText());
    assertEquals(5L, rows.get(0).get("admin_closure").asLong());
    assertEquals(290L, rows.get(0).get("total_decisions").asLong());
  }

  @Test void partYearEditionsAreIgnoredAndLatestEditionWins() {
    JsonNode rows = EoirAsylumNationalityTransformer.parse(Arrays.asList(
        page("2023", "July 13, 2023", RATES_HEADER, "CHINA 1 1% 1 1% 1 1% 3\n"),
        page("2023", "October 12, 2023", RATES_HEADER, "CHINA 2 1% 2 1% 2 1% 6\n"),
        page("2023", "January 16, 2024", RATES_HEADER, "CHINA 4 1% 4 1% 4 1% 12\n")));
    assertEquals(1, rows.size());
    assertEquals(4L, rows.get(0).get("grants").asLong());
  }
}
