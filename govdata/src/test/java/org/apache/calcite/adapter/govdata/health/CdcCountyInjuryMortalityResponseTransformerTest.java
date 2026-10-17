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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Year derivation for CDC MIOV rows, using records shaped like the live psx4-wq38 responses. */
@Tag("unit")
class CdcCountyInjuryMortalityResponseTransformerTest {
  private final CdcCountyInjuryMortalityResponseTransformer transformer =
      new CdcCountyInjuryMortalityResponseTransformer();

  private static Map<String, Object> row(String period, String range) {
    Map<String, Object> r = new HashMap<>();
    r.put("geoid", "20017");
    r.put("period", period);
    r.put("rate", "0.0");
    if (range != null) {
      r.put("ttm_date_range", range);
    }
    return r;
  }

  private RequestContext ctx() {
    return RequestContext.builder()
        .url("https://data.cdc.gov/resource/psx4-wq38.json").build();
  }

  @Test void yearlyRowTakesPeriodAsYear() {
    Map<String, Object> r = row("2023", null);
    transformer.transformRecord(r, ctx());
    assertEquals(2023, r.get("year"));
    assertEquals("20017", r.get("geoid"));
  }

  @Test void ttmRowTakesYearTheWindowEnds() {
    Map<String, Object> r = row("TTM", "June, 2025 to May, 2026");
    transformer.transformRecord(r, ctx());
    assertEquals(2026, r.get("year"));
    Map<String, Object> s = row("TTM", "April, 2025 to March, 2026");
    transformer.transformRecord(s, ctx());
    assertEquals(2026, s.get("year"));
  }

  @Test void ttmWindowWithinOneCalendarYear() {
    assertEquals(2025, CdcCountyInjuryMortalityResponseTransformer.yearOf(
        "TTM", "January, 2025 to December, 2025"));
  }

  @Test void bulkTransformAddsYear() throws Exception {
    String json = "[{\"geoid\":\"1\",\"period\":\"2019\"},"
        + "{\"geoid\":\"2\",\"period\":\"TTM\",\"ttm_date_range\":\"June, 2025 to May, 2026\"}]";
    JsonNode out = new ObjectMapper().readTree(transformer.transform(json, ctx()));
    assertEquals(2019, out.get(0).get("year").asInt());
    assertEquals(2026, out.get(1).get("year").asInt());
    assertEquals("2", out.get(1).get("geoid").asText());
  }

  @Test void unknownPeriodLabelFailsLoudly() {
    assertThrows(IllegalStateException.class,
        () -> transformer.transformRecord(row("Quarterly", null), ctx()));
    assertThrows(IllegalStateException.class,
        () -> transformer.transformRecord(row(null, null), ctx()));
  }

  @Test void ttmWithoutOrWithMalformedRangeFailsLoudly() {
    assertThrows(IllegalStateException.class,
        () -> transformer.transformRecord(row("TTM", null), ctx()));
    assertThrows(IllegalStateException.class,
        () -> transformer.transformRecord(row("TTM", "mid 2025 through 2026"), ctx()));
  }
}
