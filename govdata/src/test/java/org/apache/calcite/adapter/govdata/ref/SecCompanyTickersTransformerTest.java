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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link SecCompanyTickersTransformer}. */
@Tag("unit")
class SecCompanyTickersTransformerTest {

  @Test void stampsEveryRowWithTheUtcCaptureDate() throws Exception {
    Clock clock = Clock.fixed(Instant.parse("2026-11-26T23:59:59Z"), ZoneOffset.UTC);
    String response = "{\"0\":{\"cik_str\":1652044,\"ticker\":\"GOOGL\",\"title\":\"Alphabet Inc.\"},"
        + "\"1\":{\"cik_str\":1652044,\"ticker\":\"GOOG\",\"title\":\"Alphabet Inc.\"}}";

    JsonNode rows = new ObjectMapper().readTree(
        new SecCompanyTickersTransformer(clock).transform(response,
            RequestContext.builder().url("https://www.sec.gov/files/company_tickers.json")
                .build()));

    assertEquals(2, rows.size());
    for (JsonNode row : rows) {
      assertEquals("2026-11-26", row.get("as_of").asText());
      assertEquals("1652044", row.get("cik").asText());
    }
  }
}
