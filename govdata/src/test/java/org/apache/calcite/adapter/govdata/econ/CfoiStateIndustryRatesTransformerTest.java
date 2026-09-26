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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CfoiStateIndustryRatesTransformer} against trimmed copies of the BLS pages for
 * reference years 2018 (id-attributed row headers) and 2024 (scope-attributed row headers).
 */
@Tag("unit")
class CfoiStateIndustryRatesTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final CfoiStateIndustryRatesTransformer transformer =
      new CfoiStateIndustryRatesTransformer();

  private static String fixture(String name) throws IOException {
    try (InputStream in =
        CfoiStateIndustryRatesTransformerTest.class.getResourceAsStream("/cfoi/" + name)) {
      return new String(readAll(in), StandardCharsets.UTF_8);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  private static RequestContext context(String year) {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("effective_year", year);
    return RequestContext.builder().url("https://www.bls.gov/test/" + year).dimensionValues(dims)
        .parameters(Collections.<String, String>emptyMap())
        .headers(Collections.<String, String>emptyMap()).build();
  }

  private static JsonNode find(JsonNode rows, String jurisdiction, String sector) {
    for (JsonNode r : rows) {
      if (jurisdiction.equals(r.get("jurisdiction_name").asText())
          && sector.equals(r.get("industry_sector").asText())) {
        return r;
      }
    }
    return null;
  }

  @Test void parsesRatesAndSkipsUnpublishedCells() throws Exception {
    JsonNode rows = MAPPER.readTree(transformer.transform(fixture("state-industry-2024.htm"),
        context("2024")));

    JsonNode alaskaOverall = find(rows, "Alaska", "All industries");
    assertEquals(7.1, alaskaOverall.get("fatal_injury_rate").asDouble(), 0.0);
    assertEquals(2024, alaskaOverall.get("year").asInt());
    assertEquals("02", alaskaOverall.get("state_fips").asText());
    assertEquals("STATE", alaskaOverall.get("jurisdiction_type").asText());

    assertEquals(198.7, find(rows, "Alaska", "Agriculture, forestry, fishing and hunting")
        .get("fatal_injury_rate").asDouble(), 0.0);
    // "-" on the page: no row, not a zero.
    assertEquals(null, find(rows, "Alaska", "Construction"));
    assertEquals(8.2, find(rows, "Alabama", "Construction").get("fatal_injury_rate").asDouble(),
        0.0);
  }

  @Test void newYorkCityIsACityRowWithoutAStateFips() throws Exception {
    JsonNode rows = MAPPER.readTree(transformer.transform(fixture("state-industry-2024.htm"),
        context("2024")));
    JsonNode nyc = find(rows, "New York City", "All industries");
    assertEquals("CITY", nyc.get("jurisdiction_type").asText());
    assertTrue(nyc.get("state_fips").isNull());
    assertEquals("36", find(rows, "New York", "All industries").get("state_fips").asText());
    assertEquals("DISTRICT",
        find(rows, "District of Columbia", "All industries").get("jurisdiction_type").asText());
  }

  @Test void parsesTheOlderRowHeaderMarkup() throws Exception {
    JsonNode rows = MAPPER.readTree(transformer.transform(fixture("state-industry-2018.htm"),
        context("2018")));
    assertEquals(4.5, find(rows, "Alabama", "All industries").get("fatal_injury_rate").asDouble(),
        0.0);
    assertEquals(15.5, find(rows, "Alabama", "Construction").get("fatal_injury_rate").asDouble(),
        0.0);
    assertFalse(rows.size() == 0);
  }

  @Test void rejectsAPageForADifferentReferenceYear() throws Exception {
    String page = fixture("state-industry-2024.htm");
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> transformer.transform(page, context("2023")));
    assertTrue(e.getMessage().contains("2024 Overall Rate"), e.getMessage());
  }

  @Test void rejectsAPageThatIsNotADataTable() {
    assertThrows(IllegalStateException.class,
        () -> transformer.transform("<html><body><p>BLS home</p></body></html>",
            context("2024")));
  }

  @Test void rejectsAnUnrecognizedJurisdiction() throws Exception {
    String page = fixture("state-industry-2024.htm").replace(">Alaska</p>", ">Atlantis</p>");
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> transformer.transform(page, context("2024")));
    assertTrue(e.getMessage().contains("Atlantis"), e.getMessage());
  }
}
