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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Row shaping for the ECOS species report, using rows shaped like the live export. */
@Tag("unit")
class FisheriesEsaListingsTransformerTest {
  private final FisheriesEsaListingsTransformer transformer =
      new FisheriesEsaListingsTransformer();

  private static String record(String common, String status, String category, String desc,
      String date) {
    return "[\"" + common + "\",{\"value\":\"Oncorhynchus sp.\",\"url\":\"u\"},\"" + status
        + "\",\"" + category + "\",\"" + desc + "\",\"" + date + "\",\"NMFS\"]";
  }

  private JsonNode run(String... records) throws Exception {
    String json = "{\"meta\":{},\"data\":[" + String.join(",", records) + "]}";
    RequestContext ctx = RequestContext.builder().url("https://ecos.fws.gov/x").build();
    return new ObjectMapper().readTree(transformer.transform(json, ctx));
  }

  @Test void notListedStatusWithBlankCategoryTakesNotListed() throws Exception {
    JsonNode rows = run(record("Pink Salmon", "Not Listed", "", "", ""));
    assertEquals("Not Listed", rows.get(0).get("status_category").asText());
  }

  @Test void blankCategoryOnOtherStatusStaysNull() throws Exception {
    JsonNode rows = run(record("Coho salmon", "Status Undefined", "", "", ""));
    assertTrue(rows.get(0).get("status_category").isNull());
  }

  @Test void providedCategoryIsKept() throws Exception {
    JsonNode rows = run(record("Atlantic salmon", "Resolved Taxon", "Not Listed",
        "Kennebec River DPS", "06-19-2009"));
    assertEquals("Not Listed", rows.get(0).get("status_category").asText());
    assertEquals("2009-06-19", rows.get(0).get("listing_date").asText());
  }

  @Test void commonNameCasingIsCanonicalPerSpecies() throws Exception {
    JsonNode rows = run(
        record("Sockeye Salmon", "Not Listed", "", "", ""),
        record("Sockeye salmon", "Threatened", "Listed", "Ozette Lake ESU", "03-25-1999"),
        record("Steelhead (Rainbow Trout)", "Threatened", "Listed", "Puget Sound DPS",
            "05-11-2007"));
    assertEquals("Sockeye salmon", rows.get(0).get("common_name").asText());
    assertEquals("Sockeye salmon", rows.get(1).get("common_name").asText());
    assertEquals("Steelhead (Rainbow Trout)", rows.get(2).get("common_name").asText());
    assertEquals("sockeye", rows.get(0).get("species_group").asText());
  }
}
