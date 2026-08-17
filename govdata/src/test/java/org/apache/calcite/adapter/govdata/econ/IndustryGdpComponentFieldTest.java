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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BEA spells the description field {@code IndustrYDescription}, with a capital Y.
 *
 * <p>It was mapped from {@code IndusDesc}, which GDPbyIndustry does not return, so the column
 * was null on every row ever loaded. That is not merely a missing label. On the component
 * tables — TableID 6 "Components of Value Added by Industry" and 7, the same as a percentage of
 * value added — this field carries the COMPONENT name, not the industry name. Nulling it left
 * four anonymous rows per industry-year with no way to tell gross operating surplus from
 * compensation, so BEA's profit and margin decomposition was unusable while sitting in the
 * warehouse fully loaded.
 *
 * <p>Payload below is copied verbatim from a live call on 2026-08-17:
 * {@code datasetname=GDPbyIndustry&Industry=52&Frequency=A&Year=2024&TableID=7}.
 */
@Tag("unit")
public class IndustryGdpComponentFieldTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Verbatim BEA response rows for Finance and insurance, table 7, 2024. */
  private static final String LIVE_TABLE_7 = "["
      + "{\"TableID\":\"7\",\"Frequency\":\"A\",\"Year\":\"2024\",\"Quarter\":\"2024\","
      + "\"Industry\":\"52\",\"IndustrYDescription\":\"Gross operating surplus\","
      + "\"DataValue\":\"46.2\",\"NoteRef\":\"7\"},"
      + "{\"TableID\":\"7\",\"Frequency\":\"A\",\"Year\":\"2024\",\"Quarter\":\"2024\","
      + "\"Industry\":\"52\",\"IndustrYDescription\":"
      + "\"Taxes on production and imports less subsidies\","
      + "\"DataValue\":\"3.7\",\"NoteRef\":\"7\"},"
      + "{\"TableID\":\"7\",\"Frequency\":\"A\",\"Year\":\"2024\",\"Quarter\":\"2024\","
      + "\"Industry\":\"52\",\"IndustrYDescription\":\"Compensation of employees\","
      + "\"DataValue\":\"50.0\",\"NoteRef\":\"7\"},"
      + "{\"TableID\":\"7\",\"Frequency\":\"A\",\"Year\":\"2024\",\"Quarter\":\"2024\","
      + "\"Industry\":\"52\",\"IndustrYDescription\":\"Finance and insurance\","
      + "\"DataValue\":\"100.0\",\"NoteRef\":\"7\"}]";

  private static List<JsonNode> transformAll(String json) throws Exception {
    IndustryGdpTransformer transformer = new IndustryGdpTransformer();
    List<JsonNode> out = new ArrayList<>();
    for (JsonNode record : MAPPER.readTree(json)) {
      out.add(transformer.transformRecord(record, 2024));
    }
    return out;
  }

  @Test void namesEachComponentSoTheyAreDistinguishable() throws Exception {
    List<JsonNode> rows = transformAll(LIVE_TABLE_7);
    assertEquals(4, rows.size());

    List<String> descriptions = new ArrayList<>();
    for (JsonNode r : rows) {
      descriptions.add(r.path("industry_description").asText(null));
    }

    assertTrue(descriptions.contains("Gross operating surplus"),
        "the profit component must be identifiable — this is BEA's profit measure: "
            + descriptions);
    assertTrue(descriptions.contains("Compensation of employees"), descriptions.toString());
    assertTrue(descriptions.contains("Taxes on production and imports less subsidies"),
        descriptions.toString());
    assertTrue(descriptions.contains("Finance and insurance"),
        "the industry-named row is the 100% total: " + descriptions);
  }

  @Test void readsTheProfitMarginOffTheComponentTable() throws Exception {
    Double surplus = null;
    for (JsonNode r : transformAll(LIVE_TABLE_7)) {
      if ("Gross operating surplus".equals(r.path("industry_description").asText(null))) {
        surplus = r.path("value").asDouble();
      }
    }
    assertNotNull(surplus, "gross operating surplus row must survive the transform");
    assertEquals(46.2, surplus, 0.001,
        "Finance and insurance ran a 46.2% gross operating surplus margin in 2024");
  }

  @Test void doesNotLeaveTheDescriptionNull() throws Exception {
    // The regression itself: mapping IndusDesc yielded null for every row, which is how four
    // component rows became indistinguishable.
    for (JsonNode r : transformAll(LIVE_TABLE_7)) {
      assertNotNull(r.path("industry_description").asText(null),
          "industry_description must not be null — that nulling hid the whole component table");
    }
  }

  @Test void leavesUnitsNullBecauseTheDatasetHasNoPerRowUnit() throws Exception {
    // GDPbyIndustry returns no CL_UNIT. Units belong to table_id, not to the row, and must not
    // be guessed per row.
    for (JsonNode r : transformAll(LIVE_TABLE_7)) {
      assertTrue(r.path("units").isNull(),
          "units must be explicitly null, not filled with a guess");
    }
  }

  @Test void stillCarriesTheIndustryNameOnNonComponentTables() throws Exception {
    // On TableID 1 the same field carries the industry name, which is what makes it a usable
    // label there as well.
    String table1 = "[{\"TableID\":\"1\",\"Frequency\":\"A\",\"Year\":\"2024\","
        + "\"Quarter\":\"2024\",\"Industry\":\"52\","
        + "\"IndustrYDescription\":\"Finance and insurance\","
        + "\"DataValue\":\"2229.2\",\"NoteRef\":\"1\"}]";
    List<JsonNode> rows = transformAll(table1);
    assertEquals("Finance and insurance",
        rows.get(0).path("industry_description").asText(null));
    assertEquals(2229.2, rows.get(0).path("value").asDouble(), 0.001);
    assertNull(rows.get(0).get("units").textValue());
  }
}
