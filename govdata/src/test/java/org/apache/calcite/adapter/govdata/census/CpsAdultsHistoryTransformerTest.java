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
package org.apache.calcite.adapter.govdata.census;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the CPS Table AD-1 and AD-3 transformers, against the published workbooks. */
@Tag("unit")
class CpsAdultsHistoryTransformerTest {

  @Test void findsWorkbookLinkOnThePage() throws IOException {
    String page = "<a href=\"https://www2.census.gov/programs-surveys/demo/tables/families/"
        + "time-series/adults/ad3-18-24.xls\">x</a><a href=\"//www2.census.gov/a/ad3.xls\">y</a>";
    assertEquals("https://www2.census.gov/a/ad3.xls",
        CpsAdultsHistoryTransformer.workbookUrl(page, "ad3.xls"));
    assertThrows(IOException.class,
        () -> CpsAdultsHistoryTransformer.workbookUrl(page, "ad1.xls"));
  }

  @Test void parsesTableAd1() throws IOException {
    ArrayNode out = new ArrayNode(JsonNodeFactory.instance);
    try (HSSFWorkbook wb = open("ad1.xls")) {
      CpsYoungAdultsAtHomeTransformer.parseSheet(wb.getSheetAt(0), out);
    }
    // 51 year rows per age group, two sexes.
    assertEquals(204, out.size());
    JsonNode first1960 = find(out, 1960, "primary", "18-24", "male", null);
    assertEquals(3583, first1960.get("child_of_householder_thousands").asLong());
    assertEquals("decennial_census", first1960.get("data_source").asText());
    assertTrue(find(out, 1981, "primary", "25-34", "female", null)
        .get("child_of_householder_pct").isNull());
    assertEquals(60.2, find(out, 2021, "primary", "18-24", "male", null)
        .get("child_of_householder_pct").asDouble());
    assertEquals(59.9, find(out, 2021, "alternate", "18-24", "male", null)
        .get("child_of_householder_pct").asDouble());
  }

  @Test void parsesTableAd3AllAdults() throws IOException {
    ArrayNode out = new ArrayNode(JsonNodeFactory.instance);
    try (HSSFWorkbook wb = open("ad3.xls")) {
      CpsAdultLivingArrangementsTransformer.parseAllAdultsSheet(wb.getSheetAt(0), out);
    }
    assertEquals(58 * 6, out.size());
    JsonNode row = find(out, 2023, "primary", "18+", "total", "child_of_householder");
    assertEquals(29793, row.get("people_thousands").asLong());
    assertEquals(257871, row.get("age_group_population_thousands").asLong());
    assertEquals(11.6, row.get("percent").asDouble());
  }

  @Test void parsesTableAd3AgeGroups() throws IOException {
    ArrayNode young = new ArrayNode(JsonNodeFactory.instance);
    try (HSSFWorkbook wb = open("ad3-18-24.xls")) {
      CpsAdultLivingArrangementsTransformer.parseAgeGroupSheet(wb.getSheetAt(0), "18-24", young);
    }
    assertEquals(58 * 2 * 6, young.size());
    JsonNode z = find(young, 1967, "primary", "18-24", "female", "living_with_partner");
    assertEquals(0.0, z.get("percent").asDouble());
    assertTrue(z.get("people_thousands").isNull());
    assertEquals(10803, z.get("age_group_population_thousands").asLong());

    ArrayNode old = new ArrayNode(JsonNodeFactory.instance);
    try (HSSFWorkbook wb = open("ad3-65-74.xls")) {
      CpsAdultLivingArrangementsTransformer.parseAgeGroupSheet(wb.getSheetAt(0), "65-74", old);
    }
    // No "child of householder" column for this age group.
    assertEquals(58 * 2 * 5, old.size());
  }

  private HSSFWorkbook open(String name) throws IOException {
    try (InputStream in = getClass().getResourceAsStream("/census/" + name)) {
      return new HSSFWorkbook(in);
    }
  }

  private static JsonNode find(ArrayNode rows, int year, String vintage, String ageGroup,
      String sex, String arrangement) {
    for (JsonNode row : rows) {
      if (row.get("year").asInt() == year && vintage.equals(row.get("vintage").asText())
          && ageGroup.equals(row.get("age_group").asText())
          && sex.equals(row.get("sex").asText())
          && (arrangement == null
              || arrangement.equals(row.get("living_arrangement").asText()))) {
        return row;
      }
    }
    throw new AssertionError("no row " + year + " " + vintage + " " + ageGroup + " " + sex);
  }
}
