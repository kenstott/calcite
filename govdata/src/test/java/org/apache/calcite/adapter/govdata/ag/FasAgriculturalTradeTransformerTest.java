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
package org.apache.calcite.adapter.govdata.ag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the agricultural product classification of {@link FasAgriculturalTradeTransformer}. */
@Tag("unit")
class FasAgriculturalTradeTransformerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Set<String> CHAPTERS = new HashSet<String>(Arrays.asList("10", "52"));

  private static void add(ArrayNode catalog, String code, String type, boolean flag,
      String start, String end, String description) {
    catalog.addObject()
        .put("hS10Code", code)
        .put("productType", type)
        .put("isAgCommodity", flag)
        .put("startDate", start)
        .put("endDtate", end)
        .put("commodityDescription", description);
  }

  private static ArrayNode catalog() {
    ArrayNode catalog = MAPPER.createArrayNode();
    add(catalog, "1001992015", "X", false, "201201", null, "WHEAT EXPORT");
    add(catalog, "1005902000", "M", false, "198901", null, "CORN IMPORT");
    add(catalog, "5201001010", "X", false, "198901", "199012", "OLD COTTON");
    add(catalog, "5201001010", "X", false, "199101", null, "COTTON");
    add(catalog, "2710111510", "T", true, "201001", null, "GASOLINE");
    add(catalog, "0014012   ", "T", false, "196701", null, "SEVEN DIGIT CODE");
    return catalog;
  }

  @Test void selectsAgriculturalCodesOfTheRequestedDirection() {
    Map<String, String> exports =
        FasAgriculturalTradeTransformer.agriculturalCodes(catalog(), true, CHAPTERS);
    assertEquals(new HashSet<String>(Arrays.asList("1001992015", "5201001010")), exports.keySet());
    assertEquals("COTTON", exports.get("5201001010"));
    Map<String, String> imports =
        FasAgriculturalTradeTransformer.agriculturalCodes(catalog(), false, CHAPTERS);
    assertEquals(new HashSet<String>(Arrays.asList("1005902000")), imports.keySet());
  }

  @Test void failsWhenAnAgriculturalChapterIsNotRequested() {
    Set<String> onlyGrain = new HashSet<String>(Arrays.asList("10"));
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> FasAgriculturalTradeTransformer.agriculturalCodes(catalog(), true, onlyGrain));
    assertTrue(e.getMessage().contains("[52]"), e.getMessage());
  }

  @Test void failsWhenTheFlagMeaningInverts() {
    ArrayNode inverted = MAPPER.createArrayNode();
    for (JsonNode entry : catalog()) {
      inverted.addObject()
          .put("hS10Code", entry.get("hS10Code").asText())
          .put("productType", entry.get("productType").asText())
          .put("isAgCommodity", !entry.get("isAgCommodity").asBoolean());
    }
    assertThrows(IllegalStateException.class,
        () -> FasAgriculturalTradeTransformer.agriculturalCodes(inverted, true, CHAPTERS));
  }

  @Test void failsOnAnEmptyCatalog() {
    assertThrows(IllegalStateException.class, () -> FasAgriculturalTradeTransformer
        .agriculturalCodes(MAPPER.createArrayNode(), true, CHAPTERS));
  }

  @Test void filterKeepsAgriculturalRowsAndAddsDescription() {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (String code : new String[] {"2710111510", "1001992015", "9999999999"}) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("E_COMMODITY", code);
      rows.add(row);
    }
    Map<String, String> agricultural = new LinkedHashMap<String, String>();
    agricultural.put("1001992015", "WHEAT");
    Iterator<Map<String, Object>> kept =
        FasAgriculturalTradeTransformer.filter(rows.iterator(), "E_COMMODITY", agricultural);
    Map<String, Object> row = kept.next();
    assertEquals("1001992015", row.get("E_COMMODITY"));
    assertEquals("WHEAT", row.get("COMMODITY_DESCRIPTION"));
    assertEquals(false, kept.hasNext());
  }

  @SuppressWarnings("unchecked")
  @Test void chapterListsMatchTheSchemaDimensions() throws IOException {
    try (InputStream in = getClass().getResourceAsStream("/ag/ag-schema.yaml")) {
      LoaderOptions options = new LoaderOptions();
      options.setMaxAliasesForCollections(1000);
      options.setCodePointLimit(10 * 1024 * 1024);
      Map<String, Object> schema = new Yaml(options).load(in);
      Map<String, Object> dimensions = (Map<String, Object>) schema.get("dimension_values");
      assertEquals(FasAgriculturalTradeTransformer.EXPORT_CHAPTERS,
          ((Map<String, Object>) dimensions.get("ag_export_chapters")).get("values"));
      assertEquals(FasAgriculturalTradeTransformer.IMPORT_CHAPTERS,
          ((Map<String, Object>) dimensions.get("ag_import_chapters")).get("values"));
    }
  }

  @Test void directionComesFromTheCensusUrl() {
    assertTrue(FasAgriculturalTradeTransformer.isExports(
        "https://api.census.gov/data/timeseries/intltrade/exports/hs?get=CTY_CODE"));
    assertThrows(IllegalStateException.class,
        () -> FasAgriculturalTradeTransformer.isExports("https://example.gov/"));
  }
}
