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
package org.apache.calcite.adapter.govdata.fiscal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link CboBaselineProvider#parseCsv}. */
@Tag("unit")
class CboBaselineProviderTest {

  private static JsonNode fields() throws IOException {
    return new ObjectMapper().readTree("{\"outlays_net_interest\":{\"description\":\"Net interest\","
        + "\"unit\":\"billions_of_dollars\",\"category\":\"outlays\"}}");
  }

  private static List<Map<String, Object>> parse(String csv) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    CboBaselineProvider.parseCsv(new BufferedReader(new StringReader(csv)), "historical_budget",
        "2026-02", fields(), "test", rows);
    return rows;
  }

  @Test void rowCarriesKeyValueAndSchemaMetadata() throws IOException {
    List<Map<String, Object>> rows =
        parse("date,variable,value\nFY2025,outlays_net_interest,969.938\n");
    assertEquals(1, rows.size());
    Map<String, Object> r = rows.get(0);
    assertEquals("historical_budget", r.get("dataset"));
    assertEquals("2026-02", r.get("vintage"));
    assertEquals(Integer.valueOf(2025), r.get("fiscal_year"));
    assertEquals(Double.valueOf(969.938), r.get("value"));
    assertEquals("billions_of_dollars", r.get("unit"));
    assertEquals("outlays", r.get("category"));
    assertEquals("Net interest", r.get("description"));
  }

  @Test void unknownVariableFails() {
    assertThrows(IOException.class,
        () -> parse("date,variable,value\nFY2025,mystery_var,1.0\n"));
  }

  @Test void blankValueFails() {
    assertThrows(IOException.class,
        () -> parse("date,variable,value\nFY2025,outlays_net_interest,\n"));
  }

  @Test void badDateFails() {
    assertThrows(IOException.class,
        () -> parse("date,variable,value\n2025,outlays_net_interest,1.0\n"));
  }
}
