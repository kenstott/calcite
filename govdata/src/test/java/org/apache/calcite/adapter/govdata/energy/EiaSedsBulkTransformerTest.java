/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Validates {@link EiaSedsBulkTransformer}: SEDS series parse + explode (unit) and the real bulk path (integration). */
class EiaSedsBulkTransformerTest {

  private static final ObjectMapper M = new ObjectMapper();

  @Test @Tag("unit") void explodesSedsSeriesAndClassifiesValue() throws Exception {
    String s = "{\"series_id\":\"SEDS.TETCB.CA.A\",\"name\":\"Total energy consumption, California\","
        + "\"units\":\"Billion Btu\",\"f\":\"A\","
        + "\"data\":[[\"2024\",6855450],[\"2023\",6814876]]}";

    List<Map<String, Object>> rows = EiaSedsBulkTransformer.rowsForSeriesStatic(M.readTree(s));

    assertEquals(2, rows.size());
    Map<String, Object> r0 = rows.get(0);
    assertEquals(2024, r0.get("year"));
    assertEquals(2024, r0.get("consumption_year"));
    assertEquals("CA", r0.get("state_abbr"));
    assertEquals("California", r0.get("state_name"));
    assertEquals("TETCB", r0.get("msn"));
    assertEquals(6855450.0, r0.get("value"));
    assertEquals("Billion Btu", r0.get("units"));
    // btu units -> consumption column; expenditure/price null
    assertEquals(6855450.0, r0.get("consumption_bbtu"));
    assertNull(r0.get("expenditure_million"));
    assertNull(r0.get("price_per_mmbtu"));
  }

  @Test @Tag("unit") void priceUnitsRouteToPriceColumn() throws Exception {
    // "Dollars per Million Btu" contains both 'btu' and 'dollar' but is a price.
    String s = "{\"series_id\":\"SEDS.NGRCD.TX.A\",\"name\":\"Natural gas price, Texas\","
        + "\"units\":\"Dollars per Million Btu\",\"f\":\"A\",\"data\":[[\"2023\",4.5]]}";
    Map<String, Object> r = EiaSedsBulkTransformer.rowsForSeriesStatic(M.readTree(s)).get(0);
    assertEquals(4.5, r.get("price_per_mmbtu"));
    assertNull(r.get("consumption_bbtu"));
    assertNull(r.get("expenditure_million"));
  }

  @Test @Tag("unit") void expenditureUnitsRouteToExpenditureColumn() throws Exception {
    String s = "{\"series_id\":\"SEDS.TETCD.CA.A\",\"name\":\"Total expenditures, California\","
        + "\"units\":\"Million dollars\",\"f\":\"A\",\"data\":[[\"2023\",50000]]}";
    Map<String, Object> r = EiaSedsBulkTransformer.rowsForSeriesStatic(M.readTree(s)).get(0);
    assertEquals(50000.0, r.get("expenditure_million"));
    assertNull(r.get("consumption_bbtu"));
    assertNull(r.get("price_per_mmbtu"));
  }

  @Test @Tag("unit") void percentAndPhysicalUnitsRouteToNoMetricColumn() throws Exception {
    // "Percent" contains the substring "per" but is a share series, not a price; physical units
    // (barrels) are neither Btu nor dollars. None should populate a metric column — only value.
    String pct = "{\"series_id\":\"SEDS.NUETD.US.A\",\"name\":\"Nuclear share, U.S.\","
        + "\"units\":\"Percent\",\"f\":\"A\",\"data\":[[\"2023\",18.6]]}";
    Map<String, Object> rp = EiaSedsBulkTransformer.rowsForSeriesStatic(M.readTree(pct)).get(0);
    assertEquals(18.6, rp.get("value"));
    assertNull(rp.get("price_per_mmbtu"));
    assertNull(rp.get("consumption_bbtu"));
    assertNull(rp.get("expenditure_million"));

    String bbl = "{\"series_id\":\"SEDS.PASCB.TX.A\",\"name\":\"Petroleum, Texas\","
        + "\"units\":\"Thousand barrels\",\"f\":\"A\",\"data\":[[\"2023\",1000]]}";
    Map<String, Object> rb = EiaSedsBulkTransformer.rowsForSeriesStatic(M.readTree(bbl)).get(0);
    assertNull(rb.get("consumption_bbtu"));
    assertNull(rb.get("expenditure_million"));
    assertNull(rb.get("price_per_mmbtu"));
  }

  @Test @Tag("unit") void nonSedsSeriesFilteredOut() throws Exception {
    String s = "{\"series_id\":\"SEDS.TETCB.US.M\",\"data\":[[\"202401\",1]]}"; // monthly, not .A
    assertTrue(EiaSedsBulkTransformer.rowsForSeriesStatic(M.readTree(s)).isEmpty());
  }

  @Test @Tag("integration") void streamsRealBulkFile() throws Exception {
    RequestContext ctx = RequestContext.builder()
        .url("https://www.eia.gov/opendata/bulk/SEDS.zip").build();
    Iterator<Map<String, Object>> it = new EiaSedsBulkTransformer().fetchAndTransform(ctx);
    int rows = 0;
    Set<Object> states = new HashSet<Object>();
    while (it.hasNext() && rows < 200000) {
      Map<String, Object> r = it.next();
      rows++;
      states.add(r.get("state_abbr"));
    }
    assertTrue(rows > 10000, "expected many SEDS rows, got " + rows);
    assertTrue(states.size() >= 50, "expected >=50 states, got " + states.size());
  }
}
