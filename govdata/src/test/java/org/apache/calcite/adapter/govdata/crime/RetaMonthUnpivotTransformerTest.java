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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.RowContext;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("unit")
class RetaMonthUnpivotTransformerTest {

  private static RowContext contextForYear(String year) {
    return RowContext.builder()
        .dimensionValues(Collections.singletonMap("year", year))
        .rowNumber(0)
        .build();
  }

  @Test void unpivotsOneAgencyRowIntoTwelveMonthRows() {
    Map<String, Object> wide = new LinkedHashMap<String, Object>();
    // header fields (no month prefix)
    wide.put("ori", "AK00101");
    wide.put("state", "50");
    wide.put("agency_name", "ANCHORAGE");
    wide.put("year", "23"); // two-digit file year — must be overridden by the dimension
    // monthly fields for two months only; others omitted for brevity
    wide.put("jan_actual_murder", "00002");
    wide.put("jan_actual_robbery_total", "00027");
    wide.put("dec_actual_burglary_total", "00041");

    List<Map<String, Object>> rows =
        new RetaMonthUnpivotTransformer().transform(wide, contextForYear("2023"));

    assertEquals(12, rows.size(), "one agency-year row expands to 12 month rows");

    Map<String, Object> jan = rows.get(0);
    assertEquals(1, jan.get("month"));
    assertEquals(2023, asInt(jan.get("year")), "four-digit year comes from the dimension");
    // header copied to every month
    assertEquals("AK00101", jan.get("ori"));
    assertEquals("ANCHORAGE", jan.get("agency_name"));
    // january's monthly field, prefix stripped
    assertEquals("00002", jan.get("actual_murder"));
    assertEquals("00027", jan.get("actual_robbery_total"));
    // january must not carry december's field
    assertFalse(jan.containsKey("actual_burglary_total"));

    Map<String, Object> dec = rows.get(11);
    assertEquals(12, dec.get("month"));
    assertEquals("AK00101", dec.get("ori"));
    assertEquals("00041", dec.get("actual_burglary_total"));
    assertFalse(dec.containsKey("actual_murder"));

    // the raw month-prefixed keys must not leak into the output
    assertFalse(jan.containsKey("jan_actual_murder"));
  }

  private static int asInt(Object o) {
    return o instanceof Number ? ((Number) o).intValue() : Integer.parseInt(String.valueOf(o));
  }
}
