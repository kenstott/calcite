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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.GovDataUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the series-enumeration column comments: every long-format series table documents
 * which series it loads, and the folded YAML scalars survive the comment loader intact.
 */
@Tag("unit")
public class SeriesCommentLoadTest {

  private static final String ECON_SCHEMA = "/econ/econ-schema.yaml";

  /**
   * Every metro CPI series must have a name in the area_name CASE expression. Guards the drift
   * that made 15 of 20 area codes point at the wrong metro: add a series, forget the name, fail.
   */
  @Test public void testEveryMetroCpiSeriesHasAName() {
    List<Map<String, Object>> tables =
        GovDataUtils.loadTableDefinitions(EconSchemaFactory.class, ECON_SCHEMA);
    Map<String, Object> metroCpi = null;
    for (Map<String, Object> t : tables) {
      if ("metro_cpi".equals(t.get("name"))) {
        metroCpi = t;
      }
    }
    assertNotNull(metroCpi, "metro_cpi table not found");

    @SuppressWarnings("unchecked")
    Map<String, Object> source = (Map<String, Object>) metroCpi.get("source");
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) source.get("body");
    @SuppressWarnings("unchecked")
    List<String> seriesIds = (List<String>) body.get("seriesid");
    assertFalse(seriesIds.isEmpty(), "no series configured for metro_cpi");

    String areaName = null;
    String areaCode = null;
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> columns = (List<Map<String, Object>>) metroCpi.get("columns");
    for (Map<String, Object> c : columns) {
      if ("area_name".equals(c.get("name"))) {
        areaName = (String) c.get("expression");
      } else if ("area_code".equals(c.get("name"))) {
        areaCode = (String) c.get("expression");
      }
    }
    assertNotNull(areaName, "metro_cpi.area_name has no expression - it would be all NULL");
    assertNotNull(areaCode, "metro_cpi.area_code has no expression - it would be all NULL");

    for (String id : seriesIds) {
      // CUUR{area}SA0 - the area code the area_code expression extracts
      String area = id.substring(4, 8);
      assertTrue(areaName.contains("'" + area + "'"),
          "metro_cpi.area_name has no name for area " + area + " (series " + id + ")");
    }
  }

  /**
   * The CPI tables must not declare percent_change columns: nothing populates them at ingest
   * (BlsResponseTransformer emits only series/year/period/value), so they can only be NULL.
   * Period-over-period change belongs in the *_cpi_inflation views, which must exist.
   */
  @Test public void testCpiPercentChangeLivesInViewsNotColumns() {
    List<Map<String, Object>> tables =
        GovDataUtils.loadTableDefinitions(EconSchemaFactory.class, ECON_SCHEMA);
    for (String table : new String[] {"metro_cpi", "regional_cpi"}) {
      Map<String, Object> def = null;
      for (Map<String, Object> t : tables) {
        if (table.equals(t.get("name"))) {
          def = t;
        }
      }
      assertNotNull(def, table + " not found");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> columns = (List<Map<String, Object>>) def.get("columns");
      for (Map<String, Object> c : columns) {
        String name = (String) c.get("name");
        assertFalse(name.startsWith("percent_change"),
            table + "." + name + " cannot be populated at ingest - use the "
                + table + "_inflation view");
      }
    }
    for (String view : new String[] {"metro_cpi_inflation", "regional_cpi_inflation"}) {
      String sql = null;
      for (Map<String, Object> t : tables) {
        if (view.equals(t.get("name")) && "view".equals(t.get("type"))) {
          sql = (String) t.get("sql");
        }
      }
      assertNotNull(sql, "view " + view + " is missing");
      assertTrue(sql.contains("percent_change_month") && sql.contains("percent_change_year"),
          view + " should expose both percent change columns");
      assertTrue(sql.contains("LAG("), view + " should compute change with LAG");
    }
  }

  private String seriesComment(String schema, String table, String column) {
    Map<String, String> comments =
        GovDataUtils.loadColumnComments(EconSchemaFactory.class, schema, table);
    assertNotNull(comments, "no column comments for " + table);
    String c = comments.get(column);
    assertNotNull(c, "no comment for " + table + "." + column);
    return c;
  }

  @Test public void testSeriesEnumerationsLoad() {
    String emp = seriesComment(ECON_SCHEMA, "employment_statistics", "series");
    for (String id : new String[] {"LNS14027659", "LNS12327662", "LNS13327709", "CES0500000007",
        "LNS13008276", "LNS14032183"}) {
      assertTrue(emp.contains(id), "employment_statistics comment missing " + id);
    }
    String fred = seriesComment(ECON_SCHEMA, "fred_indicators", "series");
    for (String id : new String[] {"DFF", "MORTGAGE30US", "USREC", "PCEPILFE"}) {
      assertTrue(fred.contains(id), "fred_indicators comment missing " + id);
    }
    String metro = seriesComment(ECON_SCHEMA, "metro_cpi", "series");
    assertTrue(metro.contains("CUURS12ASA0 New York-Newark-Jersey City"),
        "metro_cpi comment should label S12A as New York");
    assertTrue(metro.contains("CUURS49GSA0"), "metro_cpi comment missing S49G");
    String jolts = seriesComment(ECON_SCHEMA, "jolts_state", "series");
    assertTrue(jolts.contains("JO job openings") && jolts.contains("jolts_dataelements"),
        "jolts_state comment should list element codes and the decoder table");
    String line = seriesComment(ECON_SCHEMA, "state_gdp", "line_code");
    assertTrue(line.contains("regional_linecodes"),
        "state_gdp line_code comment should name the decoder table");
    // folded scalars keep a trailing newline, but must not embed internal line breaks
    for (String c : new String[] {emp, fred, metro, jolts, line}) {
      assertTrue(!c.trim().contains("\n"),
          "comment should fold to one line: " + c.substring(0, 40));
      assertTrue(c.length() > 300, "comment looks truncated: " + c.length() + " chars");
    }
  }
}
