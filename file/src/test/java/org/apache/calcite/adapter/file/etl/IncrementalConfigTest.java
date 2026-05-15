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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link HttpSourceConfig.IncrementalConfig}.
 */
@Tag("unit")
class IncrementalConfigTest {

  // ── fromMap parsing ────────────────────────────────────────────────────────

  @Test void fromMapParsesDateFilter() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("sinceDate", "2024-01-01");
    map.put("filterParam", "$where");
    map.put("dateField", "date");

    HttpSourceConfig.IncrementalConfig cfg = HttpSourceConfig.IncrementalConfig.fromMap(map);

    assertEquals("2024-01-01", cfg.getSinceDate());
    assertEquals("$where", cfg.getFilterParam());
    assertEquals("date", cfg.getDateField());
    assertNull(cfg.getSinceYear());
    assertNull(cfg.getSinceQuarter());
  }

  @Test void fromMapParsesYearQuarterFilter() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("sinceYear", "2023");
    map.put("sinceQuarter", "3");
    map.put("filterParam", "$where");
    map.put("yearField", "year");
    map.put("quarterField", "quarter");

    HttpSourceConfig.IncrementalConfig cfg = HttpSourceConfig.IncrementalConfig.fromMap(map);

    assertEquals("2023", cfg.getSinceYear());
    assertEquals("3", cfg.getSinceQuarter());
    assertEquals("year", cfg.getYearField());
    assertEquals("quarter", cfg.getQuarterField());
  }

  @Test void fromMapParsesDirectParam() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("sinceDate", "2024-06-01");
    map.put("filterParam", "lastUpdatePostDate.gte");

    HttpSourceConfig.IncrementalConfig cfg = HttpSourceConfig.IncrementalConfig.fromMap(map);

    assertEquals("lastUpdatePostDate.gte", cfg.getFilterParam());
    assertNull(cfg.getDateField());
  }

  @Test void fromMapNullReturnsNull() {
    assertNull(HttpSourceConfig.IncrementalConfig.fromMap(null));
  }

  // ── buildFilterValue: WHERE-date style ────────────────────────────────────

  @Test void buildFilterValueDateWhereStyle() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", "date", null, null);

    String filter = cfg.buildFilterValue("2024-01-01", null, null, null, null);
    assertEquals("date >= '2024-01-01'", filter);
  }

  @Test void buildFilterValueDateWhereStyleDifferentField() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", "weekendingdate", null, null);

    String filter = cfg.buildFilterValue("2024-03-15", null, null, null, null);
    assertEquals("weekendingdate >= '2024-03-15'", filter);
  }

  // ── buildFilterValue: WHERE-year style ────────────────────────────────────

  @Test void buildFilterValueYearOnlyWhereStyle() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", null, "year", null);

    String filter = cfg.buildFilterValue(null, "2023", null, null, null);
    assertEquals("year >= '2023'", filter);
  }

  @Test void buildFilterValueYearAndQuarterWhereStyle() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", null, "year", "quarter");

    String filter = cfg.buildFilterValue(null, "2023", "3", null, null);
    assertEquals("(year > '2023') OR (year = '2023' AND quarter >= '3')", filter);
  }

  @Test void buildFilterValueYearAndQuarterQ1() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", null, "year", "quarter");

    String filter = cfg.buildFilterValue(null, "2024", "1", null, null);
    assertEquals("(year > '2024') OR (year = '2024' AND quarter >= '1')", filter);
  }

  // ── buildFilterValue: direct-param style ──────────────────────────────────

  @Test void buildFilterValueDirectParam() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "lastUpdatePostDate.gte", null, null, null);

    String filter = cfg.buildFilterValue("2024-06-01", null, null, null, null);
    assertEquals("2024-06-01", filter);
  }

  // ── buildFilterValue: inactive (no bounds set) ────────────────────────────

  @Test void buildFilterValueReturnsNullWhenNoDateSet() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", "date", null, null);

    assertNull(cfg.buildFilterValue(null, null, null, null, null));
  }

  @Test void buildFilterValueReturnsNullWhenEmptyDate() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", "date", null, null);

    assertNull(cfg.buildFilterValue("", "", "", null, null));
  }

  @Test void buildFilterValueReturnsNullWhenYearEmpty() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", null, "year", "quarter");

    assertNull(cfg.buildFilterValue(null, "", "3", null, null));
  }

  // ── buildFilterValue: unquoted numeric style ──────────────────────────────

  @Test void buildFilterValueNumericYearUnquoted() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", null, "year", null, false);

    String filter = cfg.buildFilterValue(null, "2023", null, null, null);
    assertEquals("year >= 2023", filter);
  }

  @Test void buildFilterValueNumericYearAndQuarterUnquoted() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", null, "year", "quarter", false);

    String filter = cfg.buildFilterValue(null, "2023", "3", null, null);
    assertEquals("(year > 2023) OR (year = 2023 AND quarter >= 3)", filter);
  }

  @Test void buildFilterValueNumericDateUnquoted() {
    HttpSourceConfig.IncrementalConfig cfg = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", "year", null, null, false);

    String filter = cfg.buildFilterValue("2024", null, null, null, null);
    assertEquals("year >= 2024", filter);
  }

  @Test void fromMapQuoteValuesFalse() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("sinceYear", "2023");
    map.put("filterParam", "$where");
    map.put("yearField", "year");
    map.put("quoteValues", false);

    HttpSourceConfig.IncrementalConfig cfg = HttpSourceConfig.IncrementalConfig.fromMap(map);

    assertEquals("year >= 2023", cfg.buildFilterValue(null, "2023", null, null, null));
  }

  @Test void fromMapQuoteValuesTrueByDefault() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("sinceYear", "2023");
    map.put("filterParam", "$where");
    map.put("yearField", "year");

    HttpSourceConfig.IncrementalConfig cfg = HttpSourceConfig.IncrementalConfig.fromMap(map);

    assertEquals("year >= '2023'", cfg.buildFilterValue(null, "2023", null, null, null));
  }

  // ── HttpSourceConfig round-trip via fromMap ────────────────────────────────

  @Test void httpSourceConfigParsesIncrementalBlock() {
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://example.com/api");

    Map<String, Object> incrementalMap = new LinkedHashMap<String, Object>();
    incrementalMap.put("sinceDate", "${HEALTH_SINCE_DATE:}");
    incrementalMap.put("filterParam", "$where");
    incrementalMap.put("dateField", "date");
    sourceMap.put("incremental", incrementalMap);

    HttpSourceConfig cfg = HttpSourceConfig.fromMap(sourceMap);

    HttpSourceConfig.IncrementalConfig incr = cfg.getIncremental();
    assertEquals("${HEALTH_SINCE_DATE:}", incr.getSinceDate());
    assertEquals("$where", incr.getFilterParam());
    assertEquals("date", incr.getDateField());
  }

  @Test void httpSourceConfigIncrementalNullWhenAbsent() {
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://example.com/api");

    HttpSourceConfig cfg = HttpSourceConfig.fromMap(sourceMap);
    assertNull(cfg.getIncremental());
  }
}
