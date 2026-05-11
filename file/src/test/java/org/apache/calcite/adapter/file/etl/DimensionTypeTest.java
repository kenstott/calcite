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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link DimensionType}.
 */
@Tag("unit")
class DimensionTypeTest {

  @Test void testFromStringRange() {
    assertEquals(DimensionType.RANGE, DimensionType.fromString("range"));
  }

  @Test void testFromStringList() {
    assertEquals(DimensionType.LIST, DimensionType.fromString("list"));
  }

  @Test void testFromStringQuery() {
    assertEquals(DimensionType.QUERY, DimensionType.fromString("query"));
  }

  @Test void testFromStringYearRange() {
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("yearRange"));
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("year_range"));
  }

  @Test void testFromStringCustom() {
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("custom"));
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("catalog"));
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("resolver"));
  }

  @Test void testFromStringJsonCatalog() {
    assertEquals(DimensionType.JSON_CATALOG, DimensionType.fromString("json_catalog"));
    assertEquals(DimensionType.JSON_CATALOG, DimensionType.fromString("jsoncatalog"));
  }

  @Test void testFromStringNull() {
    assertEquals(DimensionType.LIST, DimensionType.fromString(null));
  }

  @Test void testFromStringEmpty() {
    assertEquals(DimensionType.LIST, DimensionType.fromString(""));
  }

  @Test void testFromStringUnknown() {
    assertEquals(DimensionType.LIST, DimensionType.fromString("unknown_type"));
  }

  @Test void testFromStringCaseInsensitive() {
    assertEquals(DimensionType.RANGE, DimensionType.fromString("RANGE"));
    assertEquals(DimensionType.RANGE, DimensionType.fromString("Range"));
    assertEquals(DimensionType.QUERY, DimensionType.fromString("QUERY"));
  }
}
