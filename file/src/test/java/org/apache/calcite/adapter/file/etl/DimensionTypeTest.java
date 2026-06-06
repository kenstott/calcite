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
