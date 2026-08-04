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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * G9 - {@code financial_line_items.value_numeric} must be the value the filer reported, with the
 * iXBRL {@code scale} factor already applied, rather than the value as displayed in the document.
 *
 * <p>Before this, {@code value_numeric} held the raw displayed text: a statement "in thousands"
 * tagging 1,234 with {@code scale="3"} stored 1234 instead of 1234000, so every query against the
 * base table understated modern (inline XBRL) filings by the scale factor. Only the
 * {@code financial_facts} view compensated; {@code revenue_trends} and any direct query did not.
 */
@Tag("unit")
class XbrlScaleFactorTest {

  @Test @DisplayName("scale=3 (statement in thousands) multiplies the displayed value")
  void testThousandsScale() {
    assertEquals(Double.valueOf(1234000.0),
        XbrlToParquetConverter.applyScaleFactor(1234.0, 3));
  }

  @Test @DisplayName("scale=6 (statement in millions) multiplies the displayed value")
  void testMillionsScale() {
    assertEquals(Double.valueOf(2500000.0),
        XbrlToParquetConverter.applyScaleFactor(2.5, 6));
  }

  @Test @DisplayName("negative scale divides, e.g. a per-unit value shown scaled up")
  void testNegativeScale() {
    assertEquals(Double.valueOf(12.34),
        XbrlToParquetConverter.applyScaleFactor(1234.0, -2));
  }

  @Test @DisplayName("scale=0 leaves traditional (non-inline) XBRL values untouched")
  void testZeroScaleIsIdentity() {
    assertEquals(Double.valueOf(4567.0),
        XbrlToParquetConverter.applyScaleFactor(4567.0, 0));
  }

  @Test @DisplayName("absent scale leaves the value untouched")
  void testNullScaleIsIdentity() {
    assertEquals(Double.valueOf(4567.0),
        XbrlToParquetConverter.applyScaleFactor(4567.0, null));
  }

  @Test @DisplayName("an unparseable fact stays null regardless of scale")
  void testNullValueStaysNull() {
    assertNull(XbrlToParquetConverter.applyScaleFactor(null, 3));
    assertNull(XbrlToParquetConverter.applyScaleFactor(null, null));
  }

  @Test @DisplayName("scaling is exact - no binary floating-point drift into a dollar figure")
  void testScalingIsExact() {
    // 1.1 * 1000 in double arithmetic is 1100.0000000000001; shifting the decimal exponent
    // via BigDecimal keeps the reported dollar figure exact.
    assertEquals(Double.valueOf(1100.0),
        XbrlToParquetConverter.applyScaleFactor(1.1, 3));
    assertEquals(Double.valueOf(3300.0),
        XbrlToParquetConverter.applyScaleFactor(3.3, 3));
  }
}
