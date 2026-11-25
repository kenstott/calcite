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
package org.apache.calcite.adapter.govdata.edu;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link IpedsTuitionResponseTransformer}. */
@Tag("unit")
class IpedsTuitionResponseTransformerTest {

  private final IpedsTuitionResponseTransformer transformer =
      new IpedsTuitionResponseTransformer();

  @Test void missingCodesInChargeColumnsBecomeNull() {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("unitid", 107877);
    row.put("tuition_fees_ft", -2);
    row.put("tuition_ft", -1);
    row.put("fees_ft", -3);
    row.put("credit_hour_charge_pt", 430);
    row.put("tuition_published", -2);
    row.put("fees_published", 0);
    row.put("tuition_fees_published", null);

    transformer.transformRecord(row, null);

    assertTrue(row.containsKey("tuition_fees_ft"));
    assertNull(row.get("tuition_fees_ft"));
    assertNull(row.get("tuition_ft"));
    assertNull(row.get("fees_ft"));
    assertNull(row.get("tuition_published"));
    assertNull(row.get("tuition_fees_published"));
    assertEquals(430, row.get("credit_hour_charge_pt"));
    assertEquals(0, row.get("fees_published"));
    assertEquals(107877, row.get("unitid"));
  }

  @Test void otherNegativeValuesAreLeftForDqToReport() {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("tuition_ft", -500);

    transformer.transformRecord(row, null);

    assertEquals(-500, row.get("tuition_ft"));
  }
}
