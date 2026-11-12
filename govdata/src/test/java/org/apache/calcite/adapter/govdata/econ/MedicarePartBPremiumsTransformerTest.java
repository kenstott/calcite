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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class MedicarePartBPremiumsTransformerTest {

  private static final String T26 = "Medicare Program; Medicare Part B Monthly Actuarial Rates, "
      + "Premium Rates, and Annual Deductible Beginning January 1, 2026";

  @Test void parsesStandardWording() {
    Map<String, Object> r = MedicarePartBPremiumsTransformer.parseNotice(T26, "2025-20251",
        "2025-11-19",
        "The Part B standard monthly premium rate for all enrollees for\n2026 is\n$202.90. "
            + "The Part B annual deductible for 2026 is $283.00 for all");
    assertEquals(2026, r.get("year"));
    assertEquals(202.90, (Double) r.get("standard_premium"), 0.0);
    assertEquals(283.00, (Double) r.get("annual_deductible"), 0.0);
  }

  @Test void parsesOlderWordings() {
    String t05 = "Medicare Part B Monthly Actuarial Rates, Premium Rate, and Annual Deductible "
        + "Beginning January 1, 2005";
    Map<String, Object> r05 = MedicarePartBPremiumsTransformer.parseNotice(t05, "04-20412", "x",
        "The monthly Part B premium rate for 2005 is $78.20. The Part B deductible for 2005 is "
            + "$110.00.");
    assertEquals(78.20, (Double) r05.get("standard_premium"), 0.0);
    assertEquals(110.00, (Double) r05.get("annual_deductible"), 0.0);
    String t07 = "Medicare Part B Monthly Actuarial Rates, Premium Rates, and Annual Deductible "
        + "for Calendar Year 2007";
    Map<String, Object> r07 = MedicarePartBPremiumsTransformer.parseNotice(t07, "06-7709", "x",
        "The monthly Part B premium rates to be paid in 2007, including the income-related "
            + "monthly adjustment amounts, are $93.50 (the standard premium), $106.00. The "
            + "annual deductible for 2007 is $131.00");
    assertEquals(93.50, (Double) r07.get("standard_premium"), 0.0);
  }

  @Test void ignoresFiguresForOtherYears() {
    // The 2026 notice also states the 2025 premium; only the notice's own year counts.
    Map<String, Object> r = MedicarePartBPremiumsTransformer.parseNotice(T26, "d", "x",
        "than the 2025 standard monthly premium rate for 2025 is $185.00. The Part B standard "
            + "monthly premium rate for all enrollees for 2026 is $202.90. deductible for 2026 is "
            + "$283.00");
    assertEquals(202.90, (Double) r.get("standard_premium"), 0.0);
  }

  @Test void failsWhenPremiumMissing() {
    assertThrows(IllegalStateException.class,
        () -> MedicarePartBPremiumsTransformer.parseNotice(T26, "d", "x",
            "deductible for 2026 is $283.00"));
  }

  @Test void filtersNotices() {
    assertTrue(MedicarePartBPremiumsTransformer.isAnnualNotice(T26));
    assertFalse(MedicarePartBPremiumsTransformer.isAnnualNotice(T26 + "; Correction"));
    assertFalse(MedicarePartBPremiumsTransformer.isAnnualNotice(
        "Medicare and Medicaid Programs; Quarterly Listing of Program Issuances"));
    assertFalse(MedicarePartBPremiumsTransformer.isAnnualNotice(
        "Hospice Wage Index; Medicare Part B Monthly Actuarial Rates, Corrections"));
  }
}
