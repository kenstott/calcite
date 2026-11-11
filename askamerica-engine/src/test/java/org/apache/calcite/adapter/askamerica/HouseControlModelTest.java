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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The fitted House-control coefficients, worked by hand. */
@Tag("unit")
class HouseControlModelTest {

  @Test void the2026CallFromA2024LagGivesAboutSeventySevenPercent() {
    // Republican president, midterm, 2024 national D two-party House vote 48.65%:
    // 0.501 - 0.002 + 0.053 + 1.47 * (0.4865 - 0.5) = 0.532155 seat share.
    double share = HouseControlModel.seatShare(false, true, 0.4865);
    assertEquals(0.532155, share, 1e-9);
    // z = (218/435 - 0.53215) / 0.041 = -0.7563, so P = Phi(0.7563) = 0.775.
    double p = HouseControlModel.probabilityDemocraticControl(false, true, 0.4865);
    assertEquals(0.77, p, 0.01);
    assertEquals(0.775, p, 0.002);
  }

  @Test void aSittingDemocratOnAMidtermLosesGroundAndAPresidentialYearDoesNot() {
    double midterm = HouseControlModel.seatShare(true, true, 0.5);
    double presidentialYear = HouseControlModel.seatShare(true, false, 0.5);
    assertEquals(0.501 + 0.002 - 0.053, midterm, 1e-12);
    assertEquals(0.501 + 0.002, presidentialYear, 1e-12);
  }

  @Test void aLevelHouseVoteWithNoMidtermIsAboutAToss() {
    double p = HouseControlModel.probabilityDemocraticControl(true, false, 0.5);
    assertTrue(p > 0.5 && p < 0.6, "p=" + p);
  }

  @Test void theVoteShareMustBeAShare() {
    assertThrows(IllegalArgumentException.class,
        () -> HouseControlModel.probabilityDemocraticControl(true, true, 48.65));
    assertThrows(IllegalArgumentException.class,
        () -> HouseControlModel.probabilityDemocraticControl(true, true, 0));
  }

  @Test void theSourceCarriesTheStatedLimits() {
    String s = HouseControlModel.SOURCE;
    assertTrue(s.contains("Brier 0.109"), s);
    assertTrue(s.contains("n=24"), s);
    assertTrue(s.contains("chosen on the same 24 points"), s);
    assertTrue(s.contains("House only"), s);
  }
}
