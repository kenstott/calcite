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
package org.apache.calcite.adapter.govdata.lands;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link FiaLookups}.
 *
 * <p>OWNGRPCD domain values are taken from the FIADB Phase 2 User Guide, COND table,
 * section 2.5.13: only 10/20/30/40 exist, 30 is the combined "State and local government"
 * group (OWNCD 31/32/33), and 40 is "Private" (OWNCD 41-46).
 */
@Tag("unit")
class FiaLookupsTest {

  @Test void resolveOwnGrpMapsAllFourDocumentedCodes() {
    assertEquals("National Forest", FiaLookups.resolveOwnGrp(10));
    assertEquals("Other Federal", FiaLookups.resolveOwnGrp(20));
    assertEquals("State and local government", FiaLookups.resolveOwnGrp(30));
    assertEquals("Private", FiaLookups.resolveOwnGrp(40));
  }

  @Test void resolveOwnGrpReturnsNullForCodesOutsideTheDocumentedDomain() {
    // 50 does not exist in the OWNGRPCD domain; it must not silently resolve to "Private".
    assertNull(FiaLookups.resolveOwnGrp(50));
    assertNull(FiaLookups.resolveOwnGrp(0));
    assertNull(FiaLookups.resolveOwnGrp(-1));
  }
}
