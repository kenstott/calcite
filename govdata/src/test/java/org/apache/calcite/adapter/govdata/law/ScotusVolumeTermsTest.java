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
package org.apache.calcite.adapter.govdata.law;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ScotusVolumeTerms}; the courtTerm values are GovInfo's real ones.
 */
@Tag("unit")
class ScotusVolumeTermsTest {

  private static ScotusVolumeTerms terms() {
    Map<Integer, String> m = new HashMap<Integer, String>();
    m.put(2, "1781-1793");
    m.put(3, "1794-1799");
    m.put(50, "1850");
    m.put(571, "2013");
    m.put(572, "2013");
    m.put(583, "2017");
    return new ScotusVolumeTerms(m);
  }

  @Test void singleYearTermMapsToItsVolume() {
    assertEquals(Collections.singletonList(583), terms().volumesFor(2017));
    assertEquals(Collections.singletonList(50), terms().volumesFor(1850));
  }

  @Test void severalVolumesShareATermInVolumeOrder() {
    assertEquals(Arrays.asList(571, 572), terms().volumesFor(2013));
  }

  @Test void aSpanBelongsToItsFirstYearOnly() {
    assertEquals(Collections.singletonList(2), terms().volumesFor(1781));
    assertTrue(terms().volumesFor(1790).isEmpty());
    assertEquals(Collections.singletonList(3), terms().volumesFor(1794));
  }

  @Test void aTermWithNoVolumeIsEmptyNotAnError() {
    assertTrue(terms().volumesFor(1900).isEmpty());
  }

  @Test void firstAndLastTerm() {
    assertEquals(1781, terms().firstTerm());
    assertEquals(2017, terms().lastTerm());
  }

  @Test void aCourtTermWithoutALeadingYearIsRejected() {
    Map<Integer, String> bad = new HashMap<Integer, String>();
    bad.put(9, "October term");
    assertThrows(IllegalArgumentException.class, () -> new ScotusVolumeTerms(bad));
    Map<Integer, String> missing = new HashMap<Integer, String>();
    missing.put(9, null);
    assertThrows(IllegalArgumentException.class, () -> new ScotusVolumeTerms(missing));
  }
}
