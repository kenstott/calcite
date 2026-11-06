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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the STATE shapes the NPS ArcGIS service emits: single state, hyphenated list,
 * "X TO Y" range, territory name, and a trailing numbered footnote.
 */
@Tag("unit")
class NpsUnitBoundaryTransformerTest {

  private static java.util.List<String> parse(String raw) {
    return NpsUnitBoundaryTransformer.parseStates("TEST", raw);
  }

  @Test void singleStateIsUnchanged() {
    assertEquals(Collections.singletonList("CA"), parse("CA"));
  }

  @Test void hyphenatedListKeepsSourceOrder() {
    assertEquals(Arrays.asList("TN", "MS", "AL"), parse("TN-MS-AL"));
  }

  @Test void footnoteMarkerIsStripped() {
    assertEquals(Collections.singletonList("CO"), parse("CO 1/"));
    assertEquals(Arrays.asList("MD", "VA"), parse("MD-VA 2/"));
  }

  @Test void rangeKeepsEndStates() {
    assertEquals(Arrays.asList("ME", "GA"), parse("ME TO GA 4/"));
    assertEquals(Arrays.asList("CT", "MA"), parse("CT TO MA"));
  }

  @Test void territoryNamesMapToUspsCodes() {
    assertEquals(Collections.singletonList("GU"), parse("GUAM"));
    assertEquals(Collections.singletonList("AS"), parse("SAMOA"));
    assertEquals(Collections.singletonList("MP"), parse("NMI"));
  }

  @Test void nullStateYieldsNoStates() {
    assertEquals(Collections.emptyList(), parse(null));
  }

  @Test void unrecognisedValueFailsLoudly() {
    assertThrows(IllegalStateException.class, () -> parse("ATLANTIS"));
  }
}
