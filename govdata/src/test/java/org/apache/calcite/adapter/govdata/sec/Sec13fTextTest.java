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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link Sec13fText}. */
@Tag("unit")
class Sec13fTextTest {
  @Test void decodesRepeatedEscaping() {
    assertEquals("MCCORMICK & CO", Sec13fText.normalize("MCCORMICK &amp;amp;amp; CO"));
    assertEquals("AT&T", Sec13fText.normalize("AT&amp;T"));
    assertEquals("A<B>\"C\"'D' E", Sec13fText.normalize("A&lt;B&gt;&quot;C&quot;&apos;D&#39; E"));
    assertEquals("LEGGETT&PLATT", Sec13fText.normalize("LEGGETT&#38;PLATT"));
  }

  @Test void restoresLostAmpersand() {
    assertEquals("S&P500 EQL UTL", Sec13fText.normalize("Samp;P500 EQL UTL"));
    assertEquals("INVESCO S&P 500", Sec13fText.normalize("INVESCO SANDamp;P 500"));
    assertEquals("PG&E CORP", Sec13fText.normalize("PGamp;E CORP"));
  }

  @Test void leavesOtherTextAlone() {
    assertEquals("APPLE INC", Sec13fText.normalize("APPLE INC"));
    assertEquals("AMP; X", Sec13fText.normalize("AMP; X"));
    assertNull(Sec13fText.normalize(null));
  }
}
