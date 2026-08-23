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
package org.apache.calcite.adapter.govdata.officials;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link ElectoralCollegeTransformer#stripFootnote}: NARA marks footnotes with either a
 * trailing superscript digit (most pages) or one or more trailing asterisks (confirmed live:
 * 2008, 2016, 2020 -- e.g. "Texas***" in 2016 vs plain "Texas" every other year). The digit-only
 * pattern originally used let asterisk-marked names through unstripped, silently breaking any
 * exact-match join/filter on state_name for the affected state+year.
 */
@Tag("unit")
class ElectoralCollegeTransformerTest {

  private final ElectoralCollegeTransformer transformer = new ElectoralCollegeTransformer();

  @ParameterizedTest
  @CsvSource({
      "Texas,            Texas",
      "Texas***,         Texas",
      "Maine*,           Maine",
      "Maine**,          Maine",
      "'Nebraska *',     Nebraska",
      "Washington****,   Washington",
      "Texas1,           Texas",
      "Texas12,          Texas",
      "'New York',       New York"
  })
  void stripsTrailingFootnoteMarkers(String raw, String expected) {
    assertEquals(expected, transformer.stripFootnote(raw));
  }

  @Test void leavesANameWithNoFootnoteUnchanged() {
    assertEquals("California", transformer.stripFootnote("California"));
  }

  @ParameterizedTest
  @CsvSource({
      // Real mismatches confirmed live between NARA's summary block and its results table.
      "'Donald J. Trump',   trump",   // 2016: table says "Donald Trump" (no middle initial)
      "'Donald Trump',      trump",
      "'Bob Dole',          dole",    // 1996: table says "Robert Dole" (nickname)
      "'Robert Dole',       dole",
      "'Albert Gore, Jr.',  gore",    // trailing generational suffix must not become the key
      "'William J. Clinton', clinton"
  })
  void lastNameKeyReducesToTheSurname(String fullName, String expectedKey) {
    assertEquals(expectedKey, transformer.lastNameKey(fullName));
  }
}
