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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PeriodFormat} and {@code {var:fmt}} substitution. */
@Tag("unit")
public class PeriodFormatTest {

  @Test void zeroPad() {
    assertEquals("03", PeriodFormat.render("3", "02d"));
    assertEquals("03", PeriodFormat.render("3", "%02d"));
    assertEquals("06", PeriodFormat.render("06", "02d"));
    assertEquals("6", PeriodFormat.render("06", "d"));
  }

  @Test void quarterPrefix() {
    assertEquals("Q3", PeriodFormat.render("3", "Q"));
    assertEquals("Q3", PeriodFormat.render("3", "Q%d"));
    assertEquals("W03", PeriodFormat.render("3", "W%02d"));
  }

  @Test void monthNames() {
    assertEquals("March", PeriodFormat.render("3", "B"));
    assertEquals("March", PeriodFormat.render("3", "%B"));
    assertEquals("Mar", PeriodFormat.render("3", "b"));
    assertEquals("December", PeriodFormat.render("12", "%B"));
  }

  @Test void nonNumericOrEmptyUnchanged() {
    assertEquals("abc", PeriodFormat.render("abc", "02d"));
    assertEquals("3", PeriodFormat.render("3", null));
    assertEquals("3", PeriodFormat.render("3", ""));
  }

  @Test void substitutionAppliesFormatForApiOnly() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2026");
    vars.put("month", "3");       // canonical value stays "3" in the map
    vars.put("quarter", "3");
    assertEquals("FR-2026-03.zip",
        VariableResolver.substitute("FR-{year}-{month:02d}.zip", vars));
    assertEquals("Q3", VariableResolver.substitute("{quarter:Q}", vars));
    assertEquals("March", VariableResolver.substitute("{month:B}", vars));
    // Bare {month} stays canonical (this is what partition/marker keying uses).
    assertEquals("3", VariableResolver.substitute("{month}", vars));
  }
}
