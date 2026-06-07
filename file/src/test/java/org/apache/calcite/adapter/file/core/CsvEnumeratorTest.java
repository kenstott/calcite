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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for the {@link CsvEnumerator}.
 */
@Tag("unit")
@SuppressWarnings("SameParameterValue")
class CsvEnumeratorTest {

  @Test void testParseDecimalScaleRounding() {
    checkParse("123.45", 5, 2, "123.45");
    checkParse("123.455", 5, 2, "123.46");
    checkParse("-123.455", 5, 2, "-123.46");
    checkParse("123.454", 5, 2, "123.45");
    checkParse("-123.454", 5, 2, "-123.45");
  }

  private static void checkParse(String s, int precision, int scale,
      String expected) {
    assertThat(CsvEnumerator.parseDecimal(precision, scale, s),
        is(new BigDecimal(expected)));
  }

  @Test void testParseDecimalPrecisionExceeded() {
    checkThrows(4, 0, "1e+5");
    checkThrows(4, 0, "-1e+5");
    checkThrows(4, 0, "12345");
    checkThrows(4, 0, "-12345");
    checkThrows(4, 2, "123.45");
    checkThrows(4, 2, "-123.45");
  }

  private static void checkThrows(int precision, int scale, String s) {
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(precision, scale, s));
  }
}
