/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.duckdb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * These aggregates run only when the query could NOT be pushed down to DuckDB, so DuckDB is
 * not there to check them against at runtime. Every expected value below is therefore an
 * independently-derived one — hand-computed from the definition, or a case whose answer is
 * known a priori (an exact line has r = 1) — rather than a value observed from this code.
 */
@Tag("unit")
class DuckDBStatsFunctionsTest {

  /** y = 9.9x + 0.1 is not exact, so use a genuinely exact line for the r = 1 case. */
  private static DuckDBStatsFunctions.Regr exactLine() {
    DuckDBStatsFunctions.Regr a = DuckDBStatsFunctions.CorrUdaf.init();
    for (int x = 1; x <= 5; x++) {
      DuckDBStatsFunctions.CorrUdaf.add(a, 3.0 * x + 2.0, (double) x);
    }
    return a;
  }

  @Test @DisplayName("a perfect line gives r = 1, slope and intercept exactly")
  void perfectLine() {
    assertEquals(1.0, DuckDBStatsFunctions.CorrUdaf.result(exactLine()), 1e-12);
    assertEquals(3.0, DuckDBStatsFunctions.RegrSlopeUdaf.result(exactLine()), 1e-12);
    assertEquals(2.0, DuckDBStatsFunctions.RegrInterceptUdaf.result(exactLine()), 1e-12);
    assertEquals(1.0, DuckDBStatsFunctions.RegrR2Udaf.result(exactLine()), 1e-12);
    assertEquals(3.0, DuckDBStatsFunctions.RegrAvgXUdaf.result(exactLine()), 1e-12);
    assertEquals(11.0, DuckDBStatsFunctions.RegrAvgYUdaf.result(exactLine()), 1e-12);
  }

  @Test @DisplayName("a perfect ANTI-correlation gives r = -1, distinguishing sign")
  void negativeLine() {
    DuckDBStatsFunctions.Regr a = DuckDBStatsFunctions.CorrUdaf.init();
    for (int x = 1; x <= 4; x++) {
      DuckDBStatsFunctions.CorrUdaf.add(a, -2.0 * x, (double) x);
    }
    assertEquals(-1.0, DuckDBStatsFunctions.CorrUdaf.result(a), 1e-12);
    assertEquals(-2.0, DuckDBStatsFunctions.RegrSlopeUdaf.result(a), 1e-12);
    // r2 is the SQUARE, so it must stay positive where r is negative.
    assertEquals(1.0, DuckDBStatsFunctions.RegrR2Udaf.result(a), 1e-12);
  }

  @Test @DisplayName("r matches a hand-computed value on non-collinear data")
  void handComputedCorrelation() {
    // x = {1,2,3,4}, y = {2,4,5,9}: n=4, Sx=10, Sy=20, Sxx=30, Syy=126,
    // Sxy = 1*2 + 2*4 + 3*5 + 4*9 = 61.
    // sxx = 30 - 100/4 = 5; syy = 126 - 400/4 = 26; sxy = 61 - 200/4 = 11.
    // r = 11 / sqrt(5*26) = 11 / sqrt(130) = 0.9648 — and note r must be <= 1, which is
    // the arithmetic check that catches a slip in this comment.
    DuckDBStatsFunctions.Regr a = DuckDBStatsFunctions.CorrUdaf.init();
    double[] ys = {2, 4, 5, 9};
    for (int i = 0; i < 4; i++) {
      DuckDBStatsFunctions.CorrUdaf.add(a, ys[i], (double) (i + 1));
    }
    assertEquals(11.0 / Math.sqrt(130.0), DuckDBStatsFunctions.CorrUdaf.result(a), 1e-12);
    assertEquals(11.0 / 5.0, DuckDBStatsFunctions.RegrSlopeUdaf.result(a), 1e-12);
    assertEquals(11.0, DuckDBStatsFunctions.RegrSxyUdaf.result(a), 1e-12);
  }

  @Test @DisplayName("a row with either side null contributes to neither margin")
  void nullsSkippedOnBothSides() {
    DuckDBStatsFunctions.Regr a = DuckDBStatsFunctions.CorrUdaf.init();
    DuckDBStatsFunctions.CorrUdaf.add(a, 3.0, 1.0);
    DuckDBStatsFunctions.CorrUdaf.add(a, null, 2.0);     // y missing
    DuckDBStatsFunctions.CorrUdaf.add(a, 9.0, null);     // x missing
    DuckDBStatsFunctions.CorrUdaf.add(a, 5.0, 2.0);
    // Only (1,3) and (2,5) count: avgx = 1.5, not 1.67 (which is what counting a
    // half-null row into the x margin would give).
    assertEquals(1.5, DuckDBStatsFunctions.RegrAvgXUdaf.result(a), 1e-12);
    assertEquals(4.0, DuckDBStatsFunctions.RegrAvgYUdaf.result(a), 1e-12);
  }

  @Test @DisplayName("a constant column yields null, not a divide-by-zero NaN")
  void constantColumnIsNull() {
    DuckDBStatsFunctions.Regr a = DuckDBStatsFunctions.CorrUdaf.init();
    for (int x = 1; x <= 4; x++) {
      DuckDBStatsFunctions.CorrUdaf.add(a, 7.0, (double) x);   // y never varies
    }
    assertNull(DuckDBStatsFunctions.CorrUdaf.result(a));
    assertNull(DuckDBStatsFunctions.RegrR2Udaf.result(a));
    // slope is still defined when only y is constant: it is zero.
    assertEquals(0.0, DuckDBStatsFunctions.RegrSlopeUdaf.result(a), 1e-12);
  }

  @Test @DisplayName("too few rows yields null rather than a spurious statistic")
  void tooFewRows() {
    DuckDBStatsFunctions.Regr a = DuckDBStatsFunctions.CorrUdaf.init();
    DuckDBStatsFunctions.CorrUdaf.add(a, 1.0, 1.0);
    assertNull(DuckDBStatsFunctions.CorrUdaf.result(a));
  }

  private static DuckDBStatsFunctions.Values values(double... xs) {
    DuckDBStatsFunctions.Values v = DuckDBStatsFunctions.MedianUdaf.init();
    for (double x : xs) {
      DuckDBStatsFunctions.MedianUdaf.add(v, x);
    }
    return v;
  }

  @Test @DisplayName("median interpolates on even counts and ignores input order")
  void median() {
    assertEquals(3.0, DuckDBStatsFunctions.MedianUdaf.result(values(1, 2, 3, 4, 5)), 1e-12);
    assertEquals(2.5, DuckDBStatsFunctions.MedianUdaf.result(values(1, 2, 3, 4)), 1e-12);
    // Unsorted input must give the same answer as sorted input.
    assertEquals(2.5, DuckDBStatsFunctions.MedianUdaf.result(values(4, 1, 3, 2)), 1e-12);
  }

  @Test @DisplayName("mad is the median of absolute deviations about the median")
  void mad() {
    // values {1,2,3,4,100}: median 3; deviations {2,1,0,1,97}; their median is 1.
    // A mean-based implementation would give ~20 here, so this pins the definition.
    assertEquals(1.0, DuckDBStatsFunctions.MadUdaf.result(values(1, 2, 3, 4, 100)), 1e-12);
  }

  @Test @DisplayName("quantile_cont interpolates where quantile_disc picks a real member")
  void quantiles() {
    DuckDBStatsFunctions.Values c = DuckDBStatsFunctions.QuantileContUdaf.init();
    DuckDBStatsFunctions.Values d = DuckDBStatsFunctions.QuantileDiscUdaf.init();
    for (double x : new double[]{1, 2, 3, 4}) {
      DuckDBStatsFunctions.QuantileContUdaf.add(c, x, 0.25);
      DuckDBStatsFunctions.QuantileDiscUdaf.add(d, x, 0.25);
    }
    // pos = 0.25*3 = 0.75 -> between 1 and 2.
    assertEquals(1.75, DuckDBStatsFunctions.QuantileContUdaf.result(c), 1e-12);
    // disc must return a value that actually occurs in the input.
    assertEquals(1.0, DuckDBStatsFunctions.QuantileDiscUdaf.result(d), 1e-12);
  }

  @Test @DisplayName("a symmetric distribution has zero skew; a right tail is positive")
  void skewness() {
    DuckDBStatsFunctions.Moments sym = DuckDBStatsFunctions.SkewnessUdaf.init();
    for (double x : new double[]{1, 2, 3, 4, 5}) {
      DuckDBStatsFunctions.SkewnessUdaf.add(sym, x);
    }
    assertEquals(0.0, DuckDBStatsFunctions.SkewnessUdaf.result(sym), 1e-9);

    DuckDBStatsFunctions.Moments right = DuckDBStatsFunctions.SkewnessUdaf.init();
    for (double x : new double[]{1, 1, 1, 2, 20}) {
      DuckDBStatsFunctions.SkewnessUdaf.add(right, x);
    }
    org.junit.jupiter.api.Assertions.assertTrue(
        DuckDBStatsFunctions.SkewnessUdaf.result(right) > 1.0,
        "a long right tail must give clearly positive skew");
  }

  @Test @DisplayName("kurtosis is EXCESS kurtosis: a heavy tail is positive")
  void kurtosisIsExcess() {
    DuckDBStatsFunctions.Moments heavy = DuckDBStatsFunctions.KurtosisUdaf.init();
    for (double x : new double[]{1, 1, 1, 1, 1, 1, 1, 50}) {
      DuckDBStatsFunctions.KurtosisUdaf.add(heavy, x);
    }
    org.junit.jupiter.api.Assertions.assertTrue(
        DuckDBStatsFunctions.KurtosisUdaf.result(heavy) > 0.0,
        "an outlier-heavy sample must give positive EXCESS kurtosis");
  }

  @Test @DisplayName("moment aggregates need enough rows to be defined")
  void momentsTooFewRows() {
    DuckDBStatsFunctions.Moments m = DuckDBStatsFunctions.SkewnessUdaf.init();
    DuckDBStatsFunctions.SkewnessUdaf.add(m, 1.0);
    DuckDBStatsFunctions.SkewnessUdaf.add(m, 2.0);
    assertNull(DuckDBStatsFunctions.SkewnessUdaf.result(m), "skewness needs 3+ values");
    DuckDBStatsFunctions.SkewnessUdaf.add(m, 3.0);
    assertNull(DuckDBStatsFunctions.KurtosisUdaf.result(m), "kurtosis needs 4+ values");
  }
}
