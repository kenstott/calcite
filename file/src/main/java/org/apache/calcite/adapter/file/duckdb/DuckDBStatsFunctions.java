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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Statistical aggregates that DuckDB provides natively and Calcite core does not define.
 *
 * <p>Registering these on the schema makes them <em>validate</em> as aggregates, after which
 * the DuckDB engine pushes the aggregate down and DuckDB computes it. That is the fast path
 * and it is unchanged: when the whole aggregate can be expressed as one DuckDB query, none of
 * the Java below runs.
 *
 * <p>The Java bodies exist for the case where it cannot. Pushdown requires the aggregate's
 * input to be a single DuckDB catalog, so it fails whenever the rows come from somewhere
 * DuckDB is not: a join spanning two govdata schemas (each is its own catalog), or a relation
 * Calcite built itself — {@code VALUES}, a {@code UNION ALL} of literals, a CTE over either.
 * The literal case is not exotic; it is what an agent writes when it has assembled figures
 * from outside the warehouse and wants a correlation over them.
 *
 * <p>These previously threw {@code UnsupportedOperationException} from {@code result()}, on the
 * reasoning that failing loudly beats returning a wrong Java-computed value. The premise was
 * right and the conclusion did not follow: the alternative to a wrong value is a correct one,
 * not an error. The accumulators below compute the real statistic by the same definitions
 * DuckDB uses, so the query returns the same answer either way and the execution path stops
 * being something the caller has to know about. What the caller does lose on this path is
 * DuckDB's speed — the rows flow through Calcite's enumerable layer instead — but that cost is
 * already paid by any query that failed to push down, and the order-dependent aggregates
 * ({@code median}, {@code quantile_*}, {@code mad}) additionally retain their input values,
 * bounded by the rows Calcite had already materialized.
 *
 * <p>Every {@code add} takes {@link Number} rather than {@code Double}, and each class has
 * exactly ONE {@code add}. Both points are load-bearing. A DECIMAL literal — which is what
 * {@code VALUES (1.0, 5.0)} produces — arrives as a {@code BigDecimal}, while a DOUBLE column
 * arrives as a {@code Double}; a {@code Double} parameter binds only the second, and the
 * generated code fails to compile on the first. Overloading is not the fix: Calcite picks the
 * aggregate's SQL signature via {@code ReflectiveFunctionBase.findMethod}, which returns the
 * first method named {@code add} in {@code Class.getMethods()} order — unspecified by the JVM —
 * so a second overload could silently change the declared parameter types between runs.
 *
 * <p>Only functions whose names are NOT reserved parser keywords can be registered directly
 * (median, skewness, kurtosis, mad, quantile_cont, quantile_disc). The reserved regression
 * aggregates (corr, regr_*) are registered under {@code AGG_*} alias names via a pre-parse
 * rewrite. Those seven share one accumulator and differ only in {@code result()}, which is why
 * each needs its own class here: the operator name is the only thing that distinguishes
 * {@code corr} from {@code regr_slope}, and a static UDAF cannot see the operator name.
 */
public final class DuckDBStatsFunctions {

  private DuckDBStatsFunctions() {
  }

  // ---- bivariate regression aggregates: fn(y, x) -> DOUBLE ---------------------------

  /**
   * Running co-moments for the regression family. A pair is counted only when both sides are
   * non-null, matching SQL aggregate semantics (and DuckDB's) — a row with one side missing
   * contributes to neither margin, rather than being silently treated as a zero.
   */
  public static final class Regr {
    long n;
    double sumX;
    double sumY;
    double sumXX;
    double sumYY;
    double sumXY;

    /** Corrected sum of squares for x, i.e. sum((x - avgx)^2). */
    double sxx() {
      return sumXX - sumX * sumX / n;
    }

    /** Corrected sum of squares for y. */
    double syy() {
      return sumYY - sumY * sumY / n;
    }

    /** Corrected sum of products, i.e. sum((x - avgx) * (y - avgy)). */
    double sxy() {
      return sumXY - sumX * sumY / n;
    }
  }

  private static Regr regrInit() {
    return new Regr();
  }

  private static Regr regrAdd(Regr acc, Double y, Double x) {
    if (y == null || x == null) {
      return acc;
    }
    final double dy = y.doubleValue();
    final double dx = x.doubleValue();
    acc.n++;
    acc.sumX += dx;
    acc.sumY += dy;
    acc.sumXX += dx * dx;
    acc.sumYY += dy * dy;
    acc.sumXY += dx * dy;
    return acc;
  }

  /** {@code corr(y, x)} — Pearson correlation; null when either side is constant. */
  public static final class CorrUdaf {
    public static Regr init() {
      return regrInit();
    }

    public static Regr add(Regr acc, Double y, Double x) {
      return regrAdd(acc, y, x);
    }

    public static Double result(Regr acc) {
      if (acc.n < 2) {
        return null;
      }
      final double denom = Math.sqrt(acc.sxx() * acc.syy());
      return denom == 0.0 ? null : acc.sxy() / denom;
    }
  }

  /** {@code regr_slope(y, x)}. */
  public static final class RegrSlopeUdaf {
    public static Regr init() {
      return regrInit();
    }

    public static Regr add(Regr acc, Double y, Double x) {
      return regrAdd(acc, y, x);
    }

    public static Double result(Regr acc) {
      if (acc.n < 2) {
        return null;
      }
      final double sxx = acc.sxx();
      return sxx == 0.0 ? null : acc.sxy() / sxx;
    }
  }

  /** {@code regr_intercept(y, x)}. */
  public static final class RegrInterceptUdaf {
    public static Regr init() {
      return regrInit();
    }

    public static Regr add(Regr acc, Double y, Double x) {
      return regrAdd(acc, y, x);
    }

    public static Double result(Regr acc) {
      if (acc.n < 2) {
        return null;
      }
      final double sxx = acc.sxx();
      if (sxx == 0.0) {
        return null;
      }
      final double slope = acc.sxy() / sxx;
      return acc.sumY / acc.n - slope * (acc.sumX / acc.n);
    }
  }

  /** {@code regr_r2(y, x)} — the squared Pearson correlation. */
  public static final class RegrR2Udaf {
    public static Regr init() {
      return regrInit();
    }

    public static Regr add(Regr acc, Double y, Double x) {
      return regrAdd(acc, y, x);
    }

    public static Double result(Regr acc) {
      if (acc.n < 2) {
        return null;
      }
      final double denom = acc.sxx() * acc.syy();
      if (denom == 0.0) {
        return null;
      }
      final double sxy = acc.sxy();
      return sxy * sxy / denom;
    }
  }

  /** {@code regr_avgx(y, x)} — mean of x over pairs where both sides are non-null. */
  public static final class RegrAvgXUdaf {
    public static Regr init() {
      return regrInit();
    }

    public static Regr add(Regr acc, Double y, Double x) {
      return regrAdd(acc, y, x);
    }

    public static Double result(Regr acc) {
      return acc.n == 0 ? null : acc.sumX / acc.n;
    }
  }

  /** {@code regr_avgy(y, x)} — mean of y over pairs where both sides are non-null. */
  public static final class RegrAvgYUdaf {
    public static Regr init() {
      return regrInit();
    }

    public static Regr add(Regr acc, Double y, Double x) {
      return regrAdd(acc, y, x);
    }

    public static Double result(Regr acc) {
      return acc.n == 0 ? null : acc.sumY / acc.n;
    }
  }

  /** {@code regr_sxy(y, x)} — corrected sum of products. */
  public static final class RegrSxyUdaf {
    public static Regr init() {
      return regrInit();
    }

    public static Regr add(Regr acc, Double y, Double x) {
      return regrAdd(acc, y, x);
    }

    public static Double result(Regr acc) {
      return acc.n < 2 ? null : acc.sxy();
    }
  }

  // ---- univariate moment aggregates: fn(x) -> DOUBLE ---------------------------------

  /** Running moments up to the fourth, enough for sample skewness and excess kurtosis. */
  public static final class Moments {
    long n;
    double s1;
    double s2;
    double s3;
    double s4;
  }

  private static Moments momentsAdd(Moments acc, Double value) {
    if (value == null) {
      return acc;
    }
    final double v = value.doubleValue();
    acc.n++;
    acc.s1 += v;
    acc.s2 += v * v;
    acc.s3 += v * v * v;
    acc.s4 += v * v * v * v;
    return acc;
  }

  /** Central moment of the given order, from raw power sums. */
  private static double centralMoment(Moments a, int order) {
    final double n = a.n;
    final double m = a.s1 / n;
    switch (order) {
    case 2:
      return a.s2 / n - m * m;
    case 3:
      return a.s3 / n - 3 * m * a.s2 / n + 2 * m * m * m;
    default:
      return a.s4 / n - 4 * m * a.s3 / n + 6 * m * m * a.s2 / n - 3 * m * m * m * m;
    }
  }

  /**
   * {@code skewness(x)} — the sample (adjusted Fisher-Pearson) skewness DuckDB reports,
   * not the population moment ratio; undefined below three values.
   */
  public static final class SkewnessUdaf {
    public static Moments init() {
      return new Moments();
    }

    public static Moments add(Moments acc, Double value) {
      return momentsAdd(acc, value);
    }

    public static Double result(Moments acc) {
      final double n = acc.n;
      if (acc.n < 3) {
        return null;
      }
      final double m2 = centralMoment(acc, 2);
      if (m2 <= 0.0) {
        return null;
      }
      final double m3 = centralMoment(acc, 3);
      final double g1 = m3 / Math.pow(m2, 1.5);
      return Math.sqrt(n * (n - 1)) / (n - 2) * g1;
    }
  }

  /**
   * {@code kurtosis(x)} — sample EXCESS kurtosis (normal distribution is 0), matching
   * DuckDB's {@code kurtosis}; undefined below four values.
   */
  public static final class KurtosisUdaf {
    public static Moments init() {
      return new Moments();
    }

    public static Moments add(Moments acc, Double value) {
      return momentsAdd(acc, value);
    }

    public static Double result(Moments acc) {
      final double n = acc.n;
      if (acc.n < 4) {
        return null;
      }
      final double m2 = centralMoment(acc, 2);
      if (m2 <= 0.0) {
        return null;
      }
      final double g2 = centralMoment(acc, 4) / (m2 * m2) - 3.0;
      return ((n + 1) * g2 + 6) * (n - 1) / ((n - 2) * (n - 3));
    }
  }

  // ---- order-statistic aggregates: retain the values --------------------------------

  /**
   * Retained values for the aggregates that need the whole distribution rather than running
   * sums. Bounded by the rows Calcite had already materialized to reach this operator.
   */
  public static final class Values {
    final List<Double> values = new ArrayList<>();
    /** Fraction for the quantile aggregates; constant across rows, so last write wins. */
    Double fraction;
  }

  private static Values valuesAdd(Values acc, Double value) {
    if (value != null) {
      acc.values.add(value.doubleValue());
    }
    return acc;
  }

  /** Continuous quantile with linear interpolation between neighbours, DuckDB's default. */
  private static Double quantileCont(List<Double> sorted, double fraction) {
    if (sorted.isEmpty()) {
      return null;
    }
    if (sorted.size() == 1) {
      return sorted.get(0);
    }
    final double pos = fraction * (sorted.size() - 1);
    final int lo = (int) Math.floor(pos);
    final int hi = (int) Math.ceil(pos);
    if (lo == hi) {
      return sorted.get(lo);
    }
    return sorted.get(lo) + (pos - lo) * (sorted.get(hi) - sorted.get(lo));
  }

  /** Discrete quantile: an actual member of the input, never an interpolation. */
  private static Double quantileDisc(List<Double> sorted, double fraction) {
    if (sorted.isEmpty()) {
      return null;
    }
    final int idx = (int) Math.ceil(fraction * sorted.size()) - 1;
    return sorted.get(Math.max(0, Math.min(sorted.size() - 1, idx)));
  }

  /** {@code median(x)} — the 0.5 continuous quantile. */
  public static final class MedianUdaf {
    public static Values init() {
      return new Values();
    }

    public static Values add(Values acc, Double value) {
      return valuesAdd(acc, value);
    }

    public static Double result(Values acc) {
      Collections.sort(acc.values);
      return quantileCont(acc.values, 0.5);
    }
  }

  /** {@code mad(x)} — median absolute deviation about the median. */
  public static final class MadUdaf {
    public static Values init() {
      return new Values();
    }

    public static Values add(Values acc, Double value) {
      return valuesAdd(acc, value);
    }

    public static Double result(Values acc) {
      if (acc.values.isEmpty()) {
        return null;
      }
      Collections.sort(acc.values);
      final double median = quantileCont(acc.values, 0.5);
      final List<Double> deviations = new ArrayList<>(acc.values.size());
      for (Double v : acc.values) {
        deviations.add(Math.abs(v - median));
      }
      Collections.sort(deviations);
      return quantileCont(deviations, 0.5);
    }
  }

  /** {@code quantile_cont(x, fraction)}. */
  public static final class QuantileContUdaf {
    public static Values init() {
      return new Values();
    }

    public static Values add(Values acc, Double value, Double fraction) {
      acc.fraction = fraction;
      return valuesAdd(acc, value);
    }

    public static Double result(Values acc) {
      if (acc.fraction == null) {
        return null;
      }
      Collections.sort(acc.values);
      return quantileCont(acc.values, acc.fraction);
    }
  }

  /** {@code quantile_disc(x, fraction)}. */
  public static final class QuantileDiscUdaf {
    public static Values init() {
      return new Values();
    }

    public static Values add(Values acc, Double value, Double fraction) {
      acc.fraction = fraction;
      return valuesAdd(acc, value);
    }

    public static Double result(Values acc) {
      if (acc.fraction == null) {
        return null;
      }
      Collections.sort(acc.values);
      return quantileDisc(acc.values, acc.fraction);
    }
  }
}
