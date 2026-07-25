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

/**
 * Aggregate-function DECLARATIONS for DuckDB-native statistical aggregates that Calcite
 * core does not define. Registering these on the schema makes them <em>validate</em> as
 * aggregates; the DuckDB engine then pushes the aggregate down and DuckDB computes it
 * (proven: {@code skewness} returns a real value while {@code result} below is never
 * called). The Java bodies are never meant to run in Calcite's enumerable layer —
 * {@code result} throws, so a query that fails to push down surfaces loudly instead of
 * returning a wrong (Java-computed stub) value.
 *
 * <p>Only functions whose names are NOT reserved parser keywords can be registered here
 * directly (median, skewness, kurtosis, mad, quantile_cont, quantile_disc). The reserved
 * regression aggregates (corr, regr_slope, …) are handled via a pre-parse alias rewrite.
 */
public final class DuckDBStatsFunctions {

  private DuckDBStatsFunctions() {
  }

  private static UnsupportedOperationException notPushed(String fn) {
    return new UnsupportedOperationException(
        fn + " is a DuckDB-only aggregate and must be pushed down to the DuckDB engine; "
        + "it has no Calcite enumerable implementation.");
  }

  // ---- single-argument aggregates: fn(x) -> DOUBLE ----------------------------------

  /** {@code median(x)}. */
  public static final class MedianUdaf {
    public static Double init() {
      return 0.0;
    }

    public static Double add(Double acc, Double value) {
      return acc;
    }

    public static Double result(Double acc) {
      throw notPushed("median");
    }
  }

  /** {@code skewness(x)}. */
  public static final class SkewnessUdaf {
    public static Double init() {
      return 0.0;
    }

    public static Double add(Double acc, Double value) {
      return acc;
    }

    public static Double result(Double acc) {
      throw notPushed("skewness");
    }
  }

  /** {@code kurtosis(x)}. */
  public static final class KurtosisUdaf {
    public static Double init() {
      return 0.0;
    }

    public static Double add(Double acc, Double value) {
      return acc;
    }

    public static Double result(Double acc) {
      throw notPushed("kurtosis");
    }
  }

  /** {@code mad(x)} — median absolute deviation. */
  public static final class MadUdaf {
    public static Double init() {
      return 0.0;
    }

    public static Double add(Double acc, Double value) {
      return acc;
    }

    public static Double result(Double acc) {
      throw notPushed("mad");
    }
  }

  // ---- two-argument aggregates: fn(x, fraction) -> DOUBLE ----------------------------

  /** {@code quantile_cont(x, fraction)}. */
  public static final class QuantileContUdaf {
    public static Double init() {
      return 0.0;
    }

    public static Double add(Double acc, Double value, Double fraction) {
      return acc;
    }

    public static Double result(Double acc) {
      throw notPushed("quantile_cont");
    }
  }

  /** {@code quantile_disc(x, fraction)}. */
  public static final class QuantileDiscUdaf {
    public static Double init() {
      return 0.0;
    }

    public static Double add(Double acc, Double value, Double fraction) {
      return acc;
    }

    public static Double result(Double acc) {
      throw notPushed("quantile_disc");
    }
  }

  // ---- two-argument regression aggregates: fn(y, x) -> DOUBLE ------------------------
  // Registered under non-reserved ALIAS names (AGG_CORR, AGG_REGR_SLOPE, …) because the
  // real names (corr, regr_slope, …) are reserved parser keywords. A pre-parse rewrite
  // maps the user's corr(...) to agg_corr(...), and the DuckDB dialect maps agg_corr back
  // to corr on the way out. One declaration serves all seven; the operator NAME (the
  // registration key) is what the dialect remaps.

  /** Shared declaration for the reserved two-argument regression aggregates. */
  public static final class RegrUdaf {
    public static Double init() {
      return 0.0;
    }

    public static Double add(Double acc, Double y, Double x) {
      return acc;
    }

    public static Double result(Double acc) {
      throw notPushed("regression aggregate (corr/regr_*)");
    }
  }
}
