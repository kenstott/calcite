// Copyright (c) 2026 Kenneth Stott
// Licensed under the Business Source License 1.1 (see repo LICENSE/NOTICE).
//
// Vector-distance UDFs for the pgvector extension surface (PGW-047). Registered
// via a Calcite model ("functions") and invoked by the transpiled pgvector
// operators: <-> (L2), <=> (cosine distance), <#> (negative inner product).
// Raw List params so Calcite's ARRAY<numeric> maps cleanly by reflection.
package pgwire.vec;

import java.util.List;

public final class VecDist {
  private VecDist() {}

  private static double[] toArray(List<?> a) {
    double[] r = new double[a.size()];
    for (int i = 0; i < a.size(); i++) {
      r[i] = ((Number) a.get(i)).doubleValue();
    }
    return r;
  }

  /** Euclidean (L2) distance — pgvector <->. */
  public static double l2(List<?> a, List<?> b) {
    double[] x = toArray(a);
    double[] y = toArray(b);
    int n = Math.min(x.length, y.length);
    double s = 0;
    for (int i = 0; i < n; i++) {
      double d = x[i] - y[i];
      s += d * d;
    }
    return Math.sqrt(s);
  }

  /** Inner (dot) product — pgvector <#> uses the negative of this for ordering. */
  public static double innerProduct(List<?> a, List<?> b) {
    double[] x = toArray(a);
    double[] y = toArray(b);
    int n = Math.min(x.length, y.length);
    double s = 0;
    for (int i = 0; i < n; i++) {
      s += x[i] * y[i];
    }
    return s;
  }

  /** Cosine distance (1 - cosine similarity) — pgvector <=>. */
  public static double cosineDistance(List<?> a, List<?> b) {
    double[] x = toArray(a);
    double[] y = toArray(b);
    int n = Math.min(x.length, y.length);
    double dot = 0;
    double na = 0;
    double nb = 0;
    for (int i = 0; i < n; i++) {
      dot += x[i] * y[i];
      na += x[i] * x[i];
      nb += y[i] * y[i];
    }
    if (na == 0 || nb == 0) {
      return 1.0;
    }
    return 1.0 - dot / (Math.sqrt(na) * Math.sqrt(nb));
  }
}
