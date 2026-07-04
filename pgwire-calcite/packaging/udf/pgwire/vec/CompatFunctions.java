// Copyright (c) 2026 Kenneth Stott
// Licensed under the Business Source License 1.1 (see repo LICENSE/NOTICE).
//
// Compatibility-function UDFs (PGW-050) for the pg_trgm / pgcrypto surfaces:
// similarity()/word_similarity() (trigram) and digest() (hash). Registered via a
// Calcite model and invoked as normal functions (they pass through transpile).
package pgwire.vec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Set;

public final class CompatFunctions {
  private CompatFunctions() {}

  static Set<String> trigrams(String s) {
    Set<String> out = new HashSet<>();
    if (s == null) {
      return out;
    }
    for (String w : s.toLowerCase().split("[^a-z0-9]+")) {
      if (w.isEmpty()) {
        continue;
      }
      String p = "  " + w + " ";
      for (int i = 0; i + 3 <= p.length(); i++) {
        out.add(p.substring(i, i + 3));
      }
    }
    return out;
  }

  /** pg_trgm similarity(a, b): |trgm(a) ∩ trgm(b)| / |trgm(a) ∪ trgm(b)|. */
  public static double similarity(String a, String b) {
    Set<String> ta = trigrams(a);
    Set<String> tb = trigrams(b);
    if (ta.isEmpty() && tb.isEmpty()) {
      return 1.0;
    }
    Set<String> inter = new HashSet<>(ta);
    inter.retainAll(tb);
    Set<String> uni = new HashSet<>(ta);
    uni.addAll(tb);
    return uni.isEmpty() ? 0.0 : (double) inter.size() / uni.size();
  }

  /** pg_trgm word_similarity(a, b): best similarity of a against a word of b. */
  public static double wordSimilarity(String a, String b) {
    if (b == null) {
      return 0.0;
    }
    double best = 0.0;
    for (String w : b.toLowerCase().split("[^a-z0-9]+")) {
      if (!w.isEmpty()) {
        best = Math.max(best, similarity(a, w));
      }
    }
    return best;
  }

  /** pgcrypto digest(data, algo) -> lowercase hex. Supports md5/sha1/sha256/sha512. */
  public static String digest(String data, String algo) {
    String a = algo == null ? "sha256" : algo.toLowerCase();
    String j =
        a.equals("sha256") ? "SHA-256"
            : a.equals("sha1") ? "SHA-1"
                : a.equals("md5") ? "MD5"
                    : a.equals("sha512") ? "SHA-512" : algo;
    try {
      MessageDigest md = MessageDigest.getInstance(j);
      byte[] h = md.digest((data == null ? "" : data).getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(h.length * 2);
      for (byte x : h) {
        sb.append(String.format("%02x", x));
      }
      return sb.toString();
    } catch (Exception e) {
      throw new RuntimeException("digest: " + e.getMessage(), e);
    }
  }
}
