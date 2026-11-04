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

/**
 * Prints the User-Agent declared for direct sec.gov requests ({@code edgarUserAgent} in
 * sec-schema.yaml, resolved by VariableResolver) and exits 0; exits 1 with the reason on stderr if
 * it cannot be resolved. The fetch shell scripts call it once (via sec-cooldown.sh) so the declared
 * contact has one source and no script carries its own default.
 */
public final class SecUserAgentMain {
  private SecUserAgentMain() {
  }

  public static void main(String[] args) {
    try {
      System.out.println(EdgarRateLimiter.userAgent());
    } catch (RuntimeException e) {
      System.err.println("cannot resolve the SEC User-Agent (sec.edgarUserAgent): " + e.getMessage());
      System.exit(1);
    }
  }
}
