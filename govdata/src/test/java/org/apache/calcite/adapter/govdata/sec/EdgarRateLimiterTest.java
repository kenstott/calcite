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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused test for {@link EdgarRateLimiter} pacing.
 *
 * <p>Verifies that successive {@code acquire()} calls are spaced by at least the
 * configured interval (default 100ms = 10 req/s), i.e. the limiter actually
 * paces requests rather than letting them through unbounded.
 */
@Tag("unit")
public class EdgarRateLimiterTest {

  @Test void acquirePacesSuccessiveCalls() {
    // Default interval is 100ms. N calls => about (N-1) gaps of >=100ms each.
    int calls = 5;
    long start = System.currentTimeMillis();
    for (int i = 0; i < calls; i++) {
      EdgarRateLimiter.acquire();
    }
    long elapsed = System.currentTimeMillis() - start;

    // Conservative lower bound: at least 3 full inter-call gaps (allow scheduler slack).
    long minExpected = 3L * 100L;
    assertTrue(elapsed >= minExpected,
        "expected >= " + minExpected + "ms pacing for " + calls + " acquires, got " + elapsed + "ms");
  }
}
