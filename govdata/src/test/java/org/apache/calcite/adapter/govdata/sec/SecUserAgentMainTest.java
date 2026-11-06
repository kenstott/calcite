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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The command the fetch scripts call prints exactly the resolved declared User-Agent. */
@Tag("unit")
class SecUserAgentMainTest {

  @Test void printsTheResolvedUserAgent() {
    PrintStream orig = System.out;
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    System.setOut(new PrintStream(buf));
    try {
      SecUserAgentMain.main(new String[0]);
    } finally {
      System.setOut(orig);
    }
    assertEquals(EdgarRateLimiter.userAgent() + System.lineSeparator(), buf.toString());
  }
}
