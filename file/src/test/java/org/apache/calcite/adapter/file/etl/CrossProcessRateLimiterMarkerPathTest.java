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

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The cool-down marker lives in java.io.tmpdir; govdata/scripts/fetch-edgar-feed.sh writes
 * $GOVDATA_TMPDIR/calcite-ratelimit-sec.gov.blocked and the pool sets java.io.tmpdir to
 * $GOVDATA_TMPDIR, so the two must name the same file.
 */
@Tag("unit")
class CrossProcessRateLimiterMarkerPathTest {
  @Test void markerIsInJavaTmpdirUnderTheNameTheScriptWrites() {
    assertEquals(
        Paths.get(System.getProperty("java.io.tmpdir"), "calcite-ratelimit-sec.gov.blocked"),
        CrossProcessRateLimiter.blockFile("sec.gov"));
  }
}
