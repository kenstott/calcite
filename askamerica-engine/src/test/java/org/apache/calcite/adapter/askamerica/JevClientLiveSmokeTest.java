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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One real round trip against the live typesafe.ai {@code /v1/systemone} endpoint, confirming
 * {@link JevClient}'s request/response shape actually matches what the docs describe rather
 * than just what compiles. Skips (not fails) without {@code TYPESAFEAI_API_KEY} set, same as
 * {@link DescribeTableCoverageIntegrationTest} skips without warehouse credentials.
 */
@Tag("integration")
class JevClientLiveSmokeTest {

    @Test void scoreClaimReturnsACalibratedVerdict() throws Exception {
        Assumptions.assumeTrue(JevClient.isConfigured(),
            "TYPESAFEAI_API_KEY not set -- skipping live typesafe.ai call");

        JevClient.ScoreResult r = JevClient.scoreClaim(
            "The Earth's average radius is approximately 6,371 kilometers.",
            "Independent value: 6,371 km (NASA/IAU reference figure). No warehouse data "
                + "involved; this is a well-established physical constant, not a data claim.",
            Arrays.asList("accurate", "misleading", "false"));

        assertTrue(r.verdict != null && !r.verdict.isEmpty(),
            "expected a non-empty verdict, got: " + r.verdict);
        assertTrue(r.verdictConfidence >= 0.0 && r.verdictConfidence <= 1.0,
            "verdict_confidence out of [0,1]: " + r.verdictConfidence);
        assertTrue(r.pinocchiosCount >= 0 && r.pinocchiosCount <= 4,
            "pinocchios_count out of [0,4]: " + r.pinocchiosCount);
        assertTrue(r.pinocchiosConfidence >= 0.0 && r.pinocchiosConfidence <= 1.0,
            "pinocchios_confidence out of [0,1]: " + r.pinocchiosConfidence);
    }
}
