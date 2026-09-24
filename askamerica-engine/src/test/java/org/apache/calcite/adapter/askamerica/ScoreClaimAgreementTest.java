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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code enforceScoreClaimAgreement}'s recency-positional fallback, exercised without a live
 * typesafe.ai call: two prior live reruns of q136 (2026-09-20) showed the calling model reliably
 * calls {@code score_claim} but does not reliably wire an explicit {@code score_claim_ref} into
 * the {@code claims[]} entry it publishes, so the fallback -- pairing the last N score_claim
 * results with the N still-unmatched graded claims, both in their own order -- is what actually
 * fires in practice. This test locks that pairing logic in without needing network access or a
 * live agent run.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class ScoreClaimAgreementTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @AfterEach void clearState() throws Exception {
        scoreClaimLog().clear();
        scoreClaimResults().clear();
    }

    @SuppressWarnings("unchecked")
    private static List<JevClient.ScoreResult> scoreClaimLog() throws Exception {
        Field f = McpServer.class.getDeclaredField("SCORE_CLAIM_LOG");
        f.setAccessible(true);
        return (List<JevClient.ScoreResult>) f.get(null);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, JevClient.ScoreResult> scoreClaimResults() throws Exception {
        Field f = McpServer.class.getDeclaredField("SCORE_CLAIM_RESULTS");
        f.setAccessible(true);
        return (Map<String, JevClient.ScoreResult>) f.get(null);
    }

    private static void record(String ref, JevClient.ScoreResult r) throws Exception {
        scoreClaimResults().put(ref, r);
        scoreClaimLog().add(r);
    }

    private static String enforce(String claimsJson) throws Exception {
        JsonNode claims = MAPPER.readTree(claimsJson);
        Method m = McpServer.class.getDeclaredMethod("enforceScoreClaimAgreement", JsonNode.class);
        m.setAccessible(true);
        return (String) m.invoke(null, claims);
    }

    @Test void noScoreClaimCallsThisSessionIsSilent() throws Exception {
        assertNull(enforce("[{\"assertion\":\"x\",\"verdict\":\"true\"}]"));
    }

    @Test void notCheckableClaimsAreNeverMatched() throws Exception {
        record("sc-1", new JevClient.ScoreResult("false", 0.9, 4, 0.9));
        // The one score_claim result this session should NOT be pulled onto a claim that
        // explicitly declined scoring.
        assertNull(enforce("[{\"assertion\":\"x\",\"verdict\":\"not checkable here\"}]"));
    }

    @Test void recencyPositionalPairingAgreesWhenVerdictsMatch() throws Exception {
        // Mirrors the real q136 run: two score_claim calls in the same relative order as the
        // two claims that go on to be published, both agreeing with what gets submitted.
        record("sc-1", new JevClient.ScoreResult("true", 0.9, 0, 0.9));
        record("sc-2", new JevClient.ScoreResult("mostly true", 0.85, 1, 0.85));
        String claims = "["
            + "{\"assertion\":\"Vance's quote is accurate\",\"verdict\":\"true\"},"
            + "{\"assertion\":\"Immigrants commit less crime\",\"verdict\":\"mostly true\"}"
            + "]";
        assertNull(enforce(claims));
    }

    @Test void recencyPositionalPairingRefusesOnDisagreement() throws Exception {
        record("sc-1", new JevClient.ScoreResult("false", 0.9, 4, 0.9));
        String claims = "[{\"assertion\":\"x\",\"verdict\":\"true\"}]";
        String result = enforce(claims);
        assertNotNull(result);
        assertTrue(result.contains("validation refused"));
        assertTrue(result.contains("'true'"));
        assertTrue(result.contains("'false'"));
    }

    @Test void takesTheMostRecentCallsNotTheOldestWhenThereAreExtras() throws Exception {
        // Three score_claim calls (e.g. a stale retry plus two fresh ones) but only two
        // unmatched graded claims -- the LAST two calls must be the ones used, per the
        // documented rationale (earlier calls belong to a since-revised attempt).
        record("sc-1", new JevClient.ScoreResult("false", 0.9, 4, 0.9)); // stale, must be ignored
        record("sc-2", new JevClient.ScoreResult("true", 0.9, 0, 0.9));
        record("sc-3", new JevClient.ScoreResult("mostly true", 0.85, 1, 0.85));
        String claims = "["
            + "{\"assertion\":\"a\",\"verdict\":\"true\"},"
            + "{\"assertion\":\"b\",\"verdict\":\"mostly true\"}"
            + "]";
        assertNull(enforce(claims));
    }

    @Test void explicitRefOverridesPositionalMatchAndIsAuthoritative() throws Exception {
        record("sc-1", new JevClient.ScoreResult("false", 0.9, 4, 0.9));
        String claims = "[{\"assertion\":\"x\",\"verdict\":\"false\",\"score_claim_ref\":\"sc-1\"}]";
        assertNull(enforce(claims));
    }

    @Test void unresolvedExplicitRefIsRefusedNotSilent() throws Exception {
        record("sc-1", new JevClient.ScoreResult("true", 0.9, 0, 0.9));
        String claims = "[{\"assertion\":\"x\",\"verdict\":\"true\",\"score_claim_ref\":\"sc-typo\"}]";
        String result = enforce(claims);
        assertNotNull(result);
        assertTrue(result.contains("does not match any score_claim call"));
    }

    @Test void lowConfidenceIsRefusedEvenOnAgreement() throws Exception {
        record("sc-1", new JevClient.ScoreResult("true", 0.4, 0, 0.9));
        String claims = "[{\"assertion\":\"x\",\"verdict\":\"true\"}]";
        String result = enforce(claims);
        assertNotNull(result);
        assertTrue(result.contains("low-confidence"));
    }
}
