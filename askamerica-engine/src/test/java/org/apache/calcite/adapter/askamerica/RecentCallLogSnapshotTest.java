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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * A rejected {@code publish_report} attempt must not become the recency boundary
 * {@code recentCallLogSnapshot} uses to decide which prior tool calls still count as
 * belonging to the report being validated. Measured live (2026-09-21, an Al Jazeera tariff
 * validation): a first, rejected {@code publish_report} attempt cut a real, earlier
 * {@code search_catalog}/{@code query} pair out of the snapshot on the very next retry,
 * making {@code enforceTableProvenance}/{@code enforceResearchDepthOnGap} refuse research
 * the model had genuinely already done. Only a call that actually returned (no {@code error}
 * field on its log entry) may mark "a report concluded here."
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class RecentCallLogSnapshotTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @AfterEach void clearCallLog() throws Exception {
        callLog().clear();
    }

    @SuppressWarnings("unchecked")
    private static List<ObjectNode> callLog() throws Exception {
        Field f = McpServer.class.getDeclaredField("CALL_LOG");
        f.setAccessible(true);
        return (List<ObjectNode>) f.get(null);
    }

    @SuppressWarnings("unchecked")
    private static List<ObjectNode> snapshot() throws Exception {
        Method m = McpServer.class.getDeclaredMethod("recentCallLogSnapshot");
        m.setAccessible(true);
        return (List<ObjectNode>) m.invoke(null);
    }

    private static ObjectNode entry(String tool, String error) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("tool", tool);
        if (error != null) {
            n.put("error", error);
        }
        return n;
    }

    @Test void rejectedPublishReportIsNotABoundary() throws Exception {
        ObjectNode search = entry("search_catalog", null);
        ObjectNode query = entry("query", null);
        ObjectNode rejected = entry("publish_report", "validation refused: ...");
        callLog().add(search);
        callLog().add(query);
        callLog().add(rejected);

        List<ObjectNode> snap = snapshot();

        assertEquals(3, snap.size(),
            "a rejected publish_report must not cut earlier calls out of the snapshot");
        assertSame(search, snap.get(0));
        assertSame(query, snap.get(1));
        assertSame(rejected, snap.get(2));
    }

    @Test void successfulPublishReportIsABoundary() throws Exception {
        ObjectNode staleQuery = entry("query", null);
        ObjectNode successfulPublish = entry("publish_report", null);
        ObjectNode newSearch = entry("search_catalog", null);
        callLog().add(staleQuery);
        callLog().add(successfulPublish);
        callLog().add(newSearch);

        List<ObjectNode> snap = snapshot();

        assertEquals(1, snap.size(),
            "a genuinely successful publish_report must still cut off the prior report's calls");
        assertSame(newSearch, snap.get(0));
    }

    @Test void multipleRejectedAttemptsAllStayVisible() throws Exception {
        ObjectNode query = entry("query", null);
        ObjectNode rejected1 = entry("publish_report", "issue 1");
        ObjectNode rejected2 = entry("publish_report", "issue 2");
        callLog().add(query);
        callLog().add(rejected1);
        callLog().add(rejected2);

        List<ObjectNode> snap = snapshot();

        assertEquals(3, snap.size(),
            "a chain of rejected attempts (the documented 4-5-round-trip case) must not "
            + "progressively lose earlier real tool calls");
        assertSame(query, snap.get(0));
    }

    @Test void mostRecentSuccessfulPublishReportWinsOverEarlierRejections() throws Exception {
        ObjectNode staleQuery = entry("query", null);
        ObjectNode rejected = entry("publish_report", "issue 1");
        ObjectNode successful = entry("publish_report", null);
        ObjectNode newQuery = entry("query", null);
        callLog().add(staleQuery);
        callLog().add(rejected);
        callLog().add(successful);
        callLog().add(newQuery);

        List<ObjectNode> snap = snapshot();

        assertEquals(1, snap.size());
        assertSame(newQuery, snap.get(0));
    }
}
