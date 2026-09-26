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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code publish_report} gates judge a report against the tool calls recorded in this
 * process, so two properties keep them from returning the same refusal whatever the caller
 * changes: the call log always ends at the current call once it is full, and the plain
 * English word "did" is never read as the DiD (difference-in-differences) method.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class ValidatorSessionStateTest {

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

    private static int callLogMax() throws Exception {
        Field f = McpServer.class.getDeclaredField("CALL_LOG_MAX");
        f.setAccessible(true);
        return f.getInt(null);
    }

    private static void recordCall(String tool, ObjectNode args, int rows) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("recordCall", String.class, JsonNode.class,
            long.class, int.class, ObjectNode.class, String.class);
        m.setAccessible(true);
        m.invoke(null, tool, args, 1L, rows, null, null);
    }

    private static String statisticalProvenance(String html) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceStatisticalProvenance", List.class);
        m.setAccessible(true);
        List<ReportPage.Section> secs =
            Collections.singletonList(new ReportPage.Section("Findings", html));
        return (String) m.invoke(null, secs);
    }

    private static String researchDepthOnGap() throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceResearchDepthOnGap");
        m.setAccessible(true);
        return (String) m.invoke(null);
    }

    @Test void callsAfterTheCapAreStillRecorded() throws Exception {
        for (int i = 0; i < callLogMax(); i++) {
            recordCall("list_schemas", null, -1);
        }
        recordCall("search_catalog", MAPPER.createObjectNode().put("query", "house prices"), -1);
        ObjectNode args = MAPPER.createObjectNode();
        args.put("sql", "SELECT * FROM housing.house_price_index");
        recordCall("query", args, 20);

        List<ObjectNode> log = callLog();
        assertEquals(callLogMax(), log.size(), "the log stays bounded");
        assertEquals("query", log.get(log.size() - 1).path("tool").asText(),
            "the most recent call is the last entry once the log is full");
        assertEquals("search_catalog", log.get(log.size() - 2).path("tool").asText());
        assertNull(researchDepthOnGap(),
            "a productive query and a catalog check made after the cap must satisfy the gate");
    }

    @Test void plainDidIsNotADifferenceInDifferencesClaim() throws Exception {
        String html = "<p>Prices did fall, and the data shows that hard-hit metros "
            + "recovered more slowly. We found that the gap did not close.</p>";

        assertNull(statisticalProvenance(html));
    }

    @Test void diffInDiffAcronymWithRunClaimStillNeedsTheTool() throws Exception {
        String html = "<p>We ran a DiD comparing treated and control metros and it shows "
            + "a persistent gap.</p>";

        String refusal = statisticalProvenance(html);

        assertNotNull(refusal);
        assertTrue(refusal.contains("diff_in_diff"), refusal);
    }
}
