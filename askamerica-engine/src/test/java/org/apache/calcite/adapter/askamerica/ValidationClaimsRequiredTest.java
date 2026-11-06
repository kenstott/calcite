/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A publish naming an article under review must carry a claims array. */
@Tag("unit")
class ValidationClaimsRequiredTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String enforce(ObjectNode args) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceValidationHasClaims",
            JsonNode.class);
        m.setAccessible(true);
        return (String) m.invoke(null, args);
    }

    @Test void sourceUrlWithoutClaimsIsRefusedWithShape() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("source_url", "https://example.com/article");
        String msg = enforce(args);
        assertNotNull(msg);
        assertTrue(msg.contains("group") && msg.contains("speaker")
            && msg.contains("rests_on"));
    }

    @Test void questionNamingUrlWithoutSourceUrlIsRefused() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.putObject("question_coverage")
            .put("question", "Fact-check https://example.com/story for me");
        String msg = enforce(args);
        assertNotNull(msg);
        assertTrue(msg.contains("source_url") && msg.contains("rests_on"));
    }

    @Test void generalReportWithoutSourceUrlPasses() throws Exception {
        assertNull(enforce(MAPPER.createObjectNode()));
    }
}
