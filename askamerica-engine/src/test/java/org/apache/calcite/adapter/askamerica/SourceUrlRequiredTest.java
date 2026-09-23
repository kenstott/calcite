/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Regression test for the fix to {@code publish_report}'s {@code source_url} handling.
 *
 * <p>Measured live 2026-09-23 (an amny.com article validation): the report published
 * successfully, but the browser extension's popup on the article's own page said "No
 * validation published for this page yet." The cause was a silent fallback — when
 * {@code source_url} was omitted, the claim got recorded under {@code LAST_FETCH_URL}
 * (whatever page {@code web_fetch} last touched in the session), which is not reliably
 * the article under test whenever a reference/fact-check fetch happened afterward. The
 * fix refuses the publish instead of guessing.
 */
@Tag("unit")
class SourceUrlRequiredTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String enforce(ObjectNode args) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceSourceUrlPresent",
            com.fasterxml.jackson.databind.JsonNode.class);
        m.setAccessible(true);
        return (String) m.invoke(null, args);
    }

    @Test void missingSourceUrlIsRefused() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        assertNotNull(enforce(args));
    }

    @Test void blankSourceUrlIsRefused() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("source_url", "   ");
        assertNotNull(enforce(args));
    }

    @Test void presentSourceUrlPasses() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("source_url", "https://www.amny.com/new-york/manhattan/some-article/");
        assertNull(enforce(args));
    }
}
