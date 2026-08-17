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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An unknown argument name must be rejected, not ignored.
 *
 * <p>Observed 2026-08-17: a caller sent {@code type} instead of {@code chart_type} to
 * render_chart seven times. Because the reader defaulted a missing {@code chart_type} to
 * "line", the payload was routed down the categories path; six calls failed with errors about
 * categories and series that had nothing to do with the real mistake, and the seventh rendered
 * a line chart of two series on incompatible scales and reported success. The caller reported
 * it as a bar chart, because that is what it believed it had asked for.
 */
@Tag("unit")
public class McpServerUnknownArgTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ObjectNode args(String... names) {
        ObjectNode node = MAPPER.createObjectNode();
        for (String n : names) {
            node.put(n, "x");
        }
        return node;
    }

    @Test void rejectsAnUnknownArgumentAndNamesTheNearMiss() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkKnownArgs(args("type", "title"), "render_chart",
                "chart_type", "title", "categories", "series", "points"));

        assertTrue(e.getMessage().contains("no argument 'type'"),
            "must name the offending argument: " + e.getMessage());
        assertTrue(e.getMessage().contains("chart_type"),
            "must suggest the near miss — that suggestion IS the diagnosis: " + e.getMessage());
    }

    @Test void listsTheKnownArgumentsSoTheCallerCanSelfCorrect() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkKnownArgs(args("bogus"), "render_chart",
                "chart_type", "title", "categories"));

        assertTrue(e.getMessage().contains("Known arguments:"), e.getMessage());
        assertTrue(e.getMessage().contains("categories"), e.getMessage());
    }

    @Test void acceptsExactlyTheKnownArguments() {
        assertDoesNotThrow(() ->
            McpServer.checkKnownArgs(args("chart_type", "title", "categories", "series"),
                "render_chart", "chart_type", "title", "x_label", "y_label",
                "categories", "series", "points", "width", "height"));
    }

    @Test void acceptsASubsetSinceMostArgumentsAreOptional() {
        assertDoesNotThrow(() ->
            McpServer.checkKnownArgs(args("chart_type"), "render_chart",
                "chart_type", "title", "categories", "series", "points"));
    }

    @Test void reportsEveryUnknownArgumentNotJustTheFirst() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkKnownArgs(args("type", "titel"), "render_chart",
                "chart_type", "title", "categories"));

        assertTrue(e.getMessage().contains("'type'"), e.getMessage());
        assertTrue(e.getMessage().contains("'titel'"),
            "a caller with two typos should not have to fix them one round trip at a time: "
                + e.getMessage());
    }

    @Test void toleratesAbsentOrNonObjectArguments() {
        assertDoesNotThrow(() -> McpServer.checkKnownArgs(null, "render_chart", "chart_type"));
        assertDoesNotThrow(() ->
            McpServer.checkKnownArgs(MAPPER.createArrayNode(), "render_chart", "chart_type"));
    }
}
