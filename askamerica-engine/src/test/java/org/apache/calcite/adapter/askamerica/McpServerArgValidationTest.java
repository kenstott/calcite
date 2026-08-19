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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A required argument that never arrived must be reported, not read as an empty value.
 *
 * <p>Observed 2026-08-19: a caller sent {@code keyword} instead of {@code query} to
 * search_catalog. The unknown name was ignored, the absent {@code query} read as {@code ""},
 * and the tool answered {@code []}. The caller concluded from that empty list that the
 * warehouse held no income tables — there are four — and abandoned catalog search for the rest
 * of the investigation. An empty result is a claim about the data; it must never be what a
 * malformed call looks like.
 */
@Tag("unit")
public class McpServerArgValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Set<String> set(String... names) {
        return new LinkedHashSet<>(Arrays.asList(names));
    }

    private static ObjectNode args(String... names) {
        ObjectNode node = MAPPER.createObjectNode();
        for (String n : names) {
            node.put(n, "x");
        }
        return node;
    }

    @Test void rejectsTheCallThatCostAnInvestigation() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkArgs(args("keyword"), "search_catalog",
                set("query", "limit"), set("query")));

        assertTrue(e.getMessage().contains("no argument 'keyword'"), e.getMessage());
        assertTrue(e.getMessage().contains("requires 'query'"), e.getMessage());
    }

    @Test void mapsTheUnknownNameOntoTheOneRequiredArgumentItLeftUnfilled() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkArgs(args("keyword"), "search_catalog",
                set("query", "limit"), set("query")));

        assertTrue(e.getMessage().contains("did you mean 'query'?"),
            "nothing about the spelling relates keyword to query — the unfilled required "
                + "argument is the only evidence, and it is enough: " + e.getMessage());
    }

    @Test void reportsAMissingRequiredArgumentEvenWhenNothingIsUnknown() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkArgs(args("limit"), "search_catalog",
                set("query", "limit"), set("query")));

        assertTrue(e.getMessage().contains("requires 'query'"), e.getMessage());
    }

    @Test void treatsABlankStringAsMissingRatherThanAsAQuery() {
        ObjectNode blank = MAPPER.createObjectNode();
        blank.put("query", "   ");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkArgs(blank, "search_catalog", set("query"), set("query")));

        assertTrue(e.getMessage().contains("requires 'query'"),
            "a whitespace query searches for nothing and answers []: " + e.getMessage());
    }

    @Test void rejectsACallWithNoArgumentsAtAllWhenOneIsRequired() {
        assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkArgs(null, "search_catalog", set("query"), set("query")));
    }

    @Test void acceptsAWellFormedCall() {
        assertDoesNotThrow(() ->
            McpServer.checkArgs(args("query", "limit"), "search_catalog",
                set("query", "limit"), set("query")));
    }

    @Test void acceptsAnAbsentOptionalArgument() {
        assertDoesNotThrow(() ->
            McpServer.checkArgs(args("query"), "search_catalog",
                set("query", "limit"), set("query")));
    }

    @Test void stillCatchesATypoWhenNothingRequiredIsMissing() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
            McpServer.checkArgs(args("query", "limti"), "search_catalog",
                set("query", "limit"), set("query")));

        assertTrue(e.getMessage().contains("did you mean 'limit'?"),
            "edit distance must still catch an ordinary typo: " + e.getMessage());
    }

    @Test void toleratesAToolWithNoRequiredArguments() {
        assertDoesNotThrow(() ->
            McpServer.checkArgs(args("chart_type"), "render_chart",
                set("chart_type", "title"), Collections.<String>emptySet()));
    }
}
