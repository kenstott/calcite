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

import static org.junit.jupiter.api.Assertions.assertFalse;
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

    @Test void factCheckWordingInQuestionWithoutUrlIsRefused() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.putObject("question_coverage")
            .put("question", "Fact-check the White House release on manufacturing jobs");
        String msg = enforce(args);
        assertNotNull(msg);
        assertTrue(msg.contains("Fact-check") && msg.contains("source_url")
            && msg.contains("claims"));
    }

    @Test void pinocchioTitleWithoutUrlIsRefused() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("title", "Overdose deaths: 1 Pinocchio");
        args.putObject("question_coverage").put("question", "Are overdose deaths rising?");
        String msg = enforce(args);
        assertNotNull(msg);
        assertTrue(msg.contains("title") && msg.contains("Pinocchio")
            && msg.contains("source_url"));
    }

    @Test void validateAndDebunkWordingIsRefused() throws Exception {
        for (String q : new String[] {"Validate this claim about rents",
            "Debunk the viral post on tariffs", "Is this a validation of the jobs story?"}) {
            ObjectNode args = MAPPER.createObjectNode();
            args.putObject("question_coverage").put("question", q);
            assertNotNull(enforce(args), q);
        }
    }

    @Test void feelingsAreAskedForAsTextOrAnAddress() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.putObject("question_coverage").put("question", "Validate my feelings");
        assertTrue(enforce(args).contains("Provide me the text of your <thing>, or an address I can download it from."));
    }

    @Test void unreachableDocumentIsAskedForAsTextOrAnAddress() throws Exception {
        for (String q : new String[] {"Debunk my horoscope", "Fact-checking my diary please"}) {
            ObjectNode args = MAPPER.createObjectNode();
            args.putObject("question_coverage").put("question", q);
            assertTrue(enforce(args).contains("Provide me the text of your <thing>, or an address I can download it from."), q);
        }
    }

    @Test void instructionsAskForTheTextOrAnAddress() throws Exception {
        String text = McpServer.handleInitialize(null).toString();
        assertTrue(text.contains("debunk my horoscope") && text.contains("validate my feelings")
            && text.contains("Provide me the text of your <thing>, or an address I can download it from."));
    }

    @Test void articleValidationIsNotAskedForTheText() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("source_url", "https://example.com/article");
        args.putObject("question_coverage").put("question", "Fact-check this article");
        String msg = enforce(args);
        assertFalse(msg.contains("Provide me the text"));
    }

    @Test void ordinaryQuestionAndTitlePass() throws Exception {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("title", "Rent burden by state, 2024");
        args.putObject("question_coverage")
            .put("question", "Which states have the highest rent burden? Check the trend.");
        assertNull(enforce(args));
    }

    @Test void generalReportWithoutSourceUrlPasses() throws Exception {
        assertNull(enforce(MAPPER.createObjectNode()));
    }
}
