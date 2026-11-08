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

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code web_fetch}'s {@code find} searches every page of a document, so a figure on page 30 of
 * a 44-page report is found without reading pages 1-29.
 */
@Tag("unit")
class WebFetchFindTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A PDF whose pages each carry the given lines. */
    private static byte[] pdf(String[]... pages) throws Exception {
        try (PDDocument doc = new PDDocument(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (String[] lines : pages) {
                PDPage page = new PDPage();
                doc.addPage(page);
                try (PDPageContentStream cs = new PDPageContentStream(doc, page)) {
                    cs.beginText();
                    cs.setFont(PDType1Font.HELVETICA, 11);
                    cs.newLineAtOffset(50, 700);
                    for (String line : lines) {
                        cs.showText(line);
                        cs.newLineAtOffset(0, -14);
                    }
                    cs.endText();
                }
            }
            doc.save(out);
            return out.toByteArray();
        }
    }

    private static JsonNode find(String kind, byte[] bytes, String... terms) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("findInDocument", String.class, String.class,
            byte[].class, List.class);
        m.setAccessible(true);
        return MAPPER.readTree((String) m.invoke(null, kind, "https://example.org/r.pdf", bytes,
            Arrays.asList(terms)));
    }

    @Test void findsAFigureOnALatePageOfAPdf() throws Exception {
        List<String[]> pages = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            pages.add(new String[]{"Page " + i + " of the report, nothing relevant here."});
        }
        pages.add(new String[]{"Across the 508 eviction cases with outcome data, tenants",
            "retained possession of the unit in 319 cases (63%)."});
        JsonNode r = find("pdf", pdf(pages.toArray(new String[0][])), "319 cases (63%)",
            "tenants retained possession", "not in the document");
        assertEquals(31, r.path("total_pages").asInt());
        JsonNode hit = r.path("find").get(0);
        assertEquals(1, hit.path("total_matches").asInt());
        assertEquals(31, hit.path("matches").get(0).path("page").asInt());
        assertTrue(hit.path("matches").get(0).path("context").asText().contains("508 eviction"));
        // A phrase broken across two PDF lines still matches.
        assertEquals(1, r.path("find").get(1).path("total_matches").asInt());
        assertEquals(0, r.path("find").get(2).path("total_matches").asInt());
    }

    @Test void matchingIsCaseAndWhitespaceInsensitive() {
        ObjectNode r = McpServer.findMatches("u", List.of("ERAP funds were\n  used in 32% of"),
            List.of("erap FUNDS were used"));
        assertEquals(1, r.path("find").get(0).path("total_matches").asInt());
    }

    @Test void manyMatchesAreCappedAndCounted() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            sb.append("eviction ");
        }
        ObjectNode r = McpServer.findMatches("u", List.of(sb.toString()), List.of("eviction"));
        JsonNode hit = r.path("find").get(0);
        assertEquals(40, hit.path("total_matches").asInt());
        assertEquals(McpServer.FIND_MAX_MATCHES, hit.path("matches").size());
        assertTrue(hit.has("note"));
    }

    @Test void searchesHtml() throws Exception {
        byte[] html = ("<html><body><article><p>Landlords filed 13,051 new eviction cases in "
            + "2025.</p></article></body></html>").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        JsonNode r = find("html", html, "13,051");
        assertEquals(1, r.path("find").get(0).path("total_matches").asInt());
    }
}
