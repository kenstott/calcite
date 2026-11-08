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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A claim the warehouse did not test must show an outside check was attempted: a fetched
 * source URL (any outcome) or the searches it ran. Finding nothing passes; not looking does not.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class ExternalCheckGateTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ARTICLE = "https://51st.news/dc-record-evictions-housing-crisis";

    @org.junit.jupiter.api.BeforeEach void quietLog() {
        if (McpServer.log == null) {
            McpServer.log = new java.io.PrintStream(new java.io.ByteArrayOutputStream());
        }
    }

    @AfterEach void clearCallLog() throws Exception {
        callLog().clear();
    }

    @SuppressWarnings("unchecked")
    private static List<ObjectNode> callLog() throws Exception {
        Field f = McpServer.class.getDeclaredField("CALL_LOG");
        f.setAccessible(true);
        return (List<ObjectNode>) f.get(null);
    }

    private static void recordCall(String tool, ObjectNode args, int rows, String error)
            throws Exception {
        Method m = McpServer.class.getDeclaredMethod("recordCall", String.class, JsonNode.class,
            long.class, int.class, ObjectNode.class, String.class);
        m.setAccessible(true);
        m.invoke(null, tool, args, 1L, rows, null, error);
    }

    private static void fetch(String url, String error) throws Exception {
        ObjectNode a = MAPPER.createObjectNode();
        a.put("url", url);
        recordCall("web_fetch", a, -1, error);
    }

    private static String gate(String claimsJson) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceExternalCheckAttempted",
            JsonNode.class, String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, MAPPER.readTree(claimsJson), ARTICLE);
    }

    /** The q3 shape: warehouse rows exist for one claim, the rest carry nothing and the only
     *  fetch was the article itself. */
    @Test void hybridReportWithUncheckedClaimsIsRefused() throws Exception {
        fetch(ARTICLE, null);
        ObjectNode q = MAPPER.createObjectNode();
        q.put("sql", "SELECT 1");
        recordCall("query", q, 4, null);
        String problem = gate("[{\"assertion\":\"Prices were 4.1% higher\",\"verdict\":\"true\","
            + "\"sql\":\"SELECT 1\",\"warehouse_value\":\"4.12%\"},"
            + "{\"assertion\":\"2,420 households were evicted\",\"verdict\":\"not checkable here\","
            + "\"reason\":\"no eviction table\"}]");
        assertNotNull(problem);
        assertTrue(problem.contains("2,420 households were evicted"), problem);
        assertTrue(problem.startsWith("validation refused: 1 claim(s)"), problem);
    }

    @Test void citingTheArticleItselfDoesNotCount() throws Exception {
        fetch(ARTICLE, null);
        assertNotNull(gate("[{\"assertion\":\"Evictions hit a 12-year high\","
            + "\"verdict\":\"not checkable here\",\"reason\":\"x\","
            + "\"sources\":[{\"title\":\"The 51st\",\"url\":\"" + ARTICLE + "\"}]}]"));
    }

    @Test void aFailedFetchOfACitedSourceCounts() throws Exception {
        fetch("https://ota.dc.gov/page/eviction-data", "HTTP 404");
        assertNull(gate("[{\"assertion\":\"2,420 households were evicted\","
            + "\"verdict\":\"not checkable here\",\"reason\":\"OTA page returned 404\","
            + "\"sources\":[{\"title\":\"OTA\",\"url\":\"https://ota.dc.gov/page/eviction-data\"}]}]"));
    }

    @Test void aSourceNeverFetchedDoesNotCount() throws Exception {
        assertNotNull(gate("[{\"assertion\":\"ERAP fell to $7 million\",\"verdict\":\"true\","
            + "\"independent_value\":\"$7M\","
            + "\"sources\":[{\"title\":\"DCFPI\",\"url\":\"https://www.dcfpi.org/erap\"}]}]"));
    }

    @Test void listedSearchesCountEvenWhenNothingWasFound() throws Exception {
        assertNull(gate("[{\"assertion\":\"Unpaid rent rose to $26.8 million\","
            + "\"verdict\":\"not checkable here\",\"reason\":\"no public DCHFA figure found\","
            + "\"searched\":[\"DCHFA unpaid rent 2025 affordable properties\"]}]"));
    }

    @Test void blankSearchesDoNotCount() throws Exception {
        assertNotNull(gate("[{\"assertion\":\"Unpaid rent rose to $26.8 million\","
            + "\"verdict\":\"not checkable here\",\"reason\":\"x\",\"searched\":[\"  \"]}]"));
    }

    @Test void warehouseClaimsAndAttacksAreExempt() throws Exception {
        assertNull(gate("[{\"assertion\":\"Rent burden is 45%\",\"verdict\":\"true\","
            + "\"warehouse_value\":\"44.3%\"},"
            + "{\"assertion\":\"He is a liar\",\"kind\":\"attack\",\"verdict\":\"unsupported\","
            + "\"reason\":\"insult\"}]"));
    }

    private static final String ARTICLE_HTML = "<html><body>"
        + "<header><a href='/about'>About</a></header>"
        + "<nav><a href='https://51st.news/news'>News</a></nav>"
        + "<article><h1>D.C. evictions</h1>"
        + "<p>Per <a href='https://www.dcbarfoundation.org/_files/clcpp.pdf'>CLCPP</a> and "
        + "<a href='https://www.newamerica.org/insights/dc-evictions/?utm_source=x'>New America</a>"
        + " and <a href='/record-eviction-levels-and-rising-housing-costs-dc/'>our earlier piece</a>"
        + " (<a href='/author/james-jarvis'>Jarvis</a>, <a href='/tag/housing'>housing</a>, "
        + "<a href='https://twitter.com/intent/tweet?u=x'>share</a>, <a href='#top'>top</a>, "
        + "<a href='mailto:a@b.c'>mail</a>, "
        + "<a href='https://www.newamerica.org/insights/dc-evictions/'>again</a>).</p></article>"
        + "<footer><a href='https://51st.news/donate'>Donate</a></footer></body></html>";

    private static List<String> links() {
        org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(ARTICLE_HTML, ARTICLE);
        doc.select("nav, footer, header").remove();
        return McpServer.referencedLinks(doc, ARTICLE);
    }

    private static String referencedGate() throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceReferencedSourcesRead", String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, ARTICLE);
    }

    @Test void referencedLinksAreTheArticlesCitedSourcesOnly() {
        assertEquals(List.of("https://www.dcbarfoundation.org/_files/clcpp.pdf",
                "https://www.newamerica.org/insights/dc-evictions/?utm_source=x",
                "https://51st.news/record-eviction-levels-and-rising-housing-costs-dc/"),
            links());
    }

    /** 51st.news's theme puts related-story cards, each an {@code <article>}, before the story.
     *  The story is the element with the most paragraph text, and only its links count. */
    @Test void relatedStoryCardsBeforeTheStoryAreIgnored() {
        String html = "<html><body>"
            + "<article class='post-card'><a href='https://51st.news/other-story/'>Other</a>"
            + "<p>Teaser.</p></article>"
            + "<article class='post'><p>A long first paragraph of the actual story, citing "
            + "<a href='https://www.upo.org/eviction-report/?ref=51st.news'>UPO</a>.</p>"
            + "<p>A second long paragraph of the story with more words in it.</p></article>"
            + "</body></html>";
        org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(html, ARTICLE);
        assertEquals(List.of("https://www.upo.org/eviction-report/?ref=51st.news"),
            McpServer.referencedLinks(doc, ARTICLE));
        assertEquals("upo.org/eviction-report",
            McpServer.normalizeUrlForMatch("https://www.upo.org/eviction-report/?ref=51st.news"));
    }

    @Test void unreadReferencedSourceRefusesThePublish() throws Exception {
        McpServer.REFERENCED_LINKS.put(McpServer.normalizeUrlForMatch(ARTICLE), links());
        try {
            fetch(ARTICLE, null);
            fetch("https://newamerica.org/insights/dc-evictions", null);
            fetch("https://51st.news/record-eviction-levels-and-rising-housing-costs-dc", null);
            String problem = referencedGate();
            assertNotNull(problem);
            assertTrue(problem.contains("clcpp.pdf"), problem);
            assertTrue(problem.contains("3 source(s) and 1 were never fetched"), problem);
            fetch("https://www.dcbarfoundation.org/_files/clcpp.pdf", "HTTP 403");
            assertNull(referencedGate());
        } finally {
            McpServer.REFERENCED_LINKS.clear();
        }
    }

    @Test void articleNeverFetchedRefusesThePublish() throws Exception {
        String problem = referencedGate();
        assertNotNull(problem);
        assertTrue(problem.contains("was not fetched with web_fetch"), problem);
    }

    @Test void articleFetchedWithoutHtmlLinksPasses() throws Exception {
        fetch(ARTICLE, "HTTP 403");
        assertNull(referencedGate());
    }

    @Test void urlSpellingsOfOnePageMatch() throws Exception {
        fetch("http://www.NewAmerica.org/insights/dc-evictions/#top", null);
        assertNull(gate("[{\"assertion\":\"FY2025 evictions were 1,933\",\"verdict\":\"true\","
            + "\"independent_value\":\"1,933\",\"sources\":[{\"title\":\"New America\","
            + "\"url\":\"https://newamerica.org/insights/dc-evictions\"}]}]"));
        assertEquals("newamerica.org/insights/dc-evictions",
            McpServer.normalizeUrlForMatch("http://www.NewAmerica.org/insights/dc-evictions/#top"));
    }

    private static String figuresGate(String claimsJson) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceFiguresAccountedFor",
            JsonNode.class, String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, MAPPER.readTree(claimsJson), ARTICLE);
    }

    private static final String NEW_REPORT = "https://example.org/clcpp-jan-jun-2026.pdf";
    private static final String OLD_REPORT = "https://example.org/clcpp-jul-dec-2025.pdf";

    private void fetchedTwoReports() throws Exception {
        fetch(ARTICLE, null);
        fetch(OLD_REPORT, null);
        fetch(NEW_REPORT, null);
        McpServer.FETCHED_TEXT.put(McpServer.normalizeUrlForMatch(ARTICLE),
            List.of("The share fell from 77% to 63% across 1,487 cases."));
        McpServer.FETCHED_TEXT.put(McpServer.normalizeUrlForMatch(OLD_REPORT),
            List.of("possession fell from 77% to 66% in the current period"));
        McpServer.FETCHED_TEXT.put(McpServer.normalizeUrlForMatch(NEW_REPORT),
            List.of("intro page", "tenants retained possession in 319 cases (63%) and ...",
                "served with a complaint in 1,487 (86%)"));
    }

    @Test void distinctiveFiguresSkipsYearsAndSmallIntegers() {
        assertEquals(List.of("63%", "1,487", "$21.5", "4.1%", "$25,000", "$50,000"),
            McpServer.distinctiveFigures("In 2026, 63% of 1,487 cases; $21.5 million; 4.1% and 12 days; "
                + "\"$25,000 or $50,000,\" he said"));
    }

    @Test void findAcrossFetchedSearchesEverySourceExceptTheArticle() throws Exception {
        try {
            fetchedTwoReports();
            JsonNode r = McpServer.findAcrossFetched(List.of("63%"), ARTICLE);
            assertEquals(2, r.path("sources_searched").asInt());
            assertEquals(1, r.path("sources_with_matches").size());
            JsonNode src = r.path("sources_with_matches").get(0);
            assertEquals(NEW_REPORT, src.path("source_url").asText());
            assertEquals(2, src.path("find").get(0).path("matches").get(0).path("page").asInt());
        } finally {
            McpServer.FETCHED_TEXT.clear();
        }
    }

    @Test void negativeVerdictOnAFigureFoundInAFetchedSourceIsRefused() throws Exception {
        try {
            fetchedTwoReports();
            String problem = figuresGate("[{\"assertion\":\"among tenants represented by a CLCPP "
                + "attorney, the share who stayed home fell from 77% to 63%\","
                + "\"verdict\":\"partially false\",\"article_value\":\"63%\"}]");
            assertNotNull(problem);
            assertTrue(problem.contains("clcpp-jan-jun-2026.pdf page 2"), problem);
            assertTrue(problem.contains("319 cases (63%)"), problem);
        } finally {
            McpServer.FETCHED_TEXT.clear();
        }
    }

    @Test void whyNotOrAPositiveVerdictPasses() throws Exception {
        try {
            fetchedTwoReports();
            assertNull(figuresGate("[{\"assertion\":\"fell to 63%\",\"verdict\":\"false\","
                + "\"article_value\":\"63%\",\"why_not\":\"that 63% covers all service levels, "
                + "not fully represented tenants\"}]"));
            assertNull(figuresGate("[{\"assertion\":\"fell to 63%\",\"verdict\":\"true\","
                + "\"article_value\":\"63%\"}]"));
        } finally {
            McpServer.FETCHED_TEXT.clear();
        }
    }

    @Test void aFigureOnlyInTheArticleItselfPasses() throws Exception {
        try {
            fetchedTwoReports();
            McpServer.FETCHED_TEXT.put(McpServer.normalizeUrlForMatch(ARTICLE),
                List.of("2,420 households were evicted in fiscal year 2025."));
            assertNull(figuresGate("[{\"assertion\":\"2,420 households were evicted\","
                + "\"verdict\":\"unsupported\",\"reason\":\"x\",\"article_value\":\"2,420\"}]"));
        } finally {
            McpServer.FETCHED_TEXT.clear();
        }
    }

    private static String measureGate(String claimsJson) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceMeasureSearched", JsonNode.class);
        m.setAccessible(true);
        return (String) m.invoke(null, MAPPER.readTree(claimsJson));
    }

    @Test void unconfirmableClaimWithoutAMeasureIsRefused() throws Exception {
        String problem = measureGate("[{\"assertion\":\"prices were 4.1% higher\","
            + "\"verdict\":\"not checkable here\",\"reason\":\"BLS returned 403\"}]");
        assertNotNull(problem);
        assertTrue(problem.contains("no `measure`"), problem);
    }

    private static void searched(String query) throws Exception {
        ObjectNode a = MAPPER.createObjectNode();
        a.put("query", query);
        recordCall("search_catalog", a, -1, null);
    }

    @Test void measureMustBeASearchThisSessionRan() throws Exception {
        String claim = "[{\"assertion\":\"prices were 4.1% higher\",\"verdict\":\"not checkable here\","
            + "\"reason\":\"BLS 403\",\"measure\":\"Washington metro CPI\"}]";
        String problem = measureGate(claim);
        assertNotNull(problem);
        assertTrue(problem.contains("never called with that query"), problem);
        searched("  washington   METRO cpi ");
        assertNull(measureGate(claim));
    }

    @Test void measureCheckLeavesWarehouseAndPositiveClaimsAlone() throws Exception {
        assertNull(measureGate("[{\"assertion\":\"a\",\"verdict\":\"not checkable here\","
            + "\"reason\":\"x\",\"warehouse_value\":\"4.12%\"},"
            + "{\"assertion\":\"b\",\"verdict\":\"true\"},"
            + "{\"assertion\":\"He is a liar\",\"kind\":\"attack\",\"verdict\":\"unsupported\","
            + "\"reason\":\"insult\"}]"));
    }

    /** Run 9's false alarm: "5.9%" in an unrelated court caseload table is not the source of an
     *  unemployment claim. */
    @Test void aFigureInAnUnrelatedPassageIsNotEvidence() throws Exception {
        try {
            fetch(ARTICLE, null);
            String courts = "https://www.dccourts.gov/summary.pdf";
            fetch(courts, null);
            McpServer.FETCHED_TEXT.put(McpServer.normalizeUrlForMatch(courts),
                List.of("Tax Division Civil 622 636 773 -25.8% Criminal 5.9% Total 624"));
            assertNull(figuresGate("[{\"assertion\":\"the District's unemployment rate rose to "
                + "5.9% in August\",\"verdict\":\"partially false\",\"article_value\":\"5.9%\"}]"));
        } finally {
            McpServer.FETCHED_TEXT.clear();
        }
    }

    private static String coverageGate(String claimsJson, String unclaimedJson) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceArticleFiguresClaimed",
            JsonNode.class, String.class, JsonNode.class);
        m.setAccessible(true);
        return (String) m.invoke(null, MAPPER.readTree(claimsJson), ARTICLE,
            MAPPER.readTree(unclaimedJson));
    }

    @Test void everyArticleFigureMustBeClaimedOrExcused() throws Exception {
        try {
            McpServer.STORY_TEXT.put(McpServer.normalizeUrlForMatch(ARTICLE),
                "Prices rose 4.1% and 2,420 households were evicted; share fell to 63% across "
                + "1,487 cases. Call 202-555-0100 for help (since 2019).");
            String problem = coverageGate("[{\"assertion\":\"prices rose 4.1%\",\"verdict\":\"true\"},"
                + "{\"assertion\":\"2,420 households were evicted\",\"verdict\":\"unsupported\"}]",
                "[]");
            assertNotNull(problem);
            assertTrue(problem.contains("2 figure(s)") && problem.contains("63%")
                && problem.contains("1,487"), problem);
            assertNull(coverageGate("[{\"assertion\":\"prices rose 4.1%\",\"verdict\":\"true\"},"
                + "{\"assertion\":\"2,420 households were evicted\",\"verdict\":\"unsupported\"},"
                + "{\"assertion\":\"share fell to 63%\",\"verdict\":\"true\","
                + "\"article_value\":\"63% of 1,487 cases\"}]", "[]"));
            assertNull(coverageGate("[{\"assertion\":\"prices rose 4.1%\",\"verdict\":\"true\"},"
                + "{\"assertion\":\"2,420 households were evicted\",\"verdict\":\"unsupported\"}]",
                "[{\"figure\":\"63%\",\"why\":\"a quote's aside\"},"
                + "{\"figure\":\"1,487\",\"why\":\"page count\"}]"));
        } finally {
            McpServer.STORY_TEXT.clear();
        }
    }

    @Test void coverageGateNeedsTheArticleText() throws Exception {
        McpServer.STORY_TEXT.clear();
        assertNull(coverageGate("[]", "[]"));
    }
}
