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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The article fact-check procedure reaches the model through a tool result: the first article
 * web_fetch, or a single refusal of a validation report that never received it.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class FactCheckProcedureHandoverTest {

  @BeforeEach @AfterEach void reset() throws Exception {
    McpServer.FACT_CHECK_PROCEDURE_SERVED.set(false);
    McpServer.PROCEDURE_SERVED_BY_CALL.remove();
    callLog().clear();
  }

  @SuppressWarnings("unchecked")
  private static List<ObjectNode> callLog() throws Exception {
    Field f = McpServer.class.getDeclaredField("CALL_LOG");
    f.setAccessible(true);
    return (List<ObjectNode>) f.get(null);
  }

  private static void recordCall(String tool, String error) throws Exception {
    Method m = McpServer.class.getDeclaredMethod("recordCall", String.class, JsonNode.class,
        long.class, int.class, ObjectNode.class, String.class);
    m.setAccessible(true);
    m.invoke(null, tool, null, 1L, -1, null, error);
  }

  private static boolean recipeConsulted() throws Exception {
    Method m = McpServer.class.getDeclaredMethod("recipeConsulted");
    m.setAccessible(true);
    return (Boolean) m.invoke(null);
  }

  @Test void newsPagesAreArticles() {
    assertTrue(McpServer.isArticleUrl(
        "https://investing.com/news/stock-market-news/us-traffic-deaths-fall-5016323"));
    assertTrue(McpServer.isArticleUrl("https://www.aol.com/articles/us-traffic-deaths.html"));
  }

  @Test void governmentHostsAndDataFilesAreNotArticles() {
    assertFalse(McpServer.isArticleUrl(
        "https://www.nhtsa.gov/press-releases/decline-traffic-fatalities-first-half-2026"));
    assertFalse(McpServer.isArticleUrl("https://example.mil/page"));
    assertFalse(McpServer.isArticleUrl("https://example.org/report/813833.pdf"));
    assertFalse(McpServer.isArticleUrl("https://example.org/data/series.csv"));
    assertFalse(McpServer.isArticleUrl("not a url"));
  }

  @Test void noticeCarriesTheRecipeSteps() {
    String notice = McpServer.factCheckProcedureNotice();
    assertTrue(notice.contains("Every comparative or streak phrase MUST be its own claim"));
    assertTrue(notice.contains("The central claim MUST be graded on the evidence found"));
  }

  @Test void refusalHandsOverTheProcedureOnceThenPasses() {
    String first = McpServer.enforceFactCheckProcedureServed();
    assertNotNull(first);
    assertTrue(first.startsWith("validation refused"));
    assertTrue(first.contains("streak phrase"));
    assertNull(McpServer.enforceFactCheckProcedureServed());
  }

  @Test void procedureAlreadyServedMeansNoRefusal() {
    McpServer.FACT_CHECK_PROCEDURE_SERVED.set(true);
    assertNull(McpServer.enforceFactCheckProcedureServed());
  }

  @Test void theCallThatHandsOverTheProcedureIsLoggedAsServingIt() throws Exception {
    String refusal = McpServer.enforceFactCheckProcedureServed();
    recordCall("preview_report", refusal);
    recordCall("query", null);
    List<ObjectNode> log = callLog();
    assertEquals("fact-check-a-news-article", log.get(0).path("procedure_served").asText());
    assertFalse(log.get(1).has("procedure_served"));
  }

  @Test void aRecipeLookupCountsOnlyWithinTheCurrentTask() throws Exception {
    assertFalse(recipeConsulted());
    recordCall("find_recipe", null);
    assertTrue(recipeConsulted());
  }

  @Test void theHeadlineIsReadFromTheArticleUrl() {
    assertEquals("us traffic deaths fall in first half of 2026 lowest rate since 2014",
        McpServer.headlineFromUrl("https://investing.com/news/stock-market-news/"
            + "us-traffic-deaths-fall-in-first-half-of-2026-lowest-rate-since-2014-5016323"));
    assertEquals("us traffic deaths fell sharply in early 2026",
        McpServer.headlineFromUrl("https://in.marketscreener.com/news/"
            + "us-traffic-deaths-fell-sharply-in-early-2026-ce7f5ed9db80f22c"));
    assertEquals("traffic deaths down in first half",
        McpServer.headlineFromUrl("https://www.detroitnews.com/story/2026/11/10/"
            + "traffic-deaths-down-in-first-half/92708095007/"));
    assertNull(McpServer.headlineFromUrl("https://example.com/article/12345"));
  }

  @Test void newsFeedItemsYieldThePublishersUrls() {
    String rss = "<rss><channel><link>https://www.bing.com/news/search?q=x&amp;format=rss"
        + "</link><item><link>http://www.bing.com/news/apiclick.aspx?ref=FexRss&amp;aid=&amp;"
        + "url=https%3a%2f%2fwww.usnews.com%2fnews%2fa%2f2026-11-10%2fus-traffic-deaths&amp;"
        + "c=1&amp;mkt=en-us</link></item><item><link>http://www.bing.com/news/apiclick.aspx?"
        + "ref=FexRss&amp;url=https%3a%2f%2fwww.detroitnews.com%2fstory%2fx%2f&amp;c=2</link>"
        + "</item></channel></rss>";
    assertEquals(List.of("https://www.usnews.com/news/a/2026-11-10/us-traffic-deaths",
        "https://www.detroitnews.com/story/x/"), McpServer.newsFeedArticleUrls(rss));
  }

    @Test
    void evalValidationWithoutRunSubpathIsRefused() {
        ObjectNode args = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode();
        assertNull(McpServer.enforceRunSubpathPresent(args, false));
        String refusal = McpServer.enforceRunSubpathPresent(args, true);
        assertNotNull(refusal);
        assertTrue(refusal.contains("run_subpath"), refusal);
        args.put("run_subpath", "q14/askamerica/2026-11-10");
        assertNull(McpServer.enforceRunSubpathPresent(args, true));
    }

    @Test
    void procedureGradesAMatchingEstimateTrue() throws Exception {
        try (java.io.InputStream in = McpServer.class.getResourceAsStream("/recipes.json")) {
            assertNotNull(in);
            String json = new String(readAll(in), java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(json.contains("the figure being preliminary MUST NOT lower the verdict"));
        }
    }

    private static byte[] readAll(java.io.InputStream in) throws java.io.IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }
}
