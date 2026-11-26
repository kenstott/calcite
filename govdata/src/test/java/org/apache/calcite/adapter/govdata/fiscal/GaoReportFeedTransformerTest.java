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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class GaoReportFeedTransformerTest {

  private static final RequestContext CONTEXT =
      RequestContext.builder().url("https://www.gao.gov/rss/reports.xml").build();

  private static String item(String link, String pubDate, String description) {
    return "<item><title>Air Traffic Control: FAA &amp; Aging Systems</title><link>" + link
        + "</link><description>\n" + description + "\n</description><pubDate>" + pubDate
        + "</pubDate><guid>/products/x</guid></item>";
  }

  private static String feed(String items) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rss version=\"2.0\"><channel>"
        + "<title>Reports</title><link>https://www.gao.gov/rss/reports.xml</link>" + items
        + "</channel></rss>";
  }

  private static final String GOOD = item("https://www.gao.gov/products/gao-27-108423",
      "Mon, 23 Nov 2026 07:13:29 -0500", "What GAO Found\n\nA finding.");

  @Test void parsesOneRowPerItem() throws Exception {
    String json = new GaoReportFeedTransformer().transform(
        feed(GOOD + item("https://www.gao.gov/products/gao-27-109529",
            "Wed, 18 Nov 2026 23:45:24 -0500", "Other.")), CONTEXT);
    JsonNode rows = new ObjectMapper().readTree(json);
    assertEquals(2, rows.size());
    assertEquals("GAO-27-108423", rows.get(0).get("product_number").asText());
    assertEquals("Air Traffic Control: FAA & Aging Systems", rows.get(0).get("title").asText());
    assertEquals("https://www.gao.gov/products/gao-27-108423",
        rows.get(0).get("product_url").asText());
    assertEquals("2026-11-23", rows.get(0).get("published_date").asText());
    assertEquals("What GAO Found\n\nA finding.", rows.get(0).get("summary_text").asText());
    // The date is the one GAO printed, not its UTC conversion (19 Nov).
    assertEquals("2026-11-18", rows.get(1).get("published_date").asText());
  }

  @Test void failsOnBlockPage() {
    assertThrows(IllegalStateException.class, () -> new GaoReportFeedTransformer()
        .transform("<html><body>Access Denied</body></html>", CONTEXT));
    assertThrows(IllegalStateException.class, () -> new GaoReportFeedTransformer()
        .transform("<!DOCTYPE html><html><body>", CONTEXT));
  }

  @Test void failsOnEmptyResponseOrFeed() {
    assertThrows(IllegalStateException.class,
        () -> new GaoReportFeedTransformer().transform("", CONTEXT));
    assertThrows(IllegalStateException.class,
        () -> new GaoReportFeedTransformer().transform(feed(""), CONTEXT));
  }

  @Test void failsOnIncompleteOrUnexpectedItem() {
    String noSummary = item("https://www.gao.gov/products/gao-27-1",
        "Mon, 23 Nov 2026 07:13:29 -0500", " ");
    String otherLink = item("https://www.gao.gov/blog/post",
        "Mon, 23 Nov 2026 07:13:29 -0500", "Text.");
    String badDate = item("https://www.gao.gov/products/gao-27-1", "yesterday", "Text.");
    for (String bad : new String[] {noSummary, otherLink, badDate, GOOD}) {
      // GOOD twice: the same product listed twice in one feed.
      assertThrows(IllegalStateException.class,
          () -> new GaoReportFeedTransformer().transform(feed(GOOD + bad), CONTEXT));
    }
  }
}
