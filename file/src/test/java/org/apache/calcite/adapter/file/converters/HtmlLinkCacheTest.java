/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.converters;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HtmlLinkCache}.
 */
@Tag("unit")
class HtmlLinkCacheTest {

  @Test void testEmptyCache() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlLinkCache cache = new HtmlLinkCache(config);
    assertEquals(0, cache.size());
  }

  @Test void testClear() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlLinkCache cache = new HtmlLinkCache(config);
    cache.clear();
    assertEquals(0, cache.size());
  }

  @Test void testExtractedLinksFromEmptyContent() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com", "", config);

    assertNotNull(links);
    assertNotNull(links.getDataFileLinks());
    assertTrue(links.getDataFileLinks().isEmpty());
    assertNotNull(links.getHtmlLinks());
    assertTrue(links.getHtmlLinks().isEmpty());
    assertNotNull(links.getTables());
    assertTrue(links.getTables().isEmpty());
    assertEquals("", links.getContentHash());
  }

  @Test void testExtractedLinksFromNullContent() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com", null, config);

    assertNotNull(links);
    assertTrue(links.getDataFileLinks().isEmpty());
  }

  @Test void testExtractedLinksWithDataFiles() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    String html = "<html><body>"
        + "<a href=\"http://example.com/data.csv\">CSV</a>"
        + "<a href=\"http://example.com/data.xlsx\">Excel</a>"
        + "<a href=\"http://example.com/page.html\">Link</a>"
        + "</body></html>";

    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com", html, config);

    assertNotNull(links.getDataFileLinks());
    assertTrue(links.getDataFileLinks().contains("http://example.com/data.csv"));
    assertTrue(links.getDataFileLinks().contains("http://example.com/data.xlsx"));
    assertFalse(links.getDataFileLinks().contains("http://example.com/page.html"));
  }

  @Test void testExtractedLinksContentHash() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    String html = "<html><body>test</body></html>";

    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com", html, config);

    assertNotNull(links.getContentHash());
    assertFalse(links.getContentHash().isEmpty());
  }

  @Test void testExtractedLinksHashDeterministic() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    String html = "<html><body>same content</body></html>";

    HtmlLinkCache.ExtractedLinks links1 =
        new HtmlLinkCache.ExtractedLinks("http://example.com", html, config);
    HtmlLinkCache.ExtractedLinks links2 =
        new HtmlLinkCache.ExtractedLinks("http://example.com", html, config);

    assertEquals(links1.getContentHash(), links2.getContentHash());
  }

  @Test void testExtractedLinksHashDifferentContent() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();

    HtmlLinkCache.ExtractedLinks links1 =
        new HtmlLinkCache.ExtractedLinks("http://example.com",
            "<html><body>content A</body></html>", config);
    HtmlLinkCache.ExtractedLinks links2 =
        new HtmlLinkCache.ExtractedLinks("http://example.com",
            "<html><body>content B</body></html>", config);

    assertFalse(links1.getContentHash().equals(links2.getContentHash()));
  }

  @Test void testExtractedLinksIsExpired() throws InterruptedException, IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com",
            "<html><body>test</body></html>", config);

    // Should not be immediately expired with a long TTL
    assertFalse(links.isExpired(Duration.ofHours(1)));

    // Should be expired with a very short TTL after waiting
    Thread.sleep(50);
    assertTrue(links.isExpired(Duration.ofMillis(1)));
  }

  @Test void testExtractedLinksWithHttpMetadata() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com",
            "<html><body>test</body></html>", config);

    HtmlLinkCache.ExtractedLinks withMeta = links.withHttpMetadata("etag-123", "Mon, 01 Jan 2024");
    assertNotNull(withMeta);
    // Should maintain the same data
    assertEquals(links.getContentHash(), withMeta.getContentHash());
    assertEquals(links.getDataFileLinks(), withMeta.getDataFileLinks());
  }

  @Test void testExtractedLinksTablesDisabled() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setGenerateTablesFromHtml(false);

    String html = "<html><body>"
        + "<table><tr><th>Col1</th></tr><tr><td>val1</td></tr></table>"
        + "</body></html>";

    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com", html, config);

    assertTrue(links.getTables().isEmpty());
  }

  @Test void testExtractedLinksJsonFile() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    String html = "<html><body>"
        + "<a href=\"http://example.com/data.json\">JSON</a>"
        + "<a href=\"http://example.com/data.parquet\">Parquet</a>"
        + "<a href=\"http://example.com/data.tsv\">TSV</a>"
        + "</body></html>";

    HtmlLinkCache.ExtractedLinks links =
        new HtmlLinkCache.ExtractedLinks("http://example.com", html, config);

    assertTrue(links.getDataFileLinks().contains("http://example.com/data.json"));
    assertTrue(links.getDataFileLinks().contains("http://example.com/data.parquet"));
    assertTrue(links.getDataFileLinks().contains("http://example.com/data.tsv"));
  }
}
