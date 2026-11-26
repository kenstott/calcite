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
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringReader;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Turns GAO's reports RSS feed ({@code https://www.gao.gov/rss/reports.xml}) into the rows of
 * {@code gao_report_feed}: one row per {@code <item>}.
 *
 * <p>The feed is a rolling window with no history, so a response that is not that feed (for
 * example a bot-protection page), a feed with no items, or an item missing a field fails the
 * run rather than recording a partial capture.
 */
public class GaoReportFeedTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GaoReportFeedTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Pattern PRODUCT_LINK =
      Pattern.compile("https://www\\.gao\\.gov/products/([A-Za-z0-9][A-Za-z0-9.-]*)");

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      throw new IllegalStateException("gao_report_feed: empty response from " + context.getUrl());
    }
    Element root = parse(response.trim(), context.getUrl()).getDocumentElement();
    if (!"rss".equals(root.getTagName())) {
      throw new IllegalStateException("gao_report_feed: expected an <rss> document from "
          + context.getUrl() + ", got <" + root.getTagName() + ">");
    }
    NodeList items = root.getElementsByTagName("item");
    if (items.getLength() == 0) {
      throw new IllegalStateException("gao_report_feed: no items in feed " + context.getUrl());
    }
    ArrayNode rows = MAPPER.createArrayNode();
    Set<String> seen = new HashSet<String>();
    for (int i = 0; i < items.getLength(); i++) {
      Element item = (Element) items.item(i);
      String link = required(item, "link", i);
      Matcher m = PRODUCT_LINK.matcher(link);
      if (!m.matches()) {
        throw new IllegalStateException("gao_report_feed: item " + i
            + " link is not a GAO product page: " + link);
      }
      String productNumber = m.group(1).toUpperCase(Locale.ROOT);
      if (!seen.add(productNumber)) {
        throw new IllegalStateException("gao_report_feed: product " + productNumber
            + " is listed twice in one feed");
      }
      ObjectNode row = rows.addObject();
      row.put("product_number", productNumber);
      row.put("title", required(item, "title", i));
      row.put("product_url", link);
      row.put("published_date", publishedDate(required(item, "pubDate", i)));
      row.put("summary_text", required(item, "description", i));
    }
    LOGGER.info("gao_report_feed: {} items", Integer.valueOf(rows.size()));
    return rows.toString();
  }

  /** The calendar date as GAO printed it (US Eastern offset), from an RFC 1123 pubDate. */
  static String publishedDate(String pubDate) {
    try {
      return ZonedDateTime.parse(pubDate, DateTimeFormatter.RFC_1123_DATE_TIME)
          .toLocalDate().toString();
    } catch (DateTimeParseException e) {
      throw new IllegalStateException("gao_report_feed: unreadable pubDate '" + pubDate + "'", e);
    }
  }

  private static String required(Element item, String tag, int index) {
    NodeList nodes = item.getElementsByTagName(tag);
    String text = nodes.getLength() == 0 ? "" : nodes.item(0).getTextContent().trim();
    if (text.isEmpty()) {
      throw new IllegalStateException("gao_report_feed: item " + index + " has no <" + tag + ">");
    }
    return text;
  }

  private static Document parse(String xml, String url) {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setExpandEntityReferences(false);
      return factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
    } catch (ParserConfigurationException e) {
      throw new IllegalStateException("gao_report_feed: XML parser unavailable", e);
    } catch (SAXException e) {
      throw new IllegalStateException("gao_report_feed: response from " + url
          + " is not well-formed XML", e);
    } catch (IOException e) {
      throw new IllegalStateException("gao_report_feed: could not read response from " + url, e);
    }
  }
}
