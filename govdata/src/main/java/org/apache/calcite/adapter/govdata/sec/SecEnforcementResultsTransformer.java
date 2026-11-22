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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the SEC's annual enforcement-results press releases into fiscal-year totals.
 *
 * <p>The source URL is one press release page ({@code /newsroom/press-releases/<release>}). The
 * release links an addendum PDF whose first page carries the statistics table; that PDF is
 * downloaded through {@link EdgarRateLimiter} (the host-wide sec.gov limiter) and read with
 * PDFBox. Two table shapes exist in the PDF text: a multi-year table (the FY2024 addendum covers
 * FY2019 to FY2024) and a single-year table (FY2026). Where the addendum's table is an image with
 * no text (FY2025), the release prose gives the action counts, so the prose is parsed instead and
 * the columns the prose does not publish stay null.
 */
public class SecEnforcementResultsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern TITLE_FY = Pattern.compile("Fiscal Year (\\d{4})");
  private static final Pattern FY_HEADER = Pattern.compile("FY\\s*(\\d{4})\\d?");
  private static final Pattern NUM = Pattern.compile("\\$?([\\d,]+)");
  private static final Pattern PDF_LINK =
      Pattern.compile("(addendum|enforcement-statistics|annual-results)", Pattern.CASE_INSENSITIVE);

  @Override public String transform(String response, RequestContext context) {
    String release = context.getDimensionValues().get("release");
    if (release == null) {
      throw new IllegalStateException("sec enforcement_annual_results: no 'release' dimension");
    }
    Document page = Jsoup.parse(response, "https://www.sec.gov");
    String releaseUrl = context.getUrl();
    ArrayNode out = MAPPER.createArrayNode();
    try {
      String addendumUrl = addendumUrl(page, release);
      String pdfText = pdfText(download(addendumUrl));
      Integer suspensions = tradingSuspensions(page.text());
      if (pdfText.contains("Total Actions")) {
        if (pdfText.contains("Enforcement Actions Filed in Fiscal Years")) {
          parseMultiYear(pdfText, releaseUrl, addendumUrl, out);
        } else {
          parseSingleYear(pdfText, releaseYear(page, release), suspensions, releaseUrl, addendumUrl,
              out);
        }
      } else {
        parseProse(page.text(), releaseYear(page, release), suspensions, releaseUrl, addendumUrl,
            out);
      }
    } catch (IOException e) {
      throw new RuntimeException("sec enforcement_annual_results: failed for " + release, e);
    }
    if (out.size() == 0) {
      throw new IllegalStateException("sec enforcement_annual_results: no rows for " + release);
    }
    return out.toString();
  }

  static String addendumUrl(Document page, String release) {
    for (Element a : page.select("a[href$=.pdf]")) {
      String href = a.absUrl("href");
      if (href.contains("/files/") && !href.contains("/litigation/") && PDF_LINK.matcher(href).find()) {
        return href;
      }
    }
    throw new IllegalStateException("sec enforcement_annual_results: no addendum PDF link on " + release);
  }

  static int releaseYear(Document page, String release) {
    Matcher m = TITLE_FY.matcher(page.title());
    if (!m.find()) {
      throw new IllegalStateException("sec enforcement_annual_results: no fiscal year in title of "
          + release + ": " + page.title());
    }
    return Integer.parseInt(m.group(1));
  }

  static Integer tradingSuspensions(String text) {
    Matcher m = Pattern.compile("instituted (\\d+) trading suspensions").matcher(text);
    return m.find() ? Integer.valueOf(m.group(1)) : null;
  }

  static String pdfText(byte[] pdf) throws IOException {
    try (PDDocument doc = PDDocument.load(pdf)) {
      PDFTextStripper stripper = new PDFTextStripper();
      stripper.setSortByPosition(true);
      stripper.setStartPage(1);
      stripper.setEndPage(Math.min(2, doc.getNumberOfPages()));
      return stripper.getText(doc);
    }
  }

  /** Multi-year addendum: three tables (actions, money ordered, money distributed) by fiscal year. */
  static void parseMultiYear(String text, String releaseUrl, String addendumUrl, ArrayNode out) {
    int money = text.indexOf("Total Money Ordered");
    int dist = text.indexOf("Money Distributed to Harmed Investors");
    if (money < 0 || dist < 0) {
      throw new IllegalStateException("sec enforcement_annual_results: money tables not found");
    }
    String actions = text.substring(0, money);
    String ordered = text.substring(money, dist);
    String distributed = text.substring(dist);
    List<Integer> years = years(actions);
    List<Long> standalone = row(actions, "Standalone Enforcement Actions", years.size());
    List<Long> followOn = row(actions, "Follow -?On Admin\\. Proceedings", years.size());
    List<Long> delinquent = row(actions, "Delinquent Filings", years.size());
    List<Long> total = row(actions, "Total Actions", years.size());
    List<Long> penalties = row(ordered, "Penalties", years.size());
    List<Long> disgorgement = row(ordered, "Disgorgement", years.size());
    List<Long> totalMoney = row(ordered, "Total", years.size());
    List<Long> paid = row(distributed, "", years.size());
    for (int i = 0; i < years.size(); i++) {
      ObjectNode r = MAPPER.createObjectNode();
      r.put("fiscal_year", years.get(i));
      r.put("standalone_actions", standalone.get(i));
      r.put("follow_on_actions", followOn.get(i));
      r.put("delinquent_filings", delinquent.get(i));
      r.put("total_actions", total.get(i));
      r.putNull("trading_suspensions");
      r.put("penalties_millions", penalties.get(i));
      r.put("disgorgement_millions", disgorgement.get(i));
      r.put("total_money_ordered_millions", totalMoney.get(i));
      r.putNull("offsets_millions");
      r.put("distributed_to_investors_millions", paid.get(i));
      r.put("release_url", releaseUrl);
      r.put("addendum_url", addendumUrl);
      out.add(r);
    }
  }

  private static List<Integer> years(String text) {
    Matcher m = FY_HEADER.matcher(text);
    List<Integer> years = new ArrayList<>();
    while (m.find()) {
      years.add(Integer.parseInt(m.group(1)));
    }
    if (years.size() < 2) {
      throw new IllegalStateException("sec enforcement_annual_results: no fiscal-year header");
    }
    return years;
  }

  /** The {@code n} numbers that follow {@code label} on its line. */
  private static List<Long> row(String text, String label, int n) {
    Matcher line = Pattern.compile("(?m)^" + label + "\\s*((?:\\$?[\\d,]+\\s*){" + n + "})\\s*$")
        .matcher(text);
    if (!line.find()) {
      throw new IllegalStateException("sec enforcement_annual_results: row '" + label
          + "' with " + n + " values not found");
    }
    List<Long> values = new ArrayList<>();
    Matcher num = NUM.matcher(line.group(1));
    while (num.find()) {
      values.add(Long.parseLong(num.group(1).replace(",", "")));
    }
    return values;
  }

  /** Single-year addendum (FY2026 layout): one label, one value. */
  static void parseSingleYear(String text, int year, Integer suspensions, String releaseUrl,
      String addendumUrl, ArrayNode out) {
    ObjectNode r = MAPPER.createObjectNode();
    r.put("fiscal_year", year);
    r.put("standalone_actions", one(text, "Standalone Actions\\d*\\s+(\\d+)"));
    r.put("follow_on_actions", one(text, "Follow-On APs\\s+(\\d+)"));
    r.put("delinquent_filings", one(text, "Delinquent Filings\\s+(\\d+)"));
    r.put("total_actions", one(text, "Total Actions\\s+(\\d+)"));
    if (suspensions == null) {
      r.putNull("trading_suspensions");
    } else {
      r.put("trading_suspensions", suspensions);
    }
    r.put("penalties_millions", one(text, "Penalties\\s+\\$([\\d,]+)"));
    r.put("disgorgement_millions", one(text, "Disgorgement\\s+\\$([\\d,]+)"));
    r.put("total_money_ordered_millions", one(text, "Total\\d?\\s+\\$([\\d,]+)"));
    r.put("offsets_millions", one(text, "Certain Offsets\\d?\\s+\\$([\\d,]+)"));
    r.put("distributed_to_investors_millions", one(text, "Harmed Investors\\s+\\$([\\d,]+)"));
    r.put("release_url", releaseUrl);
    r.put("addendum_url", addendumUrl);
    out.add(r);
  }

  private static long one(String text, String regex) {
    Matcher m = Pattern.compile(regex).matcher(text);
    if (!m.find()) {
      throw new IllegalStateException("sec enforcement_annual_results: '" + regex + "' not found");
    }
    return Long.parseLong(m.group(1).replace(",", ""));
  }

  /** Prose layout (FY2025): counts and the headline money total only. */
  static void parseProse(String text, int year, Integer suspensions, String releaseUrl,
      String addendumUrl, ArrayNode out) {
    Matcher m = Pattern.compile("filed (\\d+) enforcement actions, including (\\d+) standalone"
        + " actions and (\\d+) .follow-on. administrative proceedings").matcher(text);
    if (!m.find()) {
      throw new IllegalStateException("sec enforcement_annual_results: no action counts in prose");
    }
    Matcher money = Pattern.compile("monetary relief totaling \\$([\\d.]+) (billion|million)")
        .matcher(text);
    if (!money.find()) {
      throw new IllegalStateException("sec enforcement_annual_results: no monetary relief in prose");
    }
    double amount = Double.parseDouble(money.group(1));
    long millions = Math.round("billion".equals(money.group(2)) ? amount * 1000 : amount);
    ObjectNode r = MAPPER.createObjectNode();
    r.put("fiscal_year", year);
    r.put("standalone_actions", Long.parseLong(m.group(2)));
    r.put("follow_on_actions", Long.parseLong(m.group(3)));
    r.putNull("delinquent_filings");
    r.put("total_actions", Long.parseLong(m.group(1)));
    if (suspensions == null) {
      r.putNull("trading_suspensions");
    } else {
      r.put("trading_suspensions", suspensions);
    }
    r.putNull("penalties_millions");
    r.putNull("disgorgement_millions");
    r.put("total_money_ordered_millions", millions);
    r.putNull("offsets_millions");
    r.putNull("distributed_to_investors_millions");
    r.put("release_url", releaseUrl);
    r.put("addendum_url", addendumUrl);
    out.add(r);
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = EdgarRateLimiter.connect(url, "GET", 30000, 120000);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream()) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = is.read(buf)) > 0) {
        bos.write(buf, 0, n);
      }
      return bos.toByteArray();
    }
  }
}
