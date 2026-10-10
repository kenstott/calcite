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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.govdata.GovDataException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the HTML of a Senate eFD annual/candidate report ({@code /search/view/annual/}) and of a
 * periodic transaction report ({@code /search/view/ptr/}). Every table's header row is checked
 * against the columns this parser reads, and a cell count that differs from the header's throws:
 * a template change is a parser fix, not a section to skip.
 *
 * <p>A part with no table (the form's "No" answer, or "Not required") yields no rows. Part 4a,
 * the summary of PTRs already filed separately, is not read: its rows are the PTR transactions that
 * {@link #parsePtr(String)} reads from the PTR pages themselves.
 */
final class SenateEfdReportParser {

  /** Part number to the headers its table must carry, first (blank) column included. */
  private static final Map<String, List<String>> HEADERS = new LinkedHashMap<String, List<String>>();

  static {
    HEADERS.put("1", Arrays.asList("", "#", "Date", "Activity", "Amount", "Who Paid?",
        "Who received payment?", "Comments"));
    HEADERS.put("2", Arrays.asList("", "#", "Who Was Paid", "Type", "Who Paid", "Amount Paid",
        "Comments"));
    HEADERS.put("3", Arrays.asList("", "Asset", "Asset Type", "Owner", "Value", "Income Type",
        "Income"));
    HEADERS.put("4b", Arrays.asList("", "#", "Owner", "Ticker", "Asset Name", "Transaction Type",
        "Transaction Date", "Amount", "Comments"));
    HEADERS.put("5", Arrays.asList("", "#", "Date", "Recipient", "Gift", "Value", "From",
        "Comments"));
    HEADERS.put("6", Arrays.asList("", "#", "Date(s)", "Traveler(s)", "Travel Type", "Itinerary",
        "Reimbursed For", "Who Paid", "Comments"));
    HEADERS.put("7", Arrays.asList("", "#", "Incurred", "Debtor", "Type", "Points", "Rate (Term)",
        "Amount", "Creditor", "Comments"));
    HEADERS.put("8", Arrays.asList("", "#", "Position Dates", "Position Held", "Entity",
        "Entity Type", "Comments"));
    HEADERS.put("9", Arrays.asList("", "#", "Date", "Parties Involved", "Type",
        "Status and Terms", "Comments"));
    HEADERS.put("10", Arrays.asList("", "#", "Source", "Duties", "Comments"));
  }

  private static final List<String> PTR_HEADERS = Arrays.asList("#", "Transaction Date", "Owner",
      "Ticker", "Asset Name", "Asset Type", "Type", "Amount", "Comment");

  private static final Pattern PART = Pattern.compile("^Part (\\d+[ab]?)\\.");

  private SenateEfdReportParser() {
  }

  /** Rows by part number ("1", "2", "3", "4b", "5" ... "10"); absent when the part has no table. */
  static Map<String, List<Map<String, String>>> parseAnnual(String html) {
    Document doc = Jsoup.parse(html);
    Elements headings = doc.select("h3.h4");
    boolean sawAssets = false;
    for (Element h : headings) {
      if (h.text().startsWith("Part 3.")) {
        sawAssets = true;
      }
    }
    if (!sawAssets) {
      throw new GovDataException("eFD annual report has no 'Part 3. Assets' section");
    }
    Map<String, List<Map<String, String>>> parts = new LinkedHashMap<String, List<Map<String, String>>>();
    String current = null;
    for (Element el : doc.select("h3.h4, table")) {
      if (el.tagName().equals("h3")) {
        Matcher m = PART.matcher(el.text().trim());
        current = m.find() ? m.group(1) : null;
        continue;
      }
      if (current == null || current.equals("4a")) {
        continue;
      }
      List<String> expected = HEADERS.get(current);
      if (expected == null) {
        throw new GovDataException("eFD annual report: unexpected table under Part " + current);
      }
      if (parts.containsKey(current)) {
        throw new GovDataException("eFD annual report: two tables under Part " + current);
      }
      parts.put(current, rows(el, expected, "Part " + current));
    }
    return parts;
  }

  /** The transactions of a periodic transaction report page. */
  static List<Map<String, String>> parsePtr(String html) {
    Document doc = Jsoup.parse(html);
    Elements tables = doc.select("table");
    if (tables.size() != 1) {
      throw new GovDataException("eFD PTR page has " + tables.size() + " tables, expected 1");
    }
    return rows(tables.get(0), PTR_HEADERS, "PTR");
  }

  /** The "Calendar YYYY" reporting year in an annual report's heading, or null when absent. */
  static String calendarYear(String html) {
    Matcher m = Pattern.compile("Calendar\\s+(\\d{4})").matcher(Jsoup.parse(html).select("h1").text());
    return m.find() ? m.group(1) : null;
  }

  private static List<Map<String, String>> rows(Element table, List<String> expected,
      String label) {
    List<String> headers = new ArrayList<String>();
    for (Element th : table.select("thead th")) {
      headers.add(th.text().trim());
    }
    if (!headers.equals(expected)) {
      throw new GovDataException("eFD " + label + " table columns " + headers + " differ from the "
          + "expected " + expected);
    }
    List<Map<String, String>> out = new ArrayList<Map<String, String>>();
    for (Element tr : table.select("tbody > tr")) {
      Elements tds = tr.children();
      if (tds.size() != headers.size()) {
        throw new GovDataException("eFD " + label + " row has " + tds.size() + " cells, header has "
            + headers.size() + ": " + tr.text());
      }
      Map<String, String> row = new LinkedHashMap<String, String>();
      for (int i = 0; i < headers.size(); i++) {
        String header = headers.get(i);
        Element td = tds.get(i);
        if (header.equals("Asset")) {
          Element strong = td.selectFirst("strong");
          String name = strong == null ? td.text().trim() : strong.text().trim();
          row.put("Asset", name);
          row.put("Asset detail", detail(td.text().trim(), name));
        } else if (header.equals("Asset Type")) {
          Element muted = td.selectFirst("div.muted");
          String sub = muted == null ? "" : muted.text().trim();
          row.put("Asset Type", td.ownText().trim());
          row.put("Asset Type detail", sub);
        } else {
          row.put(header.isEmpty() ? "col" + i : header, td.text().trim());
        }
      }
      if (headers.get(0).isEmpty() && headers.get(1).equals("Asset")) {
        row.put("#", row.remove("col0"));
      }
      out.add(row);
    }
    return out;
  }

  /** The asset cell's text after the bold name: location, "Type: ..." and similar notes. */
  private static String detail(String cellText, String name) {
    return cellText.startsWith(name) ? cellText.substring(name.length()).trim() : cellText;
  }
}
