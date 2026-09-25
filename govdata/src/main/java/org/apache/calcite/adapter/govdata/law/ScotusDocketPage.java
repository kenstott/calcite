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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses one docket page from supremecourt.gov
 * ({@code /search.aspx?filename=/docket/docketfiles/html/public/23-583.html}), in the layout used
 * for dockets from 2016 onward.
 *
 * <p>The page has a header table ({@code #docketinfo}) of labelled rows, most of them optional
 * ({@code Title}, {@code Docketed}, {@code Linked with}, {@code Lower Ct}, {@code Case Numbers},
 * {@code Decision Date}, {@code Discretionary Court Decision Date}, {@code Rehearing Denied}), and one {@code table.ProceedingItem} per
 * entry in the docket, each with a date, the entry's text and links to the filed documents.
 *
 * <p>A docket number the Court has no page for does not return an error: the site answers
 * {@code 200} with its generic search page. That page has no header table, and this parser throws
 * for it, so a missing docket is a failure and not an empty one.
 */
final class ScotusDocketPage {

  private static final DateTimeFormatter HEADER_DATE =
      DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.US);

  private static final DateTimeFormatter ENTRY_DATE =
      DateTimeFormatter.ofPattern("MMM d yyyy", Locale.US);

  private static final Pattern NUMBER = Pattern.compile("^No\\.\\s*(\\S+)");

  /** A date with an optional trailing annotation, e.g. {@code April 4, 2017 Rule 12.4}. */
  private static final Pattern DATE_WITH_NOTE =
      Pattern.compile("^([A-Z][a-z]+ \\d{1,2}, \\d{4})(?:\\s+(\\S.*))?$");

  private static final Pattern CASE_NUMBERS_PARENS = Pattern.compile("^\\((.*)\\)$");

  /**
   * The order granting the petition: "Petition GRANTED.", "Petition for a writ of certiorari
   * before judgment GRANTED." or, for an indigent petitioner, "Motion to proceed in forma
   * pauperis and petition for a writ of certiorari GRANTED." A motion that is merely granted
   * (an extension, divided argument) is not it.
   */
  private static final Pattern PETITION_GRANTED = Pattern.compile(
      "^(?:Motion (?:for leave )?to proceed in forma pauperis and )?[Pp]etition"
          + "(?: for (?:a )?writ of certiorari(?: before judgment)?)? GRANTED\\b.*");

  private static final Pattern ARGUED = Pattern.compile("^Argued\\..*");

  private static final Pattern JUDGMENT_ISSUED = Pattern.compile("^Judgment Issued\\..*",
      Pattern.CASE_INSENSITIVE);

  /** A document filed in the case and linked from a docket entry. */
  static final class Filing {
    final String label;
    final String url;

    Filing(String label, String url) {
      this.label = label;
      this.url = url;
    }
  }

  /** One entry in the docket's proceedings and orders. */
  static final class Entry {
    /** Position in the docket, from 1, in the page's (chronological) order. */
    final int sequence;
    final String date;
    final String text;
    final List<Filing> documents;

    Entry(int sequence, String date, String text, List<Filing> documents) {
      this.sequence = sequence;
      this.date = date;
      this.text = text;
      this.documents = documents;
    }
  }

  /** The docket header and its entries. */
  static final class Docket {
    final String number;
    final String title;
    final String docketed;
    final String linkedWith;
    final String lowerCourt;
    final String lowerCourtCaseNumbers;
    final String lowerCourtDecisionDate;
    final String lowerCourtDecisionNote;
    final String discretionaryCourtDecisionDate;
    final String rehearingDenied;
    final String questionsPresentedUrl;
    final List<Entry> entries;

    Docket(String number, String title, String docketed, String linkedWith, String lowerCourt,
        String lowerCourtCaseNumbers, String lowerCourtDecisionDate,
        String lowerCourtDecisionNote, String discretionaryCourtDecisionDate, String rehearingDenied,
        String questionsPresentedUrl, List<Entry> entries) {
      this.number = number;
      this.title = title;
      this.docketed = docketed;
      this.linkedWith = linkedWith;
      this.lowerCourt = lowerCourt;
      this.lowerCourtCaseNumbers = lowerCourtCaseNumbers;
      this.lowerCourtDecisionDate = lowerCourtDecisionDate;
      this.lowerCourtDecisionNote = lowerCourtDecisionNote;
      this.discretionaryCourtDecisionDate = discretionaryCourtDecisionDate;
      this.rehearingDenied = rehearingDenied;
      this.questionsPresentedUrl = questionsPresentedUrl;
      this.entries = entries;
    }

    /** Date of the order granting the petition, or null when the docket has none. */
    String grantedDate() {
      return firstDate(PETITION_GRANTED);
    }

    /** Date of the entry "Argued.", or null when the case was not argued. */
    String arguedDate() {
      return firstDate(ARGUED);
    }

    /**
     * Date of the entry "Judgment Issued." (older dockets print it in capitals), or null when
     * none has issued. A docket whose entry is misspelt in the source ("JUDGMENT SSUED.") has
     * none: the text is not corrected here.
     */
    String judgmentIssuedDate() {
      return firstDate(JUDGMENT_ISSUED);
    }

    private String firstDate(Pattern text) {
      for (Entry e : entries) {
        if (text.matcher(e.text).matches()) {
          return e.date;
        }
      }
      return null;
    }
  }

  private ScotusDocketPage() {
  }

  /**
   * Parses a docket page.
   *
   * @param baseUrl the page's URL, used to resolve relative document links
   * @throws IllegalArgumentException when the page is not a docket page
   */
  static Docket parse(String html, String baseUrl) {
    org.jsoup.nodes.Document page = Jsoup.parse(html, baseUrl);
    Element info = page.getElementById("docketinfo");
    if (info == null) {
      throw new IllegalArgumentException("Not a docket page (no docket header table): " + baseUrl);
    }

    Element numberSpan = info.selectFirst("span.DocketInfoTitle");
    Matcher number = NUMBER.matcher(numberSpan == null ? "" : numberSpan.text().trim());
    if (!number.find()) {
      throw new IllegalArgumentException("Docket page has no docket number: " + baseUrl);
    }

    String title = null;
    String docketed = null;
    String linkedWith = null;
    String lowerCourt = null;
    String caseNumbers = null;
    String decisionDate = null;
    String decisionNote = null;
    String discretionaryDate = null;
    String rehearing = null;
    for (Element row : info.select("tr")) {
      Elements cells = row.children();
      if (cells.size() != 2 || !"200".equals(cells.get(0).attr("width"))) {
        continue;
      }
      String label = cells.get(0).text().replace(' ', ' ').trim().replaceAll(":$", "");
      String value = cells.get(1).text().replace(' ', ' ').trim();
      if (value.isEmpty()) {
        continue;
      }
      switch (label) {
      case "Title":
        title = value;
        break;
      case "Docketed":
        docketed = headerDate(value, label);
        break;
      case "Linked with":
        linkedWith = value;
        break;
      case "Lower Ct":
        lowerCourt = value;
        break;
      case "Case Numbers":
        Matcher parens = CASE_NUMBERS_PARENS.matcher(value);
        caseNumbers = parens.matches() ? parens.group(1).trim() : value;
        break;
      case "Decision Date":
        Matcher dated = DATE_WITH_NOTE.matcher(value);
        if (!dated.matches()) {
          throw new IllegalArgumentException("Docket '" + label + "' is not a date: " + value);
        }
        decisionDate = headerDate(dated.group(1), label);
        decisionNote = dated.group(2);
        break;
      case "Discretionary Court Decision Date":
        discretionaryDate = headerDate(value, label);
        break;
      case "Rehearing Denied":
        rehearing = headerDate(value, label);
        break;
      default:
        throw new IllegalArgumentException("Docket " + number.group(1)
            + " has an unrecognised header row '" + label + "'");
      }
    }
    if (title == null || docketed == null) {
      throw new IllegalArgumentException("Docket " + number.group(1)
          + " has no Title or Docketed row");
    }

    Element qp = info.selectFirst("a[href*=/qp/]");
    return new Docket(number.group(1), title, docketed, linkedWith, lowerCourt, caseNumbers,
        decisionDate, decisionNote, discretionaryDate, rehearing, qp == null ? null : qp.absUrl("href"), entries(page));
  }

  private static List<Entry> entries(org.jsoup.nodes.Document page) {
    List<Entry> entries = new ArrayList<Entry>();
    int sequence = 0;
    for (Element item : page.select("table.ProceedingItem")) {
      Element date = item.selectFirst("td.ProceedingDate");
      Element body = date == null ? null : date.nextElementSibling();
      if (body == null) {
        throw new IllegalArgumentException("Docket entry without a date and text: "
            + item.text());
      }
      List<Filing> documents = new ArrayList<Filing>();
      for (Element link : body.select("span.documentlinks a[href]")) {
        documents.add(new Filing(link.text().trim(), link.absUrl("href")));
      }
      Element textOnly = body.clone();
      textOnly.select("span.documentlinks").remove();
      sequence++;
      entries.add(new Entry(sequence, entryDate(date.text().trim()),
          textOnly.text().replace(' ', ' ').trim(), documents));
    }
    return entries;
  }

  private static String headerDate(String value, String label) {
    try {
      return LocalDate.parse(value, HEADER_DATE).toString();
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("Docket '" + label + "' is not a date: " + value, e);
    }
  }

  private static String entryDate(String value) {
    try {
      return LocalDate.parse(value, ENTRY_DATE).toString();
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("Docket entry date is not 'Mon d yyyy': " + value, e);
    }
  }
}
