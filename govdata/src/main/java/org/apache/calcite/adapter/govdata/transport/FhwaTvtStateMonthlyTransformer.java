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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;
import org.apache.calcite.adapter.govdata.HeadedPageSession;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads Table 5 ("Changes on ALL Estimated Roads by Region and State") of the FHWA
 * <b>Traffic Volume Trends</b> monthly workbook into {@code tvt_state_monthly} rows.
 *
 * <p>highways.dot.gov (which answers HTTP 403 to non-browser clients, so everything is fetched
 * through a headed browser session) lists every published month on one index page, grouped by
 * year; each month links to a page that links the workbook. The index is the authority on what is
 * published: a month later than the newest one listed raises an "HTTP 404" error so the pipeline
 * marks the period unavailable, while an earlier month that is not listed, or a page that cannot
 * be parsed, fails the run. Table 5 is located by its title cell (the sheet index shifts between
 * releases). Layout is positional within the sheet: A state, D stations, E current-year
 * vehicle-miles (millions, preliminary), F prior-year vehicle-miles, G percent change.
 */
public class FhwaTvtStateMonthlyTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FhwaTvtStateMonthlyTransformer.class);
  static final String INDEX_URL = "/policy/policy-information/resources/tvt";
  private static final String[] MONTHS = {"January", "February", "March", "April", "May", "June",
      "July", "August", "September", "October", "November", "December"};
  private static final String TITLE = "Table - 5.";

  private static final Pattern SECTION = Pattern.compile(
      "<strong class=\"lead[^\"]*\">\\s*([^<]*?)\\s*</strong>");
  private static final Pattern LINK = Pattern.compile(
      "<a\\s[^>]*href=\"([^\"]*)\"[^>]*>(.*?)</a>", Pattern.DOTALL);
  private static final Pattern WORKBOOK = Pattern.compile("href=\"([^\"]*\\.xlsx)\"");

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "tvt_state_monthly is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    int year = Integer.parseInt(context.getDimensionValues().get("effective_year"));
    int month = Integer.parseInt(context.getDimensionValues().get("month"));
    HeadedPageSession session = HeadedPageSession.get(context);
    try {
      String index = session.fetchText(INDEX_URL);
      if (index == null) {
        throw new IllegalStateException("tvt_state_monthly: " + INDEX_URL + " answered 404");
      }
      String monthPageUrl = monthPage(parseIndex(index), year, month);
      String monthPage = session.fetchText(monthPageUrl);
      if (monthPage == null) {
        throw new IllegalStateException("tvt_state_monthly: " + monthPageUrl
            + " is listed in the index but answered 404");
      }
      String workbookUrl = workbookUrl(monthPage, year, month);
      byte[] bytes = session.fetchBytes(workbookUrl);
      if (bytes == null) {
        throw new IllegalStateException("tvt_state_monthly: " + workbookUrl
            + " is linked from " + monthPageUrl + " but answered 404");
      }
      List<Map<String, Object>> rows = parseWorkbook(bytes, workbookUrl);
      LOGGER.debug("tvt_state_monthly: {} state rows from {}", rows.size(), workbookUrl);
      return rows.iterator();
    } catch (IOException e) {
      throw new RuntimeException("tvt_state_monthly: cannot read FHWA Traffic Volume Trends for "
          + period(year, month) + ": " + e.getMessage(), e);
    }
  }

  /** "yyyy-mm" to month page URL, from the year sections of the index page. */
  static TreeMap<String, String> parseIndex(String html) {
    TreeMap<String, String> pages = new TreeMap<String, String>();
    Matcher section = SECTION.matcher(html);
    boolean found = section.find();
    while (found) {
      String label = section.group(1);
      int start = section.end();
      found = section.find();
      if (!label.matches("\\d{4}")) {
        continue;
      }
      String body = html.substring(start, found ? section.start() : html.length());
      int listEnd = body.indexOf("</ul>");
      if (listEnd < 0) {
        throw new IllegalStateException("tvt_state_monthly: index section " + label
            + " has no month list; the page layout changed");
      }
      Matcher link = LINK.matcher(body.substring(0, listEnd));
      while (link.find()) {
        String text = link.group(2).replaceAll("<[^>]+>|&nbsp;|\\s", "");
        if (text.isEmpty()) {
          continue;
        }
        pages.put(period(Integer.parseInt(label), monthNumber(text, label)), link.group(1));
      }
    }
    if (pages.isEmpty()) {
      throw new IllegalStateException("tvt_state_monthly: no month links found on " + INDEX_URL
          + "; the page layout changed");
    }
    return pages;
  }

  /** The page of a published month; "HTTP 404" when the month is newer than any listed. */
  static String monthPage(TreeMap<String, String> pages, int year, int month) {
    String period = period(year, month);
    String url = pages.get(period);
    if (url != null) {
      return url;
    }
    if (period.compareTo(pages.lastKey()) > 0) {
      // "HTTP 404" is how the pipeline recognises a period the source has not published yet.
      throw new RuntimeException("tvt_state_monthly: HTTP 404: " + period + " is not published; "
          + "the newest month listed on " + INDEX_URL + " is " + pages.lastKey());
    }
    throw new IllegalStateException("tvt_state_monthly: " + INDEX_URL + " lists months up to "
        + pages.lastKey() + " but not " + period);
  }

  /** The one workbook linked from a month page, which must be named for that month. */
  static String workbookUrl(String monthPage, int year, int month) {
    String name = String.format(Locale.ROOT, "%02d%stvt.xlsx", year % 100,
        MONTHS[month - 1].substring(0, 3).toLowerCase(Locale.ROOT));
    Matcher m = WORKBOOK.matcher(monthPage);
    while (m.find()) {
      if (m.group(1).endsWith("/" + name)) {
        return m.group(1);
      }
    }
    throw new IllegalStateException("tvt_state_monthly: the page for " + period(year, month)
        + " does not link " + name);
  }

  private static int monthNumber(String text, String year) {
    for (int i = 0; i < MONTHS.length; i++) {
      if (MONTHS[i].equalsIgnoreCase(text)) {
        return i + 1;
      }
    }
    throw new IllegalStateException("tvt_state_monthly: index section " + year
        + " has a link '" + text + "' that is not a month name");
  }

  private static String period(int year, int month) {
    return String.format(Locale.ROOT, "%04d-%02d", year, month);
  }

  private static List<Map<String, Object>> parseWorkbook(byte[] bytes, String url)
      throws IOException {
    Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
    try {
      Sheet sheet = findTable5(wb);
      if (sheet == null) {
        throw new IllegalStateException("tvt_state_monthly: Table 5 sheet not found in " + url);
      }
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
      for (int r = 0; r <= sheet.getLastRowNum(); r++) {
        Row row = sheet.getRow(r);
        if (row == null) {
          continue;
        }
        String name = cellString(row.getCell(0));
        if (name == null) {
          continue;
        }
        String fips =
            FhwaVehicleRegistrationsTransformer.STATE_FIPS.get(name.trim().toUpperCase(Locale.ROOT));
        if (fips == null) {
          continue;
        }
        Map<String, Object> o = new LinkedHashMap<String, Object>();
        o.put("state_fips", fips);
        o.put("state_name", name.trim());
        o.put("stations", wholeNumber(row.getCell(3)));
        o.put("vmt_millions", wholeNumber(row.getCell(4)));
        o.put("prior_year_vmt_millions", wholeNumber(row.getCell(5)));
        o.put("pct_change", number(row.getCell(6)));
        out.add(o);
      }
      if (out.isEmpty()) {
        throw new IllegalStateException("tvt_state_monthly: no state rows in Table 5 of " + url);
      }
      return out;
    } finally {
      wb.close();
    }
  }

  private static Sheet findTable5(Workbook wb) {
    for (int i = 0; i < wb.getNumberOfSheets(); i++) {
      Sheet s = wb.getSheetAt(i);
      int limit = Math.min(s.getLastRowNum(), 5);
      for (int r = 0; r <= limit; r++) {
        Row row = s.getRow(r);
        if (row == null) {
          continue;
        }
        for (Cell c : row) {
          if (c.getCellType() == CellType.STRING
              && c.getStringCellValue().trim().startsWith(TITLE)) {
            return s;
          }
        }
      }
    }
    return null;
  }

  private static String cellString(Cell cell) {
    if (cell == null || cell.getCellType() != CellType.STRING) {
      return null;
    }
    String s = cell.getStringCellValue();
    return s.trim().isEmpty() ? null : s;
  }

  private static Double number(Cell cell) {
    if (cell == null || cell.getCellType() != CellType.NUMERIC) {
      return null;
    }
    return cell.getNumericCellValue();
  }

  private static Long wholeNumber(Cell cell) {
    Double value = number(cell);
    return value == null ? null : Math.round(value);
  }
}
