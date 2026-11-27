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
import org.apache.calcite.adapter.govdata.HeadedPageSession;

import java.io.IOException;
import java.util.Locale;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Locates and downloads the workbooks of the FHWA <b>Highway Statistics</b> annual series on
 * highways.dot.gov (which answers HTTP 403 to non-browser clients, so everything is fetched
 * through a headed browser session).
 *
 * <p>Editions up to 2022 keep their workbooks at a fixed archive path per year and table. From
 * 2023 the publications listing names the published editions and each edition page links its
 * workbooks. The listing and the edition page are the authority on what is published: an edition
 * newer than the newest one listed, or a table the newest edition does not link yet, raises an
 * "HTTP 404" error so the pipeline marks the period unavailable. Anything else that is missing or
 * cannot be parsed fails the run.
 */
final class FhwaHighwayStatistics {

  static final String LISTING_URL = "/policy/policy-publications?combine=highway+statistics";
  private static final String ARCHIVE_PATH =
      "/sites/fhwa.dot.gov/files/policy/policyinformation/statistics/";
  private static final int LAST_ARCHIVE_YEAR = 2022;

  private static final Pattern EDITION_LINK = Pattern.compile(
      "<a\\s[^>]*href=\"([^\"]*)\"[^>]*>\\s*Highway Statistics (\\d{4})\\s*</a>");

  private FhwaHighwayStatistics() {
  }

  /** One downloaded workbook and the URL it came from. */
  static final class Workbook {
    final String url;
    final byte[] bytes;

    private Workbook(String url, byte[] bytes) {
      this.url = url;
      this.bytes = bytes;
    }
  }

  /**
   * Downloads table {@code table} (for example {@code mv1}) of the {@code year} edition.
   * {@code archiveExtension} is the file extension the table uses in the archive path.
   */
  static Workbook fetch(RequestContext context, String table, String archiveExtension, int year) {
    HeadedPageSession session = HeadedPageSession.get(context);
    try {
      String url = year <= LAST_ARCHIVE_YEAR
          ? archiveUrl(table, archiveExtension, year)
          : editionWorkbookUrl(session, table, year);
      byte[] bytes = session.fetchBytes(url);
      if (bytes == null) {
        throw new IllegalStateException(
            "FHWA Highway Statistics: " + url + " should be published but is missing");
      }
      return new Workbook(url, bytes);
    } catch (IOException e) {
      throw new RuntimeException("FHWA Highway Statistics: cannot read table " + table + " of "
          + year + ": " + e.getMessage(), e);
    }
  }

  static String archiveUrl(String table, String extension, int year) {
    return ARCHIVE_PATH + year + "/xls/" + table + "." + extension;
  }

  private static String editionWorkbookUrl(HeadedPageSession session, String table, int year)
      throws IOException {
    TreeMap<Integer, String> editions = parseListing(page(session, LISTING_URL));
    String editionUrl = editionPage(editions, year);
    return workbookUrl(page(session, editionUrl), table, year, year == editions.lastKey());
  }

  private static String page(HeadedPageSession session, String url) throws IOException {
    String body = session.fetchText(url);
    if (body == null) {
      // Not worded as an HTTP status: a missing listed page is a failure, not an unposted period.
      throw new IllegalStateException(
          "FHWA Highway Statistics: " + url + " is missing from the site");
    }
    return body;
  }

  /** Edition year to edition page URL, from the publications listing. */
  static TreeMap<Integer, String> parseListing(String html) {
    TreeMap<Integer, String> editions = new TreeMap<Integer, String>();
    Matcher link = EDITION_LINK.matcher(html);
    while (link.find()) {
      editions.put(Integer.parseInt(link.group(2)), link.group(1));
    }
    if (editions.isEmpty()) {
      throw new IllegalStateException("FHWA Highway Statistics: no edition links found on "
          + LISTING_URL + "; the page layout changed");
    }
    return editions;
  }

  /** The page of a published edition; "HTTP 404" when the year is newer than any listed. */
  static String editionPage(TreeMap<Integer, String> editions, int year) {
    String url = editions.get(year);
    if (url != null) {
      return url;
    }
    if (year > editions.lastKey()) {
      // "HTTP 404" is how the pipeline recognises a period the source has not published yet.
      throw new RuntimeException("FHWA Highway Statistics: HTTP 404: the " + year
          + " edition is not published; the newest listed on " + LISTING_URL + " is "
          + editions.lastKey());
    }
    throw new IllegalStateException("FHWA Highway Statistics: " + LISTING_URL
        + " lists editions up to " + editions.lastKey() + " but not " + year);
  }

  /**
   * The workbook an edition page links for {@code table}. The newest edition is posted table by
   * table, so a table it does not link yet is unpublished, provided the page links other tables of
   * that edition under the expected names.
   */
  static String workbookUrl(String editionPage, String table, int year, boolean newestEdition) {
    String prefix = "highway_statistics_" + year + "_";
    Matcher m = Pattern.compile(
        "href=\"([^\"]*/" + prefix + Pattern.quote(table) + "\\.xlsx?)\"").matcher(editionPage);
    if (m.find()) {
      return m.group(1);
    }
    boolean linksOtherTables = Pattern.compile(
        "href=\"[^\"]*/" + prefix + "[a-z0-9]+\\.xlsx?\"").matcher(editionPage).find();
    if (newestEdition && linksOtherTables) {
      throw new RuntimeException(String.format(Locale.ROOT, "FHWA Highway Statistics: HTTP 404: "
          + "table %s is not published yet in the %d edition", table, year));
    }
    throw new IllegalStateException("FHWA Highway Statistics: the " + year
        + " edition page does not link a workbook for table " + table);
  }
}
