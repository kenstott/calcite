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
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses one term's "Opinions of the Court" listing on supremecourt.gov
 * ({@code /opinions/slipopinion/24}) into one entry per decided case.
 *
 * <p>Each opinion row has six cells: the release number within the term (a decree in an
 * original-jurisdiction case is numbered {@code D1}), the decision date
 * ({@code 6/30/25}), the docket number, a link whose text is the case name and whose
 * {@code title} is the Court's own one-line statement of the holding, the author ({@code PC}
 * for a per curiam, otherwise the justice's initials), and the United States Reports citation.
 *
 * <p>Where the link points differs by term. Recent terms link one PDF per opinion
 * ({@code /opinions/24pdf/606us2r67_8nka.pdf}); terms 2017 to 2019 (and the oldest opinions of
 * 2020) link into a preliminary-print or bound-volume PDF at a page
 * ({@code /opinions/preliminaryprint/585US2PP_final.pdf#page=474}). {@link Entry#pdfPage} is
 * that page, null for a per-opinion PDF.
 *
 * <p>The citation column is a full citation ({@code 606 U.S. 942}) once the opinion has a page
 * in the United States Reports, and {@code 609/2} (volume and print part) before that, in which
 * case {@link Entry#volume} is set and {@link Entry#firstPage} is null.
 *
 * <p>A row with a link into {@code /opinions/} that does not have the six cells throws: a
 * layout change is a failure to fix, not a row to skip.
 */
final class ScotusSlipListing {

  private static final int CELLS = 6;

  private static final Pattern DATE = Pattern.compile("^(\\d{1,2})/(\\d{1,2})/(\\d{2})$");

  /** {@code 606 U.S. 942}, tolerating the spacing and missing periods seen in the listings. */
  private static final Pattern CITATION =
      Pattern.compile("^(\\d+)\\s*U\\.?\\s*S\\.?\\s*(\\d+)$");

  /** {@code 609/2}: volume 609, preliminary print part 2, with no page assigned yet. */
  private static final Pattern VOLUME_PART = Pattern.compile("^(\\d+)/(\\d+)$");

  private static final Pattern PAGE_FRAGMENT = Pattern.compile("#page=(\\d+)$");

  /** One decided case as the listing describes it. */
  static final class Entry {
    /** Release number within the term ("67"), or a label such as "D1" for a decree. */
    final String release;
    final String decisionDate;
    final String docket;
    final String caseName;
    final String holdingSummary;
    final String authorInitials;
    final Integer volume;
    final Integer firstPage;
    final String href;
    final Integer pdfPage;

    Entry(String release, String decisionDate, String docket, String caseName,
        String holdingSummary, String authorInitials, Integer volume, Integer firstPage,
        String href, Integer pdfPage) {
      this.release = release;
      this.decisionDate = decisionDate;
      this.docket = docket;
      this.caseName = caseName;
      this.holdingSummary = holdingSummary;
      this.authorInitials = authorInitials;
      this.volume = volume;
      this.firstPage = firstPage;
      this.href = href;
      this.pdfPage = pdfPage;
    }

    /**
     * The citation as {@code 606 U.S. 942}, or null when the listing gives no page: a
     * recently decided opinion is listed by volume and print part ({@code 609/2}) until the
     * page is assigned.
     */
    String usCitation() {
      return firstPage == null ? null : volume + " U.S. " + firstPage;
    }

    /** The PDF path without its page fragment, e.g. {@code /opinions/24pdf/606us2r67_8nka.pdf}. */
    String pdfPath() {
      int hash = href.indexOf('#');
      return hash < 0 ? href : href.substring(0, hash);
    }
  }

  private ScotusSlipListing() {
  }

  /** Parses the listing page; entries are in the page's order (newest first). */
  static List<Entry> parse(String html) {
    Document doc = Jsoup.parse(html);
    List<Entry> entries = new ArrayList<Entry>();
    for (Element row : doc.select("tr")) {
      Element link = row.selectFirst("td > a[href^=/opinions/]");
      if (link == null) {
        continue;
      }
      Elements cells = row.children();
      if (cells.size() != CELLS) {
        throw new IllegalArgumentException("Slip-opinion row has " + cells.size()
            + " cells, expected " + CELLS + ": " + row.text());
      }
      entries.add(entry(cells, link));
    }
    return entries;
  }

  private static Entry entry(Elements cells, Element link) {
    String release = cells.get(0).text().trim();
    if (release.isEmpty()) {
      throw new IllegalArgumentException("Slip-opinion row has no release number: "
          + link.text());
    }

    String summary = link.attr("title").trim();
    String href = link.attr("href");
    Matcher page = PAGE_FRAGMENT.matcher(href);

    String citation = cells.get(5).text().trim();
    Integer volume = null;
    Integer firstPage = null;
    if (!citation.isEmpty()) {
      Matcher c = CITATION.matcher(citation);
      Matcher part = VOLUME_PART.matcher(citation);
      if (c.matches()) {
        volume = Integer.valueOf(c.group(1));
        firstPage = Integer.valueOf(c.group(2));
      } else if (part.matches()) {
        volume = Integer.valueOf(part.group(1));
      } else {
        throw new IllegalArgumentException("Slip-opinion citation is not 'NNN U.S. NNN' or "
            + "'NNN/N': " + citation);
      }
    }

    return new Entry(release, isoDate(cells.get(1).text().trim()),
        cells.get(2).text().trim().replaceAll("[–—]", "-"), link.text().trim(),
        summary.isEmpty() ? null : summary, cells.get(4).text().trim(), volume, firstPage,
        href, page.find() ? Integer.valueOf(page.group(1)) : null);
  }

  /** {@code 6/30/25} becomes {@code 2025-06-30}. */
  private static String isoDate(String date) {
    Matcher m = DATE.matcher(date);
    if (!m.matches()) {
      throw new IllegalArgumentException("Slip-opinion date is not M/D/YY: " + date);
    }
    return String.format("20%s-%02d-%02d", m.group(3), Integer.parseInt(m.group(1)),
        Integer.parseInt(m.group(2)));
  }
}
