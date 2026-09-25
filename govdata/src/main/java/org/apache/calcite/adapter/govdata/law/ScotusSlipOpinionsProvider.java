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

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.GovDataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * DataProvider for {@code scotus_slip_opinions}: the opinions of the Court from supremecourt.gov
 * for Court terms 2017 onward, the terms the bound United States Reports on GovInfo do not cover.
 *
 * <p>The {@code year} dimension is the Court term. A term's listing
 * ({@code /opinions/slipopinion/24}, see {@link ScotusSlipListing}) names each case, its docket
 * number, the Court's one-line statement of the holding and where its opinion is. Recent terms
 * have one PDF per opinion. Terms 2017 to 2019 (and the oldest opinions of 2020) link into a
 * volume PDF at a page; those PDFs are downloaded once and cut into cases by
 * {@link ScotusCasePages}, so a volume's cases are held together only until its rows are emitted.
 *
 * <p>Where a linked preliminary print has been superseded and now returns 404, the bound volume
 * {@code /opinions/boundvolumes/{volume}BV.pdf} of the cited United States Reports volume is used
 * instead, and each case is found by its printed page. If that is missing too the term fails.
 *
 * <p>The docket numbers and disposition are read from the opinion text
 * ({@link ScotusDocketNumbers}, {@link ScotusDispositionParser}); the case name, date, author
 * initials, citation and holding summary are the listing's own.
 */
public class ScotusSlipOpinionsProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScotusSlipOpinionsProvider.class);

  static final String TABLE = "scotus_slip_opinions";

  private static final String SITE = "https://www.supremecourt.gov";
  private static final int DOCKET_PAGES = 3;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    if (!TABLE.equals(config.getName())) {
      throw new GovDataException("ScotusSlipOpinionsProvider does not serve table '"
          + config.getName() + "'");
    }
    String yearValue = variables.get("year");
    if (yearValue == null || yearValue.isEmpty()) {
      throw new IOException(TABLE + ": the 'year' (Court term) dimension is required");
    }
    int term = Integer.parseInt(yearValue);

    String listingUrl = SITE + "/opinions/slipopinion/" + String.format("%02d", term % 100);
    String html = SupremeCourtSite.DEFAULT.getHtml(listingUrl);
    List<ScotusSlipListing.Entry> entries = ScotusSlipListing.parse(html);
    if (entries.isEmpty()) {
      LOGGER.warn("{}: term {} lists no opinions ({}); expected only at the start of a term",
          TABLE, term, listingUrl);
    } else {
      LOGGER.info("{}: term {} lists {} opinions", TABLE, term, entries.size());
    }

    Deque<Task> tasks = new ArrayDeque<Task>();
    Map<String, List<ScotusSlipListing.Entry>> volumePdfs =
        new LinkedHashMap<String, List<ScotusSlipListing.Entry>>();
    for (ScotusSlipListing.Entry entry : entries) {
      if (entry.pdfPage == null) {
        tasks.addLast(new Task(entry.pdfPath(), java.util.Collections.singletonList(entry)));
      } else {
        List<ScotusSlipListing.Entry> group = volumePdfs.get(entry.pdfPath());
        if (group == null) {
          group = new ArrayList<ScotusSlipListing.Entry>();
          volumePdfs.put(entry.pdfPath(), group);
        }
        group.add(entry);
      }
    }
    for (Map.Entry<String, List<ScotusSlipListing.Entry>> e : volumePdfs.entrySet()) {
      tasks.addLast(new Task(e.getKey(), e.getValue()));
    }
    return new RowIterator(tasks);
  }

  /** One PDF and the listed cases it holds; a per-opinion PDF holds one. */
  private static final class Task {
    final String pdfPath;
    final List<ScotusSlipListing.Entry> entries;

    Task(String pdfPath, List<ScotusSlipListing.Entry> entries) {
      this.pdfPath = pdfPath;
      this.entries = entries;
    }
  }

  /** Works through the PDFs lazily, holding only the rows of the PDF being read. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final Deque<Task> tasks;
    private final Deque<Map<String, Object>> rows = new ArrayDeque<Map<String, Object>>();

    RowIterator(Deque<Task> tasks) {
      this.tasks = tasks;
    }

    @Override public boolean hasNext() {
      while (rows.isEmpty() && !tasks.isEmpty()) {
        Task task = tasks.removeFirst();
        try {
          read(task, rows);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return !rows.isEmpty();
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return rows.removeFirst();
    }
  }

  private static void read(Task task, Deque<Map<String, Object>> out) throws IOException {
    if (task.entries.size() == 1 && task.entries.get(0).pdfPage == null) {
      readWholePdf(task.entries.get(0), out);
    } else {
      readVolumePdf(task, out);
    }
  }

  /** A per-opinion PDF is one case from its first page to its last. */
  private static void readWholePdf(final ScotusSlipListing.Entry entry,
      Deque<Map<String, Object>> out) throws IOException {
    String url = SITE + entry.pdfPath();
    File pdf = File.createTempFile("scotus-slip-", ".pdf");
    try {
      SupremeCourtSite.DEFAULT.download(url, pdf);
      final List<String> pages = new ArrayList<String>();
      PdfPageTexts.forEachPage(pdf, new PdfPageTexts.PageSink() {
        @Override public void page(int pageNumber, String text) {
          pages.add(text);
        }
      });
      out.addLast(row(entry, url, null, pages));
    } finally {
      Files.deleteIfExists(pdf.toPath());
    }
  }

  /** A volume PDF is downloaded once and cut into the listed cases. */
  private static void readVolumePdf(Task task, final Deque<Map<String, Object>> out)
      throws IOException {
    String url = SITE + task.pdfPath;
    File pdf = File.createTempFile("scotus-volume-", ".pdf");
    try {
      try {
        SupremeCourtSite.DEFAULT.download(url, pdf);
        sliceByListedPage(task, url, pdf, out);
      } catch (SupremeCourtSite.NotFoundException gone) {
        // Only a 404 or 410 means the preliminary print was superseded. A 403, a 429 or a server
        // error is a refusal (SupremeCourtSite retries it and then fails), and is not a reason to
        // read a different file.
        sliceBoundVolume(task, url, pdf, out, gone);
      }
    } finally {
      Files.deleteIfExists(pdf.toPath());
    }
  }

  private static void sliceByListedPage(Task task, final String url, File pdf,
      final Deque<Map<String, Object>> out) throws IOException {
    final Map<Integer, ScotusSlipListing.Entry> byPage =
        new HashMap<Integer, ScotusSlipListing.Entry>();
    for (ScotusSlipListing.Entry e : task.entries) {
      byPage.put(e.pdfPage, e);
    }
    ScotusCasePages.Slicer slicer = ScotusCasePages.slicer(
        new ScotusCasePages.StartRule() {
          @Override public boolean startsCase(int pdfPage, String head) {
            return byPage.containsKey(pdfPage);
          }
        },
        new ScotusCasePages.CaseSink() {
          @Override public void accept(ScotusCasePages.CasePages c) {
            out.addLast(row(byPage.get(c.startPage), url, Integer.valueOf(c.startPage), c.pages));
          }
        });
    PdfPageTexts.forEachPage(pdf, slicer);
    slicer.finish();
    requireAll(task, url, byPage.size(), out);
  }

  /**
   * The linked preliminary print is gone; the cited volume's bound volume has the same opinions,
   * found by printed page. Every case in the group must cite the same volume.
   */
  private static void sliceBoundVolume(Task task, String goneUrl, File pdf,
      final Deque<Map<String, Object>> out, SupremeCourtSite.NotFoundException gone)
      throws IOException {
    Integer volume = task.entries.get(0).volume;
    final Map<Integer, ScotusSlipListing.Entry> byCitedPage =
        new HashMap<Integer, ScotusSlipListing.Entry>();
    for (ScotusSlipListing.Entry e : task.entries) {
      if (e.volume == null || !e.volume.equals(volume) || e.firstPage == null) {
        throw new IOException(goneUrl + " is gone and case '" + e.caseName
            + "' has no page citation in volume " + volume + " to find it in the bound volume",
            gone);
      }
      byCitedPage.put(e.firstPage, e);
    }
    final String url = SITE + "/opinions/boundvolumes/" + volume + "BV.pdf";
    LOGGER.info("{}: {} is gone; reading bound volume {}", TABLE, goneUrl, url);
    try {
      SupremeCourtSite.DEFAULT.download(url, pdf);
    } catch (SupremeCourtSite.NotFoundException also) {
      throw new IOException(goneUrl + " is gone (HTTP " + gone.status + ") and so is its bound "
          + "volume " + url + " (HTTP " + also.status + ")", also);
    }

    final ScotusCasePages.PrintedPageRule rule = ScotusCasePages.printedPages(
        new HashSet<Integer>(byCitedPage.keySet()));
    ScotusCasePages.Slicer slicer = ScotusCasePages.slicer(rule,
        new ScotusCasePages.CaseSink() {
          @Override public void accept(ScotusCasePages.CasePages c) {
            ScotusSlipListing.Entry entry = byCitedPage.get(rule.citedPageAt(c.startPage));
            out.addLast(row(entry, url, Integer.valueOf(c.startPage), c.pages));
          }
        });
    PdfPageTexts.forEachPage(pdf, slicer);
    slicer.finish();
    requireAll(task, url, byCitedPage.size(), out);
  }

  /** Every listed case in the PDF must have been found; a missing one means a layout change. */
  private static void requireAll(Task task, String url, int expected,
      Deque<Map<String, Object>> out) throws IOException {
    int found = 0;
    for (Map<String, Object> row : out) {
      if (url.equals(row.get("pdf_url"))) {
        found++;
      }
    }
    if (found != expected) {
      throw new IOException(url + ": found " + found + " of " + expected + " listed cases ("
          + task.entries.get(0).caseName + " ...)");
    }
  }

  private static Map<String, Object> row(ScotusSlipListing.Entry entry, String pdfUrl,
      Integer pdfPage, List<String> pages) {
    List<String> opening = pages.subList(0, Math.min(ScotusDispositionParser.MAX_PAGES,
        pages.size()));
    StringBuilder text = new StringBuilder();
    for (String page : pages) {
      text.append(page);
    }

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("release", entry.release);
    row.put("decision_date", entry.decisionDate);
    row.put("listing_docket", entry.docket);
    row.put("docket_numbers", ScotusDocketNumbers.parse(
        opening.subList(0, Math.min(DOCKET_PAGES, opening.size()))));
    row.put("case_name", entry.caseName);
    if (entry.holdingSummary != null) {
      row.put("holding_summary", entry.holdingSummary);
    }
    row.put("author_initials", entry.authorInitials);
    if (entry.volume != null) {
      row.put("volume", entry.volume);
    }
    if (entry.firstPage != null) {
      row.put("first_page", entry.firstPage);
      row.put("us_citation", entry.usCitation());
    }
    ScotusDispositionParser.Result outcome = ScotusDispositionParser.parse(opening);
    if (outcome.kind != ScotusDispositionParser.Kind.NONE) {
      row.put("disposition", outcome.disposition);
      row.put("disposition_source", "opinion_text");
    }
    if (outcome.decisionType != null) {
      row.put("decision_type", outcome.decisionType);
    }
    row.put("pdf_url", pdfUrl);
    if (pdfPage != null) {
      row.put("pdf_page", pdfPage);
    }
    row.put("page_count", Integer.valueOf(pages.size()));
    row.put("opinion_text", text.toString());
    return row;
  }
}
