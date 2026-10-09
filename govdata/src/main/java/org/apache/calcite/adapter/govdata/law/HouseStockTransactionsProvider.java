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
// storage-provider-guard:ignore-file - audited: every filesystem op here targets a local temp
// file or the local temp directory ZipDownloadUtils.downloadZipToTempDir returns, never an
// object-store URI.

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.GovDataException;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.xml.stream.XMLStreamException;

/**
 * DataProvider for {@code member_stock_transactions}: House members' Periodic Transaction Reports
 * (PTRs) — the STOCK Act disclosures of a member's, spouse's or dependent child's stock trades —
 * from the Clerk of the House's own bulk financial-disclosure system.
 *
 * <p>Coverage is House only. The Senate's parallel eFD system (efdsearch.senate.gov) gates every
 * request behind a click-through agreement enforcing 5 U.S.C. app. {@literal @} 105(c)'s
 * restrictions on commercial use, solicitation and credit-worthiness use of these reports — a
 * legal question, not an engineering one, so it is deliberately not automated past here.
 *
 * <p>The {@code year} dimension selects one annual index,
 * {@code disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip}, which lists every
 * filing of every kind (annual reports, amendments, terminations, PTRs, ...) by every House
 * member and candidate that year. Only {@link HouseFinancialDisclosureIndex.Entry#filingType}
 * {@code "P"} entries are PTRs; each is one PDF at
 * {@code public_disc/ptr-pdfs/{year}/{docId}.pdf}, fetched and parsed lazily so only one is held
 * in memory at a time. A PTR with no parseable transaction row throws — a template change to fix,
 * not a filing to skip, since {@link HousePtrTextParser} is the only place that knows the
 * template's shape.
 */
public class HouseStockTransactionsProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(HouseStockTransactionsProvider.class);

  static final String TABLE = "member_stock_transactions";

  private static final String SITE = "https://disclosures-clerk.house.gov/public_disc";

  private static final String PTR_FILING_TYPE = "P";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    if (!TABLE.equals(config.getName())) {
      throw new GovDataException("HouseStockTransactionsProvider does not serve table '"
          + config.getName() + "'");
    }
    String year = variables.get("year");
    if (year == null || year.isEmpty()) {
      throw new IOException(TABLE + ": the 'year' dimension is required");
    }

    List<HouseFinancialDisclosureIndex.Entry> ptrs = fetchPtrEntries(year);
    LOGGER.info("{}: {} index lists {} periodic transaction reports", TABLE, year, ptrs.size());

    Deque<HouseFinancialDisclosureIndex.Entry> tasks =
        new ArrayDeque<HouseFinancialDisclosureIndex.Entry>(ptrs);
    return new RowIterator(year, tasks);
  }

  private static List<HouseFinancialDisclosureIndex.Entry> fetchPtrEntries(String year)
      throws IOException {
    String indexUrl = SITE + "/financial-pdfs/" + year + "FD.zip";
    File dir = ZipDownloadUtils.downloadZipToTempDir(indexUrl, null, "house-fd-" + year);
    try {
      File xml = new File(dir, year + "FD.xml");
      List<HouseFinancialDisclosureIndex.Entry> all;
      try (InputStream in = new FileInputStream(xml)) {
        all = HouseFinancialDisclosureIndex.parse(in);
      } catch (XMLStreamException e) {
        throw new IOException(indexUrl + ": failed to parse " + xml.getName(), e);
      }
      List<HouseFinancialDisclosureIndex.Entry> ptrs =
          new ArrayList<HouseFinancialDisclosureIndex.Entry>();
      for (HouseFinancialDisclosureIndex.Entry entry : all) {
        if (PTR_FILING_TYPE.equals(entry.filingType)) {
          ptrs.add(entry);
        }
      }
      return ptrs;
    } finally {
      deleteRecursively(dir);
    }
  }

  private static void deleteRecursively(File dir) {
    File[] children = dir.listFiles();
    if (children != null) {
      for (File child : children) {
        deleteRecursively(child);
      }
    }
    dir.delete();
  }

  /** Works through the PTR PDFs lazily, holding only the rows of the filing being read. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final String year;
    private final Deque<HouseFinancialDisclosureIndex.Entry> tasks;
    private final Deque<Map<String, Object>> rows = new ArrayDeque<Map<String, Object>>();

    RowIterator(String year, Deque<HouseFinancialDisclosureIndex.Entry> tasks) {
      this.year = year;
      this.tasks = tasks;
    }

    @Override public boolean hasNext() {
      while (rows.isEmpty() && !tasks.isEmpty()) {
        HouseFinancialDisclosureIndex.Entry entry = tasks.removeFirst();
        try {
          readFiling(year, entry, rows);
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

  private static void readFiling(String year, HouseFinancialDisclosureIndex.Entry entry,
      Deque<Map<String, Object>> out) throws IOException {
    String url = SITE + "/ptr-pdfs/" + year + "/" + entry.docId + ".pdf";
    File pdf = File.createTempFile("house-ptr-", ".pdf");
    try {
      ZipDownloadUtils.downloadToFile(url, null, pdf);
      final List<String> pages = new ArrayList<String>();
      PdfPageTexts.forEachPage(pdf, new PdfPageTexts.PageSink() {
        @Override public void page(int pageNumber, String text) {
          pages.add(text);
        }
      });
      if (!hasExtractableText(pages)) {
        // A small share of PTRs are a scanned image of the paper form with no text layer at
        // all (confirmed live 2026-09-27, filing 9116249): OCR is out of scope here, so this
        // filing's transactions are not recoverable from the source PDF. Skipped, not failed —
        // the template HousePtrTextParser reads is unaffected.
        LOGGER.warn("{}: filing {} ({}) has no extractable text (a scanned image?); skipped", url,
            entry.docId, entry.lastName);
        return;
      }
      List<HousePtrTextParser.Row> transactions = HousePtrTextParser.parse(pages);
      if (transactions.isEmpty()) {
        // A small share of filings use a PDF template variant HousePtrTextParser doesn't yet
        // recognize (confirmed live 2026-09-28: a handful of pre-2023 filings lack the
        // "cap. gains >$200?" column entirely, or omit zero-padded dates) rather than being a
        // genuine zero-transaction/negative report. Either way this filing's transactions are not
        // recoverable here: skipped, not failed, so one unreadable filing does not discard every
        // other filing's already-parsed rows for the whole year (confirmed live 2026-09-28: this
        // used to throw and abort the entire batch, silently dropping real data for the other
        // ~99% of filings in the same year).
        LOGGER.warn("{}: no transaction rows found in a periodic transaction report with "
            + "extractable text (filing {}, {}); skipped", url, entry.docId, entry.lastName);
        return;
      }
      int rowSeq = 1;
      for (HousePtrTextParser.Row t : transactions) {
        out.addLast(row(entry, url, rowSeq++, t));
      }
    } finally {
      Files.deleteIfExists(pdf.toPath());
    }
  }

  private static Map<String, Object> row(HouseFinancialDisclosureIndex.Entry entry,
      String pdfUrl, int rowSeq, HousePtrTextParser.Row t) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("chamber", "house");
    row.put("filer_last_name", entry.lastName);
    row.put("filer_first_name", entry.firstName);
    row.put("state", stateOf(entry.stateDistrict));
    row.put("district", districtOf(entry.stateDistrict));
    row.put("filing_id", entry.docId);
    row.put("filing_date", isoFilingDate(entry.filingDate));
    row.put("row_seq", Integer.valueOf(rowSeq));
    if (t.ownerCode != null) {
      row.put("owner_code", t.ownerCode);
    }
    row.put("asset_name", t.assetName);
    if (t.ticker != null) {
      row.put("ticker", t.ticker);
    }
    row.put("asset_type_code", t.assetTypeCode);
    row.put("transaction_type", t.transactionType);
    row.put("transaction_date", t.transactionDate);
    row.put("notification_date", t.notificationDate);
    row.put("amount_range", t.amountRange);
    if (t.amountMin != null) {
      row.put("amount_min", t.amountMin);
    }
    if (t.amountMax != null) {
      row.put("amount_max", t.amountMax);
    }
    if (t.filingStatus != null) {
      row.put("filing_status", t.filingStatus);
    }
    row.put("pdf_url", pdfUrl);
    return row;
  }

  private static final int MIN_EXTRACTABLE_TEXT_CHARS = 50;

  private static boolean hasExtractableText(List<String> pages) {
    int total = 0;
    for (String page : pages) {
      total += page.trim().length();
    }
    return total >= MIN_EXTRACTABLE_TEXT_CHARS;
  }

  /** {@code "CA11"} to {@code "CA"}; blank when the index gives no state/district. */
  private static String stateOf(String stateDistrict) {
    return stateDistrict.length() >= 2 ? stateDistrict.substring(0, 2) : null;
  }

  /** {@code "CA11"} to {@code "11"}; null when the index gives no state/district. */
  private static String districtOf(String stateDistrict) {
    return stateDistrict.length() > 2 ? stateDistrict.substring(2) : null;
  }

  /** {@code "9/10/2025"} to {@code "2025-09-10"}. */
  private static String isoFilingDate(String date) {
    String[] parts = date.split("/");
    return String.format("%s-%02d-%02d", parts[2], Integer.parseInt(parts[0]),
        Integer.parseInt(parts[1]));
  }
}
