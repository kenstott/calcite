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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code sba_loan_approvals} — SBA 7(a) and 504 loan approvals
 * (FOIA data) from the Drupal-based data.sba.gov open data site.
 *
 * <p>The site rolls each CSV filename by an {@code asof} date and, since the
 * site's DKAN-to-Drupal migration, splits each program's history across
 * several year-range files rather than one file per program (e.g. {@code
 * FOIA_7a_FY2020_Present_asof_*.csv} plus three older-vintage files). This
 * provider fetches the dataset landing page with browser headers, scrapes
 * every {@code uploaded_resources/*.csv} href belonging to the requested
 * program, and streams all of them in sequence. The program ({@code 7a} /
 * {@code 504}) is a partition dimension supplied by the writer — it is NOT
 * emitted as a row column. {@code year} ({@code ApprovalFY}) is a data
 * column, not a partition.
 */
public class SbaLoansProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SbaLoansProvider.class);

  private static final String DATASET_PAGE = "https://data.sba.gov/dataset/7a-504-foia";

  /** Any data.sba.gov uploaded_resources href ending in .csv. */
  private static final Pattern CSV_HREF =
      Pattern.compile("href=\"([^\"]*/uploaded_resources/[^\"]*\\.csv)\"", Pattern.CASE_INSENSITIVE);

  /** {output column, source header, kind}. lender_name resolved separately. */
  private static final String[][] COLUMNS = {
      {"borrower_name", "BorrName", "s"},
      {"borrower_city", "BorrCity", "s"},
      {"borrower_state", "BorrState", "s"},
      {"borrower_zip", "BorrZip", "s"},
      {"gross_approval", "GrossApproval", "d"},
      {"sba_guaranteed", "SBAGuaranteedApproval", "d"},
      {"approval_date", "ApprovalDate", "s"},
      {"year", "ApprovalFY", "i"},
      {"delivery_method", "ProcessingMethod", "s"},
      {"naics_code", "NaicsCode", "s"},
      {"naics_description", "NaicsDescription", "s"},
      {"project_county", "ProjectCounty", "s"},
      {"project_state", "ProjectState", "s"},
      {"business_type", "BusinessType", "s"},
      {"loan_status", "LoanStatus", "s"},
      {"jobs_supported", "JobsSupported", "i"},
  };

  private static final String[] REQUIRED = {"BorrName", "GrossApproval", "ApprovalFY"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String program = variables.get("program");
    if (program == null || program.isEmpty()) {
      LOGGER.warn("sba_loan_approvals: no program in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    List<String> csvUrls = resolveCsvUrls(program);
    LOGGER.info("sba_loan_approvals: program {} resolved to {} file(s): {}",
        program, csvUrls.size(), csvUrls);
    return new MultiCsvIterator(csvUrls);
  }

  /**
   * Fetches the dataset landing page (browser headers) and scrapes every CSV
   * href whose filename belongs to the requested program (e.g. {@code
   * FOIA_7a_*.csv} or {@code FOIA_504_*.csv}).
   */
  private List<String> resolveCsvUrls(String program) throws IOException {
    HttpURLConnection conn = FiscalHttp.openGet(DATASET_PAGE);
    String html;
    InputStream in = conn.getInputStream();
    try {
      html = FiscalHttp.readAll(in);
    } finally {
      in.close();
    }
    String prefix = "FOIA_" + program + "_";
    Matcher m = CSV_HREF.matcher(html);
    List<String> urls = new ArrayList<String>();
    while (m.find()) {
      String href = m.group(1);
      String fileName = href.substring(href.lastIndexOf('/') + 1);
      if (fileName.regionMatches(true, 0, prefix, 0, prefix.length())) {
        urls.add(href.startsWith("http") ? href
            : href.startsWith("/") ? "https://data.sba.gov" + href : "https://data.sba.gov/" + href);
      }
    }
    if (urls.isEmpty()) {
      throw new IOException("sba_loan_approvals: no CSV files matching '" + prefix
          + "*.csv' found on " + DATASET_PAGE
          + " (the anti-bot page may have been served instead)");
    }
    Collections.sort(urls);
    return urls;
  }

  private static Map<String, Object> toRow(List<String> cols, Map<String, Integer> idx) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    for (String[] c : COLUMNS) {
      String raw = FiscalHttp.cell(cols, FiscalHttp.col(idx, c[1]));
      if ("d".equals(c[2])) {
        row.put(c[0], FiscalHttp.toDouble(raw));
      } else if ("i".equals(c[2])) {
        row.put(c[0], FiscalHttp.toInt(raw));
      } else {
        row.put(c[0], FiscalHttp.str(raw));
      }
    }
    // Lender: bank for 7(a), third-party lender for 504.
    String lender = FiscalHttp.str(FiscalHttp.cell(cols, FiscalHttp.col(idx, "BankName")));
    if (lender == null) {
      lender = FiscalHttp.str(FiscalHttp.cell(cols, FiscalHttp.col(idx, "ThirdPartyLender_Name")));
    }
    row.put("lender_name", lender);
    return row;
  }

  /**
   * Streams rows across multiple CSV files in sequence, re-parsing the header
   * (and re-checking {@link #REQUIRED}) at the start of each file since column
   * sets can vary across the year-range vintages (e.g. 504 files carry no
   * {@code SBAGuaranteedApproval} column).
   */
  private static final class MultiCsvIterator implements Iterator<Map<String, Object>> {
    private final Iterator<String> urls;
    private BufferedReader reader;
    private Map<String, Integer> idx;
    private String currentUrl;
    private Map<String, Object> nextRow;
    private boolean done;

    MultiCsvIterator(List<String> urls) {
      this.urls = urls.iterator();
    }

    private boolean openNext() throws IOException {
      if (!urls.hasNext()) {
        return false;
      }
      currentUrl = urls.next();
      LOGGER.info("sba_loan_approvals: streaming {}", currentUrl);
      HttpURLConnection conn = FiscalHttp.openGet(currentUrl);
      reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      String headerRecord = CsvRecordReader.readRecord(reader);
      if (headerRecord == null) {
        reader.close();
        throw new IOException("sba_loan_approvals: empty CSV at " + currentUrl);
      }
      idx = FiscalHttp.headerIndex(CsvRecordReader.splitFields(headerRecord, ','));
      for (String req : REQUIRED) {
        FiscalHttp.required(idx, req, currentUrl);
      }
      return true;
    }

    private void advance() {
      if (nextRow != null || done) {
        return;
      }
      try {
        while (true) {
          if (reader == null) {
            if (!openNext()) {
              done = true;
              return;
            }
          }
          String record = CsvRecordReader.readRecord(reader);
          if (record == null) {
            reader.close();
            reader = null;
            continue;
          }
          List<String> cols = CsvRecordReader.splitFields(record, ',');
          if (cols.isEmpty()) {
            continue;
          }
          nextRow = toRow(cols, idx);
          return;
        }
      } catch (IOException e) {
        throw new RuntimeException("sba_loan_approvals: streaming failed at " + currentUrl, e);
      }
    }

    @Override public boolean hasNext() {
      advance();
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      advance();
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      nextRow = null;
      return row;
    }
  }
}
