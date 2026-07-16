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
 * (FOIA data) from the DKAN portal at data.sba.gov.
 *
 * <p>The portal blocks non-browser clients and rolls the CSV filename each
 * quarter (an {@code asof} date), so this provider resolves the download URL at
 * run time: it fetches the program's resource landing page with browser headers
 * and scrapes the {@code .../download/*.csv} href, then streams that CSV. The
 * program ({@code 7a} / {@code 504}) is a partition dimension supplied by the
 * writer — it is NOT emitted as a row column. {@code year} (ApprovalFiscalYear)
 * is a data column, not a partition.
 */
public class SbaLoansProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SbaLoansProvider.class);

  private static final Map<String, String> RESOURCE_PAGE = buildResourcePages();

  /** First data.sba.gov download href ending in .csv. */
  private static final Pattern CSV_HREF =
      Pattern.compile("href=\"([^\"]*/download/[^\"]*\\.csv)\"", Pattern.CASE_INSENSITIVE);

  /** {output column, source header, kind}. lender_name resolved separately. */
  private static final String[][] COLUMNS = {
      {"borrower_name", "BorrName", "s"},
      {"borrower_city", "BorrCity", "s"},
      {"borrower_state", "BorrState", "s"},
      {"borrower_zip", "BorrZip", "s"},
      {"gross_approval", "GrossApproval", "d"},
      {"sba_guaranteed", "SBAGuaranteedApproval", "d"},
      {"approval_date", "ApprovalDate", "s"},
      {"year", "ApprovalFiscalYear", "i"},
      {"delivery_method", "DeliveryMethod", "s"},
      {"subprogram", "subpgmdesc", "s"},
      {"naics_code", "NaicsCode", "s"},
      {"naics_description", "NaicsDescription", "s"},
      {"project_county", "ProjectCounty", "s"},
      {"project_state", "ProjectState", "s"},
      {"business_type", "BusinessType", "s"},
      {"loan_status", "LoanStatus", "s"},
      {"jobs_supported", "JobsSupported", "i"},
  };

  private static final String[] REQUIRED = {"BorrName", "GrossApproval", "ApprovalFiscalYear"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String program = variables.get("program");
    if (program == null || program.isEmpty()) {
      LOGGER.warn("sba_loan_approvals: no program in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    String landing = RESOURCE_PAGE.get(program.toLowerCase());
    if (landing == null) {
      throw new IOException("sba_loan_approvals: unknown program '" + program + "'");
    }
    String csvUrl = resolveCsvUrl(landing);
    LOGGER.info("sba_loan_approvals: program {} resolved to {}", program, csvUrl);

    HttpURLConnection conn = FiscalHttp.openGet(csvUrl);
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      reader.close();
      throw new IOException("sba_loan_approvals: empty CSV at " + csvUrl);
    }
    final Map<String, Integer> idx = FiscalHttp.headerIndex(CsvRecordReader.splitFields(headerRecord, ','));
    for (String req : REQUIRED) {
      FiscalHttp.required(idx, req, csvUrl);
    }

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> nextRow;
      private boolean done;

      private void advance() {
        if (nextRow != null || done) {
          return;
        }
        try {
          String record;
          while ((record = CsvRecordReader.readRecord(reader)) != null) {
            List<String> cols = CsvRecordReader.splitFields(record, ',');
            if (cols.isEmpty()) {
              continue;
            }
            nextRow = toRow(cols, idx);
            return;
          }
          done = true;
          reader.close();
        } catch (IOException e) {
          throw new RuntimeException("sba_loan_approvals: streaming failed at " + csvUrl, e);
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
    };
  }

  /** Fetches the resource landing page (browser headers) and scrapes the CSV href. */
  private String resolveCsvUrl(String landingUrl) throws IOException {
    HttpURLConnection conn = FiscalHttp.openGet(landingUrl);
    String html;
    InputStream in = conn.getInputStream();
    try {
      html = FiscalHttp.readAll(in);
    } finally {
      in.close();
    }
    Matcher m = CSV_HREF.matcher(html);
    if (!m.find()) {
      throw new IOException("sba_loan_approvals: no /download/*.csv link found on " + landingUrl
          + " (the DKAN anti-bot page may have been served instead)");
    }
    String href = m.group(1);
    if (href.startsWith("http")) {
      return href;
    }
    return href.startsWith("/") ? "https://data.sba.gov" + href : "https://data.sba.gov/" + href;
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

  private static Map<String, String> buildResourcePages() {
    Map<String, String> m = new java.util.HashMap<String, String>();
    m.put("7a", "https://data.sba.gov/en/dataset/7-a-504-foia/resource/d67d3ccb-2002-4134-a288-481b51cd3479");
    m.put("504", "https://data.sba.gov/en/dataset/7-a-504-foia/resource/4ad7f0f1-9da6-4d90-8bdb-89a6f821a1a9");
    return Collections.unmodifiableMap(m);
  }
}
