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

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for {@code usaspending_account_balances} — budgetary resources, obligations
 * incurred and gross outlays by federal account (Treasury Account Symbol roll-up) for every
 * published reporting period of a fiscal year.
 *
 * <p>Source is the USAspending Account Data download
 * ({@code POST /api/v2/download/accounts/}, {@code account_level=federal_account},
 * {@code submission_types=[account_balances]}). One request returns one reporting period; a
 * period that has not been published yet returns a header-only file and contributes no rows.
 * Amounts are cumulative from the start of the fiscal year through the period. Fiscal years
 * 2017-2019 were reported quarterly (periods 3, 6, 9, 12); later years monthly (2-12), with
 * some agencies still reporting quarterly. A period's file also holds each agency's latest
 * earlier submission; only rows submitted for the requested period itself are kept.
 */
public class UsaSpendingAccountBalancesProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsaSpendingAccountBalancesProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT = "https://api.usaspending.gov/api/v2/download/accounts/";
  private static final String STATUS = "https://api.usaspending.gov/api/v2/download/status?file_name=";
  private static final int FIRST_PERIOD = 2;
  private static final int LAST_PERIOD = 12;
  private static final long POLL_INTERVAL_MS = 5000L;
  private static final long REQUEST_SPACING_MS = 45000L;
  private static final long THROTTLE_WAIT_MS = 300000L;
  private static final int THROTTLE_RETRIES = 5;
  private static final int OPEN_ATTEMPTS = 5;
  private static final long OPEN_BACKOFF_MS = 15000L;
  /** Serializes download requests across the pipeline's worker threads. */
  private static final Object GATE = new Object();
  private static long lastRequestAt;
  private static final int MAX_POLLS = 120;
  private static final Pattern SUBMISSION = Pattern.compile("^FY(\\d{4})([PQ])(\\d{1,2})$");

  /** {output column, source header, kind: s=string, d=double}. */
  private static final String[][] COLUMNS = {
      {"owning_agency_name", "owning_agency_name", "s"},
      {"reporting_agency_name", "reporting_agency_name", "s"},
      {"submission_period", "submission_period", "s"},
      {"federal_account_symbol", "federal_account_symbol", "s"},
      {"federal_account_name", "federal_account_name", "s"},
      {"agency_identifier_name", "agency_identifier_name", "s"},
      {"budget_function", "budget_function", "s"},
      {"budget_subfunction", "budget_subfunction", "s"},
      {"unobligated_balance_brought_forward",
          "budget_authority_unobligated_balance_brought_forward", "d"},
      {"adjustments_to_unobligated_balance_brought_forward",
          "adjustments_to_unobligated_balance_brought_forward_cpe", "d"},
      {"budget_authority_appropriated", "budget_authority_appropriated_amount", "d"},
      {"borrowing_authority", "borrowing_authority_amount", "d"},
      {"contract_authority", "contract_authority_amount", "d"},
      {"spending_authority_from_offsetting_collections",
          "spending_authority_from_offsetting_collections_amount", "d"},
      {"total_other_budgetary_resources", "total_other_budgetary_resources_amount", "d"},
      {"total_budgetary_resources", "total_budgetary_resources", "d"},
      {"obligations_incurred", "obligations_incurred", "d"},
      {"deobligations_recoveries_refunds",
          "deobligations_or_recoveries_or_refunds_from_prior_year", "d"},
      {"unobligated_balance", "unobligated_balance", "d"},
      {"gross_outlays", "gross_outlay_amount", "d"},
      {"last_modified_date", "last_modified_date", "s"},
  };

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      throw new IOException("usaspending_account_balances: no effective_year in " + variables);
    }
    final int fy = Integer.parseInt(year.trim());
    // A period's file changes as agencies certify later submissions, so the cache is keyed by
    // the calendar month: each month re-reads the year, within a month it is read once.
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    String stamp = today.getYear() + "-" + today.getMonthValue();
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int period = FIRST_PERIOD; period <= LAST_PERIOD; period++) {
      final int p = period;
      String key = ENDPOINT + "fy=" + fy + "/period=" + p + "?asof=" + stamp;
      InputStream in = rawCache.openStream(key, new RawCache.ContentSupplier() {
        @Override public InputStream open() throws IOException {
          return openFile(fy, p);
        }
      });
      int before = rows.size();
      try {
        readZip(in, fy, p, rows);
      } finally {
        in.close();
      }
      LOGGER.info("usaspending_account_balances: fy {} period {} -> {} rows", fy, p,
          rows.size() - before);
    }
    return rows.iterator();
  }

  /**
   * Requests, polls and opens one period's file. api.usaspending.gov and files.usaspending.gov
   * sometimes close a connection without a response; the whole cycle is repeated after a
   * pause, and the failure is thrown once the attempts are used up.
   */
  private static InputStream openFile(int fy, int period) throws IOException {
    IOException failure = null;
    for (int attempt = 1; attempt <= OPEN_ATTEMPTS; attempt++) {
      try {
        return AbstractBulkCsvProvider.openUrl(generate(fy, period));
      } catch (IOException e) {
        failure = e;
        LOGGER.warn("usaspending_account_balances: FY{} period {} file request failed (attempt "
            + "{}/{}): {}", fy, period, attempt, OPEN_ATTEMPTS, e.toString());
        if (attempt < OPEN_ATTEMPTS) {
          try {
            Thread.sleep(OPEN_BACKOFF_MS * attempt);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted opening FY" + fy + " period " + period, ie);
          }
        }
      }
    }
    throw failure;
  }

  /** Requests the file for one period, waits for it to be built and returns its URL. */
  private static String generate(int fy, int period) throws IOException {
    // One download is built at a time across all worker threads: request and status polls
    // included, since concurrent polling alone gets connections reset.
    synchronized (GATE) {
      try {
        return generateOne(fy, period);
      } finally {
        lastRequestAt = System.currentTimeMillis();
      }
    }
  }

  private static String generateOne(int fy, int period) throws IOException {
    String body = "{\"account_level\":\"federal_account\",\"filters\":{\"fy\":\"" + fy
        + "\",\"period\":" + period + ",\"budget_function\":\"all\",\"agency\":\"all\","
        + "\"submission_types\":[\"account_balances\"]},\"file_format\":\"csv\"}";
    JsonNode req = null;
    IOException failure = null;
    for (int attempt = 1; attempt <= THROTTLE_RETRIES && req == null; attempt++) {
      try {
        awaitTurn(fy, period);
        req = postJson(body);
      } catch (IOException e) {
        // The API resets connections once it has built several downloads in a row; waiting
        // minutes, not seconds, lets it accept the request again.
        failure = e;
        LOGGER.warn("usaspending_account_balances: FY{} period {} request refused (attempt {}/{}), "
            + "waiting {}s: {}", fy, period, attempt, THROTTLE_RETRIES, THROTTLE_WAIT_MS / 1000,
            e.getMessage());
        try {
          Thread.sleep(THROTTLE_WAIT_MS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted waiting to retry FY" + fy + " period " + period, ie);
        }
      }
    }
    if (req == null) {
      throw failure;
    }
    String fileName = req.path("file_name").asText();
    String fileUrl = req.path("file_url").asText();
    if (fileName.isEmpty() || fileUrl.isEmpty()) {
      throw new IOException("usaspending_account_balances: no file in response to " + body);
    }
    for (int i = 0; i < MAX_POLLS; i++) {
      JsonNode st = getJson(STATUS + fileName);
      String status = st.path("status").asText();
      if ("finished".equals(status)) {
        return fileUrl;
      }
      if ("failed".equals(status)) {
        throw new IOException("usaspending_account_balances: download " + fileName + " failed: "
            + st.path("message").asText());
      }
      try {
        Thread.sleep(POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for " + fileName, e);
      }
    }
    throw new IOException("usaspending_account_balances: " + fileName + " not finished after "
        + MAX_POLLS * POLL_INTERVAL_MS / 1000 + "s");
  }

  /**
   * api.usaspending.gov resets connections when download requests arrive back to back or in
   * parallel, so every uncached request, from any thread, waits until the spacing has passed
   * since the previous one.
   */
  private static void awaitTurn(int fy, int period) throws IOException {
    synchronized (GATE) {
      long wait = lastRequestAt + REQUEST_SPACING_MS - System.currentTimeMillis();
      if (wait > 0) {
        try {
          Thread.sleep(wait);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted before requesting FY" + fy + " period " + period, e);
        }
      }
      lastRequestAt = System.currentTimeMillis();
    }
  }

  private static JsonNode postJson(String body) throws IOException {
    InputStream in = FiscalHttp.openPostJsonWithRetry(ENDPOINT, body).getInputStream();
    try {
      return MAPPER.readTree(in);
    } finally {
      in.close();
    }
  }

  private static JsonNode getJson(String url) throws IOException {
    InputStream in = FiscalHttp.openGetWithRetry(url).getInputStream();
    try {
      return MAPPER.readTree(in);
    } finally {
      in.close();
    }
  }

  private static void readZip(InputStream raw, int fy, int period, List<Map<String, Object>> out)
      throws IOException {
    ZipInputStream zip = new ZipInputStream(raw);
    ZipEntry e;
    boolean sawCsv = false;
    while ((e = zip.getNextEntry()) != null) {
      if (e.isDirectory() || !e.getName().toLowerCase(java.util.Locale.ROOT).endsWith(".csv")) {
        continue;
      }
      sawCsv = true;
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(zip, StandardCharsets.UTF_8), 1 << 16);
      String header = CsvRecordReader.readRecord(reader);
      if (header == null) {
        throw new IOException("usaspending_account_balances: empty CSV " + e.getName());
      }
      if (!header.isEmpty() && header.charAt(0) == '﻿') {
        header = header.substring(1);
      }
      Map<String, Integer> h = FiscalHttp.headerIndex(CsvRecordReader.splitFields(header, ','));
      int[] idx = new int[COLUMNS.length];
      for (int i = 0; i < COLUMNS.length; i++) {
        idx[i] = FiscalHttp.required(h, COLUMNS[i][1], e.getName());
      }
      String record;
      while ((record = CsvRecordReader.readRecord(reader)) != null) {
        List<String> cells = CsvRecordReader.splitFields(record, ',');
        if (cells.isEmpty()) {
          continue;
        }
        Map<String, Object> row = toRow(cells, idx, fy, period, e.getName());
        if (row != null) {
          out.add(row);
        }
      }
    }
    if (!sawCsv) {
      throw new IOException("usaspending_account_balances: no CSV in the FY" + fy + " period "
          + period + " download");
    }
  }

  private static Map<String, Object> toRow(List<String> cells, int[] idx, int fy, int period,
      String entry) throws IOException {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    for (int i = 0; i < COLUMNS.length; i++) {
      if (idx[i] >= cells.size()) {
        throw new IOException("usaspending_account_balances: row in " + entry + " has "
            + cells.size() + " cells, '" + COLUMNS[i][1] + "' is at " + idx[i]);
      }
      String raw = cells.get(idx[i]).trim();
      if (raw.isEmpty()) {
        row.put(COLUMNS[i][0], null);
      } else if ("d".equals(COLUMNS[i][2])) {
        try {
          row.put(COLUMNS[i][0], Double.valueOf(raw));
        } catch (NumberFormatException ex) {
          throw new IOException("usaspending_account_balances: '" + COLUMNS[i][1] + "' value '"
              + raw + "' is not a number in " + entry, ex);
        }
      } else {
        row.put(COLUMNS[i][0], raw);
      }
    }
    String submission = (String) row.get("submission_period");
    Matcher m = submission == null ? null : SUBMISSION.matcher(submission);
    if (m == null || !m.matches()) {
      throw new IOException("usaspending_account_balances: unrecognised submission_period '"
          + submission + "' in " + entry);
    }
    int n = Integer.parseInt(m.group(3));
    int reported = "Q".equals(m.group(2)) ? n * 3 : n;
    if (Integer.parseInt(m.group(1)) != fy || reported > period) {
      throw new IOException("usaspending_account_balances: requested FY" + fy + " period "
          + period + " but the file holds " + submission);
    }
    if (reported < period) {
      // A period's file carries each agency's latest submission up to that period, so an
      // agency that reports quarterly appears with its earlier quarter; that row belongs to
      // the file of its own period.
      return null;
    }
    row.put("fiscal_period", reported);
    return row;
  }
}
