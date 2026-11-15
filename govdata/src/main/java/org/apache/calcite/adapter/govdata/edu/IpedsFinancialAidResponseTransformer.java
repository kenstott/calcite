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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Streaming transformer for the NCES IPEDS Student Financial Aid and Net Price bulk CSV
 * ({@code SFA{yy1}{yy2}.zip}).
 *
 * <p>One row per institution per academic year. The ZIP holds a base file and a revised
 * {@code _rv} file; the revised file is preferred. {@code X}-prefixed columns are
 * imputation flags and are skipped. {@code year} is the academic-year-ending vintage
 * (effective_year), injected from the dimension context.
 */
public class IpedsFinancialAidResponseTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IpedsFinancialAidResponseTransformer.class);

  private static final Map<String, String> COLUMN_MAP;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("UNITID", "unitid");
    m.put("SCUGFFN", "ftft_cohort_n");
    m.put("SCUGFFP", "ftft_cohort_pct_of_undergrad");
    m.put("ANYAIDN", "any_aid_n");
    m.put("ANYAIDP", "any_aid_pct");
    m.put("AGRNT_N", "any_grant_n");
    m.put("AGRNT_P", "any_grant_pct");
    m.put("AGRNT_T", "any_grant_total");
    m.put("AGRNT_A", "any_grant_avg");
    m.put("FGRNT_N", "federal_grant_n");
    m.put("FGRNT_P", "federal_grant_pct");
    m.put("FGRNT_T", "federal_grant_total");
    m.put("FGRNT_A", "federal_grant_avg");
    m.put("PGRNT_N", "pell_grant_n");
    m.put("PGRNT_P", "pell_grant_pct");
    m.put("PGRNT_T", "pell_grant_total");
    m.put("PGRNT_A", "pell_grant_avg");
    m.put("SGRNT_N", "state_local_grant_n");
    m.put("SGRNT_P", "state_local_grant_pct");
    m.put("SGRNT_T", "state_local_grant_total");
    m.put("SGRNT_A", "state_local_grant_avg");
    m.put("IGRNT_N", "institutional_grant_n");
    m.put("IGRNT_P", "institutional_grant_pct");
    m.put("IGRNT_T", "institutional_grant_total");
    m.put("IGRNT_A", "institutional_grant_avg");
    m.put("LOAN_N", "any_loan_n");
    m.put("LOAN_P", "any_loan_pct");
    m.put("LOAN_T", "any_loan_total");
    m.put("LOAN_A", "any_loan_avg");
    m.put("FLOAN_N", "federal_loan_n");
    m.put("FLOAN_P", "federal_loan_pct");
    m.put("FLOAN_T", "federal_loan_total");
    m.put("FLOAN_A", "federal_loan_avg");
    m.put("NPIST2", "avg_net_price_title_iv_yr2");
    m.put("NPIST1", "avg_net_price_title_iv_yr1");
    m.put("NPIST0", "avg_net_price_title_iv_yr0");
    m.put("NPGRN2", "avg_net_price_grant_aid_yr2");
    m.put("NPGRN1", "avg_net_price_grant_aid_yr1");
    m.put("NPGRN0", "avg_net_price_grant_aid_yr0");
    COLUMN_MAP = Collections.unmodifiableMap(m);
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearStr = context.getDimensionValues().get("effective_year");
    if (yearStr == null || yearStr.isEmpty()) {
      yearStr = context.getDimensionValues().get("year");
    }
    final int year = Integer.parseInt(yearStr);
    final String prefix = String.format("sfa%02d%02d", (year - 1) % 100, year % 100);

    File tempZip = IpedsFinancialsResponseTransformer.downloadToTemp(context.getUrl());
    String entryName = findCsvEntry(tempZip, prefix);
    if (entryName == null) {
      tempZip.delete();
      LOGGER.warn("IpedsFinancialAid: no {}*.csv in ZIP for year={}", prefix, year);
      return Collections.emptyIterator();
    }

    ZipFile zf = new ZipFile(tempZip);
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(zf.getInputStream(zf.getEntry(entryName)),
            StandardCharsets.UTF_8));
    return new LazyIterator(reader, zf, tempZip, year);
  }

  private static String findCsvEntry(File tempZip, String prefix) throws IOException {
    String preferred = null;
    String base = null;
    try (ZipFile zf = new ZipFile(tempZip)) {
      Enumeration<? extends ZipEntry> entries = zf.entries();
      while (entries.hasMoreElements()) {
        String name = entries.nextElement().getName();
        String lower = name.toLowerCase();
        if (!lower.startsWith(prefix) || !lower.endsWith(".csv")) {
          continue;
        }
        if (lower.equals(prefix + "_rv.csv")) {
          preferred = name;
        } else if (lower.equals(prefix + ".csv")) {
          base = name;
        }
      }
    }
    return preferred != null ? preferred : base;
  }

  private static final class LazyIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final ZipFile zipFile;
    private final File tempZip;
    private final int year;
    private final String[] header;
    private Map<String, Object> next;
    private boolean closed;

    LazyIterator(BufferedReader reader, ZipFile zipFile, File tempZip, int year)
        throws IOException {
      this.reader = reader;
      this.zipFile = zipFile;
      this.tempZip = tempZip;
      this.year = year;

      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        this.header = new String[0];
        close();
      } else {
        if (!headerLine.isEmpty() && headerLine.charAt(0) == '\uFEFF') {
          headerLine = headerLine.substring(1);
        }
        String[] raw = IpedsFinancialsResponseTransformer.parseCsvLine(headerLine);
        for (int i = 0; i < raw.length; i++) {
          raw[i] = raw[i].trim().toUpperCase();
        }
        this.header = raw;
        advance();
      }
    }

    private void advance() {
      while (!closed) {
        String line;
        try {
          line = CsvRecordReader.readRecord(reader);
        } catch (IOException e) {
          close();
          throw new IllegalStateException("IpedsFinancialAid: read failed for year=" + year, e);
        }
        if (line == null) {
          close();
          break;
        }
        String[] values = IpedsFinancialsResponseTransformer.parseCsvLine(line);
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("year", year);
        for (int c = 0; c < header.length && c < values.length; c++) {
          String canonical = COLUMN_MAP.get(header[c]);
          if (canonical == null) {
            continue;
          }
          String raw = values[c].trim();
          // Blank and dot-valued cells are IPEDS "not reported / not applicable".
          if (raw.isEmpty() || ".".equals(raw)) {
            row.put(canonical, null);
          } else {
            row.put(canonical, Integer.valueOf(raw));
          }
        }
        if (row.get("unitid") != null) {
          next = row;
          return;
        }
      }
      next = null;
    }

    private void close() {
      if (!closed) {
        closed = true;
        try {
          reader.close();
        } catch (IOException e) {
          LOGGER.debug("IpedsFinancialAid: reader close failed", e);
        }
        try {
          zipFile.close();
        } catch (IOException e) {
          LOGGER.debug("IpedsFinancialAid: zip close failed", e);
        }
        tempZip.delete();
      }
    }

    @Override public boolean hasNext() {
      return next != null;
    }

    @Override public Map<String, Object> next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> result = next;
      advance();
      return result;
    }
  }
}
