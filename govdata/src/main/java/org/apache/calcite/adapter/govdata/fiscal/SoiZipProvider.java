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
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * DataProvider for {@code soi_income_by_zip} — IRS SOI individual income tax
 * statistics by ZIP x AGI size class x tax year.
 *
 * <p>The file is addressed by a 2-digit tax year:
 * {@code https://www.irs.gov/pub/irs-soi/{YY}zpallagi.csv} (e.g. {@code 22zpallagi.csv}
 * for TY2022). The templating layer does not synthesize a 2-digit year, so this
 * provider builds the URL from the {@code effective_year} dimension and streams
 * the CSV lazily. Header casing is inconsistent (zipcode/agi_stub lowercase;
 * N1/MARS2 uppercase) so lookups are case-insensitive. Amounts are in $1000s and
 * may be negative; {@code zipcode=00000} rows are the state total (kept).
 */
public class SoiZipProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SoiZipProvider.class);

  /** {output column, source header, kind} where kind is s(tring)/l(ong)/d(ouble). */
  private static final String[][] COLUMNS = {
      {"state_fips", "STATEFIPS", "fips2"},
      {"state_abbr", "STATE", "s"},
      {"zip_code", "zipcode", "zip5"},
      {"agi_bracket", "agi_stub", "s"},
      {"num_returns", "N1", "l"},
      {"num_individuals", "N2", "l"},
      {"num_single_returns", "mars1", "l"},
      {"num_joint_returns", "MARS2", "l"},
      {"num_hoh_returns", "MARS4", "l"},
      {"num_elderly_returns", "ELDERLY", "l"},
      {"adjusted_gross_income", "A00100", "d"},
      {"total_income_amount", "A02650", "d"},
      {"salaries_wages", "A00200", "d"},
      {"taxable_interest", "A00300", "d"},
      {"ordinary_dividends", "A00600", "d"},
      {"business_net_income", "A00900", "d"},
      {"net_capital_gain", "A01000", "d"},
      {"taxable_pensions", "A01700", "d"},
      {"unemployment_comp", "A02300", "d"},
      {"taxable_social_security", "A02500", "d"},
      {"total_itemized_deductions", "A04470", "d"},
      {"taxable_income", "A04800", "d"},
      {"income_tax_before_credits", "A05800", "d"},
      {"total_income_tax", "A06500", "d"},
      {"total_tax_liability", "A10300", "d"},
      {"eitc_amount", "A59660", "d"},
  };

  /** Columns that must exist in the header (fail loud otherwise). */
  private static final String[] REQUIRED = {"STATEFIPS", "STATE", "zipcode", "agi_stub", "N1"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("soi_income_by_zip: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    final String url = "https://www.irs.gov/pub/irs-soi/" + FiscalHttp.twoDigitYear(year) + "zpallagi.csv";
    LOGGER.info("soi_income_by_zip: fetching {}", url);

    HttpURLConnection conn = FiscalHttp.openGet(url);
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      reader.close();
      throw new IOException("soi_income_by_zip: empty CSV at " + url);
    }
    final Map<String, Integer> idx = FiscalHttp.headerIndex(CsvRecordReader.splitFields(headerRecord, ','));
    for (String req : REQUIRED) {
      FiscalHttp.required(idx, req, url);
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
          throw new RuntimeException("soi_income_by_zip: streaming failed at " + url, e);
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

  private static Map<String, Object> toRow(List<String> cols, Map<String, Integer> idx) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    for (String[] c : COLUMNS) {
      String raw = FiscalHttp.cell(cols, FiscalHttp.col(idx, c[1]));
      row.put(c[0], coerce(raw, c[2]));
    }
    return row;
  }

  private static Object coerce(String raw, String kind) {
    if ("l".equals(kind)) {
      return FiscalHttp.toLong(raw);
    }
    if ("d".equals(kind)) {
      return FiscalHttp.toDouble(raw);
    }
    if ("fips2".equals(kind)) {
      return FiscalHttp.pad(raw, 2);
    }
    if ("zip5".equals(kind)) {
      return FiscalHttp.pad(raw, 5);
    }
    return FiscalHttp.str(raw);
  }
}
