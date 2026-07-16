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
 * DataProvider for {@code soi_income_by_county} — IRS SOI individual income tax
 * statistics by county x AGI size class x tax year.
 *
 * <p>URL {@code https://www.irs.gov/pub/irs-soi/{YY}incyallagi.csv}. Identical to
 * the ZIP file except {@code zipcode} is replaced by {@code COUNTYFIPS} (3-digit)
 * + {@code COUNTYNAME}; {@code county_fips} is the 5-digit STATEFIPS||COUNTYFIPS
 * GEOID. {@code COUNTYFIPS=000} rows are the state total (kept). Amounts $1000s.
 */
public class SoiCountyProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SoiCountyProvider.class);

  /** {output column, source header, kind}. county_fips is derived separately. */
  private static final String[][] COLUMNS = {
      {"state_fips", "STATEFIPS", "fips2"},
      {"state_abbr", "STATE", "s"},
      {"county_name", "COUNTYNAME", "s"},
      {"agi_bracket", "agi_stub", "s"},
      {"num_returns", "N1", "l"},
      {"num_individuals", "N2", "l"},
      {"adjusted_gross_income", "A00100", "d"},
      {"total_income_amount", "A02650", "d"},
      {"salaries_wages", "A00200", "d"},
      {"business_net_income", "A00900", "d"},
      {"net_capital_gain", "A01000", "d"},
      {"taxable_income", "A04800", "d"},
      {"total_income_tax", "A06500", "d"},
      {"total_tax_liability", "A10300", "d"},
  };

  private static final String[] REQUIRED = {"STATEFIPS", "STATE", "COUNTYFIPS", "COUNTYNAME", "agi_stub", "N1"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("soi_income_by_county: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    final String url = "https://www.irs.gov/pub/irs-soi/" + FiscalHttp.twoDigitYear(year) + "incyallagi.csv";
    LOGGER.info("soi_income_by_county: fetching {}", url);

    HttpURLConnection conn = FiscalHttp.openGet(url);
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      reader.close();
      throw new IOException("soi_income_by_county: empty CSV at " + url);
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
          throw new RuntimeException("soi_income_by_county: streaming failed at " + url, e);
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
    String stateFips = FiscalHttp.pad(FiscalHttp.cell(cols, FiscalHttp.col(idx, "STATEFIPS")), 2);
    String countyPart = FiscalHttp.pad(FiscalHttp.cell(cols, FiscalHttp.col(idx, "COUNTYFIPS")), 3);
    String countyFips = (stateFips == null || countyPart == null) ? null : stateFips + countyPart;
    for (String[] c : COLUMNS) {
      String raw = FiscalHttp.cell(cols, FiscalHttp.col(idx, c[1]));
      row.put(c[0], coerce(raw, c[2]));
    }
    row.put("county_fips", countyFips);
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
    return FiscalHttp.str(raw);
  }
}
