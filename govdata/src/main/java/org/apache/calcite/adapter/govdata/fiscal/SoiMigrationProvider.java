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
 * DataProvider for {@code county_migration_flows} — IRS SOI county-to-county
 * migration, from the OUTFLOW file
 * {@code https://www.irs.gov/pub/irs-soi/countyoutflow{YYZZ}.csv} where
 * {@code YYZZ} is the year pair (e.g. 2021->2022 == {@code 2122}).
 *
 * <p>Convention Y1 = origin, Y2 = destination (flow Y1->Y2). Summary/foreign
 * buckets (state FIPS >= 57; county 000), non-migrant same-county rows, and
 * suppressed cells (n1 &lt; 0) are filtered out. AGI in $1000s.
 */
public class SoiMigrationProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SoiMigrationProvider.class);

  private static final String[] REQUIRED = {
      "y1_statefips", "y1_countyfips", "y2_statefips", "y2_countyfips", "n1", "n2", "agi"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("county_migration_flows: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    int yr;
    try {
      yr = Integer.parseInt(year.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("county_migration_flows: non-numeric year {}", year);
      return Collections.emptyIterator();
    }
    // Year pair token: (year-1)(year) as two 2-digit tokens, e.g. 2022 -> "2122".
    final String pair = FiscalHttp.twoDigitYear(String.valueOf(yr - 1)) + FiscalHttp.twoDigitYear(String.valueOf(yr));
    final String url = "https://www.irs.gov/pub/irs-soi/countyoutflow" + pair + ".csv";
    LOGGER.info("county_migration_flows: fetching {}", url);

    HttpURLConnection conn = FiscalHttp.openGet(url);
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      reader.close();
      throw new IOException("county_migration_flows: empty CSV at " + url);
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
            Map<String, Object> row = toRow(cols, idx);
            if (row != null) {
              nextRow = row;
              return;
            }
          }
          done = true;
          reader.close();
        } catch (IOException e) {
          throw new RuntimeException("county_migration_flows: streaming failed at " + url, e);
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

  /** Returns null for a filtered summary/sentinel/non-migrant row. */
  private static Map<String, Object> toRow(List<String> cols, Map<String, Integer> idx) {
    String oSt = FiscalHttp.str(FiscalHttp.cell(cols, FiscalHttp.col(idx, "y1_statefips")));
    String oCo = FiscalHttp.str(FiscalHttp.cell(cols, FiscalHttp.col(idx, "y1_countyfips")));
    String dSt = FiscalHttp.str(FiscalHttp.cell(cols, FiscalHttp.col(idx, "y2_statefips")));
    String dCo = FiscalHttp.str(FiscalHttp.cell(cols, FiscalHttp.col(idx, "y2_countyfips")));
    if (oSt == null || oCo == null || dSt == null || dCo == null) {
      return null;
    }
    // Drop summary/foreign buckets (state FIPS >= 57 covers 57/58/59 and 96/97/98) and 000 counties.
    if (asInt(oSt) >= 57 || asInt(dSt) >= 57 || "000".equals(oCo) || "000".equals(dCo)) {
      return null;
    }
    Long n1 = FiscalHttp.toLong(FiscalHttp.cell(cols, FiscalHttp.col(idx, "n1")));
    if (n1 != null && n1.longValue() < 0) {
      return null;  // suppressed cell
    }
    String originFips = FiscalHttp.pad(oSt, 2) + FiscalHttp.pad(oCo, 3);
    String destFips = FiscalHttp.pad(dSt, 2) + FiscalHttp.pad(dCo, 3);
    if (originFips.equals(destFips)) {
      return null;  // non-migrant (stayed in same county)
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("origin_state_fips", FiscalHttp.pad(oSt, 2));
    row.put("origin_county_fips", originFips);
    row.put("dest_state_fips", FiscalHttp.pad(dSt, 2));
    row.put("dest_county_fips", destFips);
    row.put("dest_county_name", FiscalHttp.str(FiscalHttp.cell(cols, FiscalHttp.col(idx, "y2_countyname"))));
    row.put("num_returns", n1);
    row.put("num_individuals", FiscalHttp.toLong(FiscalHttp.cell(cols, FiscalHttp.col(idx, "n2"))));
    row.put("agi", FiscalHttp.toDouble(FiscalHttp.cell(cols, FiscalHttp.col(idx, "agi"))));
    return row;
  }

  private static int asInt(String s) {
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return 999;  // treat unparseable FIPS as a summary bucket -> filtered
    }
  }
}
