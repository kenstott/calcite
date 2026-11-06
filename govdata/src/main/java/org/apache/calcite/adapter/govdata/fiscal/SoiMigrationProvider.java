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
import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
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
 * suppressed cells (n1 &lt; 0) are filtered out. AGI in $1000s. The 2007-08 .. 2010-11 files carry
 * a different header set (see {@link #canonicalIndex}); they are aliased onto the canonical names.
 */
public class SoiMigrationProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SoiMigrationProvider.class);

  private static final String[] REQUIRED = {
      "y1_statefips", "y1_countyfips", "y2_statefips", "y2_countyfips", "n1", "n2", "agi"};

  /** The complete pre-2011-12 header set (lower-cased, as FiscalHttp.headerIndex keys them). */
  private static final String[] LEGACY_HEADERS = {
      "state_code_origin", "county_code_origin", "state_code_dest", "county_code_dest",
      "state_abbrv", "county_name", "return_num", "exmpt_num", "aggr_agi"};

  /** Legacy header, canonical name pairs. */
  private static final String[] LEGACY_ALIASES = {
      "state_code_origin", "y1_statefips",
      "county_code_origin", "y1_countyfips",
      "state_code_dest", "y2_statefips",
      "county_code_dest", "y2_countyfips",
      "county_name", "y2_countyname",
      "return_num", "n1",
      "exmpt_num", "n2",
      "aggr_agi", "agi"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
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
    // fallback-guard: allow narrow guard on a framework-supplied dimension value, before any download/parse; bad value is logged
    } catch (NumberFormatException e) {
      LOGGER.warn("county_migration_flows: non-numeric year {}", year);
      return Collections.emptyIterator();
    }
    // Year pair token: (year-1)(year) as two 2-digit tokens, e.g. 2022 -> "2122".
    final String pair = FiscalHttp.twoDigitYear(String.valueOf(yr - 1)) + FiscalHttp.twoDigitYear(String.valueOf(yr));
    final String url = "https://www.irs.gov/pub/irs-soi/countyoutflow" + pair + ".csv";
    LOGGER.info("county_migration_flows: fetching {}", url);

    // Read through the pipeline's raw cache rather than opening a connection. This provider
    // still has to exist -- it drops foreign and summary buckets, suppressed cells and
    // non-migrant rows, which no declarative filter can express -- but none of that is a reason
    // to re-download the file on every run.
    // The 2009-2014 files carry county names in ISO-8859-1 ("Dona Ana" with a tilde n, one byte
    // 0xF1), which is not valid UTF-8 and not ASCII, so the stream is decoded as ISO-8859-1.
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(rawCache.openStream(url), StandardCharsets.ISO_8859_1));
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      reader.close();
      throw new IOException("county_migration_flows: empty CSV at " + url);
    }
    final Map<String, Integer> idx = canonicalIndex(
        FiscalHttp.headerIndex(CsvRecordReader.splitFields(headerRecord, ',')));
    for (String req : REQUIRED) {
      FiscalHttp.required(idx, req, url);
    }

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> nextRow;
      private boolean done;
      // Value signature per (origin, destination) already emitted from this file: the IRS
      // countyoutflow1314 file lists 984 county pairs twice, as identical rows.
      private final Map<String, String> seen = new HashMap<String, String>();
      private int duplicates;

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
            if (row != null && !isRepeat(row)) {
              nextRow = row;
              return;
            }
          }
          done = true;
          if (duplicates > 0) {
            LOGGER.warn("county_migration_flows: {} dropped {} exact duplicate county-pair rows",
                url, Integer.valueOf(duplicates));
          }
          reader.close();
        } catch (IOException e) {
          throw new RuntimeException("county_migration_flows: streaming failed at " + url, e);
        }
      }

      /**
       * True when the row repeats a county pair already emitted with identical values. A repeat
       * with different values is not a duplicate but a contradiction in the source and fails
       * the load.
       */
      private boolean isRepeat(Map<String, Object> row) {
        String key = row.get("origin_county_fips") + ">" + row.get("dest_county_fips");
        String signature = row.get("num_returns") + "|" + row.get("num_individuals") + "|"
            + row.get("agi");
        String previous = seen.put(key, signature);
        if (previous == null) {
          return false;
        }
        if (!previous.equals(signature)) {
          throw new IllegalStateException("county_migration_flows: " + url + " lists " + key
              + " twice with different values (" + previous + " vs " + signature + ")");
        }
        duplicates++;
        return true;
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

  /**
   * Maps the header layout of the 2007-08 .. 2010-11 outflow files onto the canonical names.
   *
   * <p>The IRS used a different header set before the 2011-12 file: {@code State_Code_Origin,
   * County_Code_Origin, State_Code_Dest, County_Code_Dest, State_Abbrv, County_Name,
   * Return_Num, Exmpt_Num, Aggr_AGI}. The columns mean the same thing as their canonical
   * counterparts (origin, destination, destination county name, returns, exemptions, AGI in
   * $1000s). The alias is applied only when the header carries the complete legacy set; any
   * other header set is returned unchanged, so a layout the provider does not know still fails
   * loudly on the required-column check.
   */
  static Map<String, Integer> canonicalIndex(Map<String, Integer> idx) {
    for (String legacy : LEGACY_HEADERS) {
      if (!idx.containsKey(legacy)) {
        return idx;
      }
    }
    Map<String, Integer> canonical = new HashMap<String, Integer>();
    for (int i = 0; i < LEGACY_ALIASES.length; i += 2) {
      canonical.put(LEGACY_ALIASES[i + 1], idx.get(LEGACY_ALIASES[i]));
    }
    return canonical;
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
    // fallback-guard: allow intentional design: unparseable FIPS values are IRS "summary bucket" rows meant to be filtered out via the 999 sentinel, not real per-county records
    } catch (NumberFormatException e) {
      return 999;  // treat unparseable FIPS as a summary bucket -> filtered
    }
  }
}
