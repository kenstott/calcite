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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * DataProvider for {@code exempt_org_master} — the IRS Exempt Organizations
 * Business Master File. One logical source physically sharded across four
 * regional CSVs ({@code eo1.csv}..{@code eo4.csv}); this provider streams them
 * sequentially (open shard 1, exhaust, open shard 2, ...) with O(1) memory.
 * Cumulative monthly snapshot; the whole {@code type} partition is overwritten.
 */
public class IrsEoBmfProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(IrsEoBmfProvider.class);

  private static final String[] SHARDS = {
      "https://www.irs.gov/pub/irs-soi/eo1.csv",
      "https://www.irs.gov/pub/irs-soi/eo2.csv",
      "https://www.irs.gov/pub/irs-soi/eo3.csv",
      "https://www.irs.gov/pub/irs-soi/eo4.csv",
  };

  /** {output column, source header, kind}. */
  private static final String[][] COLUMNS = {
      {"ein", "EIN", "ein9"},
      {"org_name", "NAME", "s"},
      {"street", "STREET", "s"},
      {"city", "CITY", "s"},
      {"state_abbr", "STATE", "s"},
      {"zip_code", "ZIP", "s"},
      {"subsection_code", "SUBSECTION", "s"},
      {"classification_code", "CLASSIFICATION", "s"},
      {"ruling_date", "RULING", "s"},
      {"deductibility_code", "DEDUCTIBILITY", "s"},
      {"foundation_code", "FOUNDATION", "s"},
      {"organization_code", "ORGANIZATION", "s"},
      {"exempt_status", "STATUS", "s"},
      {"tax_period", "TAX_PERIOD", "s"},
      {"asset_amount", "ASSET_AMT", "d"},
      {"income_amount", "INCOME_AMT", "d"},
      {"revenue_amount", "REVENUE_AMT", "d"},
      {"ntee_code", "NTEE_CD", "s"},
      {"sort_name", "SORT_NAME", "s"},
  };

  private static final String[] REQUIRED = {"EIN", "NAME", "STATE"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    return new ShardIterator();
  }

  /** Lazily walks the four regional shards, streaming rows across all of them. */
  private static final class ShardIterator implements Iterator<Map<String, Object>> {
    private int shard = -1;
    private BufferedReader reader;
    private Map<String, Integer> idx;
    private Map<String, Object> nextRow;
    private boolean done;

    private boolean openNextShard() {
      closeReader();
      shard++;
      if (shard >= SHARDS.length) {
        return false;
      }
      String url = SHARDS[shard];
      try {
        LOGGER.info("exempt_org_master: fetching shard {}", url);
        HttpURLConnection conn = FiscalHttp.openGet(url);
        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));
        String header = CsvRecordReader.readRecord(reader);
        if (header == null) {
          throw new IOException("exempt_org_master: empty shard " + url);
        }
        idx = FiscalHttp.headerIndex(CsvRecordReader.splitFields(header, ','));
        for (String req : REQUIRED) {
          FiscalHttp.required(idx, req, url);
        }
        return true;
      } catch (IOException e) {
        throw new RuntimeException("exempt_org_master: failed to open shard " + url, e);
      }
    }

    private void closeReader() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ignore) {
          // best effort
        }
        reader = null;
      }
    }

    private void advance() {
      if (nextRow != null || done) {
        return;
      }
      try {
        while (true) {
          if (reader == null) {
            if (!openNextShard()) {
              done = true;
              return;
            }
          }
          String record = CsvRecordReader.readRecord(reader);
          if (record == null) {
            closeReader();
            continue;  // move to next shard
          }
          List<String> cols = CsvRecordReader.splitFields(record, ',');
          if (cols.isEmpty()) {
            continue;
          }
          nextRow = toRow(cols, idx);
          return;
        }
      } catch (IOException e) {
        throw new RuntimeException("exempt_org_master: streaming failed", e);
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

  private static Map<String, Object> toRow(List<String> cols, Map<String, Integer> idx) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    for (String[] c : COLUMNS) {
      String raw = FiscalHttp.cell(cols, FiscalHttp.col(idx, c[1]));
      if ("d".equals(c[2])) {
        row.put(c[0], FiscalHttp.toDouble(raw));
      } else if ("ein9".equals(c[2])) {
        row.put(c[0], FiscalHttp.pad(raw, 9));
      } else {
        row.put(c[0], FiscalHttp.str(raw));
      }
    }
    return row;
  }
}
