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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code house_price_index} (FHFA {@code hpi_master.csv}).
 *
 * <p>The master file is ~19 MB with every index type/flavor/frequency/geography in one CSV
 * (header {@code hpi_type,hpi_flavor,frequency,level,place_name,place_id,yr,period,index_nsa,index_sa}).
 * It is streamed record-by-record via {@link CsvRecordReader} (values are quoted) so heap stays
 * O(1) per row rather than materializing the whole file. Rows missing the required
 * identity fields ({@code place_id}/{@code yr}/{@code period}) are skipped rather than guessed.
 */
public class FhfaHpiStreamingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FhfaHpiStreamingTransformer.class);

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 600_000;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    LOGGER.info("house_price_index: streaming {}", url);

    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("HTTP " + status + " from " + url);
    }
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));

    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      reader.close();
      conn.disconnect();
      throw new IOException("house_price_index: empty file " + url);
    }
    Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(headerRecord, ','));
    return new HpiRowIterator(reader, conn, idx);
  }

  private static Map<String, Integer> headerIndex(List<String> header) {
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < header.size(); i++) {
      idx.put(header.get(i).trim(), i);
    }
    return idx;
  }

  /** Lazy row iterator over the CSV; closes the stream on exhaustion. */
  private static final class HpiRowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final HttpURLConnection conn;
    private final Map<String, Integer> idx;
    private Map<String, Object> nextRow;
    private boolean done;

    HpiRowIterator(BufferedReader reader, HttpURLConnection conn, Map<String, Integer> idx) {
      this.reader = reader;
      this.conn = conn;
      this.idx = idx;
      advance();
    }

    private void advance() {
      nextRow = null;
      try {
        String record;
        while ((record = CsvRecordReader.readRecord(reader)) != null) {
          List<String> cols = CsvRecordReader.splitFields(record, ',');
          Map<String, Object> row = mapRow(cols);
          if (row != null) {
            nextRow = row;
            return;
          }
        }
        close();
      } catch (IOException e) {
        close();
        throw new RuntimeException("house_price_index: read failed", e);
      }
    }

    private void close() {
      if (done) {
        return;
      }
      done = true;
      try {
        reader.close();
      } catch (IOException ignored) {
        // best-effort
      }
      conn.disconnect();
    }

    @Override public boolean hasNext() {
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      advance();
      return row;
    }

    /** Returns the mapped row, or null when required identity fields are absent (skip). */
    private Map<String, Object> mapRow(List<String> cols) {
      String placeId = str(cols, "place_id");
      Integer dataYear = intg(str(cols, "yr"));
      Integer period = intg(str(cols, "period"));
      if (placeId == null || dataYear == null || period == null) {
        return null;
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("hpi_type", str(cols, "hpi_type"));
      row.put("hpi_flavor", str(cols, "hpi_flavor"));
      row.put("frequency", str(cols, "frequency"));
      row.put("level", str(cols, "level"));
      row.put("place_name", str(cols, "place_name"));
      row.put("place_id", placeId);
      row.put("data_year", dataYear);
      row.put("period", period);
      row.put("index_nsa", dbl(str(cols, "index_nsa")));
      row.put("index_sa", dbl(str(cols, "index_sa")));
      return row;
    }

    private String str(List<String> cols, String name) {
      Integer i = idx.get(name);
      if (i == null || i >= cols.size()) {
        return null;
      }
      String v = cols.get(i).trim();
      return v.isEmpty() ? null : v;
    }
  }

  private static Integer intg(String v) {
    if (v == null) {
      return null;
    }
    try {
      return (int) Double.parseDouble(v);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Double dbl(String v) {
    if (v == null) {
      return null;
    }
    try {
      return Double.parseDouble(v);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
