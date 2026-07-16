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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streaming transformer for {@code research.nsf_herd_by_institution} — the NSF NCSES
 * Higher Education R&amp;D (HERD) survey institution-level microdata.
 *
 * <p>There is no NCSES JSON API; each survey year is published as a per-year ZIP of
 * institution microdata at {@code .../higher_education_r_and_d_{year}.zip}, containing
 * {@code herd{year}.csv}. The CSV is a tall EAV file (~264k rows/year for 2024): each row
 * is one cell of one questionnaire item, self-described by human-readable {@code question},
 * {@code row}, and {@code column} text fields (verified against herd2024.csv on 2026-07-16).
 * This class implements {@link StreamingResponseTransformer}: {@code HttpSource} calls
 * {@link #fetchAndTransform} directly, which opens the ZIP, positions on the CSV entry, and
 * streams one CSV record at a time via {@link CsvRecordReader}, keeping heap O(1) per row.
 *
 * <p>Only the two R&amp;D-expenditure question families are projected into the clean tall
 * table (all other questionnaire items are ignored):
 * <ul>
 *   <li><b>"Federal expenditures by field and agency"</b> — {@code row}=S&amp;E field,
 *       {@code column}=federal agency ⇒ {@code funding_source="Federal"},
 *       {@code federal_agency}=column.</li>
 *   <li><b>"Nonfederal expenditures by field and source"</b> — {@code row}=S&amp;E field,
 *       {@code column}=nonfederal source ⇒ {@code funding_source}=column,
 *       {@code federal_agency}=null.</li>
 * </ul>
 * Dollar values are in <b>thousands</b> (HERD PUF convention). {@code state_fips} is derived
 * from the USPS {@code inst_state_code}; an unmapped non-blank postal code fails loudly
 * (project rule #6 — never default to a placeholder FIPS). {@code ipeds_unitid} is passed
 * through as published (null when NCSES itself did not match the institution) — a blank id is
 * a legitimate null, never a fabricated one. {@code county_fips} and {@code control} are not
 * present in this microdata and are emitted null.
 */
public class NsfHerdTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NsfHerdTransformer.class);

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;

  private static final String Q_FEDERAL = "Federal expenditures by field and agency";
  private static final String Q_NONFEDERAL = "Nonfederal expenditures by field and source";

  /** USPS 2-letter code -> 2-digit state/territory FIPS (50 states + DC + territories). */
  private static final Map<String, String> STATE_FIPS;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("AL", "01"); m.put("AK", "02"); m.put("AZ", "04"); m.put("AR", "05");
    m.put("CA", "06"); m.put("CO", "08"); m.put("CT", "09"); m.put("DE", "10");
    m.put("DC", "11"); m.put("FL", "12"); m.put("GA", "13"); m.put("HI", "15");
    m.put("ID", "16"); m.put("IL", "17"); m.put("IN", "18"); m.put("IA", "19");
    m.put("KS", "20"); m.put("KY", "21"); m.put("LA", "22"); m.put("ME", "23");
    m.put("MD", "24"); m.put("MA", "25"); m.put("MI", "26"); m.put("MN", "27");
    m.put("MS", "28"); m.put("MO", "29"); m.put("MT", "30"); m.put("NE", "31");
    m.put("NV", "32"); m.put("NH", "33"); m.put("NJ", "34"); m.put("NM", "35");
    m.put("NY", "36"); m.put("NC", "37"); m.put("ND", "38"); m.put("OH", "39");
    m.put("OK", "40"); m.put("OR", "41"); m.put("PA", "42"); m.put("RI", "44");
    m.put("SC", "45"); m.put("SD", "46"); m.put("TN", "47"); m.put("TX", "48");
    m.put("UT", "49"); m.put("VT", "50"); m.put("VA", "51"); m.put("WA", "53");
    m.put("WV", "54"); m.put("WI", "55"); m.put("WY", "56"); m.put("PR", "72");
    m.put("VI", "78"); m.put("GU", "66"); m.put("AS", "60"); m.put("MP", "69");
    STATE_FIPS = Collections.unmodifiableMap(m);
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("NsfHerdTransformer: no URL in context");
    }
    LOGGER.info("herd: streaming {}", url);

    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status == HttpURLConnection.HTTP_NOT_FOUND) {
      // No microdata ZIP for this year yet — honest absence, not a fabricated row.
      conn.disconnect();
      LOGGER.warn("herd: HTTP 404 (no microdata) for {}", url);
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    if (status != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("HTTP " + status + " from " + url);
    }

    InputStream raw = conn.getInputStream();
    ZipInputStream zis = new ZipInputStream(raw);
    ZipEntry entry = zis.getNextEntry();
    while (entry != null && !entry.getName().toLowerCase().endsWith(".csv")) {
      entry = zis.getNextEntry();
    }
    if (entry == null) {
      zis.close();
      conn.disconnect();
      throw new IOException("herd: no .csv entry in ZIP " + url);
    }

    BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
    String header = CsvRecordReader.readRecord(reader);
    if (header == null) {
      reader.close();
      conn.disconnect();
      throw new IOException("herd: empty CSV in " + url);
    }
    Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(header, ','));
    return new HerdRowIterator(reader, conn, idx, url);
  }

  private static Map<String, Integer> headerIndex(List<String> header) {
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < header.size(); i++) {
      idx.put(header.get(i).trim(), i);
    }
    return idx;
  }

  /** Lazy row iterator over the ZIP'd CSV; closes the stream on exhaustion. */
  private static final class HerdRowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final HttpURLConnection conn;
    private final Map<String, Integer> idx;
    private final String url;
    private Map<String, Object> mapped;
    private boolean done;

    HerdRowIterator(BufferedReader reader, HttpURLConnection conn, Map<String, Integer> idx,
        String url) {
      this.reader = reader;
      this.conn = conn;
      this.idx = idx;
      this.url = url;
      advance();
    }

    private void advance() {
      try {
        String record = CsvRecordReader.readRecord(reader);
        while (record != null) {
          List<String> cols = CsvRecordReader.splitFields(record, ',');
          Map<String, Object> row = mapRow(cols);
          if (row != null) {
            mapped = row;
            return;
          }
          record = CsvRecordReader.readRecord(reader);
        }
        mapped = null;
        close();
      } catch (IOException e) {
        close();
        throw new RuntimeException("herd: read failed for " + url, e);
      }
    }

    private Map<String, Object> mapRow(List<String> cols) {
      String question = pick(cols, "question");
      if (question == null) {
        return null;
      }
      boolean federal = Q_FEDERAL.equals(question);
      boolean nonfederal = Q_NONFEDERAL.equals(question);
      if (!federal && !nonfederal) {
        return null; // only the two R&D-expenditure question families are projected
      }

      Integer year = intVal(pick(cols, "year"));
      if (year == null) {
        return null; // no year → cannot partition
      }
      Double amount = dbl(pick(cols, "data"));
      if (amount == null) {
        return null; // suppressed / blank cell → honest absence, not a zero
      }

      String field = pick(cols, "row");
      String colLabel = pick(cols, "column");

      Map<String, Object> out = new LinkedHashMap<String, Object>();
      out.put("year", year);
      out.put("institution", pick(cols, "inst_name_long"));
      // IPEDS UnitID is always numeric; blank means NCSES did not match the institution
      // (legitimate null, never fabricated). Emitted as INTEGER to join edu.ipeds_institutions.unitid.
      out.put("ipeds_unitid", intVal(pick(cols, "ipeds_unitid")));
      out.put("state_fips", stateFips(pick(cols, "inst_state_code")));
      out.put("county_fips", null);
      out.put("control", null);
      out.put("rd_field", field);
      if (federal) {
        out.put("funding_source", "Federal");
        out.put("federal_agency", colLabel);
      } else {
        out.put("funding_source", colLabel);
        out.put("federal_agency", null);
      }
      out.put("rd_expenditure_usd_thousand", amount);
      return out;
    }

    private String stateFips(String usps) {
      if (usps == null) {
        return null; // missing state — legitimately null, not an error
      }
      String code = usps.trim().toUpperCase();
      if (code.isEmpty()) {
        return null;
      }
      String fips = STATE_FIPS.get(code);
      if (fips == null) {
        // A real, non-blank postal code we do not recognize is a data-quality signal —
        // fail loudly rather than default to a placeholder FIPS (project rule #6).
        throw new IllegalStateException("herd: unmapped institution state code '" + code
            + "' in " + url);
      }
      return fips;
    }

    private String pick(List<String> cols, String name) {
      Integer i = idx.get(name);
      if (i != null && i < cols.size()) {
        String v = cols.get(i).trim();
        if (!v.isEmpty()) {
          return v;
        }
      }
      return null;
    }

    private static Integer intVal(String v) {
      if (v == null) {
        return null;
      }
      try {
        return Integer.parseInt(v.trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private static Double dbl(String v) {
      if (v == null) {
        return null;
      }
      try {
        return Double.parseDouble(v.replace(",", ""));
      } catch (NumberFormatException e) {
        return null;
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
      return mapped != null;
    }

    @Override public Map<String, Object> next() {
      if (mapped == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = mapped;
      advance();
      return row;
    }
  }
}
