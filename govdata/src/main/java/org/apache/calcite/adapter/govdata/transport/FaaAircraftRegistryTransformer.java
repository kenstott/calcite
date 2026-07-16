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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Streams the FAA Releasable Aircraft Registry into {@code faa_aircraft_registry} rows.
 *
 * <p>The download ({@code ReleasableAircraft.zip}, ~73 MB, verified 2026-07-16) is a ZIP of
 * fixed-width, comma-delimited, header-bearing text files. Two are used, joined on the 7-char
 * Aircraft Mfr/Model/Series code:
 * <ul>
 *   <li>{@code ACFTREF.txt} ({@code CODE}) — manufacturer, model, aircraft type, engine type,
 *       category. Read fully into a map (~90K models, ~15 MB) during setup.</li>
 *   <li>{@code MASTER.txt} ({@code MFR MDL CODE}) — the ~300K registered aircraft. Streamed
 *       row-by-row so peak heap stays O(chunk) regardless of the 193 MB uncompressed size.</li>
 * </ul>
 *
 * <p>This is a snapshot table (partition {@code type} only); {@code snapshot_month} is the
 * first-of-month at ingest. {@code STATE} is a USPS abbreviation and {@code COUNTY} a 3-digit
 * FIPS county code, so {@code county_fips = state_fips + COUNTY} for US ({@code COUNTRY=US})
 * records; non-US registrants leave both null. Coded fields (registrant type, aircraft type,
 * engine type, category, status) are decoded from the FAA {@code ardata.pdf} code tables.
 */
public class FaaAircraftRegistryTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FaaAircraftRegistryTransformer.class);

  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GovData/1.0";
  private static final String MASTER_ENTRY = "MASTER.txt";
  private static final String ACFTREF_ENTRY = "ACFTREF.txt";

  private static final Map<String, String> STATE_FIPS = buildStateFips();
  private static final Map<String, String> REGISTRANT_TYPE = buildRegistrantType();
  private static final Map<String, String> AIRCRAFT_TYPE = buildAircraftType();
  private static final Map<String, String> ENGINE_TYPE = buildEngineType();
  private static final Map<String, String> AC_CATEGORY = buildAcCategory();
  private static final Map<String, String> STATUS_CODE = buildStatusCode();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    File zipFile = downloadToTemp(url);
    final ZipFile zip = new ZipFile(zipFile);
    boolean ok = false;
    try {
      Map<String, String[]> acftRef = loadAcftRef(zip);
      LOGGER.info("faa_aircraft_registry: loaded {} aircraft-reference models", acftRef.size());
      Iterator<Map<String, Object>> it = masterIterator(zip, zipFile, acftRef);
      ok = true;
      return it;
    } finally {
      if (!ok) {
        closeQuietly(zip);
        deleteQuietly(zipFile);
      }
    }
  }

  /** Reads ACFTREF.txt into {@code CODE -> [mfr, model, aircraftType, engineType, category]}. */
  private Map<String, String[]> loadAcftRef(ZipFile zip) throws IOException {
    ZipEntry entry = zip.getEntry(ACFTREF_ENTRY);
    if (entry == null) {
      throw new IOException("faa_aircraft_registry: " + ACFTREF_ENTRY + " missing from zip");
    }
    Map<String, String[]> map = new HashMap<String, String[]>(131072);
    InputStream in = zip.getInputStream(entry);
    try {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      Map<String, Integer> h = readHeader(reader);
      int cCode = col(h, "CODE");
      int cMfr = col(h, "MFR");
      int cModel = col(h, "MODEL");
      int cType = col(h, "TYPE-ACFT");
      int cEng = col(h, "TYPE-ENG");
      int cCat = col(h, "AC-CAT");
      String record;
      while ((record = CsvRecordReader.readRecord(reader)) != null) {
        List<String> f = CsvRecordReader.splitFields(record, ',');
        String code = get(f, cCode);
        if (code == null) {
          continue;
        }
        map.put(code, new String[] {
            get(f, cMfr),
            get(f, cModel),
            decode(AIRCRAFT_TYPE, get(f, cType)),
            decode(ENGINE_TYPE, get(f, cEng)),
            decode(AC_CATEGORY, get(f, cCat))
        });
      }
    } finally {
      in.close();
    }
    return map;
  }

  /** Lazy iterator over MASTER.txt rows, joining ACFTREF and closing the zip when exhausted. */
  private Iterator<Map<String, Object>> masterIterator(final ZipFile zip, final File zipFile,
      final Map<String, String[]> acftRef) throws IOException {
    ZipEntry entry = zip.getEntry(MASTER_ENTRY);
    if (entry == null) {
      closeQuietly(zip);
      deleteQuietly(zipFile);
      throw new IOException("faa_aircraft_registry: " + MASTER_ENTRY + " missing from zip");
    }
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(zip.getInputStream(entry), StandardCharsets.UTF_8));
    final Map<String, Integer> h = readHeader(reader);
    final String snapshotMonth =
        LocalDate.now().withDayOfMonth(1).format(DateTimeFormatter.ISO_LOCAL_DATE);

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> next = advance();

      private Map<String, Object> advance() {
        try {
          String record;
          while ((record = CsvRecordReader.readRecord(reader)) != null) {
            Map<String, Object> row = toRow(CsvRecordReader.splitFields(record, ','),
                h, acftRef, snapshotMonth);
            if (row != null) {
              return row;
            }
          }
        } catch (IOException e) {
          closeQuietly(zip);
          deleteQuietly(zipFile);
          throw new RuntimeException("faa_aircraft_registry: read failed", e);
        }
        closeQuietly(reader);
        closeQuietly(zip);
        deleteQuietly(zipFile);
        return null;
      }

      @Override public boolean hasNext() {
        return next != null;
      }

      @Override public Map<String, Object> next() {
        if (next == null) {
          throw new NoSuchElementException();
        }
        Map<String, Object> current = next;
        next = advance();
        return current;
      }

      @Override public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private Map<String, Object> toRow(List<String> f, Map<String, Integer> h,
      Map<String, String[]> acftRef, String snapshotMonth) {
    String nNumber = get(f, col(h, "N-NUMBER"));
    if (nNumber == null) {
      return null;
    }
    String country = get(f, col(h, "COUNTRY"));
    String stateAbbr = get(f, col(h, "STATE"));
    String stateFips = stateAbbr == null ? null : STATE_FIPS.get(stateAbbr);
    String county = get(f, col(h, "COUNTY"));
    String countyFips = null;
    if (stateFips != null && county != null && "US".equals(country)) {
      String c = county.length() > 3 ? county.substring(county.length() - 3) : county;
      c = pad3(c);
      if (c != null) {
        countyFips = stateFips + c;
      }
    }

    String mfrMdlCode = get(f, col(h, "MFR MDL CODE"));
    String[] ref = mfrMdlCode == null ? null : acftRef.get(mfrMdlCode);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("n_number", "N" + nNumber);
    row.put("snapshot_month", snapshotMonth);
    row.put("serial_number", get(f, col(h, "SERIAL NUMBER")));
    row.put("mfr_mdl_code", mfrMdlCode);
    row.put("manufacturer", ref == null ? null : ref[0]);
    row.put("model", ref == null ? null : ref[1]);
    row.put("aircraft_category", ref == null ? null : ref[4]);
    // Prefer ACFTREF's type/engine; fall back to MASTER's denormalized codes.
    row.put("aircraft_type", ref != null && ref[2] != null
        ? ref[2] : decode(AIRCRAFT_TYPE, get(f, col(h, "TYPE AIRCRAFT"))));
    row.put("engine_type", ref != null && ref[3] != null
        ? ref[3] : decode(ENGINE_TYPE, get(f, col(h, "TYPE ENGINE"))));
    row.put("year_manufactured", toYear(get(f, col(h, "YEAR MFR"))));
    row.put("registrant_name", get(f, col(h, "NAME")));
    row.put("registrant_type", decode(REGISTRANT_TYPE, get(f, col(h, "TYPE REGISTRANT"))));
    row.put("state_abbr", stateAbbr);
    row.put("state_fips", stateFips);
    row.put("county_fips", countyFips);
    row.put("status", decode(STATUS_CODE, get(f, col(h, "STATUS CODE"))));
    row.put("cert_issue_date", toIsoDate(get(f, col(h, "CERT ISSUE DATE"))));
    row.put("airworthiness_date", toIsoDate(get(f, col(h, "AIR WORTH DATE"))));
    return row;
  }

  // --- helpers --------------------------------------------------------------

  private static Map<String, Integer> readHeader(BufferedReader reader) throws IOException {
    String header = CsvRecordReader.readRecord(reader);
    if (header == null) {
      throw new IOException("faa_aircraft_registry: empty file (no header)");
    }
    List<String> names = CsvRecordReader.splitFields(header, ',');
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (int i = 0; i < names.size(); i++) {
      String n = names.get(i);
      if (n == null) {
        continue;
      }
      // Strip a leading UTF-8 BOM on the first field.
      n = n.replace("﻿", "").trim().toUpperCase();
      if (!n.isEmpty() && !map.containsKey(n)) {
        map.put(n, i);
      }
    }
    return map;
  }

  private static int col(Map<String, Integer> header, String name) {
    Integer i = header.get(name);
    if (i == null) {
      throw new IllegalStateException(
          "faa_aircraft_registry: expected column '" + name + "' not present in " + header.keySet());
    }
    return i;
  }

  private static String get(List<String> fields, int idx) {
    if (idx < 0 || idx >= fields.size()) {
      return null;
    }
    String v = fields.get(idx);
    if (v == null) {
      return null;
    }
    v = v.trim();
    return v.isEmpty() ? null : v;
  }

  private static String decode(Map<String, String> table, String code) {
    if (code == null) {
      return null;
    }
    String label = table.get(code);
    // Unknown codes pass through raw (honest: no fabricated label).
    return label != null ? label : code;
  }

  private static String pad3(String c) {
    if (c == null) {
      return null;
    }
    for (int i = 0; i < c.length(); i++) {
      if (!Character.isDigit(c.charAt(i))) {
        return null;
      }
    }
    while (c.length() < 3) {
      c = "0" + c;
    }
    return c;
  }

  private static Integer toYear(String s) {
    if (s == null) {
      return null;
    }
    try {
      int y = Integer.parseInt(s.trim());
      return y == 0 ? null : Integer.valueOf(y);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** FAA dates are {@code YYYYMMDD}; emit an ISO {@code YYYY-MM-DD} literal or null. */
  private static String toIsoDate(String s) {
    if (s == null || s.length() != 8) {
      return null;
    }
    for (int i = 0; i < 8; i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return null;
      }
    }
    return s.substring(0, 4) + "-" + s.substring(4, 6) + "-" + s.substring(6, 8);
  }

  private static File downloadToTemp(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("faa_aircraft_registry: HTTP " + status + " from " + url);
    }
    File tmp = Files.createTempFile("faa-registry-", ".zip").toFile();
    InputStream in = conn.getInputStream();
    try {
      OutputStream out = Files.newOutputStream(tmp.toPath());
      try {
        byte[] buf = new byte[65536];
        int n;
        while ((n = in.read(buf)) > 0) {
          out.write(buf, 0, n);
        }
      } finally {
        out.close();
      }
    } finally {
      in.close();
    }
    LOGGER.info("faa_aircraft_registry: downloaded {} ({} bytes)", url, tmp.length());
    return tmp;
  }

  private static void closeQuietly(java.io.Closeable c) {
    if (c != null) {
      try {
        c.close();
      } catch (IOException ignore) {
        // best-effort close
      }
    }
  }

  private static void deleteQuietly(File f) {
    if (f != null && f.exists() && !f.delete()) {
      f.deleteOnExit();
    }
  }

  // --- code tables (FAA ardata.pdf, verified 2026-07-16) --------------------

  private static Map<String, String> buildRegistrantType() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("1", "Individual");
    m.put("2", "Partnership");
    m.put("3", "Corporation");
    m.put("4", "Co-Owned");
    m.put("5", "Government");
    m.put("7", "LLC");
    m.put("8", "Non-Citizen Corporation");
    m.put("9", "Non-Citizen Co-Owned");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildAircraftType() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("1", "Glider");
    m.put("2", "Balloon");
    m.put("3", "Blimp/Dirigible");
    m.put("4", "Fixed wing single engine");
    m.put("5", "Fixed wing multi engine");
    m.put("6", "Rotorcraft");
    m.put("7", "Weight-shift-control");
    m.put("8", "Powered Parachute");
    m.put("9", "Gyroplane");
    m.put("H", "Hybrid Lift");
    m.put("O", "Other");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildEngineType() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("0", "None");
    m.put("1", "Reciprocating");
    m.put("2", "Turbo-prop");
    m.put("3", "Turbo-shaft");
    m.put("4", "Turbo-jet");
    m.put("5", "Turbo-fan");
    m.put("6", "Ramjet");
    m.put("7", "2 Cycle");
    m.put("8", "4 Cycle");
    m.put("9", "Unknown");
    m.put("10", "Electric");
    m.put("11", "Rotary");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildAcCategory() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("1", "Land");
    m.put("2", "Sea");
    m.put("3", "Amphibian");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildStatusCode() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("V", "Valid Registration");
    m.put("A", "Triennial form mailed, not returned");
    m.put("D", "Expired Dealer");
    m.put("E", "Revoked by enforcement");
    m.put("M", "Assigned to manufacturer under dealer certificate");
    m.put("N", "Non-citizen corporation without flight-hour report");
    m.put("R", "Registration pending");
    m.put("T", "Valid Registration (from trainee)");
    m.put("W", "Certificate ineffective/invalid");
    m.put("X", "Enforcement Letter");
    m.put("Z", "Permanent Reserved");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildStateFips() {
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
    m.put("WV", "54"); m.put("WI", "55"); m.put("WY", "56");
    m.put("AS", "60"); m.put("GU", "66"); m.put("MP", "69"); m.put("PR", "72");
    m.put("VI", "78");
    return Collections.unmodifiableMap(m);
  }
}
