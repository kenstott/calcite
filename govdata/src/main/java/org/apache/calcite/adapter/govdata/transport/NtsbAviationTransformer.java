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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streams the NTSB aviation accident database into {@code ntsb_aviation_accidents} rows.
 *
 * <p>The download ({@code avall.zip}, ~95 MB) contains a single Microsoft Access database
 * ({@code avall.mdb}, ~553 MB, all events 1982-present, verified 2026-07-16). It is read in-JVM
 * with Jackcess (no native driver). One output row per aircraft involved in an event: the
 * {@code aircraft} table (grain {@code ev_id} + {@code Aircraft_Key}) is streamed and joined to
 * pre-loaded maps of {@code events} (grain {@code ev_id}) and the {@code narratives.narr_cause}
 * probable-cause text (grain {@code ev_id} + {@code Aircraft_Key}).
 *
 * <p>The MDB has no county column (only {@code ev_city} / {@code ev_state}), so only
 * {@code state_fips} is derived (from the USPS {@code ev_state}); coded fields
 * ({@code ev_type}, {@code damage}, {@code ev_highest_injury}) are decoded, and
 * {@code phase_flt_spec} is emitted as its raw NTSB phase code (the code dictionary is not
 * fabricated). Snapshot table (partition {@code type} only): the whole MDB is re-ingested and
 * the partition overwritten each run.
 */
public class NtsbAviationTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NtsbAviationTransformer.class);

  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GovData/1.0";
  private static final int CAUSE_MAX = 4000;

  private static final Map<String, String> STATE_FIPS = buildStateFips();
  private static final Map<String, String> EVENT_TYPE = buildEventType();
  private static final Map<String, String> DAMAGE = buildDamage();
  private static final Map<String, String> INJURY = buildInjury();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    File zip = downloadToTemp(context.getUrl());
    File mdb = null;
    Database db = null;
    boolean ok = false;
    try {
      mdb = unzipMdb(zip);
      db = new DatabaseBuilder().setFile(mdb).setReadOnly(true).open();
      Map<String, EventInfo> events = loadEvents(db);
      Map<String, String> causes = loadCauses(db);
      LOGGER.info("ntsb_aviation_accidents: loaded {} events, {} probable-cause narratives",
          events.size(), causes.size());
      Iterator<Map<String, Object>> it = aircraftIterator(db, zip, mdb, events, causes);
      ok = true;
      return it;
    } finally {
      if (!ok) {
        closeQuietly(db);
        deleteQuietly(mdb);
        deleteQuietly(zip);
      }
    }
  }

  private Map<String, EventInfo> loadEvents(Database db) throws IOException {
    Table t = db.getTable("events");
    Map<String, EventInfo> map = new HashMap<String, EventInfo>(262144);
    for (Row row : t) {
      String evId = row.getString("ev_id");
      if (evId == null) {
        continue;
      }
      EventInfo e = new EventInfo();
      e.eventDate = isoDate(row.getLocalDateTime("ev_date"));
      e.eventYear = toInt(row.get("ev_year"));
      e.eventType = decode(EVENT_TYPE, row.getString("ev_type"));
      e.city = row.getString("ev_city");
      e.state = row.getString("ev_state");
      e.country = row.getString("ev_country");
      e.airportId = row.getString("ev_nr_apt_id");
      e.airportName = row.getString("apt_name");
      e.injurySeverity = decode(INJURY, row.getString("ev_highest_injury"));
      e.fatalInjuries = toInt(row.get("inj_tot_f"));
      e.seriousInjuries = toInt(row.get("inj_tot_s"));
      e.minorInjuries = toInt(row.get("inj_tot_m"));
      e.latitude = toDouble(row.get("dec_latitude"));
      e.longitude = toDouble(row.get("dec_longitude"));
      map.put(evId, e);
    }
    return map;
  }

  private Map<String, String> loadCauses(Database db) throws IOException {
    Table t = db.getTable("narratives");
    Map<String, String> map = new HashMap<String, String>(262144);
    for (Row row : t) {
      String cause = row.getString("narr_cause");
      if (cause == null || cause.isEmpty()) {
        continue;
      }
      if (cause.length() > CAUSE_MAX) {
        cause = cause.substring(0, CAUSE_MAX);
      }
      map.put(narrKey(row.getString("ev_id"), toInt(row.get("Aircraft_Key"))), cause);
    }
    return map;
  }

  private Iterator<Map<String, Object>> aircraftIterator(final Database db, final File zip,
      final File mdb, final Map<String, EventInfo> events, final Map<String, String> causes)
      throws IOException {
    Table aircraft = db.getTable("aircraft");
    final Iterator<Row> rows = aircraft.iterator();

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> next = advance();

      private Map<String, Object> advance() {
        while (rows.hasNext()) {
          Map<String, Object> row = toRow(rows.next(), events, causes);
          if (row != null) {
            return row;
          }
        }
        closeQuietly(db);
        deleteQuietly(mdb);
        deleteQuietly(zip);
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

  private Map<String, Object> toRow(Row a, Map<String, EventInfo> events,
      Map<String, String> causes) {
    String evId = a.getString("ev_id");
    if (evId == null) {
      return null;
    }
    Integer acftKey = toInt(a.get("Aircraft_Key"));
    EventInfo e = events.get(evId);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("event_id", evId);
    row.put("aircraft_key", acftKey);
    row.put("event_year", e == null ? null : e.eventYear);
    row.put("event_date", e == null ? null : e.eventDate);
    row.put("event_type", e == null ? null : e.eventType);
    row.put("aircraft_n_number", canonicalN(a.getString("regis_no")));
    row.put("aircraft_make", a.getString("acft_make"));
    row.put("aircraft_model", a.getString("acft_model"));
    row.put("aircraft_category", a.getString("acft_category"));
    row.put("far_part", a.getString("far_part"));
    row.put("carrier_name", a.getString("oper_name"));
    row.put("city", e == null ? null : e.city);
    String state = e == null ? null : e.state;
    row.put("state_abbr", state);
    row.put("state_fips", state == null ? null : STATE_FIPS.get(state));
    row.put("country", e == null ? null : e.country);
    row.put("airport_id", e == null ? null : e.airportId);
    row.put("airport_name", e == null ? null : e.airportName);
    row.put("injury_severity", e == null ? null : e.injurySeverity);
    row.put("fatal_injuries", e == null ? null : e.fatalInjuries);
    row.put("serious_injuries", e == null ? null : e.seriousInjuries);
    row.put("minor_injuries", e == null ? null : e.minorInjuries);
    row.put("aircraft_damage", decode(DAMAGE, a.getString("damage")));
    row.put("phase_flt_code", toInt(a.get("phase_flt_spec")));
    row.put("latitude", e == null ? null : e.latitude);
    row.put("longitude", e == null ? null : e.longitude);
    row.put("probable_cause", causes.get(narrKey(evId, acftKey)));
    return row;
  }

  // --- helpers --------------------------------------------------------------

  private static String narrKey(String evId, Integer aircraftKey) {
    return evId + "|" + (aircraftKey == null ? "" : aircraftKey.toString());
  }

  /**
   * Canonical N-prefixed tail number so it matches {@code faa_aircraft_master.n_number}
   * regardless of whether NTSB stores {@code regis_no} with or without the leading N.
   * FAA registration suffixes never begin with N, so stripping one leading N is safe.
   */
  private static String canonicalN(String regis) {
    if (regis == null) {
      return null;
    }
    String s = regis.trim().toUpperCase();
    if (s.isEmpty()) {
      return null;
    }
    if (s.charAt(0) == 'N') {
      s = s.substring(1);
    }
    return s.isEmpty() ? null : "N" + s;
  }

  private static String decode(Map<String, String> table, String code) {
    if (code == null) {
      return null;
    }
    String c = code.trim();
    if (c.isEmpty()) {
      return null;
    }
    String label = table.get(c);
    // Unknown codes pass through raw (honest: no fabricated label).
    return label != null ? label : c;
  }

  private static String isoDate(LocalDateTime dt) {
    return dt == null ? null : dt.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
  }

  private static Integer toInt(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Number) {
      return ((Number) v).intValue();
    }
    try {
      return Integer.valueOf(v.toString().trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Double toDouble(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Number) {
      return ((Number) v).doubleValue();
    }
    try {
      return Double.valueOf(v.toString().trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private File unzipMdb(File zip) throws IOException {
    InputStream fis = Files.newInputStream(zip.toPath());
    try {
      ZipInputStream zis = new ZipInputStream(fis);
      try {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          if (entry.getName().toLowerCase().endsWith(".mdb")) {
            File mdb = Files.createTempFile("ntsb-avall-", ".mdb").toFile();
            OutputStream out = Files.newOutputStream(mdb.toPath());
            try {
              byte[] buf = new byte[1 << 20];
              int n;
              while ((n = zis.read(buf)) > 0) {
                out.write(buf, 0, n);
              }
            } finally {
              out.close();
            }
            LOGGER.info("ntsb_aviation_accidents: extracted {} ({} bytes)",
                entry.getName(), mdb.length());
            return mdb;
          }
        }
      } finally {
        zis.close();
      }
    } finally {
      fis.close();
    }
    throw new IOException("ntsb_aviation_accidents: no .mdb entry in " + zip);
  }

  private static File downloadToTemp(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(600000);
    conn.setInstanceFollowRedirects(true);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("ntsb_aviation_accidents: HTTP " + status + " from " + url);
    }
    File tmp = Files.createTempFile("ntsb-avall-", ".zip").toFile();
    InputStream in = conn.getInputStream();
    try {
      OutputStream out = Files.newOutputStream(tmp.toPath());
      try {
        byte[] buf = new byte[1 << 20];
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
    LOGGER.info("ntsb_aviation_accidents: downloaded {} ({} bytes)", url, tmp.length());
    return tmp;
  }

  private static void closeQuietly(Database db) {
    if (db != null) {
      try {
        db.close();
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

  /** Event-level fields joined onto each aircraft row. */
  private static final class EventInfo {
    String eventDate;
    Integer eventYear;
    String eventType;
    String city;
    String state;
    String country;
    String airportId;
    String airportName;
    String injurySeverity;
    Integer fatalInjuries;
    Integer seriousInjuries;
    Integer minorInjuries;
    Double latitude;
    Double longitude;
  }

  // --- code tables (NTSB eADMSPUB dictionary, verified 2026-07-16) -----------

  private static Map<String, String> buildEventType() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("ACC", "Accident");
    m.put("INC", "Incident");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildDamage() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("DEST", "Destroyed");
    m.put("SUBS", "Substantial");
    m.put("MINR", "Minor");
    m.put("NONE", "None");
    m.put("UNK", "Unknown");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildInjury() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("FATL", "Fatal");
    m.put("SERS", "Serious");
    m.put("MINR", "Minor");
    m.put("NONE", "None");
    m.put("UNKN", "Unknown");
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
