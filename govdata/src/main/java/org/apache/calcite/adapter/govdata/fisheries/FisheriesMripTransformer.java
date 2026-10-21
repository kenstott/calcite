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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streams NOAA Fisheries MRIP (Marine Recreational Information Program) wave-level estimate
 * files into rows. One transformer serves both {@code mrip_recreational_catch} and
 * {@code mrip_recreational_effort}; the file kind comes from the request URL
 * ({@code mrip_catch_bywave_<year>.csv} or {@code mrip_effort_bywave_<year>.csv}).
 *
 * <p>NOAA publishes 2020 onward as one plain CSV per year, but 1981-2019 only inside decade
 * bundles ({@code mrip_catch_bywave_1981_1989_csv.zip}, {@code ..._1990_1999_csv.zip}, ...) that
 * hold one CSV per year. The schema URL always names the per-year file; for a year before 2020
 * this class resolves it to the bundle that contains it and reads that entry, so a single table
 * definition covers the whole history.
 *
 * <p>Numbers arrive as quoted, comma-grouped strings ({@code "1,551,848"}); blanks mean the
 * estimate is not available (null), they are never zero-filled. Standard error is derived from
 * the published variance as {@code sqrt(var)}. {@code ST} is a state FIPS code and is mapped to
 * the USPS abbreviation; an unmapped code is an error rather than a null.
 */
public class FisheriesMripTransformer implements StreamingResponseTransformer {

  private static final Pattern FILE =
      Pattern.compile("^(.*/)mrip_(catch|effort)_bywave_(\\d{4})\\.csv$");
  private static final int FIRST_PLAIN_YEAR = 2020;

  /** Catch measures: output name, estimate column, variance column, PSE column. */
  private static final String[][] CATCH_MEASURES = {
      {"claimed_harvest_fish", "ESTCLAIM", "ESTCLVAR", "PSE_A"},
      {"reported_harvest_fish", "ESTHARV", "ESTHVAR", "PSE_B1"},
      {"landings_fish", "LANDING", "LAND_VAR", "PSE_L"},
      {"released_fish", "ESTREL", "ESTRLVAR", "PSE_B2"},
      {"total_catch_fish", "TOT_CAT", "TOT_VAR", "PSE_TC"},
      {"landings_pounds", "LBS_AB1", "VAR_LBS", "PSE_WL"},
      {"landings_kg", "WGT_AB1", "VAR_WAB1", "PSE_WK"},
  };

  private static final Map<String, String[]> STATES = new HashMap<String, String[]>();

  static {
    String[][] s = {
        {"01", "AL"}, {"02", "AK"}, {"04", "AZ"}, {"05", "AR"}, {"06", "CA"}, {"08", "CO"},
        {"09", "CT"}, {"10", "DE"}, {"11", "DC"}, {"12", "FL"}, {"13", "GA"}, {"15", "HI"},
        {"16", "ID"}, {"17", "IL"}, {"18", "IN"}, {"19", "IA"}, {"20", "KS"}, {"21", "KY"},
        {"22", "LA"}, {"23", "ME"}, {"24", "MD"}, {"25", "MA"}, {"26", "MI"}, {"27", "MN"},
        {"28", "MS"}, {"29", "MO"}, {"30", "MT"}, {"31", "NE"}, {"32", "NV"}, {"33", "NH"},
        {"34", "NJ"}, {"35", "NM"}, {"36", "NY"}, {"37", "NC"}, {"38", "ND"}, {"39", "OH"},
        {"40", "OK"}, {"41", "OR"}, {"42", "PA"}, {"44", "RI"}, {"45", "SC"}, {"46", "SD"},
        {"47", "TN"}, {"48", "TX"}, {"49", "UT"}, {"50", "VT"}, {"51", "VA"}, {"53", "WA"},
        {"54", "WV"}, {"55", "WI"}, {"56", "WY"}, {"72", "PR"}, {"78", "VI"},
    };
    for (String[] e : s) {
      STATES.put(e[0], e);
    }
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String url = context.getUrl();
    Matcher m = FILE.matcher(url);
    if (!m.matches()) {
      throw new IllegalStateException("MRIP: URL is not a mrip_(catch|effort)_bywave_<year>.csv "
          + "file: " + url);
    }
    final boolean catchFile = "catch".equals(m.group(2));
    int year = Integer.parseInt(m.group(3));
    InputStream in = year >= FIRST_PLAIN_YEAR
        ? open(url)
        : openBundleEntry(m.group(1), m.group(2), year);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    final String[] header = splitCsv(requireLine(reader, url), url);
    final Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < header.length; i++) {
      idx.put(header[i].trim(), i);
    }
    final String[] required = catchFile
        ? new String[] {"STATUS", "YEAR", "WAVE", "SUB_REG", "SUB_REG_F", "ST", "MODE_FX", "MODE_FX_F",
            "AREA_X", "AREA_X_F", "SP_CODE", "COMMON", "ALT_FLAG"}
        : new String[] {"STATUS", "YEAR", "WAVE", "SUB_REG", "SUB_REG_F", "ST", "MODE_FX", "MODE_FX_F",
            "AREA_X", "AREA_X_F", "ESTRIPS", "NUMVAR", "PSE_E"};
    for (String col : required) {
      if (!idx.containsKey(col)) {
        throw new IllegalStateException("MRIP: column " + col + " missing from header of " + url);
      }
    }
    if (catchFile) {
      for (String[] mm : CATCH_MEASURES) {
        for (int k = 1; k < mm.length; k++) {
          if (!idx.containsKey(mm[k])) {
            throw new IllegalStateException("MRIP: column " + mm[k] + " missing from header of "
                + url);
          }
        }
      }
    }
    final int expectedYear = year;

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending;
      private boolean closed;

      private void fill() {
        if (pending != null || closed) {
          return;
        }
        try {
          String line;
          while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
              continue;
            }
            String[] f = splitCsv(line, url);
            if (f.length != header.length) {
              throw new IllegalStateException("MRIP: row has " + f.length + " fields, header has "
                  + header.length + " in " + url + ": " + line);
            }
            pending = catchFile ? catchRow(f, idx, url, expectedYear)
                : effortRow(f, idx, url, expectedYear);
            return;
          }
          closed = true;
          reader.close();
        } catch (IOException e) {
          throw new IllegalStateException("MRIP: failed reading " + url, e);
        }
      }

      @Override public boolean hasNext() {
        fill();
        return pending != null;
      }

      @Override public Map<String, Object> next() {
        fill();
        if (pending == null) {
          throw new NoSuchElementException();
        }
        Map<String, Object> r = pending;
        pending = null;
        return r;
      }
    };
  }

  private static Map<String, Object> catchRow(String[] f, Map<String, Integer> idx, String url,
      int expectedYear) {
    Map<String, Object> row = common(f, idx, url, expectedYear);
    row.put("alt_flag", integer(f, idx, "ALT_FLAG", url));
    row.put("species_code", str(f, idx, "SP_CODE"));
    row.put("species_name", str(f, idx, "COMMON"));
    for (String[] mm : CATCH_MEASURES) {
      Double est = number(f, idx, mm[1], url);
      Double var = number(f, idx, mm[2], url);
      row.put(mm[0], est);
      row.put(mm[0] + "_std_err", var == null ? null : Double.valueOf(Math.sqrt(var.doubleValue())));
      row.put(mm[0] + "_pse", number(f, idx, mm[3], url));
    }
    return row;
  }

  private static Map<String, Object> effortRow(String[] f, Map<String, Integer> idx, String url,
      int expectedYear) {
    Map<String, Object> row = common(f, idx, url, expectedYear);
    Double var = number(f, idx, "NUMVAR", url);
    row.put("angler_trips", number(f, idx, "ESTRIPS", url));
    row.put("angler_trips_std_err",
        var == null ? null : Double.valueOf(Math.sqrt(var.doubleValue())));
    row.put("angler_trips_pse", number(f, idx, "PSE_E", url));
    return row;
  }

  /** Dimensions shared by the catch and effort files. */
  private static Map<String, Object> common(String[] f, Map<String, Integer> idx, String url,
      int expectedYear) {
    Integer year = integer(f, idx, "YEAR", url);
    if (year == null || year.intValue() != expectedYear) {
      throw new IllegalStateException("MRIP: row YEAR " + year + " differs from requested "
          + expectedYear + " in " + url);
    }
    String fips = str(f, idx, "ST");
    if (fips == null) {
      throw new IllegalStateException("MRIP: blank ST in " + url);
    }
    if (fips.length() < 2) {
      fips = "0" + fips;
    }
    String[] state = STATES.get(fips);
    if (state == null) {
      throw new IllegalStateException("MRIP: state FIPS " + fips + " has no mapping in "
          + "FisheriesMripTransformer (" + url + ")");
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("catch_year", year);
    row.put("wave", integer(f, idx, "WAVE", url));
    row.put("status", str(f, idx, "STATUS"));
    row.put("sub_region_code", str(f, idx, "SUB_REG"));
    row.put("sub_region_name", str(f, idx, "SUB_REG_F"));
    row.put("state_fips", fips);
    row.put("state_abbr", state[1]);
    row.put("state_name", str(f, idx, "ST_F"));
    row.put("mode_code", str(f, idx, "MODE_FX"));
    row.put("mode_name", str(f, idx, "MODE_FX_F"));
    row.put("area_code", str(f, idx, "AREA_X"));
    row.put("area_name", str(f, idx, "AREA_X_F"));
    return row;
  }

  private static String str(String[] f, Map<String, Integer> idx, String col) {
    String v = f[idx.get(col).intValue()].trim();
    return v.isEmpty() ? null : v;
  }

  private static Integer integer(String[] f, Map<String, Integer> idx, String col, String url) {
    String v = str(f, idx, col);
    if (v == null) {
      return null;
    }
    try {
      return Integer.valueOf(v.replace(",", ""));
    } catch (NumberFormatException e) {
      throw new IllegalStateException("MRIP: " + col + " '" + v + "' is not an integer in " + url, e);
    }
  }

  private static Double number(String[] f, Map<String, Integer> idx, String col, String url) {
    String v = str(f, idx, col);
    if (v == null) {
      return null;
    }
    try {
      return Double.valueOf(v.replace(",", ""));
    } catch (NumberFormatException e) {
      throw new IllegalStateException("MRIP: " + col + " '" + v + "' is not a number in " + url, e);
    }
  }

  /** Splits one CSV record honoring double-quoted fields (which carry the comma-grouped numbers). */
  static String[] splitCsv(String line, String url) {
    List<String> out = new ArrayList<String>();
    StringBuilder cur = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (quoted) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            cur.append('"');
            i++;
          } else {
            quoted = false;
          }
        } else {
          cur.append(c);
        }
      } else if (c == '"') {
        quoted = true;
      } else if (c == ',') {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    if (quoted) {
      throw new IllegalStateException("MRIP: unterminated quote in " + url + ": " + line);
    }
    out.add(cur.toString());
    return out.toArray(new String[0]);
  }

  private static String requireLine(BufferedReader reader, String url) throws IOException {
    String line = reader.readLine();
    if (line == null) {
      throw new IllegalStateException("MRIP: empty file " + url);
    }
    if (line.startsWith("﻿")) {
      line = line.substring(1);
    }
    return line;
  }

  private static InputStream open(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setRequestProperty("User-Agent", "Mozilla/5.0 (govdata-etl)");
    int status = conn.getResponseCode();
    if (status != 200) {
      conn.disconnect();
      throw new IOException("MRIP: HTTP " + status + " for " + url);
    }
    return CompleteBody.of(conn);
  }

  /** Opens the per-year CSV inside the decade bundle that holds the given pre-2020 year. */
  private static InputStream openBundleEntry(String base, String kind, int year)
      throws IOException {
    int start = year < 1990 ? 1981 : (year / 10) * 10;
    int end = year < 1990 ? 1989 : start + 9;
    String zipUrl = base + "mrip_" + kind + "_bywave_" + start + "_" + end + "_csv.zip";
    String entry = "mrip_" + kind + "_bywave_" + year + ".csv";
    ZipInputStream zis = new ZipInputStream(open(zipUrl));
    ZipEntry e;
    while ((e = zis.getNextEntry()) != null) {
      if (!e.isDirectory() && entry.equals(e.getName())) {
        return zis;
      }
      zis.closeEntry();
    }
    zis.close();
    throw new IOException("MRIP: entry " + entry + " not found in " + zipUrl);
  }
}
