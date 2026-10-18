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
package org.apache.calcite.adapter.govdata.disasters;

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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transformer for {@code disasters.nws_zone_county} — the National Weather Service
 * zone-county correlation file (one row per zone x county).
 *
 * <p>The source URL is the NWS release listing page ({@code weather.gov/gis/ZoneCounty}). Data
 * file names embed the release date ({@code bp16ap26.dbx} = 16 Apr 2026). Each file is
 * pipe-delimited: {@code STATE|ZONE|CWA|NAME|STATE_ZONE|COUNTY|FIPS|TIME_ZONE|FE_AREA|LAT|LON},
 * where {@code FIPS} is the 5-digit county FIPS and a zone covering several counties appears once
 * per county.
 *
 * <p>NWS retires and renumbers zones between releases, but NOAA Storm Events rows keep the zone
 * number in force when the event was recorded. So the crosswalk reads every release it can see
 * (the listing, plus {@link #ARCHIVED_RELEASES} that NWS still serves but no longer lists) and, for
 * each (state, zone), takes the counties from the newest release that contains that zone. The
 * {@code release} column records which file supplied the row.
 */
public class NwsZoneCountyTransformer implements StreamingResponseTransformer {

  private static final String FILE_BASE = "https://www.weather.gov/source/gis/Shapefiles/County/";
  private static final Pattern RELEASE = Pattern.compile("bp(\\d{2})([a-z]{2})(\\d{2})\\.dbx");
  private static final List<String> MONTHS =
      Arrays.asList("ja", "fe", "mr", "ap", "my", "jn", "jl", "au", "se", "oc", "no", "de");
  /** Still served by NWS but dropped from the listing page; carries zones later retired. */
  private static final List<String> ARCHIVED_RELEASES = Arrays.asList("bp05mr24.dbx");
  /** Listed by NWS but documented there as "now invalid" (pre-dates the current CWA layout). */
  private static final List<String> INVALID_RELEASES = Arrays.asList("bp01my18.dbx");
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 120_000;
  private static final int FIELDS = 11;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String listingUrl = context.getUrl();
    if (listingUrl == null || listingUrl.isEmpty()) {
      throw new IOException("NwsZoneCountyTransformer: no source URL in context");
    }
    List<String> releases = releases(get(listingUrl));
    Map<String, Map<String, Object>> byKey = new LinkedHashMap<String, Map<String, Object>>();
    Map<String, String> zoneRelease = new LinkedHashMap<String, String>();
    for (String file : releases) {
      for (Map<String, Object> row : parse(get(FILE_BASE + file))) {
        String zoneKey = row.get("state_abbr") + "|" + row.get("zone");
        String owner = zoneRelease.get(zoneKey);
        if (owner == null) {
          zoneRelease.put(zoneKey, file);
        } else if (!owner.equals(file)) {
          continue;
        }
        row.put("release", releaseDate(file));
        byKey.put(zoneKey + "|" + row.get("county_fips"), row);
      }
    }
    if (byKey.isEmpty()) {
      throw new IOException("NwsZoneCountyTransformer: releases " + releases + " produced no rows");
    }
    return new ArrayList<Map<String, Object>>(byKey.values()).iterator();
  }

  /** Release files to read, newest first: everything on the listing plus the archived ones. */
  static List<String> releases(String listingHtml) throws IOException {
    Map<Integer, String> byDate = new java.util.TreeMap<Integer, String>(
        java.util.Collections.reverseOrder());
    Matcher m = RELEASE.matcher(listingHtml);
    List<String> found = new ArrayList<String>(ARCHIVED_RELEASES);
    while (m.find()) {
      found.add(m.group());
    }
    for (String file : found) {
      if (INVALID_RELEASES.contains(file)) {
        continue;
      }
      byDate.put(dateKey(file), file);
    }
    if (byDate.isEmpty()) {
      throw new IOException("NwsZoneCountyTransformer: no bpDDmmYY.dbx release found on listing");
    }
    return new ArrayList<String>(byDate.values());
  }

  private static int dateKey(String file) throws IOException {
    Matcher m = RELEASE.matcher(file);
    if (!m.matches() || MONTHS.indexOf(m.group(2)) < 0) {
      throw new IOException("NwsZoneCountyTransformer: unparseable release name " + file);
    }
    return (2000 + Integer.parseInt(m.group(3))) * 10000 + (MONTHS.indexOf(m.group(2)) + 1) * 100
        + Integer.parseInt(m.group(1));
  }

  private static String releaseDate(String file) throws IOException {
    int k = dateKey(file);
    return String.format("%04d-%02d-%02d", k / 10000, (k / 100) % 100, k % 100);
  }

  static List<Map<String, Object>> parse(String body) throws IOException {
    // NWS repeats a (zone, county) pair when a zone has several polygon parts (differing only in
    // centroid lat/lon, or exact duplicates); the crosswalk key is the pair, so keep the first.
    Map<String, Map<String, Object>> byKey = new LinkedHashMap<String, Map<String, Object>>();
    for (String line : body.split("\n")) {
      if (line.trim().isEmpty()) {
        continue;
      }
      String[] f = line.split("\\|", -1);
      if (f.length < FIELDS) {
        throw new IOException("NwsZoneCountyTransformer: malformed line (" + f.length
            + " fields): " + line);
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      String countyFips = f[6].trim();
      row.put("state_fips", countyFips.length() == 5 ? countyFips.substring(0, 2) : null);
      row.put("county_fips", countyFips);
      row.put("state_abbr", f[0].trim());
      row.put("zone", f[1].trim());
      row.put("state_zone", f[4].trim());
      row.put("zone_name", f[3].trim());
      row.put("county_name", f[5].trim());
      row.put("cwa", f[2].trim());
      row.put("time_zone", f[7].trim());
      row.put("zone_lat", dbl(f[9]));
      row.put("zone_lon", dbl(f[10]));
      String key = f[0].trim() + "|" + f[1].trim() + "|" + countyFips;
      if (!byKey.containsKey(key)) {
        byKey.put(key, row);
      }
    }
    return new ArrayList<Map<String, Object>>(byKey.values());
  }

  private static Double dbl(String s) {
    String t = s.trim();
    return t.isEmpty() ? null : Double.valueOf(t);
  }

  private static String get(String url) throws IOException {
    HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL().openConnection();
    c.setConnectTimeout(CONNECT_TIMEOUT_MS);
    c.setReadTimeout(READ_TIMEOUT_MS);
    c.setRequestProperty("User-Agent", "calcite-govdata (+mailto:kennethstott@gmail.com)");
    int code = c.getResponseCode();
    if (code != 200) {
      throw new IOException("NWS zone-county: HTTP " + code + " from " + url);
    }
    StringBuilder sb = new StringBuilder();
    try (InputStream in = c.getInputStream();
         BufferedReader r = new BufferedReader(
             new InputStreamReader(in, StandardCharsets.ISO_8859_1))) {
      char[] buf = new char[8192];
      int n;
      while ((n = r.read(buf)) > 0) {
        sb.append(buf, 0, n);
      }
    }
    return sb.toString();
  }
}
