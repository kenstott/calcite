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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RetryableHttp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Authoritative station-to-state assignment from the NCEI {@code ghcnd-stations.txt} station
 * file, the same source {@code ghcnd_stations_with_county} derives {@code state_fips} from.
 *
 * <p>NOAA CDO's {@code locationid=FIPS:<state>} filter also returns stations just across the
 * border, and its results carry no state of their own, so a station's state cannot be taken from
 * the state that was requested. The file is read line by line, once per JVM.
 */
final class GhcndStationStates {

  private static final Logger LOGGER = LoggerFactory.getLogger(GhcndStationStates.class);

  private static final String STATIONS_URL =
      "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt";

  private static final int ID_END = 11;
  private static final int STATE_START = 38;
  private static final int STATE_END = 40;

  // Null until a fetch has fully succeeded; a failed fetch throws rather than caching a partial map.
  private static volatile Map<String, String> stateFipsByStation = null;
  private static final Object LOCK = new Object();

  private GhcndStationStates() {
  }

  /**
   * Returns the 2-digit state FIPS of the station, or null when the station file carries no
   * state for it (or does not list it).
   */
  static String stateFips(String stationId, HttpSourceConfig.RateLimitConfig rateLimit)
      throws IOException {
    return load(rateLimit).get(stationId);
  }

  private static Map<String, String> load(HttpSourceConfig.RateLimitConfig rateLimit)
      throws IOException {
    Map<String, String> loaded = stateFipsByStation;
    if (loaded != null) {
      return loaded;
    }
    synchronized (LOCK) {
      if (stateFipsByStation != null) {
        return stateFipsByStation;
      }
      Map<String, String> map = new HashMap<String, String>();
      HttpURLConnection conn = RetryableHttp.openWithRetry(STATIONS_URL,
          new LinkedHashMap<String, String>(), rateLimit, false);
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.length() < STATE_END) {
            continue;
          }
          String fips = GhcndStationTransformer.STATE_FIPS.get(
              line.substring(STATE_START, STATE_END).trim());
          if (fips != null) {
            map.put(line.substring(0, ID_END).trim(), fips);
          }
        }
      } finally {
        conn.disconnect();
      }
      LOGGER.debug("GHCND stations: {} stations with a state assignment", map.size());
      stateFipsByStation = Collections.unmodifiableMap(map);
      return stateFipsByStation;
    }
  }
}
