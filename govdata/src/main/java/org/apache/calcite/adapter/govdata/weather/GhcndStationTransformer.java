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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transforms the NOAA GHCN-Daily station inventory file ({@code ghcnd-stations.txt})
 * into a flat JSON array.
 *
 * <p>The input is fixed-width text with one station per line:
 * <ul>
 *   <li>Cols 1-11: station_id</li>
 *   <li>Cols 13-20: latitude</li>
 *   <li>Cols 22-30: longitude</li>
 *   <li>Cols 32-37: elevation_m</li>
 *   <li>Cols 39-40: state_code (2-letter)</li>
 *   <li>Cols 42-71: station_name</li>
 * </ul>
 *
 * <p>Enrichments applied for US stations only (station_id prefix "US"):
 * <ul>
 *   <li>data_start_year and data_end_year from the GHCND inventory file
 *       (lazily fetched and cached on first use)</li>
 *   <li>county_fips via nearest Census TIGERweb county centroid
 *       (lazily fetched and cached on first use)</li>
 *   <li>distance_to_county_centroid_km computed from Euclidean lat/lon distance</li>
 * </ul>
 */
public class GhcndStationTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GhcndStationTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String INVENTORY_URL =
      "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt";
  private static final String TIGERWEB_URL =
      "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/1/query"
      + "?where=1%3D1&outFields=GEOID,INTPTLAT,INTPTLON&returnGeometry=false&f=json"
      + "&resultOffset=0&resultRecordCount=3200";

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
    m.put("WV", "54"); m.put("WI", "55"); m.put("WY", "56");
    STATE_FIPS = Collections.unmodifiableMap(m);
  }

  // Keyed by station_id; value is int[2] = {data_start_year, data_end_year}.
  // Null map reference means "not yet attempted"; empty map means "fetch failed or produced nothing".
  private static volatile Map<String, int[]> inventoryCache = null;
  private static final Object INVENTORY_LOCK = new Object();

  // Each entry: double[3] = {lat, lon, parsed county_fips as double (stored as String separately)}.
  // Stored as a parallel structure for performance: List<double[]> coords, List<String> fips.
  private static volatile List<double[]> countyCoords = null;
  private static volatile List<String> countyFips = null;
  private static final Object COUNTY_LOCK = new Object();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("GHCND Station: Empty response for {}", context.getUrl());
      return "[]";
    }

    Map<String, int[]> inventory = getInventory();
    List<double[]> coords = getCountyCoords();
    List<String> fips = getCountyFips();

    ArrayNode result = MAPPER.createArrayNode();
    String[] lines = response.split("\n");

    for (String line : lines) {
      if (line.length() < 11) {
        continue;
      }

      String stationId = extractField(line, 0, 11);
      if (!stationId.startsWith("US")) {
        continue;
      }

      String latStr = extractField(line, 12, 20);
      String lonStr = extractField(line, 21, 30);
      String elevStr = extractField(line, 31, 37);
      String stateCode = line.length() >= 40 ? extractField(line, 38, 40) : "";
      String stationName = line.length() >= 71 ? extractField(line, 41, 71)
          : (line.length() > 41 ? line.substring(41).trim() : "");

      double lat = 0.0;
      double lon = 0.0;
      boolean latLonValid = false;
      try {
        lat = Double.parseDouble(latStr);
        lon = Double.parseDouble(lonStr);
        latLonValid = true;
      } catch (NumberFormatException e) {
        // leave latLonValid false
      }

      Double elevation = null;
      try {
        double e = Double.parseDouble(elevStr);
        // GHCND uses -999.9 to indicate missing elevation
        if (e > -999.0) {
          elevation = e;
        }
      } catch (NumberFormatException e) {
        // leave elevation null
      }

      String stateFips = STATE_FIPS.get(stateCode.trim());

      ObjectNode row = MAPPER.createObjectNode();
      row.put("station_id", stationId);
      row.put("station_name", stationName);
      if (stateFips != null) {
        row.put("state_fips", stateFips);
      } else {
        row.putNull("state_fips");
      }

      if (latLonValid) {
        row.put("latitude", lat);
        row.put("longitude", lon);
      } else {
        row.putNull("latitude");
        row.putNull("longitude");
      }

      if (elevation != null) {
        row.put("elevation_m", elevation);
      } else {
        row.putNull("elevation_m");
      }

      int[] yearRange = inventory != null ? inventory.get(stationId) : null;
      if (yearRange != null) {
        row.put("data_start_year", yearRange[0]);
        row.put("data_end_year", yearRange[1]);
      } else {
        row.putNull("data_start_year");
        row.putNull("data_end_year");
      }

      if (coords != null && fips != null && !coords.isEmpty() && latLonValid) {
        int nearestIdx = findNearestCounty(lat, lon, coords);
        if (nearestIdx >= 0) {
          row.put("county_fips", fips.get(nearestIdx));
          double[] nearest = coords.get(nearestIdx);
          double dlat = lat - nearest[0];
          double dlon = lon - nearest[1];
          double latRad = Math.toRadians(lat);
          double distKm = Math.sqrt(
              Math.pow(dlat * 111.0, 2.0)
              + Math.pow(dlon * 111.0 * Math.cos(latRad), 2.0));
          row.put("distance_to_county_centroid_km", distKm);
        } else {
          row.putNull("county_fips");
          row.putNull("distance_to_county_centroid_km");
        }
      } else {
        row.putNull("county_fips");
        row.putNull("distance_to_county_centroid_km");
      }

      result.add(row);
    }

    LOGGER.debug("GHCND Station: Transformed {} US stations", result.size());
    return result.toString();
  }

  private static int findNearestCounty(double lat, double lon, List<double[]> coords) {
    int bestIdx = -1;
    double bestDist = Double.MAX_VALUE;
    for (int i = 0; i < coords.size(); i++) {
      double[] c = coords.get(i);
      double dlat = lat - c[0];
      double dlon = lon - c[1];
      double dist = dlat * dlat + dlon * dlon;
      if (dist < bestDist) {
        bestDist = dist;
        bestIdx = i;
      }
    }
    return bestIdx;
  }

  private static Map<String, int[]> getInventory() {
    if (inventoryCache != null) {
      return inventoryCache;
    }
    synchronized (INVENTORY_LOCK) {
      if (inventoryCache != null) {
        return inventoryCache;
      }
      Map<String, int[]> cache = new HashMap<String, int[]>();
      try {
        String text = fetchText(INVENTORY_URL);
        String[] lines = text.split("\n");
        for (String line : lines) {
          if (line.length() < 45) {
            continue;
          }
          String stationId = extractField(line, 0, 11);
          String firstYearStr = extractField(line, 36, 40);
          String lastYearStr = extractField(line, 41, 45);
          try {
            int firstYear = Integer.parseInt(firstYearStr);
            int lastYear = Integer.parseInt(lastYearStr);
            int[] existing = cache.get(stationId);
            if (existing == null) {
              cache.put(stationId, new int[]{firstYear, lastYear});
            } else {
              if (firstYear < existing[0]) {
                existing[0] = firstYear;
              }
              if (lastYear > existing[1]) {
                existing[1] = lastYear;
              }
            }
          } catch (NumberFormatException e) {
            // skip malformed lines
          }
        }
        LOGGER.debug("GHCND Inventory: Loaded {} station year ranges", cache.size());
      } catch (Exception e) {
        LOGGER.warn("GHCND Inventory: Failed to fetch inventory, year ranges will be null: {}",
            e.getMessage());
      }
      inventoryCache = cache;
      return inventoryCache;
    }
  }

  private static List<double[]> getCountyCoords() {
    if (countyCoords != null) {
      return countyCoords;
    }
    loadCountyData();
    return countyCoords;
  }

  private static List<String> getCountyFips() {
    if (countyFips != null) {
      return countyFips;
    }
    loadCountyData();
    return countyFips;
  }

  private static void loadCountyData() {
    synchronized (COUNTY_LOCK) {
      if (countyCoords != null) {
        return;
      }
      List<double[]> coordsList = new ArrayList<double[]>();
      List<String> fipsList = new ArrayList<String>();
      try {
        String text = fetchText(TIGERWEB_URL);
        JsonNode root = MAPPER.readTree(text);
        JsonNode features = root.get("features");
        if (features != null && features.isArray()) {
          for (JsonNode feature : features) {
            JsonNode attrs = feature.get("attributes");
            if (attrs == null) {
              continue;
            }
            JsonNode geoidNode = attrs.get("GEOID");
            JsonNode latNode = attrs.get("INTPTLAT");
            JsonNode lonNode = attrs.get("INTPTLON");
            if (geoidNode == null || latNode == null || lonNode == null) {
              continue;
            }
            try {
              String geoid = geoidNode.asText();
              double cLat = Double.parseDouble(latNode.asText());
              double cLon = Double.parseDouble(lonNode.asText());
              coordsList.add(new double[]{cLat, cLon});
              fipsList.add(geoid);
            } catch (NumberFormatException e) {
              // skip malformed entries
            }
          }
        }
        LOGGER.debug("GHCND Station: Loaded {} county centroids from TIGERweb", coordsList.size());
      } catch (Exception e) {
        LOGGER.warn("GHCND Station: Failed to fetch TIGERweb county centroids, "
            + "county_fips will be null: {}", e.getMessage());
      }
      countyCoords = coordsList;
      countyFips = fipsList;
    }
  }

  private static String fetchText(String urlStr) throws Exception {
    URL url = URI.create(urlStr).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);
    conn.setRequestMethod("GET");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new RuntimeException("HTTP " + status + " from " + urlStr);
    }
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line).append('\n');
    }
    reader.close();
    conn.disconnect();
    return sb.toString();
  }

  // Zero-based start and end (exclusive), mirrors String.substring semantics.
  private static String extractField(String line, int start, int end) {
    int actualEnd = Math.min(end, line.length());
    if (start >= actualEnd) {
      return "";
    }
    return line.substring(start, actualEnd).trim();
  }
}
