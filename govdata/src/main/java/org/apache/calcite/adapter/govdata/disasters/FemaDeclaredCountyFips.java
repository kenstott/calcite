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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Resolves the county FIPS code of a FEMA housing-assistance row.
 *
 * <p>The OpenFEMA housing-assistance datasets name the county as text ({@code "Aransas (County)"})
 * and carry no FIPS code. FEMA's own {@code DisasterDeclarationsSummaries} lists every designated
 * area of every declaration in exactly that spelling together with {@code fipsStateCode} and
 * {@code fipsCountyCode}, so the code comes from the declaration for the same disaster number,
 * state and area name. That covers Puerto Rico municipios, territories and independent cities,
 * which a plain county-name crosswalk does not.
 *
 * <p>A row resolves only when the declaration names exactly one county code other than the
 * statewide {@code 000}. Tribal areas (reported by FEMA with county code 000), and area names the
 * declarations do not list, stay unresolved and the caller stores a null county FIPS; nothing is
 * guessed. The map is loaded once per JVM. A failed load throws.
 */
final class FemaDeclaredCountyFips {

  private static final Logger LOGGER = LoggerFactory.getLogger(FemaDeclaredCountyFips.class);
  private static final String URL =
      "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
          + "?$select=disasterNumber,state,designatedArea,fipsStateCode,fipsCountyCode"
          + "&$orderby=id&$top=10000&$skip=";
  private static final int PAGE = 10000;
  private static final int ATTEMPTS = 5;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static Map<String, Set<String>> byKey;

  private FemaDeclaredCountyFips() {
  }

  /**
   * Returns the 5-digit state-plus-county code the declarations give for the disaster, state and
   * area name, or {@code null} when they do not name exactly one code for it (unknown area, or
   * an area name shared by several counties). A statewide or tribal area comes back with county
   * part {@code 000}; {@link #countyFips} turns that into a null county.
   */
  static synchronized String lookup(int disasterNumber, String state, String area)
      throws IOException {
    if (state == null || area == null) {
      return null;
    }
    if (byKey == null) {
      byKey = load();
    }
    Set<String> codes = byKey.get(key(disasterNumber, state, area));
    if (codes == null || codes.size() != 1) {
      return null;
    }
    return codes.iterator().next();
  }

  /** The county FIPS of a code from {@link #lookup}, or null for a statewide/tribal {@code 000}. */
  static String countyFips(String code) {
    return code == null || code.endsWith("000") ? null : code;
  }

  private static String key(int disasterNumber, String state, String area) {
    return disasterNumber + "|" + state.trim() + "|" + area.trim();
  }

  private static Map<String, Set<String>> load() throws IOException {
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    Map<String, Set<String>> map = new HashMap<String, Set<String>>();
    int skip = 0;
    while (true) {
      JsonNode records = fetchPage(client, skip);
      for (JsonNode r : records) {
        String stateFips = r.path("fipsStateCode").asText("");
        String countyCode = r.path("fipsCountyCode").asText("");
        if (stateFips.length() != 2 || countyCode.length() != 3) {
          continue;
        }
        String k = key(r.path("disasterNumber").asInt(), r.path("state").asText(""),
            r.path("designatedArea").asText(""));
        Set<String> codes = map.get(k);
        if (codes == null) {
          codes = new HashSet<String>();
          map.put(k, codes);
        }
        codes.add(stateFips + countyCode);
      }
      skip += records.size();
      if (records.size() < PAGE) {
        break;
      }
    }
    LOGGER.info("FemaDeclaredCountyFips: {} declaration areas loaded ({} declaration rows)",
        Integer.valueOf(map.size()), Integer.valueOf(skip));
    if (map.isEmpty()) {
      throw new IOException("DisasterDeclarationsSummaries returned no designated areas");
    }
    return map;
  }

  private static JsonNode fetchPage(HttpClient client, int skip) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
      try {
        HttpRequest req = HttpRequest.newBuilder(URI.create(URL + skip))
            .timeout(Duration.ofMinutes(3)).GET().build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 200) {
          JsonNode records = MAPPER.readTree(resp.body()).path("DisasterDeclarationsSummaries");
          if (records.isArray()) {
            return records;
          }
          last = new IOException("no DisasterDeclarationsSummaries array at skip " + skip
              + " (first 200 chars: " + resp.body().substring(0, Math.min(200, resp.body().length()))
              + ")");
        } else {
          last = new IOException("HTTP " + resp.statusCode() + " at skip " + skip);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted loading DisasterDeclarationsSummaries", e);
      } catch (IOException e) {
        last = e;
      }
      LOGGER.warn("FemaDeclaredCountyFips: attempt {}/{} failed: {}", Integer.valueOf(attempt),
          Integer.valueOf(ATTEMPTS), last.getMessage());
      try {
        Thread.sleep(5000L * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted loading DisasterDeclarationsSummaries", e);
      }
    }
    throw new IOException("DisasterDeclarationsSummaries load failed after " + ATTEMPTS
        + " attempts", last);
  }
}
