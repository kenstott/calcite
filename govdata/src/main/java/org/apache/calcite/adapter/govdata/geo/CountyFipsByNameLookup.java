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
package org.apache.calcite.adapter.govdata.geo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Exact-match lookup of 5-digit county FIPS codes by (state abbreviation, county name),
 * backed by a static crosswalk ({@code geo/county_fips_by_name.json}) generated from the
 * authoritative Census TIGER county boundaries ({@code geo.counties} joined to
 * {@code geo.state_ref}).
 *
 * <p>The crosswalk includes every (state, county-name) pairing across all TIGER vintage
 * years that resolves to exactly one FIPS code; names that are ambiguous across vintages
 * (renamed or merged counties mapping to more than one FIPS) are excluded rather than
 * guessed. {@link #lookup} likewise returns {@code null} for any name it cannot match —
 * a source dataset that reports a county name but no native FIPS code (e.g. EIA-860)
 * should treat a null result as "unknown," never fall back to a fabricated value.
 */
public final class CountyFipsByNameLookup {
  private static final Logger LOGGER = LoggerFactory.getLogger(CountyFipsByNameLookup.class);
  private static final String RESOURCE_PATH = "geo/county_fips_by_name.json";
  private static final Map<String, String> FIPS_BY_KEY = load();

  private CountyFipsByNameLookup() {
  }

  /**
   * Returns the 5-digit county FIPS code for the given state abbreviation and county
   * name, or {@code null} if the pair does not exactly match (case/whitespace
   * insensitive) an unambiguous entry in the crosswalk.
   */
  public static String lookup(String stateAbbr, String countyName) {
    if (stateAbbr == null || countyName == null) {
      return null;
    }
    return FIPS_BY_KEY.get(key(stateAbbr, countyName));
  }

  private static String key(String stateAbbr, String countyName) {
    return stateAbbr.trim().toUpperCase(Locale.ROOT) + "|"
        + countyName.trim().toUpperCase(Locale.ROOT);
  }

  private static Map<String, String> load() {
    ObjectMapper mapper = new ObjectMapper();
    try (InputStream is = CountyFipsByNameLookup.class.getClassLoader()
        .getResourceAsStream(RESOURCE_PATH)) {
      if (is == null) {
        LOGGER.warn("{} not found on classpath — county FIPS-by-name lookup disabled",
            RESOURCE_PATH);
        return Collections.emptyMap();
      }
      JsonNode root = mapper.readTree(is);
      Map<String, String> result = new HashMap<>();
      for (JsonNode entry : root) {
        String stateAbbr = entry.get("state_abbr").asText();
        String countyName = entry.get("county_name").asText();
        String countyFips = entry.get("county_fips").asText();
        result.put(key(stateAbbr, countyName), countyFips);
      }
      LOGGER.info("Loaded {} county FIPS-by-name crosswalk entries", result.size());
      return result;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load " + RESOURCE_PATH, e);
    }
  }
}
