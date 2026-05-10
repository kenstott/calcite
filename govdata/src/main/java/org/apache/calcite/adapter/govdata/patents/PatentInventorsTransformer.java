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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Transforms PatentsView g_inventor_disambiguated.tsv into patent_inventors rows.
 *
 * <p>Downloads and caches 3 full-dump files (g_patent, g_inventor_disambiguated,
 * g_location_disambiguated). Eager setup: collects patent_ids for the year, then
 * the location_ids referenced by those inventors, then loads the locations lookup.
 * Returns a lazy iterator streaming the inventor file, emitting one row per inventor.
 * No intermediate StringWriter — memory is O(lookup_size + chunk_size).
 */
public class PatentInventorsTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PatentInventorsTransformer.class);

  private static final String BASE_URL =
      "https://s3.amazonaws.com/data.patentsview.org/download/";

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String yearStr = getYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("PatentInventors: missing year dimension");
      return Collections.emptyIterator();
    }

    String patentFile = downloadAndCacheTsv(
        BASE_URL + "g_patent.tsv.zip", cacheFile("g_patent.tsv"));
    final String inventorFile = downloadAndCacheTsv(
        BASE_URL + "g_inventor_disambiguated.tsv.zip",
        cacheFile("g_inventor_disambiguated.tsv"));
    String locationFile = downloadAndCacheTsv(
        BASE_URL + "g_location_disambiguated.tsv.zip",
        cacheFile("g_location_disambiguated.tsv"));

    final Set<String> patentIds = readPatentIdsForYear(patentFile, yearStr);
    LOGGER.info("PatentInventors: {} patent IDs for year {}", patentIds.size(), yearStr);

    Set<String> locationIds = readTsvKeysByPatentIds(inventorFile, patentIds, "location_id");
    final Map<String, Map<String, String>> locations = readTsvAsLookupForKeys(
        locationFile, "location_id", locationIds,
        "state_fips", "county_fips", "disambig_country", "latitude", "longitude");

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(inventorFile),
            StandardCharsets.UTF_8));
    String headerLine = reader.readLine();
    if (headerLine == null) {
      reader.close();
      return Collections.emptyIterator();
    }
    final Map<String, Integer> hdr = buildHeaderMap(splitTsv(headerLine));
    final int[] count = {0};

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending;
      { advance(); }

      private void advance() {
        pending = null;
        try {
          String line;
          while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
              continue;
            }
            String[] parts = splitTsv(line);
            String patentId = getField(parts, hdr, "patent_id");
            if (patentId == null || !patentIds.contains(patentId)) {
              continue;
            }
            String locationId = getField(parts, hdr, "location_id");
            Map<String, String> loc = locationId != null ? locations.get(locationId) : null;

            Map<String, Object> row = new HashMap<>();
            row.put("patent_id", strVal(patentId));
            row.put("grant_year", intVal(yearStr));
            row.put("inventor_id", strVal(getField(parts, hdr, "inventor_id")));
            row.put("inventor_sequence",
                intVal(getField(parts, hdr, "inventor_sequence")));
            row.put("name_first",
                strVal(getField(parts, hdr, "disambig_inventor_name_first")));
            row.put("name_last",
                strVal(getField(parts, hdr, "disambig_inventor_name_last")));
            row.put("gender_code", strVal(getField(parts, hdr, "gender_code")));
            row.put("state_fips", strVal(loc != null ? loc.get("state_fips") : null));
            row.put("county_fips", strVal(loc != null ? loc.get("county_fips") : null));
            row.put("country_code",
                strVal(loc != null ? loc.get("disambig_country") : null));
            row.put("latitude", doubleVal(loc != null ? loc.get("latitude") : null));
            row.put("longitude", doubleVal(loc != null ? loc.get("longitude") : null));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentInventors: {} records for year {}", count[0], yearStr);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException ignored) { }
          throw new RuntimeException("PatentInventorsTransformer read failed", e);
        }
      }

      @Override public boolean hasNext() { return pending != null; }

      @Override public Map<String, Object> next() {
        Map<String, Object> row = pending;
        advance();
        return row;
      }
    };
  }
}
