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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Faithful recreation of PatentsView g_location_disambiguated.tsv as patent_locations rows —
 * one row per disambiguated location, keyed by location_id, no joins and no year. The
 * foreign-key target for patent_inventors.location_id and patent_assignees.location_id.
 * Loaded once as a snapshot.
 */
public class PatentLocationsTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PatentLocationsTransformer.class);

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String q = quarterToken(context);
    final String locationFile = downloadAndCacheTsv(
        ODP_PVGPATDIS_BASE + "g_location_disambiguated.tsv.zip",
        cacheFile("g_location_disambiguated_" + q + ".tsv"));

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(locationFile),
            StandardCharsets.UTF_8));
    String headerLine = CsvRecordReader.readRecord(reader);
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
          while ((line = CsvRecordReader.readRecord(reader)) != null) {
            if (line.isEmpty()) {
              continue;
            }
            String[] parts = splitTsv(line);
            String locationId = getField(parts, hdr, "location_id");
            if (locationId == null || locationId.isEmpty()) {
              continue;
            }
            Map<String, Object> row = new HashMap<>();
            row.put("location_id", strVal(locationId));
            row.put("state_fips", strVal(getField(parts, hdr, "state_fips")));
            row.put("county_fips", strVal(getField(parts, hdr, "county_fips")));
            row.put("country_code", strVal(getField(parts, hdr, "disambig_country")));
            row.put("latitude", doubleVal(getField(parts, hdr, "latitude")));
            row.put("longitude", doubleVal(getField(parts, hdr, "longitude")));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentLocations: {} records (snapshot)", count[0]);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException closeEx) { LOGGER.debug("close failed", closeEx); }
          throw new UncheckedIOException("PatentLocationsTransformer read failed", e);
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
