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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Parses the Census Bureau's published County Adjacency File
 * ({@code https://www2.census.gov/geo/docs/reference/county_adjacency.txt}) into one row per
 * adjacent county pair.
 *
 * <p>Real Census-published adjacency, replacing the {@code ST_Touches}-on-TIGER-boundary-polygons
 * derivation previously used for {@code county_adjacency} (kept, unused, as
 * {@code county_adjacency_st_touches_legacy} under {@code views:} for comparison — see that
 * view's comment). The prior ST_Touches approach only covered roughly 15% of true adjacency
 * because TIGER shared-edge polygons rarely align exactly enough for an exact-touch predicate.
 *
 * <p>File format (tab-delimited, verified live 2026-08-21, 22,200 lines, 726,724 bytes): each
 * county starts a block with all 4 columns populated — county name (quoted, {@code "NAME, ST"}),
 * county FIPS, its own name and FIPS repeated as the first "neighbor" (a self-pair, since every
 * county's own record technically starts its own neighbor list — this is dropped here rather
 * than emitted as a spurious self-adjacency row). Every subsequent row in that county's block
 * has its first two columns blank (fill-down: "same county as the last non-blank row") and
 * carries one real neighbor (name, FIPS) per row. The file is already undirected-symmetric — if
 * A borders B, a B-block elsewhere in the file lists A as a neighbor of B — so no directionality
 * transform is applied here, matching the source as-is.
 */
public class CountyAdjacencyResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CountyAdjacencyResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Strips a trailing ", XX" state-abbreviation suffix (e.g. "Autauga County, AL" ->
  // "Autauga County") so county_name/neighbor_county_name match this schema's convention
  // (geo.counties.county_name carries no state suffix).
  private static final Pattern STATE_SUFFIX = Pattern.compile(",\\s*[A-Za-z]{2}$");

  @Override public String transform(String response, RequestContext context) {
    String yearStr = context.getDimensionValues().get("year");
    if (response == null || response.isEmpty()) {
      LOGGER.warn("County Adjacency: empty response for year={}", yearStr);
      return "[]";
    }

    Integer year = parseIntOrNull(yearStr);
    ArrayNode out = MAPPER.createArrayNode();

    String currentFips = null;
    String currentName = null;

    try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        List<String> fields = CsvRecordReader.splitFields(line, '\t');
        String name = fields.size() > 0 ? fields.get(0).trim() : "";
        String fips = fields.size() > 1 ? fields.get(1).trim() : "";
        String neighborName = fields.size() > 2 ? fields.get(2).trim() : "";
        String neighborFips = fields.size() > 3 ? fields.get(3).trim() : "";

        if (!fips.isEmpty()) {
          // New county block starts here.
          currentFips = fips;
          currentName = name;
        }

        if (currentFips == null || neighborFips.isEmpty()) {
          continue;
        }
        // Every county's block starts with itself as the first "neighbor" — not a real
        // adjacency, drop it (same exclusion the prior ST_Touches view applied).
        if (neighborFips.equals(currentFips)) {
          continue;
        }
        if (currentFips.length() < 2 || neighborFips.length() < 2) {
          LOGGER.warn("County Adjacency: malformed FIPS pair '{}'/'{}', skipping row",
              currentFips, neighborFips);
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("county_fips", currentFips);
        row.put("state_fips", currentFips.substring(0, 2));
        putOrNull(row, "county_name", stripStateSuffix(currentName));
        row.put("neighbor_county_fips", neighborFips);
        row.put("neighbor_state_fips", neighborFips.substring(0, 2));
        putOrNull(row, "neighbor_county_name", stripStateSuffix(neighborName));
        if (year != null) {
          row.put("year", year);
        } else {
          row.putNull("year");
        }
        out.add(row);
      }
    } catch (IOException e) {
      throw new RuntimeException("County Adjacency: failed to parse response for "
          + context.getUrl() + " (year=" + yearStr + ")", e);
    }

    LOGGER.debug("County Adjacency: parsed {} adjacency-pair rows for year={}", out.size(), yearStr);
    return out.toString();
  }

  private String stripStateSuffix(String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    // Quoted names arrive with their surrounding double quotes stripped by
    // CsvRecordReader.splitFields already; just remove the trailing ", XX".
    return STATE_SUFFIX.matcher(name).replaceAll("").trim();
  }

  private void putOrNull(ObjectNode row, String key, String value) {
    if (value == null || value.isEmpty()) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }

  private Integer parseIntOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Integer.valueOf(Integer.parseInt(s.trim()));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
