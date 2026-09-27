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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Backfills {@code epa_facilities.naics_codes} from {@code sic_codes} when the EPA ECHO
 * Exporter feed carries a SIC classification but no NAICS one at all (govdata-ops#694).
 *
 * <p>ECHO Exporter's {@code FAC_NAICS_CODES} is null for facilities registered under the
 * older SIC system that were never re-classified in FRS; {@code FAC_SIC_CODES} is the only
 * classification these facilities carry. A SIC code maps to several precise 6-digit NAICS
 * codes in general (SIC is coarser), so this only fills in the 2-digit NAICS sector — never a
 * fabricated 6-digit code — and only when every SIC code on the row resolves, via the bundled
 * {@code /ref/sic-naics-sector-crosswalk.json} crosswalk, to the same single sector. A
 * multi-sector or unrecognized SIC leaves {@code naics_codes} null rather than guess.
 * {@code NAICS_CODES_DERIVED} is set on every row ("Y"/"N") so
 * {@code epa_facilities.naics_codes_derived} can flag a backfilled value as approximate rather
 * than source-native.
 */
public class EpaFacilitiesNaicsBackfillTransformer implements RowTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EpaFacilitiesNaicsBackfillTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CROSSWALK_RESOURCE = "/ref/sic-naics-sector-crosswalk.json";
  private static final String NAICS_FIELD = "FAC_NAICS_CODES";
  private static final String SIC_FIELD = "FAC_SIC_CODES";
  private static final String DERIVED_FIELD = "NAICS_CODES_DERIVED";

  /** Lazily-loaded, immutable after first read: SIC code (4-digit) -> NAICS 2-digit sector. */
  private static volatile Map<String, String> crosswalk;

  private static Map<String, String> crosswalk() {
    Map<String, String> local = crosswalk;
    if (local == null) {
      synchronized (EpaFacilitiesNaicsBackfillTransformer.class) {
        local = crosswalk;
        if (local == null) {
          local = loadCrosswalk();
          crosswalk = local;
        }
      }
    }
    return local;
  }

  private static Map<String, String> loadCrosswalk() {
    try (InputStream in =
        EpaFacilitiesNaicsBackfillTransformer.class.getResourceAsStream(CROSSWALK_RESOURCE)) {
      if (in == null) {
        throw new IllegalStateException(
            "Missing bundled resource " + CROSSWALK_RESOURCE
                + " (run scripts/build-sic-naics-crosswalk.py)");
      }
      JsonNode root = MAPPER.readTree(in);
      Map<String, String> map = new HashMap<>();
      root.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
      return map;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load " + CROSSWALK_RESOURCE + ": " + e.getMessage(),
          e);
    }
  }

  @Override public List<Map<String, Object>> transform(Map<String, Object> row,
      RowContext context) {
    Object naics = row.get(NAICS_FIELD);
    Object sic = row.get(SIC_FIELD);
    row.put(DERIVED_FIELD, "N");

    if (!isBlank(naics) && isBlank(sic)) {
      return Collections.singletonList(row);
    }
    if (isBlank(sic)) {
      return Collections.singletonList(row);
    }
    if (!isBlank(naics)) {
      return Collections.singletonList(row);
    }

    String sector = resolveSector(sic.toString());
    if (sector != null) {
      row.put(NAICS_FIELD, sector);
      row.put(DERIVED_FIELD, "Y");
    }
    return Collections.singletonList(row);
  }

  /**
   * Returns the single NAICS 2-digit sector every space-separated SIC code on the row agrees
   * on, or {@code null} when any SIC code is unrecognized or the codes span more than one
   * sector.
   */
  private static String resolveSector(String sicCodes) {
    Map<String, String> table = crosswalk();
    Set<String> sectors = new HashSet<>();
    for (String token : sicCodes.trim().split("\\s+")) {
      String sic = normalizeSic(token);
      if (sic.isEmpty()) {
        continue;
      }
      String sector = table.get(sic);
      if (sector == null) {
        LOGGER.debug("EPA facilities: no SIC->NAICS crosswalk entry for SIC {}", sic);
        return null;
      }
      sectors.add(sector);
    }
    return sectors.size() == 1 ? sectors.iterator().next() : null;
  }

  private static String normalizeSic(String token) {
    String sic = token.trim();
    while (sic.length() < 4) {
      sic = "0" + sic;
    }
    return sic;
  }

  private static boolean isBlank(Object value) {
    return value == null || value.toString().trim().isEmpty();
  }
}
