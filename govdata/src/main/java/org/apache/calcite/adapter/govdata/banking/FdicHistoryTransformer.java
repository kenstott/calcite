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
package org.apache.calcite.adapter.govdata.banking;

import org.apache.calcite.adapter.govdata.geo.CountyFipsByNameLookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps FDIC BankFind Suite {@code /banks/history} records into {@code history} rows — the
 * branch open/close/merger event log. PK is {@code transnum}; windowed by {@code EFFYEAR}.
 */
public class FdicHistoryTransformer extends AbstractFdicTransformer {

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putLong(row, "transnum", rec, "TRANSNUM");
    putLong(row, "cert", rec, "CERT");
    putText(row, "institution_name", rec, "INSTNAME");
    putInt(row, "changecode", rec, "CHANGECODE");
    putText(row, "changecode_label", rec, "CHANGECODE_DESC");
    putFdicDate(row, "effective_date", rec, "EFFDATE");
    putText(row, "office_name", rec, "OFF_NAME");
    putText(row, "office_city", rec, "OFF_PCITY");
    putText(row, "office_state_abbr", rec, "OFF_PSTALP");
    putText(row, "office_county_name", rec, "OFF_CNTYNAME");
    // FDIC reports the county as free text, never a native FIPS code - derive it via
    // exact match against the Census-sourced crosswalk, same as eia_power_plants. A name
    // the crosswalk cannot resolve stays null rather than being guessed: this includes
    // Puerto Rico and US Virgin Islands county-equivalents (the crosswalk has no PR/VI
    // entries at all - a real gap in that reference, not a punctuation mismatch this
    // lookup can fix) and DC branches, whose OFF_CNTYNAME is genuinely absent because DC
    // has no county subdivision to report.
    String stateAbbr = rec.path("OFF_PSTALP").asText(null);
    String countyName = rec.path("OFF_CNTYNAME").asText(null);
    String countyFips = CountyFipsByNameLookup.lookup(stateAbbr, countyName);
    if (countyFips != null) {
      row.put("county_fips", countyFips);
    } else {
      row.putNull("county_fips");
    }
    putDouble(row, "latitude", rec, "OFF_LATITUDE");
    putDouble(row, "longitude", rec, "OFF_LONGITUDE");
    putFdicDate(row, "proc_date", rec, "PROCDATE");
  }
}
