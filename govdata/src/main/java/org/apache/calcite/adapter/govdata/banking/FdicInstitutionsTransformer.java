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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps FDIC BankFind Suite {@code /banks/institutions} records into {@code institutions} rows.
 * Snapshot of every FDIC-insured institution (active and inactive); PK is {@code cert}.
 */
public class FdicInstitutionsTransformer extends AbstractFdicTransformer {

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putLong(row, "cert", rec, "CERT");
    putText(row, "name", rec, "NAME");
    putBool(row, "active", rec, "ACTIVE");
    putText(row, "charter_class", rec, "BKCLASS");
    putText(row, "charter_agency", rec, "CHRTAGNT");
    putText(row, "regulator", rec, "REGAGNT");
    putText(row, "city", rec, "CITY");
    putText(row, "state_abbr", rec, "STALP");
    putText(row, "zip", rec, "ZIP");
    putText(row, "county_fips", rec, "STCNTY");
    putDouble(row, "latitude", rec, "LATITUDE");
    putDouble(row, "longitude", rec, "LONGITUDE");
    putFdicDate(row, "established_date", rec, "ESTYMD");
    putFdicDate(row, "effective_date", rec, "EFFDATE");
    putFdicDate(row, "inactive_date", rec, "ENDEFYMD");
    putLong(row, "total_assets_thousands", rec, "ASSET");
    putLong(row, "total_deposits_thousands", rec, "DEP");
    putText(row, "webaddr", rec, "WEBADDR");
    putText(row, "specialization_group", rec, "SPECGRPN");
    putFdicDate(row, "report_date", rec, "REPDTE");
  }
}
