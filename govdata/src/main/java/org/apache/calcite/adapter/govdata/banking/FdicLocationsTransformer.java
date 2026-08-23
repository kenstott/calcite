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
 * Maps FDIC BankFind Suite {@code /banks/locations} records into {@code locations} rows.
 * Snapshot of every FDIC-insured institution's branch/office locations; PK is {@code uninum}.
 */
public class FdicLocationsTransformer extends AbstractFdicTransformer {

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putLong(row, "uninum", rec, "UNINUM");
    putLong(row, "cert", rec, "CERT");
    putText(row, "office_name", rec, "OFFNAME");
    putBool(row, "is_main_office", rec, "MAINOFF");
    putText(row, "service_type", rec, "SERVTYPE_DESC");
    putText(row, "address", rec, "ADDRESS");
    putText(row, "city", rec, "CITY");
    putText(row, "state_abbr", rec, "STALP");
    putText(row, "zip", rec, "ZIP");
    putText(row, "county_fips", rec, "STCNTY");
    putText(row, "cbsa_name", rec, "CBSA");
    putDouble(row, "latitude", rec, "LATITUDE");
    putDouble(row, "longitude", rec, "LONGITUDE");
    putFdicDate(row, "established_date", rec, "ESTYMD");
    putFdicDate(row, "acquired_date", rec, "ACQDATE");
  }
}
