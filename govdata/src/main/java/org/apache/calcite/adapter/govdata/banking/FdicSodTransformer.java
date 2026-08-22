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
 * Maps FDIC BankFind Suite {@code /banks/sod} (Summary of Deposits) records into {@code sod}
 * rows — one row per branch per year, with the branch's and its owning institution's deposit
 * totals. Windowed by the literal {@code YEAR} field (not a range filter).
 */
public class FdicSodTransformer extends AbstractFdicTransformer {

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putLong(row, "uninum", rec, "UNINUMBR");
    putLong(row, "cert", rec, "CERT");
    putText(row, "office_name", rec, "NAMEBR");
    putText(row, "address", rec, "ADDRESBR");
    putText(row, "city", rec, "CITY2BR");
    putText(row, "state_abbr", rec, "STALPBR");
    putText(row, "county_fips", rec, "STCNTYBR");
    putText(row, "zip", rec, "ZIPBR");
    putDouble(row, "latitude", rec, "SIMS_LATITUDE");
    putDouble(row, "longitude", rec, "SIMS_LONGITUDE");
    putLong(row, "deposits_branch_thousands", rec, "DEPSUMBR");
    putLong(row, "deposits_institution_total_thousands", rec, "DEPSUM");
  }
}
