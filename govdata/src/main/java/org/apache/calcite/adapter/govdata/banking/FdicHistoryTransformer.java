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
    putDouble(row, "latitude", rec, "OFF_LATITUDE");
    putDouble(row, "longitude", rec, "OFF_LONGITUDE");
    putFdicDate(row, "proc_date", rec, "PROCDATE");
  }
}
