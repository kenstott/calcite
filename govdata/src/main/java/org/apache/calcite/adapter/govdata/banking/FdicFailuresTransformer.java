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
 * Maps FDIC BankFind Suite {@code /banks/failures} records into {@code failures} rows. PK is
 * {@code id}; windowed by {@code FAILYR}. {@code cert} is nullable — some very old records carry
 * no CERT.
 */
public class FdicFailuresTransformer extends AbstractFdicTransformer {

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putInt(row, "id", rec, "ID");
    putLong(row, "cert", rec, "CERT");
    putText(row, "institution_name", rec, "NAME");
    putText(row, "city", rec, "CITY");
    putText(row, "state_abbr", rec, "PSTALP");
    putFdicDate(row, "fail_date", rec, "FAILDATE");
    putText(row, "resolution_type", rec, "RESTYPE1");
    putLong(row, "deposits_at_failure_thousands", rec, "QBFDEP");
    putLong(row, "assets_at_failure_thousands", rec, "QBFASSET");
    putText(row, "acquirer_name", rec, "BIDNAME");
    putText(row, "acquirer_city", rec, "BIDCITY");
    putText(row, "acquirer_state_abbr", rec, "BIDSTATE");
    putDouble(row, "cost_thousands", rec, "COST");
  }
}
