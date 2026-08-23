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
 * Maps FDIC BankFind Suite {@code /banks/summary} records into {@code summary} rows — annual
 * industry aggregates, either a per-state row or (when {@code STALP} is absent) a nationwide
 * rollup. Windowed by the literal {@code YEAR} field, which FDIC returns as a numeric string
 * (e.g. {@code "1934"}); {@link #putInt} parses it. Many fields are legitimately null in
 * 1930s-40s records because call reports genuinely did not collect them then.
 */
public class FdicSummaryTransformer extends AbstractFdicTransformer {

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putInt(row, "year", rec, "YEAR");
    putText(row, "state_abbr", rec, "STALP");
    putInt(row, "num_banks", rec, "BANKS");
    putLong(row, "net_income_thousands", rec, "NETINC");
    putLong(row, "total_interest_income_thousands", rec, "INTINC");
    putLong(row, "total_liabilities_thousands", rec, "LIAB");
    putLong(row, "total_equity_thousands", rec, "EQUP");
  }
}
