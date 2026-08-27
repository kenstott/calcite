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
 * Maps FDIC BankFind Suite {@code /banks/financials} call-report records into
 * {@code financials} rows — one row per (cert, report date). Windowed by a {@code REPDTE} date
 * range (calendar year); VERY LARGE nationwide (millions of rows all-history).
 */
public class FdicFinancialsTransformer extends AbstractFdicTransformer {

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    putLong(row, "cert", rec, "CERT");
    putFdicDate(row, "repdte", rec, "REPDTE");
    putLong(row, "total_assets_thousands", rec, "ASSET");
    putLong(row, "domestic_deposits_thousands", rec, "DEPDOM");
    putLong(row, "net_income_thousands", rec, "NETINC");
    putLong(row, "total_liabilities_thousands", rec, "LIAB");
    putInt(row, "num_employees", rec, "NUMEMP");
    putDouble(row, "equity_to_assets_pct", rec, "EQV");
    putDouble(row, "net_interest_margin_pct", rec, "NIMY");
    putLong(row, "interest_expense_thousands", rec, "EINTEXP");
    putLong(row, "cre_nonfarm_nonresidential_thousands", rec, "LNRENRES");
    putLong(row, "cre_construction_land_dev_thousands", rec, "LNRECONS");
    putLong(row, "cre_multifamily_thousands", rec, "LNREMULT");
    putLong(row, "total_risk_based_capital_thousands", rec, "RBC");
  }
}
