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
package org.apache.calcite.adapter.govdata.econ;

/**
 * Transforms OMB's Historical Table 5.1 ("Budget Authority by Function and Subfunction") XLSX
 * into one JSON row per (year, budget function).
 *
 * <p>Table 5.1 is laid out exactly like Table 3.2 (outlays): the same function/subfunction
 * blocks, "Total, &lt;Function&gt;" rollup rows and fiscal-year columns including the "TQ"
 * transition-quarter column, which is skipped. Only the table number and the value column name
 * differ, so the parser is inherited from {@link FederalOutlaysByFunctionTransformer}.
 */
public class FederalBudgetAuthorityByFunctionTransformer extends FederalOutlaysByFunctionTransformer {

  @Override
  protected String tableNumber() {
    return "5.1";
  }

  @Override
  protected String valueColumn() {
    return "budget_authority_millions";
  }
}
