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
package org.apache.calcite.adapter.govdata.fiscal;

/**
 * DataProvider for {@code usaspending_contract_recipients_by_district} — the top 100
 * recipients by obligated dollars for each place-of-performance congressional district
 * and fiscal year, ranked over procurement contract and IDV award types only.
 *
 * <p>Same endpoint, request shape and row layout as
 * {@link UsaSpendingDistrictRecipientsProvider}; only the {@code award_type_codes}
 * filter differs. The ranking is computed server-side over contracts alone, so a
 * district's top 100 contractors are not crowded out by grant, direct-payment or loan
 * recipients that an all-award-types ranking mixes in.
 */
public class UsaSpendingDistrictContractRecipientsProvider
    extends UsaSpendingDistrictRecipientsProvider {

  /** Procurement contract and IDV codes — the same set as
   * {@link UsaSpendingDistrictProvider}'s {@code obligated_amount_contracts}. */
  private static final String CONTRACT_AWARD_TYPE_CODES =
      "\"A\",\"B\",\"C\",\"D\",\"IDV_A\",\"IDV_B\",\"IDV_C\",\"IDV_D\",\"IDV_E\"";

  @Override protected String tableName() {
    return "usaspending_contract_recipients_by_district";
  }

  @Override protected String recipientAwardTypeCodes() {
    return CONTRACT_AWARD_TYPE_CODES;
  }
}
