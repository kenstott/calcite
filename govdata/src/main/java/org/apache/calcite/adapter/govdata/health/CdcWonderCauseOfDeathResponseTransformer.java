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
package org.apache.calcite.adapter.govdata.health;

/**
 * Parses CDC WONDER Underlying Cause of Death, 1999-2020 (database D76) responses, grouped
 * by Year (B_1) then ICD-10 Sub-Chapter (B_2) — national-level only, since the WONDER API
 * rejects location grouping for National Vital Statistics System data. Measures: Deaths
 * (M_1), Population (M_2), Crude Rate (M_3).
 */
public class CdcWonderCauseOfDeathResponseTransformer extends AbstractCdcWonderResponseTransformer {
  private static final GroupLevel[] LEVELS = {
      new GroupLevel("year"),
      new GroupLevel("icd_subchapter", "icd_subchapter_code"),
  };
  private static final String[] MEASURES = {"deaths", "population", "crude_rate"};

  @Override
  protected GroupLevel[] groupLevels() {
    return LEVELS;
  }

  @Override
  protected String[] measureColumns() {
    return MEASURES;
  }
}
