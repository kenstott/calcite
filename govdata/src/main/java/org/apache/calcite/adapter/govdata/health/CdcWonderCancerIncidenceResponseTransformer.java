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
 * Parses CDC WONDER United States Cancer Statistics incidence responses (database D205),
 * grouped by Year (B_1) then State (B_2), measure Case Count (M_1).
 */
public class CdcWonderCancerIncidenceResponseTransformer extends AbstractCdcWonderResponseTransformer {
  private static final GroupLevel[] LEVELS = {
      new GroupLevel("year"),
      new GroupLevel("state", "state_fips"),
  };
  private static final String[] MEASURES = {"cases"};

  @Override
  protected GroupLevel[] groupLevels() {
    return LEVELS;
  }

  @Override
  protected String[] measureColumns() {
    return MEASURES;
  }
}
