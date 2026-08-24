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
 * Parses CDC WONDER Vaccine Adverse Event Reporting System (VAERS) responses (database D8),
 * grouped by Year Reported (B_1) then State/Territory (B_2). Measures: Events Reported (M_1),
 * Percent of Total (M_2).
 */
public class CdcWonderVaersResponseTransformer extends AbstractCdcWonderResponseTransformer {
  private static final GroupLevel[] LEVELS = {
      new GroupLevel("year"),
      new GroupLevel("state"),
  };
  private static final String[] MEASURES = {"events", "pct_of_total"};

  @Override
  protected GroupLevel[] groupLevels() {
    return LEVELS;
  }

  @Override
  protected String[] measureColumns() {
    return MEASURES;
  }
}
