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
 * Parses CDC WONDER Natality (expanded, 2016-2024) responses (database D149), grouped by
 * Year (B_1) only — the WONDER API rejects location grouping for National Vital Statistics
 * System data (mortality, natality), so this table is national-level. Measure: Births (M_002).
 */
public class CdcWonderNatalityResponseTransformer extends AbstractCdcWonderResponseTransformer {
  private static final GroupLevel[] LEVELS = {
      new GroupLevel("year"),
  };
  private static final String[] MEASURES = {"births"};

  @Override
  protected GroupLevel[] groupLevels() {
    return LEVELS;
  }

  @Override
  protected String[] measureColumns() {
    return MEASURES;
  }
}
