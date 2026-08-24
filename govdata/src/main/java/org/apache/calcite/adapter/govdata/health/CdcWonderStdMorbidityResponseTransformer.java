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

import org.w3c.dom.Element;

/**
 * Parses CDC WONDER Sexually Transmitted Disease Morbidity responses (database D127),
 * grouped by Year (B_1), State (B_2), then Disease (B_3); measures Case Count (M_1),
 * Population (M_2), and Rate per 100,000 (M_3).
 *
 * <p>The Disease level is a rollup hierarchy, not a flat list — e.g. "Total Syphilis" is
 * the parent of "Primary and Secondary Syphilis", which is itself the parent of "Primary
 * Syphilis" and "Secondary Syphilis". WONDER marks each disease cell's depth with an
 * {@code h=} attribute (h=1 is a top-level disease, h=2/h=3 are its descendants); this is
 * surfaced as {@code disease_hierarchy_level} so consumers can filter to one level instead
 * of double-counting by summing across parent and child rows.
 */
public class CdcWonderStdMorbidityResponseTransformer extends AbstractCdcWonderResponseTransformer {
  private static final GroupLevel[] LEVELS = {
      new GroupLevel("year"),
      new GroupLevel("state", "state_fips"),
      new GroupLevel("disease"),
  };
  private static final String[] MEASURES = {"cases", "population", "rate_per_100k"};
  private static final int DISEASE_LEVEL = 2;

  @Override
  protected GroupLevel[] groupLevels() {
    return LEVELS;
  }

  @Override
  protected String[] measureColumns() {
    return MEASURES;
  }

  @Override
  protected String extraAttribute(int level, Element labelCell) {
    if (level == DISEASE_LEVEL && labelCell.hasAttribute("h")) {
      return labelCell.getAttribute("h");
    }
    return super.extraAttribute(level, labelCell);
  }

  @Override
  protected String extraColumnName(int level) {
    return level == DISEASE_LEVEL ? "disease_hierarchy_level" : super.extraColumnName(level);
  }
}
