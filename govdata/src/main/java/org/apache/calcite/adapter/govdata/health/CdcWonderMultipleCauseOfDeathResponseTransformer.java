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
 * Parses CDC WONDER Multiple Cause of Death responses (databases D77, 1999-2020, and D157,
 * 2018-2024), grouped by Year (B_1) then MCD Drug/Alcohol Induced Cause (B_2) — national-level
 * only, since the WONDER API rejects location grouping for National Vital Statistics System
 * data. Measures: Deaths (M_1), Population (M_2), Crude Rate (M_3).
 *
 * <p>Unlike {@link CdcWonderCauseOfDeathResponseTransformer}'s ICD sub-chapter grouping, the
 * cause label here (e.g. "Drug poisonings (overdose) Unintentional (X40-X44)") already embeds
 * its ICD-10 code range in the text rather than exposing a separate {@code cd=} attribute, so
 * there is no separate code column.
 *
 * <p>This is a <em>multiple</em> cause count: a death is counted here if the cause appears
 * anywhere among its up to 20 listed causes, not only as the underlying cause — the standard
 * epidemiological definition used for drug overdose mortality surveillance, and the reason this
 * table (not {@code cdc_wonder_cause_of_death}, which is underlying-cause-only) is the one that
 * actually answers an opioid/drug-involvement question.
 */
public class CdcWonderMultipleCauseOfDeathResponseTransformer extends AbstractCdcWonderResponseTransformer {
  private static final GroupLevel[] LEVELS = {
      new GroupLevel("year"),
      new GroupLevel("cause"),
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
