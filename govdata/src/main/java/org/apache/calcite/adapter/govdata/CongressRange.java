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
package org.apache.calcite.adapter.govdata;

import java.util.Map;

/**
 * Derives the Congress range a schema's {@code congress_range} dimension iterates from the
 * standard {@code startYear}/{@code endYear} operand.
 *
 * <p>Congress.gov and GovInfo address data by Congress, but the only demarc a caller ever sets is
 * GOVDATA_START_YEAR/GOVDATA_END_YEAR, same as every other schema. Congress N spans
 * [1789+2(N-1), 1791+2(N-1)), so year Y falls in Congress (Y-1789)/2 + 1. The result is exposed
 * as the system properties {@code GOVDATA_START_CONGRESS} / {@code GOVDATA_END_CONGRESS}, which the
 * schema YAML resolves as {@code ${GOVDATA_START_CONGRESS:117}}.
 *
 * <p>Must run from {@code GovDataSubSchemaFactory.deriveEarlyProperties}, before the schema YAML
 * is parsed: a property set after the YAML's placeholders are resolved cannot affect the build.
 */
public final class CongressRange {

  private CongressRange() {
  }

  /**
   * Sets each Congress property from the operand's matching year, independently: a run that
   * supplies only an end year still gets the schema's own default start.
   */
  public static void deriveEarlyProperties(Map<String, Object> operand) {
    Object startYearObj = operand.get("startYear");
    Object endYearObj = operand.get("endYear");
    if (startYearObj != null) {
      int startYear = Integer.parseInt(String.valueOf(startYearObj));
      System.setProperty("GOVDATA_START_CONGRESS", String.valueOf(congressOf(startYear)));
    }
    if (endYearObj != null) {
      int endYear = Integer.parseInt(String.valueOf(endYearObj));
      System.setProperty("GOVDATA_END_CONGRESS", String.valueOf(congressOf(endYear)));
    }
  }

  /** The Congress covering calendar year {@code year}. */
  static int congressOf(int year) {
    return (year - 1789) / 2 + 1;
  }
}
