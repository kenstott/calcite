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
package org.apache.calcite.adapter.govdata.crime;

/**
 * EOIR "Total Asylum Applications": affirmative, defensive and total applications filed in the
 * immigration courts and applications pending at fiscal-year end.
 */
public class EoirAsylumApplicationsTransformer extends EoirFiscalYearTransformer {

  @Override protected String linkTitle() {
    return "Total Asylum Applications";
  }

  @Override protected String header() {
    return "Fiscal Year Affirmative Filed Defensive Filed Total Filed Remain Pending2";
  }

  @Override protected String[] countColumns() {
    return new String[] {"affirmative_filed", "defensive_filed", "total_filed", "pending"};
  }

  @Override protected void check(int fiscalYear, long[] counts) {
    if (counts[0] + counts[1] != counts[2]) {
      throw new IllegalStateException("EOIR asylum applications FY" + fiscalYear
          + ": affirmative " + counts[0] + " + defensive " + counts[1] + " != total " + counts[2]);
    }
  }
}
