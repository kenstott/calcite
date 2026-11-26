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
 * EOIR "Asylum Decisions": immigration-court asylum decisions by outcome and fiscal year.
 */
public class EoirAsylumDecisionsTransformer extends EoirFiscalYearTransformer {

  @Override protected String linkTitle() {
    return "Asylum Decisions";
  }

  @Override protected String header() {
    return "FY Grants Denials Abandonment Not Adjudicated Withdrawn Admin Closure2 Total";
  }

  @Override protected String[] countColumns() {
    return new String[] {"grants", "denials", "abandonment", "not_adjudicated", "withdrawn",
        "admin_closure", "total_decisions"};
  }

  @Override protected void check(int fiscalYear, long[] counts) {
    long sum = 0;
    for (int i = 0; i < counts.length - 1; i++) {
      sum += counts[i];
    }
    if (sum != counts[counts.length - 1]) {
      throw new IllegalStateException("EOIR asylum decisions FY" + fiscalYear + ": outcomes sum to "
          + sum + " but printed total is " + counts[counts.length - 1]);
    }
  }
}
