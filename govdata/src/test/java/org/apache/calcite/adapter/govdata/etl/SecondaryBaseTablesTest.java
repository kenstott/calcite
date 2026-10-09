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
package org.apache.calcite.adapter.govdata.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link GovDataModelVerificationRunner#secondaryBaseTables} names the tables a seed-generation
 * run must count beyond the primary schema. The runner probes only the first schema of a
 * connection, so a schema left out here ships in the seed with no row counts.
 */
@Tag("unit")
class SecondaryBaseTablesTest {

  private static Set<String> names(List<String[]> tables) {
    Set<String> names = new LinkedHashSet<String>();
    for (String[] t : tables) {
      names.add(t[0] + "." + t[1]);
    }
    return names;
  }

  @Test void everySchemaAfterTheFirstContributesItsBaseTables() {
    Set<String> names = names(GovDataModelVerificationRunner.secondaryBaseTables("sec, CFTC,ref"));

    assertTrue(names.contains("cftc.cot_disaggregated_futures"), names.toString());
    assertTrue(names.contains("ref.calendar"), names.toString());
  }

  @Test void thePrimarySchemaAndViewsAreLeftOut() {
    Set<String> names = names(GovDataModelVerificationRunner.secondaryBaseTables("sec,cftc,sec"));

    for (String name : names) {
      assertTrue(name.startsWith("cftc."), name);
    }
    assertFalse(names.isEmpty());
    // sec.financial_facts is a view, and sec is the primary either way.
    assertFalse(names(GovDataModelVerificationRunner.secondaryBaseTables("cftc,sec"))
        .contains("sec.financial_facts"));
  }

  @Test void aLoneSchemaHasNothingLeftToCount() {
    assertTrue(GovDataModelVerificationRunner.secondaryBaseTables("sec").isEmpty());
  }
}
