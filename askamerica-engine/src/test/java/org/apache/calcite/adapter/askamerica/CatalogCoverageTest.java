/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Year;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Defect Register B2-3: {@code Catalog#coverage} previously had no {@code releaseMonth}
 * handling at all, so its declared ceiling for census.acs1_income (dataLag 1,
 * releaseMonth 9) overstated the real ceiling by one year whenever today falls before
 * September in the current calendar year — the same pullback
 * {@code DimensionIterator#resolveYearRange} (file/ module) already applied at ETL time.
 * This exercises the fix directly against the real system clock, no MCP server required.
 */
class CatalogCoverageTest {

  @Test void acs1IncomeCeilingMatchesReleaseMonthPullback() {
    ObjectNode cov = Catalog.coverage("census", "acs1_income");
    assertNotNull(cov, "expected a coverage node for census.acs1_income");
    assertNotNull(cov.get("last_year"), "expected a resolved last_year");

    int currentYear = Year.now(ZoneOffset.UTC).getValue();
    int currentMonth = LocalDate.now(ZoneOffset.UTC).getMonthValue();
    int dataLag = 1;
    int releaseMonth = 9;
    int expectedLastYear = currentYear - dataLag - (currentMonth < releaseMonth ? 1 : 0);

    assertEquals(expectedLastYear, cov.get("last_year").asInt());
  }
}
