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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers a Defect Register finding on {@code research.nsf_herd_by_institution}: NCSES's own
 * "Total" column on the nonfederal-sources question sums only the five nonfederal sources
 * (excludes Federal entirely), but the transformer passed it through as bare
 * {@code funding_source="Total"}, reading as a grand total. {@link NsfHerdTransformer} now
 * relabels it {@code "Total nonfederal"}; the true grand total is that value plus the row
 * where {@code funding_source='Federal' AND federal_agency='Total'}. No mocked HTTP handle
 * exists for this class's ZIP-streaming path, so this hits the live NCSES endpoint like the
 * sibling live-API tests in this module.
 */
class NsfHerdTransformerTest {

  @Test @Tag("integration") void relabelsNonfederalTotalAndKeepsFederalTotalDistinct()
      throws Exception {
    String url = "https://ncses.nsf.gov/821/assets/0/files/higher_education_r_and_d_2022.zip";
    RequestContext context = RequestContext.builder().url(url).build();

    Iterator<Map<String, Object>> rows = new NsfHerdTransformer().fetchAndTransform(context);
    assertTrue(rows.hasNext(), "expected transformed rows from the live HERD 2022 ZIP");

    boolean sawTotalNonfederal = false;
    boolean sawFederalTotal = false;
    while (rows.hasNext()) {
      Map<String, Object> row = rows.next();
      Object fundingSource = row.get("funding_source");
      assertFalse("Total".equals(fundingSource),
          "funding_source must never be bare 'Total': " + row);
      if ("Total nonfederal".equals(fundingSource)) {
        sawTotalNonfederal = true;
      }
      if ("Federal".equals(fundingSource) && "Total".equals(row.get("federal_agency"))) {
        sawFederalTotal = true;
      }
    }
    assertTrue(sawTotalNonfederal, "expected at least one 'Total nonfederal' row");
    assertTrue(sawFederalTotal, "expected at least one Federal/Total (all-agency) row");
  }
}
