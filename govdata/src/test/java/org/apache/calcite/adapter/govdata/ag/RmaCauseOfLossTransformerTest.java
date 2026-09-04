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
package org.apache.calcite.adapter.govdata.ag;

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Streams a real RMA Cause of Loss bulk file end to end and checks the parsed
 * shape against the published record layout (30 fields + derived county_fips).
 */
@Tag("integration")
class RmaCauseOfLossTransformerTest {

  @Test void streamsRealCauseOfLossFile() throws Exception {
    RequestContext context = RequestContext.builder()
        .url("https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/Summary_of_Business/"
            + "cause_of_loss/colsom_2022.zip")
        .build();

    Iterator<Map<String, Object>> rows = new RmaCauseOfLossTransformer().fetchAndTransform(context);
    assertTrue(rows.hasNext(), "expected at least one row");

    Map<String, Object> first = rows.next();
    assertEquals(31, first.size(), "30 published fields + derived county_fips");
    assertThat(first.get("year"), notNullValue());
    assertThat(first.get("cause_of_loss_code"), notNullValue());
    assertThat(first.get("cause_of_loss_desc"), notNullValue());
    assertThat(first.get("month_of_loss_name"), notNullValue());
    assertThat(first.get("indemnity_amount"), notNullValue());
    assertEquals(5, ((String) first.get("county_fips")).length());

    long count = 1;
    while (rows.hasNext()) {
      Map<String, Object> row = rows.next();
      assertEquals(31, row.size());
      count++;
    }
    assertTrue(count > 100000, "2022 cause-of-loss file should have well over 100k rows, got " + count);
  }
}
