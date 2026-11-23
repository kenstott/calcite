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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link AcsHealthInsuranceTransformer}. */
@Tag("unit")
class AcsHealthInsuranceTransformerTest {

  private static final String DETAILED =
      "https://api.census.gov/data/2022/acs/acs5?get=NAME,B27001_001E,B27001_001M&for=state:*&key=K";

  @Test void subjectUrlSwapsDatasetAndVariables() {
    String url = AcsHealthInsuranceTransformer.subjectUrl(DETAILED);
    assertEquals("https://api.census.gov/data/2022/acs/acs5/subject?get=NAME,S2703_C02_001E,"
        + "S2703_C02_001M,S2704_C02_001E,S2704_C02_001M&for=state:*&key=K", url);
  }

  @Test void preSubjectVintageGetsExplicitNulls() throws Exception {
    String url = DETAILED.replace("/2022/", "/2016/");
    String response = "[[\"NAME\",\"B27001_001E\",\"state\"],[\"California\",\"100\",\"06\"]]";
    RequestContext ctx = RequestContext.builder().url(url).build();
    String out = new AcsHealthInsuranceTransformer().transform(response, ctx);
    JsonNode row = new ObjectMapper().readTree(out).get(0);
    assertEquals("100", row.get("B27001_001E").asText());
    for (String v : AcsHealthInsuranceTransformer.SUBJECT_VARIABLES) {
      assertTrue(row.has(v) && row.get(v).isNull(), v);
    }
  }
}
