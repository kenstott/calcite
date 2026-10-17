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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link AcsCongressionalDistrictTransformer}. The rows are shaped like the
 * Census API response for the 2022 vintage (Alabama district 1, Connecticut "not defined").
 */
@Tag("unit")
class AcsCongressionalDistrictTransformerTest {

  private static final RequestContext CONTEXT = RequestContext.builder()
      .url("https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=congressional%20district:*")
      .build();

  @Test void dropsNotDefinedRowsAndKeepsDistricts() throws Exception {
    String response = "[[\"NAME\",\"B01003_001E\",\"state\",\"congressional district\"],"
        + "[\"Congressional District 1 (118th Congress), Alabama\",\"718903\",\"01\",\"01\"],"
        + "[\"Congressional Districts not defined (118th Congress), Connecticut\",\"0\",\"09\",\"ZZ\"],"
        + "[\"Delegate District (at Large) (118th Congress), District of Columbia\",\"670587\",\"11\",\"98\"]]";
    JsonNode out = new ObjectMapper().readTree(new AcsCongressionalDistrictTransformer()
        .transform(response, CONTEXT));
    assertEquals(2, out.size());
    assertEquals("01", out.get(0).get("congressional district").asText());
    assertEquals("718903", out.get(0).get("B01003_001E").asText());
    assertEquals("98", out.get(1).get("congressional district").asText());
  }

  @Test void rowWithoutDistrictFieldFailsLoudly() {
    String response = "[[\"NAME\",\"B01003_001E\",\"state\"],[\"Alabama\",\"5\",\"01\"]]";
    assertThrows(IllegalStateException.class,
        () -> new AcsCongressionalDistrictTransformer().transform(response, CONTEXT));
  }
}
