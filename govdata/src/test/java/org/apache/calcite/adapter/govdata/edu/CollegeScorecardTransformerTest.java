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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that both Scorecard transformers read the fields of the requested data year. */
@Tag("unit")
class CollegeScorecardTransformerTest {

  private static final RequestContext YEAR_2018 = RequestContext.builder()
      .url("http://localhost/schools.json")
      .dimensionValues(Collections.singletonMap("year", "2018"))
      .build();

  private static JsonNode parse(String json) throws Exception {
    return new ObjectMapper().readTree(json);
  }

  @Test void institutionRowHoldsTheRequestedYear() throws Exception {
    String response = "{\"results\":[{\"id\":100663,\"2018.school.name\":\"UAB\","
        + "\"school.locale\":12,\"2018.student.size\":13186,"
        + "\"2018.aid.pell_grant_rate\":\"PrivacySuppressed\"},"
        + "{\"id\":2,\"2018.school.name\":null,\"2018.student.size\":null}]}";

    JsonNode rows =
        parse(new CollegeScorecardResponseTransformer().transform(response, YEAR_2018));

    assertEquals(1, rows.size(), "an institution absent from the year's file yields no row");
    JsonNode row = rows.get(0);
    assertEquals(2018, row.get("year").asInt());
    assertEquals("UAB", row.get("name").asText());
    assertEquals(12, row.get("locale").asInt());
    assertEquals(13186, row.get("size").asInt());
    assertTrue(row.get("pell_grant_rate").isNull());
  }

  @Test void institutionResponseForAnotherYearFails() {
    String response = "{\"results\":[{\"id\":1,\"latest.student.size\":5}]}";

    assertThrows(RuntimeException.class,
        () -> new CollegeScorecardResponseTransformer().transform(response, YEAR_2018));
  }

  @Test void programRowsComeFromTheRequestedYear() throws Exception {
    String response = "{\"results\":[{\"2018.programs.cip_4_digit\":[{\"unit_id\":100663,"
        + "\"code\":\"0502\",\"credential\":{\"level\":3},"
        + "\"counts\":{\"ipeds_awards1\":5}}]},{\"2018.programs.cip_4_digit\":null},{}]}";

    JsonNode rows =
        parse(new CollegeScorecardProgramsResponseTransformer().transform(response, YEAR_2018));

    assertEquals(1, rows.size());
    assertEquals(2018, rows.get(0).get("year").asInt());
    assertEquals(5, rows.get(0).get("ipeds_awards").asInt());
  }

  @Test void programResponseForAnotherYearFails() {
    String response = "{\"results\":[{\"latest.programs.cip_4_digit\":[]}]}";

    assertThrows(RuntimeException.class,
        () -> new CollegeScorecardProgramsResponseTransformer().transform(response, YEAR_2018));
  }
}
