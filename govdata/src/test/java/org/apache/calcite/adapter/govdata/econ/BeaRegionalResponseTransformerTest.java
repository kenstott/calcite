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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BeaRegionalResponseTransformer}.
 */
@Tag("unit")
class BeaRegionalResponseTransformerTest {

  private static final String RESPONSE = "{\"BEAAPI\":{\"Results\":{\"Data\":["
      + "{\"GeoFips\":\"01001\",\"DataValue\":\"8271\"},"
      + "{\"GeoFips\":\"01003\",\"DataValue\":\"0\",\"NoteRef\":\"(D)\"},"
      + "{\"GeoFips\":\"01005\",\"DataValue\":\"0\",\"NoteRef\":\"(NA) 9 *\"},"
      + "{\"GeoFips\":\"01007\",\"DataValue\":\"0\",\"NoteRef\":\"*\"},"
      + "{\"GeoFips\":\"01009\",\"DataValue\":\"0\"}"
      + "]}}}";

  @Test void testMarkerRowsBecomeNullWithFlag() throws Exception {
    JsonNode rows = new ObjectMapper().readTree(new BeaRegionalResponseTransformer().transform(
        RESPONSE, RequestContext.builder().url("https://apps.bea.gov/api/data").build()));

    assertEquals("8271", rows.get(0).get("DataValue").asText());
    assertFalse(rows.get(0).has("ValueFlag"));
    assertTrue(rows.get(1).get("DataValue").isNull());
    assertEquals("(D)", rows.get(1).get("ValueFlag").asText());
    assertTrue(rows.get(2).get("DataValue").isNull());
    assertEquals("(NA)", rows.get(2).get("ValueFlag").asText());
  }

  @Test void testGenuineZeroIsKept() throws Exception {
    JsonNode rows = new ObjectMapper().readTree(new BeaRegionalResponseTransformer().transform(
        RESPONSE, RequestContext.builder().url("https://apps.bea.gov/api/data").build()));

    assertEquals("0", rows.get(3).get("DataValue").asText());
    assertFalse(rows.get(3).has("ValueFlag"));
    assertEquals("0", rows.get(4).get("DataValue").asText());
    assertFalse(rows.get(4).has("ValueFlag"));
  }
}
