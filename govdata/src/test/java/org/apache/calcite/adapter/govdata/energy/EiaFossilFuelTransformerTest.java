/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the crude-oil-only contract of {@code eia_fossil_fuel_production}: the transformer
 * consumes the single {@code petroleum/crd/crpdn} response and every emitted row's
 * {@code fuel_type} is 'Crude Oil' (or 'ANS Crude Oil' for Alaska North Slope). Natural gas
 * comes from a separate endpoint that this table does not fetch — any prior union claim was
 * unimplemented.
 */
class EiaFossilFuelTransformerTest {

  private static final ObjectMapper M = new ObjectMapper();

  @Test @Tag("unit") void mapsCrudeOilRowFields() throws Exception {
    String crude = "[{\"period\":\"2023-01\",\"duoarea\":\"STX\",\"product-name\":\"Crude Oil\","
        + "\"process\":\"FPF\",\"process-name\":\"Field Production\","
        + "\"series\":\"MCRFPTX1\",\"series-description\":\"Texas Crude Oil Production\","
        + "\"value\":\"120000\",\"units\":\"MBBL\"}]";

    EiaFossilFuelTransformer transformer = new EiaFossilFuelTransformer();
    RequestContext ctx = RequestContext.builder().url("https://api.eia.gov/v2/test").build();
    JsonNode result = M.readTree(transformer.transform(crude, ctx));

    assertEquals(1, result.size());
    JsonNode row = result.get(0);
    assertEquals("Crude Oil", row.get("fuel_type").asText());
    assertEquals("TX", row.get("state_abbr").asText());
    assertEquals(2023, row.get("production_year").asInt());
    assertEquals(1, row.get("production_month").asInt());
    assertEquals(120000.0, row.get("production_volume").asDouble());
    assertEquals("MBBL", row.get("production_unit").asText());
    assertEquals("MCRFPTX1", row.get("series_id").asText());
  }

  @Test @Tag("unit") void mapsAlaskaNorthSlopeCrudeVariant() throws Exception {
    String ans = "[{\"period\":\"2024-06\",\"duoarea\":\"SAK\",\"product-name\":\"ANS Crude Oil\","
        + "\"process\":\"FPF\",\"process-name\":\"Field Production\","
        + "\"series\":\"MANFPAK1\",\"series-description\":\"Alaska North Slope Crude Oil Production\","
        + "\"value\":\"14100\",\"units\":\"MBBL\"}]";

    EiaFossilFuelTransformer transformer = new EiaFossilFuelTransformer();
    RequestContext ctx = RequestContext.builder().url("https://api.eia.gov/v2/test").build();
    JsonNode result = M.readTree(transformer.transform(ans, ctx));

    assertEquals(1, result.size());
    // "ANS Crude Oil" matches the "crude"/"petroleum" branch → 'Crude Oil'
    assertEquals("Crude Oil", result.get(0).get("fuel_type").asText());
    assertEquals("AK", result.get(0).get("state_abbr").asText());
  }
}
