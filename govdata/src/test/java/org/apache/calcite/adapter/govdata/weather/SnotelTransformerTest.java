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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Shape of the NRCS AWDB responses and how {@link SnotelTransformer} flattens them. */
@Tag("unit")
class SnotelTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static RequestContext context(String type) {
    Map<String, String> dims = new HashMap<>();
    dims.put("type", type);
    dims.put("state_abbr", "CO");
    dims.put("year", "2026");
    return RequestContext.builder().url("https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data")
        .dimensionValues(dims).build();
  }

  @Test void stationRowCarriesHucPrefixesAndOpenEndDateIsNull() throws Exception {
    String response = "[{\"stationTriplet\":\"713:CO:SNTL\",\"stationId\":\"713\","
        + "\"stateCode\":\"CO\",\"networkCode\":\"SNTL\",\"name\":\"Red Mountain Pass\","
        + "\"countyName\":\"San Juan\",\"huc\":\"140801040103\",\"elevation\":11060.0,"
        + "\"latitude\":37.89168,\"longitude\":-107.71389,\"shefId\":\"RMPC2\","
        + "\"beginDate\":\"1979-10-01 00:00\",\"endDate\":\"2100-01-01 00:00\"}]";
    JsonNode row = MAPPER.readTree(
        new SnotelTransformer().transform(response, context("snotel_station"))).get(0);
    assertEquals("713:CO:SNTL", row.get("station_id").asText());
    assertEquals("140801040103", row.get("huc12").asText());
    assertEquals("14080104", row.get("huc8").asText());
    assertEquals("140801", row.get("huc6").asText());
    assertEquals("1979-10-01", row.get("begin_date").asText());
    assertTrue(row.get("end_date").isNull());
  }

  @Test void dailyElementsAreMergedPerStationDateWithPercentOfMedian() throws Exception {
    String response = "[{\"stationTriplet\":\"713:CO:SNTL\",\"data\":["
        + "{\"stationElement\":{\"elementCode\":\"PREC\"},\"values\":"
        + "[{\"date\":\"2026-04-01\",\"value\":20.4,\"median\":24.6}]},"
        + "{\"stationElement\":{\"elementCode\":\"SNWD\"},\"values\":"
        + "[{\"date\":\"2026-04-01\",\"value\":34}]},"
        + "{\"stationElement\":{\"elementCode\":\"WTEQ\"},\"values\":"
        + "[{\"date\":\"2026-04-01\",\"value\":6.6,\"median\":22.6},"
        + "{\"date\":\"2026-08-01\",\"value\":0.0,\"median\":0.0}]}]}]";
    JsonNode rows = MAPPER.readTree(
        new SnotelTransformer().transform(response, context("snotel_daily")));
    assertEquals(2, rows.size());
    JsonNode apr = rows.get(0);
    assertEquals("2026-04-01", apr.get("date").asText());
    assertEquals(2026, apr.get("year").asInt());
    assertEquals("CO", apr.get("state_abbr").asText());
    assertEquals(6.6, apr.get("wteq_in").asDouble(), 1e-9);
    assertEquals(22.6, apr.get("wteq_median_in").asDouble(), 1e-9);
    assertEquals(29.2, apr.get("wteq_pct_median").asDouble(), 1e-9);
    assertEquals(34.0, apr.get("snwd_in").asDouble(), 1e-9);
    assertEquals(20.4, apr.get("prec_accum_in").asDouble(), 1e-9);
    assertEquals(24.6, apr.get("prec_accum_median_in").asDouble(), 1e-9);
    JsonNode aug = rows.get(1);
    assertTrue(aug.get("wteq_pct_median").isNull(), "median 0 leaves the ratio undefined");
    assertTrue(aug.get("snwd_in").isNull());
  }

  @Test void unexpectedElementFailsTheRequest() {
    String response = "[{\"stationTriplet\":\"1:CO:SNTL\",\"data\":["
        + "{\"stationElement\":{\"elementCode\":\"TOBS\"},\"values\":"
        + "[{\"date\":\"2026-04-01\",\"value\":1}]}]}]";
    assertThrows(IllegalStateException.class,
        () -> new SnotelTransformer().transform(response, context("snotel_daily")));
  }
}
