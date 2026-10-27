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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The CO-OPS Data API answers a station/year with nothing as an {@code error} object. Observed
 * products word it "No data was found", predictions "No Predictions data was found"; both are an
 * empty result, and any other error is a failed request.
 */
@Tag("unit")
class CoopsTransformerTest {

  private static RequestContext context(String type) {
    Map<String, String> dims = new HashMap<>();
    dims.put("type", type);
    dims.put("station_id", "8454000");
    return RequestContext.builder().url("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter")
        .dimensionValues(dims).build();
  }

  @Test void observedNoDataIsAnEmptyResult() {
    String response = "{\"error\":{\"message\":\"No data was found. This product may not be "
        + "offered at this station at the requested time.\"}}";
    assertEquals("[]", new CoopsTransformer().transform(response, context("coops_high_low")));
  }

  @Test void predictionsNoDataIsAnEmptyResult() {
    String response = "{\"error\":{\"message\":\"No Predictions data was found. Please make sure "
        + "the Datum input is valid.\"}}";
    assertEquals("[]",
        new CoopsTransformer().transform(response, context("coops_tide_predictions")));
  }

  @Test void anyOtherErrorStillFailsTheRequest() {
    String response = "{\"error\":{\"message\":\"Wrong Station ID: station not found\"}}";
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new CoopsTransformer().transform(response, context("coops_tide_predictions")));
    assertTrue(e.getMessage().contains("Wrong Station ID"), e.getMessage());
  }
}
