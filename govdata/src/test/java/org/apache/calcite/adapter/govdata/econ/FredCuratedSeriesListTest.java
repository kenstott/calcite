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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.GovDataUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * econ_reference.fred_curated_series must carry metadata for exactly the series that
 * econ.fred_indicators ingests, or fred_indicators_enriched silently drops the difference.
 */
@Tag("unit")
public class FredCuratedSeriesListTest {

  @Test public void testCuratedSeriesListsMatch() {
    List<String> ingested = seriesDimension("/econ/econ-schema.yaml", EconSchemaFactory.class,
        "fred_indicators");
    List<String> described = seriesDimension("/econ/econ-reference-schema.yaml",
        EconSchemaFactory.class, "fred_curated_series");
    assertFalse(ingested.isEmpty(), "fred_indicators has no series dimension");
    assertEquals(ingested, described,
        "fred_curated_series series dimension drifted from fred_indicators");
  }

  @SuppressWarnings("unchecked")
  private static List<String> seriesDimension(String resource, Class<?> anchor, String table) {
    for (Map<String, Object> t : GovDataUtils.loadTableDefinitions(anchor, resource)) {
      if (table.equals(t.get("name"))) {
        Map<String, Object> dims = (Map<String, Object>) t.get("dimensions");
        assertNotNull(dims, table + " has no dimensions");
        return new ArrayList<String>((List<String>) dims.get("series"));
      }
    }
    throw new AssertionError(table + " not found in " + resource);
  }
}
