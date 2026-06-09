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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prototype validation for {@link EiaNgStorageBulkTransformer}: the v1-series parse + data-array
 * explode (unit), and the full bulk download → stream → explode path (integration).
 */
class EiaNgStorageBulkTransformerTest {

  private static final ObjectMapper M = new ObjectMapper();

  @Test @Tag("unit") void explodesStorageSeriesAndParsesDimensions() throws Exception {
    String storage = "{\"series_id\":\"NG.NW2_EPG0_SWO_R31_BCF.W\","
        + "\"name\":\"Weekly East Region Natural Gas Working Underground Storage, Weekly\","
        + "\"unitsshort\":\"BCF\",\"f\":\"W\","
        + "\"data\":[[\"20260529\",250],[\"20260522\",240]]}";

    List<Map<String, Object>> rows =
        EiaNgStorageBulkTransformer.rowsForSeries(M.readTree(storage));

    assertEquals(2, rows.size(), "one row per data point (explode)");
    Map<String, Object> r0 = rows.get(0);
    assertEquals("NG.NW2_EPG0_SWO_R31_BCF.W", r0.get("series_id"));
    assertEquals("2026-05-29", r0.get("report_date"));
    assertEquals(2026, r0.get("storage_year"));
    assertEquals("R31", r0.get("eia_region_code"));
    assertEquals("East", r0.get("region"));
    assertEquals("SWO", r0.get("storage_type_code"));
    assertEquals("Working Gas", r0.get("storage_type"));
    assertEquals(250.0, r0.get("volume_bcf"));
    assertEquals("BCF", r0.get("units"));
    assertEquals("2026-05-22", rows.get(1).get("report_date"));
  }

  @Test @Tag("unit") void nonStorageSeriesIsFilteredOut() throws Exception {
    String prod = "{\"series_id\":\"NG.N9070US2.M\",\"name\":\"US NG Marketed Production\","
        + "\"data\":[[\"202604\",3000]]}";
    assertTrue(EiaNgStorageBulkTransformer.rowsForSeries(M.readTree(prod)).isEmpty(),
        "only weekly storage-by-region series should produce rows");
  }

  /** Full path: download the real NG.zip, stream-explode, sanity-check the storage rows. */
  @Test @Tag("integration") void streamsRealBulkFile() throws Exception {
    RequestContext ctx = RequestContext.builder()
        .url("https://www.eia.gov/opendata/bulk/NG.zip")
        .build();
    Iterator<Map<String, Object>> it =
        new EiaNgStorageBulkTransformer().fetchAndTransform(ctx);

    int rows = 0;
    Set<Object> regions = new HashSet<Object>();
    int recent = 0;
    while (it.hasNext()) {
      Map<String, Object> r = it.next();
      rows++;
      regions.add(r.get("eia_region_code"));
      Object year = r.get("storage_year");
      if (year instanceof Integer && (Integer) year >= 2025) {
        recent++;
      }
      // spot-check shape on the first row
      if (rows == 1) {
        assertTrue(r.get("volume_bcf") instanceof Double, "volume_bcf must be numeric");
        assertTrue(((String) r.get("report_date")).matches("\\d{4}-\\d{2}-\\d{2}"));
      }
    }
    assertTrue(rows > 1000, "expected thousands of weekly storage rows, got " + rows);
    assertTrue(regions.size() >= 5, "expected multiple storage regions, got " + regions);
    assertTrue(recent > 0, "expected current (>=2025) storage rows from the cumulative file");
  }
}
