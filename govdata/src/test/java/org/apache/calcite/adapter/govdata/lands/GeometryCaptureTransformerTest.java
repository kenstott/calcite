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
package org.apache.calcite.adapter.govdata.lands;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@code geometry_wkt} capture (D-054) for {@link UsfsForestBoundaryTransformer},
 * {@link NpsUnitBoundaryTransformer}, and {@link BlmFieldOfficeTransformer} against
 * ArcGIS response shapes matching what each live service returns with
 * {@code outSR=4326}: a WGS84 polygon (rings) for the USFS and NPS boundary layers,
 * and a WGS84 point (x/y) for the BLM office-points layer.
 */
@Tag("unit")
class GeometryCaptureTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String SQUARE_RING_GEOMETRY =
      "{\"rings\": [[[-109.5, 38.5], [-109.5, 39.0], [-109.0, 39.0], "
          + "[-109.0, 38.5], [-109.5, 38.5]]]}";

  @Test void usfsForestBoundaryCapturesPolygonWkt() throws Exception {
    String response = "{\"features\": [{"
        + "\"attributes\": {\"forestnumber\": \"02\", \"forestname\": \"Test Forest\", "
        + "\"region\": \"01\", \"gis_acres\": 100.0}, "
        + "\"geometry\": " + SQUARE_RING_GEOMETRY
        + "}]}";

    String result = new UsfsForestBoundaryTransformer().transform(response, null);
    JsonNode row = MAPPER.readTree(result).get(0);

    assertEquals("02", row.get("forest_id").asText());
    JsonNode wkt = row.get("geometry_wkt");
    assertNotNull(wkt);
    assertTrue(wkt.asText().startsWith("POLYGON"), "expected a POLYGON WKT, got: " + wkt.asText());
  }

  @Test void npsUnitBoundaryCapturesPolygonWktAlongsideAreaCorrection() throws Exception {
    String response = "{\"features\": [{"
        + "\"attributes\": {\"UNIT_CODE\": \"TEST\", \"UNIT_NAME\": \"Test Park\", "
        + "\"UNIT_TYPE\": \"National Parks\", \"STATE\": \"UT\", \"REGION\": \"Intermountain\", "
        + "\"Shape__Area\": 5000000.0}, "
        + "\"geometry\": " + SQUARE_RING_GEOMETRY
        + "}]}";

    String result = new NpsUnitBoundaryTransformer().transform(response, null);
    JsonNode row = MAPPER.readTree(result).get(0);

    assertEquals("TEST", row.get("unit_code").asText());
    assertNotNull(row.get("gross_acres"));
    JsonNode wkt = row.get("geometry_wkt");
    assertNotNull(wkt);
    assertTrue(wkt.asText().startsWith("POLYGON"), "expected a POLYGON WKT, got: " + wkt.asText());
  }

  @Test void blmFieldOfficeCapturesPointWkt() throws Exception {
    String response = "{\"features\": [{"
        + "\"attributes\": {\"ADM_UNIT_CD\": \"UTMO\", \"ADMU_NAME\": \"Moab Field Office\", "
        + "\"BLM_ORG_TYPE\": \"Field Office\", \"ADMIN_ST\": \"UT\"}, "
        + "\"geometry\": {\"x\": -109.549, \"y\": 38.573}"
        + "}]}";

    String result = new BlmFieldOfficeTransformer().transform(response, null);
    JsonNode row = MAPPER.readTree(result).get(0);

    assertEquals("UTMO", row.get("office_code").asText());
    assertEquals("POINT (-109.549 38.573)", row.get("geometry_wkt").asText());
  }

  @Test void blmFieldOfficeToleratesMissingGeometry() throws Exception {
    String response = "{\"features\": [{"
        + "\"attributes\": {\"ADM_UNIT_CD\": \"UTMO\", \"ADMU_NAME\": \"Moab Field Office\", "
        + "\"BLM_ORG_TYPE\": \"Field Office\", \"ADMIN_ST\": \"UT\"}"
        + "}]}";

    String result = new BlmFieldOfficeTransformer().transform(response, null);
    JsonNode row = MAPPER.readTree(result).get(0);

    assertEquals("UTMO", row.get("office_code").asText());
    assertTrue(row.path("geometry_wkt").isNull());
  }
}
