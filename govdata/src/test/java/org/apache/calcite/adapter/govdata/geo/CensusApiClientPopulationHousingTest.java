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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link CensusApiClient#parsePopulationHousingResponse}, the join-by-geography-
 * code logic that enriches {@code geo.gazetteer_zctas}/{@code geo.gazetteer_places} with ACS
 * 5-year population and housing_units (D-048) — the Gazetteer source files carry no such
 * columns themselves.
 */
@Tag("unit")
class CensusApiClientPopulationHousingTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CensusApiClient client(Path tempDir) {
    return new CensusApiClient("test-api-key", tempDir.toFile());
  }

  private ArrayNode acsResponse(String... headerAndRows) throws IOException {
    // Each element of headerAndRows is one JSON array row, e.g. "[\"B01003_001E\",...]".
    ArrayNode result = MAPPER.createArrayNode();
    for (String row : headerAndRows) {
      result.add((ArrayNode) MAPPER.readTree(row));
    }
    return result;
  }

  @Test void zctaResponseJoinsByGeoIdAlone(@TempDir Path tempDir) throws IOException {
    JsonNode response = acsResponse(
        "[\"B01003_001E\",\"B25001_001E\",\"zip code tabulation area\"]",
        "[\"27004\",\"16975\",\"10001\"]",
        "[\"19180\",\"9625\",\"90210\"]");

    Map<String, int[]> result =
        client(tempDir).parsePopulationHousingResponse(response, "zip code tabulation area", null);

    assertThat(result.size(), is(2));
    assertThat(result.get("10001"), notNullValue());
    assertThat(result.get("10001")[0], is(27004));
    assertThat(result.get("10001")[1], is(16975));
    assertThat(result.get("90210")[0], is(19180));
    assertThat(result.get("90210")[1], is(9625));
  }

  @Test void placeResponseJoinsByStatePrefixPlusPlaceCode(@TempDir Path tempDir) throws IOException {
    JsonNode response = acsResponse(
        "[\"NAME\",\"B01003_001E\",\"B25001_001E\",\"state\",\"place\"]",
        "[\"Chicago city, Illinois\",\"2721914\",\"1258704\",\"17\",\"14000\"]");

    Map<String, int[]> result =
        client(tempDir).parsePopulationHousingResponse(response, "place", "state");

    assertThat(result.size(), is(1));
    // 7-digit key: 2-digit state FIPS + 5-digit place code, matching gazetteer_places.place_fips
    assertThat(result.get("1714000"), notNullValue());
    assertThat(result.get("1714000")[0], is(2721914));
    assertThat(result.get("1714000")[1], is(1258704));
  }

  @Test void nullAcsCellsAreSkippedNotZeroed(@TempDir Path tempDir) throws IOException {
    JsonNode response = acsResponse(
        "[\"B01003_001E\",\"B25001_001E\",\"zip code tabulation area\"]",
        "[null,\"500\",\"00601\"]",
        "[\"1200\",null,\"00602\"]",
        "[\"1300\",\"600\",\"00603\"]");

    Map<String, int[]> result =
        client(tempDir).parsePopulationHousingResponse(response, "zip code tabulation area", null);

    // Suppressed cells stay unmapped so the gazetteer row correctly keeps NULL rather than 0
    assertThat(result.containsKey("00601"), is(false));
    assertThat(result.containsKey("00602"), is(false));
    assertThat(result.get("00603")[0], is(1300));
    assertThat(result.get("00603")[1], is(600));
  }

  @Test void malformedHeaderReturnsEmptyMapWithoutThrowing(@TempDir Path tempDir) throws IOException {
    JsonNode response = acsResponse(
        "[\"NAME\",\"state\"]",
        "[\"Nowhere\",\"99\"]");

    Map<String, int[]> result =
        client(tempDir).parsePopulationHousingResponse(response, "place", "state");

    assertThat(result.size(), is(0));
  }

  @Test void emptyOrShortResponseReturnsEmptyMap(@TempDir Path tempDir) {
    Map<String, int[]> onlyHeader =
        client(tempDir).parsePopulationHousingResponse(
            MAPPER.createArrayNode(), "zip code tabulation area", null);
    assertThat(onlyHeader.size(), is(0));

    Map<String, int[]> nullResponse =
        client(tempDir).parsePopulationHousingResponse(null, "zip code tabulation area", null);
    assertThat(nullResponse.size(), is(0));
  }
}
