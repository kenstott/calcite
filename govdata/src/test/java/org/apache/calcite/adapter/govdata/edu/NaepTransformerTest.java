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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for NAEP response transformers verifying state-level jurisdiction handling.
 *
 * <p>Exercises {@code jurisdictionToFips()} and {@code processResultArray()} with a mixed
 * NP + state response — no network calls.
 */
@Tag("unit")
class NaepTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static RequestContext scoresCtx(String subscale, String grade, String year) {
    Map<String, String> dims = new LinkedHashMap<String, String>();
    dims.put("subscale", subscale);
    dims.put("grade", grade);
    dims.put("year", year);
    return RequestContext.builder()
        .url("https://test/naep")
        .dimensionValues(dims)
        .build();
  }

  private static RequestContext aldCtx(String subscale, String grade, String year, String level) {
    Map<String, String> dims = new LinkedHashMap<String, String>();
    dims.put("subscale", subscale);
    dims.put("grade", grade);
    dims.put("year", year);
    dims.put("level", level);
    return RequestContext.builder()
        .url("https://test/naep-ald")
        .dimensionValues(dims)
        .build();
  }

  /** Simulates the real API response: one national row + two state rows (abbr format). */
  private static String mixedJurisdictionResponse() {
    return "{"
        + "\"result\":["
        + "{\"jurisdiction\":\"NP\",\"jurisLabel\":\"National public\",\"year\":2019,"
        + " \"grade\":4,\"variable\":\"TOTAL\",\"varValueLabel\":\"All students\","
        + " \"value\":240.1,\"isStatDisplayable\":1},"
        + "{\"jurisdiction\":\"CA\",\"jurisLabel\":\"California\",\"year\":2019,"
        + " \"grade\":4,\"variable\":\"TOTAL\",\"varValueLabel\":\"All students\","
        + " \"value\":235.5,\"isStatDisplayable\":1},"
        + "{\"jurisdiction\":\"TX\",\"jurisLabel\":\"Texas\",\"year\":2019,"
        + " \"grade\":4,\"variable\":\"TOTAL\",\"varValueLabel\":\"All students\","
        + " \"value\":243.0,\"isStatDisplayable\":1}"
        + "]}";
  }

  private static String aldMixedResponse() {
    return "{"
        + "\"result\":["
        + "{\"jurisdiction\":\"NP\",\"jurisLabel\":\"National public\",\"year\":2019,"
        + " \"grade\":4,\"variable\":\"TOTAL\",\"varValueLabel\":\"All students\","
        + " \"value\":28.3,\"isStatDisplayable\":1},"
        + "{\"jurisdiction\":\"CA\",\"jurisLabel\":\"California\",\"year\":2019,"
        + " \"grade\":4,\"variable\":\"TOTAL\",\"varValueLabel\":\"All students\","
        + " \"value\":25.1,\"isStatDisplayable\":1}"
        + "]}";
  }

  // ── naep_scores transformer ────────────────────────────────────────────────

  @Test void scoresTransformNationalRow() throws Exception {
    NaepScoresResponseTransformer t = new NaepScoresResponseTransformer();
    String json = t.transform(mixedJurisdictionResponse(), scoresCtx("MRPCM", "4", "2019"));
    JsonNode rows = MAPPER.readTree(json);
    assertTrue(rows.isArray(), "output must be a JSON array");
    assertEquals(3, rows.size(), "expected 3 rows (NP + 2 states)");

    JsonNode np = rows.get(0);
    assertEquals(0, np.path("jurisdiction").asInt(), "NP should map to FIPS 0");
    assertEquals("National public", np.path("jurisdiction_name").asText());
    assertEquals("MAT", np.path("subject").asText());
    assertEquals(4, np.path("grade").asInt());
    assertEquals(240.1, np.path("avg_score").asDouble(), 0.001);
  }

  @Test void scoresTransformCaliforniaRow() throws Exception {
    NaepScoresResponseTransformer t = new NaepScoresResponseTransformer();
    String json = t.transform(mixedJurisdictionResponse(), scoresCtx("MRPCM", "4", "2019"));
    JsonNode rows = MAPPER.readTree(json);

    JsonNode ca = rows.get(1);
    assertEquals(6, ca.path("jurisdiction").asInt(), "\"06\" should map to FIPS 6");
    assertEquals("California", ca.path("jurisdiction_name").asText());
    assertEquals(235.5, ca.path("avg_score").asDouble(), 0.001);
  }

  @Test void scoresTransformTexasRow() throws Exception {
    NaepScoresResponseTransformer t = new NaepScoresResponseTransformer();
    String json = t.transform(mixedJurisdictionResponse(), scoresCtx("MRPCM", "4", "2019"));
    JsonNode rows = MAPPER.readTree(json);

    JsonNode tx = rows.get(2);
    assertEquals(48, tx.path("jurisdiction").asInt(), "\"48\" should map to FIPS 48");
    assertEquals("Texas", tx.path("jurisdiction_name").asText());
  }

  @Test void scoresTransformReadingSubject() throws Exception {
    NaepScoresResponseTransformer t = new NaepScoresResponseTransformer();
    String json = t.transform(mixedJurisdictionResponse(), scoresCtx("RRPCM", "8", "2019"));
    JsonNode rows = MAPPER.readTree(json);
    assertFalse(rows.isEmpty());
    assertEquals("RED", rows.get(0).path("subject").asText(), "RRPCM should map to RED");
  }

  @Test void scoresTransformEmptyResponse() {
    NaepScoresResponseTransformer t = new NaepScoresResponseTransformer();
    String json = t.transform("", scoresCtx("MRPCM", "4", "2019"));
    assertEquals("[]", json);
  }

  @Test void scoresTransformApiError() throws Exception {
    NaepScoresResponseTransformer t = new NaepScoresResponseTransformer();
    String json = t.transform("{\"statusCode\":404,\"result\":[]}", scoresCtx("MRPCM", "4", "2019"));
    JsonNode rows = MAPPER.readTree(json);
    assertEquals(0, rows.size(), "API error response should yield 0 rows");
  }

  // ── naep_achievement_levels transformer ───────────────────────────────────

  @Test void aldTransformNationalRow() throws Exception {
    NaepAchievementLevelsResponseTransformer t = new NaepAchievementLevelsResponseTransformer();
    String json = t.transform(aldMixedResponse(), aldCtx("MRPCM", "4", "2019", "BA"));
    JsonNode rows = MAPPER.readTree(json);
    assertEquals(2, rows.size());

    JsonNode np = rows.get(0);
    assertEquals(0, np.path("jurisdiction").asInt());
    assertEquals("below_basic", np.path("level").asText());
    assertEquals(28.3, np.path("pct").asDouble(), 0.001);
  }

  @Test void aldTransformStateRow() throws Exception {
    NaepAchievementLevelsResponseTransformer t = new NaepAchievementLevelsResponseTransformer();
    String json = t.transform(aldMixedResponse(), aldCtx("MRPCM", "4", "2019", "PR"));
    JsonNode rows = MAPPER.readTree(json);

    JsonNode ca = rows.get(1);
    assertEquals(6, ca.path("jurisdiction").asInt(), "\"06\" should map to FIPS 6");
    assertEquals("proficient", ca.path("level").asText());
    assertEquals("California", ca.path("jurisdiction_name").asText());
  }

  @Test void aldLevelCodes() throws Exception {
    NaepAchievementLevelsResponseTransformer t = new NaepAchievementLevelsResponseTransformer();
    for (String[] pair : new String[][]{
        {"BA", "below_basic"}, {"BC", "basic"}, {"PR", "proficient"}, {"AD", "advanced"}}) {
      String json = t.transform(aldMixedResponse(), aldCtx("MRPCM", "4", "2019", pair[0]));
      JsonNode rows = MAPPER.readTree(json);
      assertEquals(pair[1], rows.get(0).path("level").asText(),
          "level code " + pair[0] + " should map to " + pair[1]);
    }
  }
}
