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
package org.apache.calcite.adapter.govdata.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The Iowa Mesonet answers a second in-flight request from one IP with HTTP 429, so
 * {@code asos_observations} must fetch one unit at a time.
 */
@Tag("unit")
class AsosObservationsFetchPaceTest {

  @Test void asosObservationsFetchesOneRequestAtATime() throws Exception {
    try (InputStream in =
        AsosObservationsFetchPaceTest.class.getResourceAsStream("/weather/weather-schema.yaml")) {
      assertNotNull(in, "weather-schema.yaml on the classpath");
      JsonNode root = new ObjectMapper(new YAMLFactory()).readTree(in);
      for (JsonNode t : root.get("partitionedTables")) {
        if ("asos_observations".equals(t.get("name").asText())) {
          assertEquals(1, t.get("source").get("parallel").asInt());
          assertEquals(1, t.get("source").get("rateLimit").get("requestsPerSecond").asInt());
          assertEquals(5000, t.get("source").get("rateLimit").get("retryBackoffMs").asInt());
          return;
        }
      }
    }
    throw new AssertionError("asos_observations not declared");
  }
}
