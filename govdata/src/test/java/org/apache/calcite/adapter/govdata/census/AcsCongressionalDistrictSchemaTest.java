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
package org.apache.calcite.adapter.govdata.census;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structural checks on the {@code acs_congressional_district} table in census-schema.yaml: the
 * Census API accepts at most 50 variables per request, every column expression reads a field the
 * request asks for, and every column is documented.
 */
@Tag("unit")
class AcsCongressionalDistrictSchemaTest {

  private static JsonNode table() throws Exception {
    try (InputStream in =
        AcsCongressionalDistrictSchemaTest.class.getResourceAsStream("/census/census-schema.yaml")) {
      assertNotNull(in, "census-schema.yaml on the classpath");
      JsonNode root = new ObjectMapper(new YAMLFactory()).readTree(in);
      for (JsonNode t : root.get("partitionedTables")) {
        if ("acs_congressional_district".equals(t.get("name").asText())) {
          return t;
        }
      }
    }
    throw new AssertionError("acs_congressional_district not declared");
  }

  private static Set<String> requestedFields(JsonNode t) {
    String url = t.get("source").get("url").asText();
    Matcher m = Pattern.compile("[?&]get=([^&]+)").matcher(url);
    assertTrue(m.find(), "get= parameter in " + url);
    return new HashSet<String>(Arrays.asList(m.group(1).split(",")));
  }

  @Test void requestStaysUnderTheApiVariableLimitAndUsesCdGeography() throws Exception {
    JsonNode t = table();
    Set<String> fields = requestedFields(t);
    assertTrue(fields.size() <= 50, "variables per request: " + fields.size());
    String url = t.get("source").get("url").asText();
    assertTrue(url.contains("for=congressional%20district:*"), url);
    assertTrue(url.contains("in=state:*"), url);
    assertTrue(url.contains("{env:CENSUS_API_KEY}"), url);
  }

  @Test void everyExpressionReadsOnlyRequestedFields() throws Exception {
    JsonNode t = table();
    Set<String> allowed = requestedFields(t);
    allowed.add("state");
    allowed.add("congressional district");
    Pattern ref = Pattern.compile("src\\.\"([^\"]+)\"");
    Set<String> used = new HashSet<String>();
    for (JsonNode c : t.get("columns")) {
      Matcher m = ref.matcher(c.get("expression").asText());
      while (m.find()) {
        assertTrue(allowed.contains(m.group(1)),
            c.get("name").asText() + " reads " + m.group(1) + " which the request does not fetch");
        used.add(m.group(1));
      }
    }
    for (String f : requestedFields(t)) {
      assertTrue(used.contains(f), "requested field " + f + " is used by no column");
    }
  }

  @Test void everyColumnIsDocumentedAndKeysExist() throws Exception {
    JsonNode t = table();
    Set<String> names = new HashSet<String>();
    for (JsonNode c : t.get("columns")) {
      String name = c.get("name").asText();
      assertTrue(names.add(name), "duplicate column " + name);
      assertFalse(c.path("comment").asText().isEmpty(), name + " has no comment");
    }
    for (String key : new String[] {"cd_fips", "state_fips", "district", "congress", "geo_name",
        "total_population", "median_household_income", "unemployment_rate_pct",
        "poverty_rate_pct", "population_65_plus_pct", "bachelors_or_higher_pct",
        "foreign_born_pct", "renter_cost_burden_pct"}) {
      assertTrue(names.contains(key), "missing column " + key);
    }
    assertEquals("acs_congressional_district",
        t.get("materialize").get("iceberg").get("tableName").asText());
  }
}
