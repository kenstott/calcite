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
package org.apache.calcite.adapter.govdata.law;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the lobbying tables declared in law-schema.yaml against what {@link LdaApiProvider}
 * actually emits, so a column that is declared but never filled (silently NULL), or filled but
 * never declared (silently dropped), fails here instead of in production data.
 */
@Tag("unit")
class LdaSchemaTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Partition columns the materializer adds itself; the provider does not emit them. */
  private static final Set<String> PARTITION_COLUMNS =
      new HashSet<String>(java.util.Arrays.asList("year", "quarter", "type"));

  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, Object>> lobbyingTables() throws IOException {
    LoaderOptions options = new LoaderOptions();
    options.setMaxAliasesForCollections(500);
    try (InputStream in = LdaSchemaTest.class.getResourceAsStream("/law/law-schema.yaml")) {
      assertTrue(in != null, "law-schema.yaml is not on the classpath");
      Map<String, Object> schema = new Yaml(options).load(in);
      Map<String, Map<String, Object>> out = new LinkedHashMap<String, Map<String, Object>>();
      for (Object o : (List<Object>) schema.get("partitionedTables")) {
        Map<String, Object> t = (Map<String, Object>) o;
        String name = (String) t.get("name");
        if (name.startsWith("lobbying_") || "lobbyists".equals(name)) {
          out.put(name, t);
        }
      }
      return out;
    }
  }

  private static JsonNode fixtureRecords(String fixture) throws IOException {
    try (InputStream in = LdaSchemaTest.class.getResourceAsStream("/law/lda/" + fixture)) {
      JsonNode root = MAPPER.readTree(in);
      return root.isArray() ? root : root.get("results");
    }
  }

  private static String fixtureFor(String table) {
    switch (table) {
    case LdaApiProvider.CONTRIBUTION_REPORTS:
    case LdaApiProvider.CONTRIBUTION_ITEMS:
    case LdaApiProvider.CONTRIBUTION_PACS:
      return "contributions_page.json";
    case LdaApiProvider.LOBBYISTS:
      return "lobbyists_page.json";
    case LdaApiProvider.REGISTRANTS:
      return "registrants_page.json";
    case LdaApiProvider.CLIENTS:
      return "clients_page.json";
    case LdaApiProvider.ISSUE_CODES:
      return "constants_issues.json";
    case LdaApiProvider.GOVERNMENT_ENTITY_CODES:
      return "constants_government_entities.json";
    case LdaApiProvider.FILING_TYPES:
      return "constants_filing_types.json";
    case LdaApiProvider.CONTRIBUTION_ITEM_TYPES:
      return "constants_item_types.json";
    default:
      return "filings_page.json";
    }
  }

  @Test void schemaAndProviderDeclareTheSameTables() throws IOException {
    assertEquals(new TreeSet<String>(LdaApiProvider.tables()),
        new TreeSet<String>(lobbyingTables().keySet()));
  }

  @Test void everyDeclaredColumnIsFilledAndEveryFilledColumnIsDeclared() throws IOException {
    for (Map.Entry<String, Map<String, Object>> e : lobbyingTables().entrySet()) {
      String table = e.getKey();
      Set<String> declared = new TreeSet<String>();
      @SuppressWarnings("unchecked")
      List<Object> columns = (List<Object>) e.getValue().get("columns");
      for (Object c : columns) {
        @SuppressWarnings("unchecked")
        String name = (String) ((Map<String, Object>) c).get("name");
        if (!PARTITION_COLUMNS.contains(name)) {
          declared.add(name);
        }
      }
      Set<String> emitted = new TreeSet<String>();
      for (JsonNode record : fixtureRecords(fixtureFor(table))) {
        for (Map<String, Object> row : LdaApiProvider.rowsOf(table, record)) {
          emitted.addAll(row.keySet());
        }
      }
      assertEquals(declared, emitted, table + ": declared columns differ from emitted columns");
    }
  }

  @Test void everyTableIsWiredTheSameWay() throws IOException {
    for (Map.Entry<String, Map<String, Object>> e : lobbyingTables().entrySet()) {
      String table = e.getKey();
      Map<String, Object> t = e.getValue();
      @SuppressWarnings("unchecked")
      Map<String, Object> hooks = (Map<String, Object>) t.get("hooks");
      assertEquals("org.apache.calcite.adapter.govdata.law.LdaApiProvider",
          hooks.get("dataProvider"), table);
      @SuppressWarnings("unchecked")
      Map<String, Object> source = (Map<String, Object>) t.get("source");
      @SuppressWarnings("unchecked")
      Map<String, Object> headers = (Map<String, Object>) source.get("headers");
      assertEquals("Token ${LDA_API_KEY}", headers.get("Authorization"), table);
      assertTrue(((String) source.get("url")).startsWith("https://lda.gov/api/v1/"), table);
      @SuppressWarnings("unchecked")
      Map<String, Object> dims = (Map<String, Object>) t.get("dimensions");
      boolean windowed = dims.containsKey("year");
      if (windowed) {
        assertEquals("quarterly", t.get("backfill_period"), table);
        assertTrue(dims.containsKey("quarter"), table + " needs a quarter dimension");
        assertEquals("delta", t.get("dataset_type"), table);
      } else {
        assertEquals("snapshot", t.get("dataset_type"), table);
      }
    }
  }
}
