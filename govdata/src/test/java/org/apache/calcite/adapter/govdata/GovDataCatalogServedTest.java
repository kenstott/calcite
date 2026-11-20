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
package org.apache.calcite.adapter.govdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The served catalog lists only what the jar's seed carries. */
@Tag("unit")
class GovDataCatalogServedTest {

  @Test void servedCatalogListsOnlySeedTables() {
    Set<String> seed = GovDataCatalog.seedTables();
    ArrayNode served = GovDataCatalog.buildServed(Arrays.asList("disasters", "econ"));
    assertTrue(served.size() > 0, "served catalog must not be empty");
    for (JsonNode sc : served) {
      for (JsonNode t : sc.path("tables")) {
        String name = sc.path("schema").asText() + "." + t.path("name").asText();
        assertTrue(seed.contains(name), name + " is listed but not in the seed");
      }
    }
  }

  @Test void yamlTableMissingFromSeedIsNotServed() {
    List<String> schemas = Arrays.asList("econ");
    Set<String> seedColumns = allYamlColumns(schemas);
    String absent = firstTable(schemas);
    seedColumns.removeIf(c -> c.startsWith(absent + "."));
    Set<String> seedTables = new HashSet<>();
    for (String c : seedColumns) {
      seedTables.add(c.substring(0, c.lastIndexOf('.')));
    }
    assertFalse(seedTables.contains(absent), "fixture must leave " + absent + " out of the seed");
    int served = 0;
    for (JsonNode sc : GovDataCatalog.buildServed(schemas, seedTables, seedColumns)) {
      for (JsonNode t : sc.path("tables")) {
        String name = sc.path("schema").asText() + "." + t.path("name").asText();
        assertFalse(absent.equals(name), name + " served though absent from seed");
        served++;
      }
    }
    assertTrue(served > 0, "tables present in the seed must still be served");
  }

  @Test void servedCatalogListsOnlySeedColumns() {
    List<String> schemas = Arrays.asList("edu");
    Set<String> seedColumns = allYamlColumns(schemas);
    String absent = firstColumn(schemas);
    seedColumns.remove(absent);
    String table = absent.substring(0, absent.lastIndexOf('.'));
    Set<String> seedTables = new HashSet<>();
    for (String c : seedColumns) {
      seedTables.add(c.substring(0, c.lastIndexOf('.')));
    }
    boolean tableServed = false;
    for (JsonNode sc : GovDataCatalog.buildServed(schemas, seedTables, seedColumns)) {
      for (JsonNode t : sc.path("tables")) {
        String tableName = sc.path("schema").asText() + "." + t.path("name").asText();
        tableServed |= tableName.equals(table);
        for (JsonNode c : t.path("columns")) {
          String col = tableName + "." + c.path("name").asText();
          assertTrue(seedColumns.contains(col), col + " is listed but not in the seed");
          assertFalse(absent.equals(col), col + " listed though absent from seed");
        }
      }
    }
    assertTrue(tableServed, table + " must still be served with its remaining columns");
  }

  private static Set<String> allYamlColumns(List<String> schemas) {
    Set<String> cols = new HashSet<>();
    for (JsonNode sc : GovDataCatalog.build(schemas)) {
      for (JsonNode t : sc.path("tables")) {
        for (JsonNode c : t.path("columns")) {
          cols.add(sc.path("schema").asText() + "." + t.path("name").asText() + "."
              + c.path("name").asText());
        }
      }
    }
    return cols;
  }

  /** First table, in YAML order, that has at least one column. */
  private static String firstTable(List<String> schemas) {
    return firstColumn(schemas, false);
  }

  /** First column of a table that has at least two columns, so the table survives its removal. */
  private static String firstColumn(List<String> schemas) {
    return firstColumn(schemas, true);
  }

  private static String firstColumn(List<String> schemas, boolean column) {
    for (JsonNode sc : GovDataCatalog.build(schemas)) {
      for (JsonNode t : sc.path("tables")) {
        String table = sc.path("schema").asText() + "." + t.path("name").asText();
        if (t.path("columns").size() >= 2) {
          return column ? table + "." + t.path("columns").get(0).path("name").asText() : table;
        }
      }
    }
    throw new AssertionError("no YAML table with columns in " + schemas);
  }
}
