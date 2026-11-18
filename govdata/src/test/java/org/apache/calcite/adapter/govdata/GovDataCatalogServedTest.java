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
    Set<String> seed = GovDataCatalog.seedTables();
    for (JsonNode sc : GovDataCatalog.build(Arrays.asList("disasters", "econ", "sec"))) {
      String schema = sc.path("schema").asText();
      for (JsonNode t : sc.path("tables")) {
        String name = schema + "." + t.path("name").asText();
        if (!seed.contains(name)) {
          for (JsonNode s : GovDataCatalog.buildServed(Arrays.asList(schema))) {
            for (JsonNode st : s.path("tables")) {
              assertFalse(name.equals(schema + "." + st.path("name").asText()),
                  name + " served though absent from seed");
            }
          }
          return;
        }
      }
    }
    throw new AssertionError("no YAML table missing from the seed found to exercise the filter");
  }
}
