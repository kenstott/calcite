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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the per-source {@code isEnabled} gates in {@link EnergySchemaFactory}: every name a
 * gate registers must be a physical table in {@code energy-schema.yaml}. A view name there
 * gates nothing, and leaves the base tables behind it ungated.
 */
class EnergySchemaFactoryGateTest {

  private static final String[] GATE_SETS = {
      "EIA_API_TABLES", "EIA_BULK_TABLES", "MSHA_TABLES", "NREL_TABLES", "PJM_TABLES"
  };

  @Test @Tag("unit") void gatedNamesAreDeclaredTablesNotViews() throws Exception {
    Set<String> tables = names("partitionedTables");
    Set<String> views = names("views");

    for (String setName : GATE_SETS) {
      for (String name : gated(setName)) {
        assertFalse(views.contains(name),
            setName + " gates '" + name + "', which is a view in energy-schema.yaml");
        assertTrue(tables.contains(name),
            setName + " gates '" + name + "', which is not a table in energy-schema.yaml");
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<String> gated(String setName) throws Exception {
    Field f = EnergySchemaFactory.class.getDeclaredField(setName);
    f.setAccessible(true);
    return (Set<String>) f.get(null);
  }

  private static Set<String> names(String section) throws Exception {
    try (InputStream in =
        EnergySchemaFactoryGateTest.class.getResourceAsStream("/energy/energy-schema.yaml")) {
      JsonNode root = new ObjectMapper(new YAMLFactory()).readTree(in);
      Set<String> out = new HashSet<>();
      for (JsonNode n : root.get(section)) {
        out.add(n.get("name").asText());
      }
      return out;
    }
  }
}
