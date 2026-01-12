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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.GovDataUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that all ECON schema tables have proper comment definitions in the schema file.
 */
@Tag("unit")
public class EconTableCommentsTest {

  private static final String ECON_SCHEMA = "/econ/econ-schema.yaml";

  @Test public void testAllEconTablesHaveComments() {
    // Load table comments from schema file
    Map<String, String> tableComments =
        GovDataUtils.loadTableComments(EconSchemaFactory.class, ECON_SCHEMA);

    // List of core ECON tables that should have comments
    String[] tables = {
        "employment_statistics",
        "inflation_metrics",
        "treasury_yields",
        "federal_debt",
        "world_indicators",
        "fred_indicators",
        "national_accounts",
        "regional_income"
    };

    for (String tableName : tables) {
      // Check table comment exists
      String tableComment = tableComments.get(tableName);
      assertNotNull(tableComment, "Table comment missing for: " + tableName);
      assertFalse(tableComment.isEmpty(), "Table comment empty for: " + tableName);

      // Check column comments exist
      Map<String, String> columnComments =
          GovDataUtils.loadColumnComments(EconSchemaFactory.class, ECON_SCHEMA, tableName);
      assertNotNull(columnComments, "Column comments missing for: " + tableName);
      assertFalse(columnComments.isEmpty(), "No column comments for: " + tableName);
    }
  }

  @Test public void testEconTableCommentsAreDescriptive() {
    Map<String, String> tableComments =
        GovDataUtils.loadTableComments(EconSchemaFactory.class, ECON_SCHEMA);

    // Verify that table comments mention appropriate data sources
    String employmentComment = tableComments.get("employment_statistics");
    if (employmentComment != null) {
      assertTrue(employmentComment.toLowerCase().contains("bls")
              || employmentComment.toLowerCase().contains("labor"),
          "Employment statistics should mention BLS or labor");
    }

    String treasuryComment = tableComments.get("treasury_yields");
    if (treasuryComment != null) {
      assertTrue(treasuryComment.toLowerCase().contains("treasury")
              || treasuryComment.toLowerCase().contains("yield"),
          "Treasury yields should mention Treasury or yield");
    }

    String worldComment = tableComments.get("world_indicators");
    if (worldComment != null) {
      assertTrue(worldComment.toLowerCase().contains("world")
              || worldComment.toLowerCase().contains("international"),
          "World indicators should mention World or international");
    }
  }
}
