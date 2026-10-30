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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.tools.Frameworks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for SharePoint metadata schema functionality.
 * Tests the integration of PostgreSQL-compatible system catalogs.
 */
@EnabledIfEnvironmentVariable(named = "SHAREPOINT_INTEGRATION_TESTS", matches = "true")
class SharePointMetadataIntegrationTest {

  @Test void testMetadataSchemaIntegration() {
    // Test that the metadata schemas are available using the new Lookup API
    Lookup<? extends Schema> subSchemaLookup = metadataSchemas();
    assertTrue(subSchemaLookup.get("pg_catalog") != null);
    assertTrue(subSchemaLookup.get("information_schema") != null);

    // Test accessing pg_catalog schema
    Schema pgCatalog = subSchemaLookup.get("pg_catalog");
    assertNotNull(pgCatalog);

    // Test accessing information_schema
    Schema informationSchema = subSchemaLookup.get("information_schema");
    assertNotNull(informationSchema);

    // Test that metadata tables are available
    Table pgTables = pgCatalog.tables().get("pg_tables");
    assertNotNull(pgTables);

    Table infoTables = informationSchema.tables().get("TABLES");
    assertNotNull(infoTables);

    Table infoColumns = informationSchema.tables().get("COLUMNS");
    assertNotNull(infoColumns);

    Table infoSchemata = informationSchema.tables().get("SCHEMATA");
    assertNotNull(infoSchemata);

    Table spLists = pgCatalog.tables().get("sharepoint_lists");
    assertNotNull(spLists);
  }

  /**
   * The schemas the factory registers beside the SharePoint schema: pg_catalog and
   * information_schema are siblings of it, not sub-schemas.
   */
  private Lookup<? extends SchemaPlus> metadataSchemas() {
    Map<String, Object> operand = new HashMap<>(createAuthConfig());
    operand.put("siteUrl", loadTestConfig().getProperty("SHAREPOINT_SITE_URL"));
    SchemaPlus root = Frameworks.createRootSchema(false);
    root.add("sharepoint", SharePointListSchemaFactory.INSTANCE.create(root, "sharepoint", operand));
    return root.subSchemas();
  }

  private Map<String, Object> createAuthConfig() {
    // Load from local-test.properties file
    Properties props = loadTestConfig();

    Map<String, Object> authConfig = new HashMap<>();
    authConfig.putAll(SharePointTestCredentials.load().authConfig());
    return authConfig;
  }

  private Properties loadTestConfig() {
    return SharePointTestCredentials.load().properties();
  }
}
